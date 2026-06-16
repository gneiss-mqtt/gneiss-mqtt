/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
TBI
 */

use std::io::{Read, Write};

use gneiss_mqtt::error::{GneissError, GneissResult};
use gneiss_mqtt::client::config::MqttClientOptions;
use gneiss_mqtt::client::{OperationOptions, SyncClientHandle, SyncResultReceiver};

use std::net;
use net::TcpStream;
use std::time::Duration;
use std::sync::{Arc, Condvar, Mutex};

pub trait Stream {
    fn get_read(&mut self) -> &mut dyn Read;

    fn get_write(&mut self) -> &mut dyn Write;
}

impl <T> Stream for T where T : Read + Write {
    fn get_read(&mut self) -> &mut dyn Read {
        self
    }

    fn get_write(&mut self) -> &mut dyn Write {
        self
    }
}

pub struct StreamHandle {
    stream: Box<dyn Stream>
}

impl StreamHandle {
    pub fn new<T>(stream : T) -> StreamHandle where T : Stream + 'static {
        StreamHandle {
            stream: Box::new(stream)
        }
    }
}

impl Read for StreamHandle {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream.get_read().read(buf)
    }
}

impl Write for StreamHandle {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.stream.get_write().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.get_write().flush()
    }
}

pub trait SyncStreamSource {
    fn create_source(&self) -> GneissResult<StreamHandle>;

    fn host(&self) -> &str;

    fn port(&self) -> u16;
}

pub struct TcpStreamSource {
    host: String,
    port: u16
}

impl TcpStreamSource {
    pub fn new(host: &str, port: u16) -> TcpStreamSource {
        TcpStreamSource {
            host: host.to_string(),
            port
        }
    }
}

impl SyncStreamSource for TcpStreamSource {
    fn create_source(&self) -> GneissResult<StreamHandle> {
        let stream = TcpStream::connect((self.host.as_str(), self.port))?;
        Ok(StreamHandle::new(stream))
    }

    fn host(&self) -> &str { &self.host }

    fn port(&self) -> u16 { self.port }
}


#[derive(Clone)]
pub struct SyncClientConnectionFactory {
    source: Arc<dyn SyncStreamSource>,

    stream_wrapper: Arc<dyn Fn(StreamHandle) -> GneissResult<StreamHandle> + Send + Sync>,
}

impl SyncClientConnectionFactory {
    pub fn new(host: &str, port: u16) -> SyncClientConnectionFactory {
        SyncClientConnectionFactory {
            source: Arc::new(TcpStreamSource::new(host, port)),
            stream_wrapper: Arc::new(|stream| {
                Ok(stream)
            })
        }
    }

    pub fn apply_transform(&mut self, stream_transform: Arc<dyn Fn(StreamHandle) -> GneissResult<StreamHandle> + Send + Sync>) -> &mut Self {
        let base_stream_wrapper = self.stream_wrapper.clone();
        let new_stream_wrapper = stream_transform.clone();

        self.stream_wrapper = Arc::new(move |stream| {
            let base_stream = base_stream_wrapper(stream)?;
            let wrapped_stream = new_stream_wrapper(base_stream)?;

            Ok(wrapped_stream)
        });

        self
    }

    pub fn replace_source(&mut self, source_transform: Arc<dyn Fn(Arc<dyn SyncStreamSource>) -> GneissResult<Arc<dyn SyncStreamSource>> + Send + Sync>) -> GneissResult<()> {
        let old_source = self.source.clone();
        self.source = source_transform(old_source)?;

        Ok(())
    }

    pub fn connect(&self) -> GneissResult<StreamHandle> {
        let base_stream = self.source.create_source()?;
        let wrapper_stream = (self.stream_wrapper)(base_stream)?;

        Ok(wrapper_stream)
    }
}

pub struct SyncStreamTransform {
    transform: Arc<dyn Fn(StreamHandle) -> GneissResult<StreamHandle> + Send + Sync>
}

impl SyncStreamTransform {
    pub fn transform(&self) -> Arc<dyn Fn(StreamHandle) -> GneissResult<StreamHandle> + Send + Sync> {
        self.transform.clone()
    }
}

#[derive(Default, Clone)]
pub struct ThreadedOptions {
    pub(crate) idle_service_sleep: Option<Duration>,
}

impl ThreadedOptions {

    /// Creates a new builder for ThreadedClientOptions instances
    pub fn builder() -> ThreadedOptionsBuilder {
        ThreadedOptionsBuilder::new()
    }
}

/// Builder type for threaded client configuration
pub struct ThreadedOptionsBuilder {
    config: ThreadedOptions
}

impl ThreadedOptionsBuilder {

    pub(crate) fn new() -> Self {
        ThreadedOptionsBuilder {
            config: ThreadedOptions {
                idle_service_sleep: None,
            }
        }
    }

    /// Configures the time interval to sleep the thread the client runs on between io
    /// processing events.
    ///
    /// Only used if no events occurred on the previous iteration.  If the
    /// client is handling significant work, it will not sleep, but if there's nothing
    /// happening, it will.
    ///
    /// If not set, defaults to 20 milliseconds.
    pub fn with_idle_service_sleep(&mut self, duration: Duration) {
        self.config.idle_service_sleep = Some(duration);
    }

    /// Builds a new set of threaded client configuration options
    pub fn build(self) -> ThreadedOptions {
        self.config
    }
}

pub struct SyncClientBuilder {
    endpoint: String,
    port: u16,

    connection_factory: SyncClientConnectionFactory,
    threaded_options: Option<ThreadedOptions>,
    client_options: Option<MqttClientOptions>
}

impl SyncClientBuilder {
    pub fn new(endpoint: &str, port: u16) -> SyncClientBuilder {
        SyncClientBuilder {
            endpoint: endpoint.to_string(),
            port,
            connection_factory: SyncClientConnectionFactory::new(endpoint, port),
            threaded_options: None,
            client_options: None
        }
    }

    pub fn apply_connection_transform(&mut self, transform: &SyncStreamTransform) -> &mut Self {
        self.connection_factory.apply_transform(transform.transform());

        self
    }

    pub fn with_threaded_options(&mut self, options: ThreadedOptions) -> &mut Self {
        self.threaded_options = Some(options);

        self
    }

    pub fn with_client_options(&mut self, options: MqttClientOptions) -> &mut Self {
        self.client_options = Some(options);

        self
    }

    pub fn build(&self) -> GneissResult<SyncClientHandle> {

    }
}

#[derive(Copy, Clone)]
struct ThreadedClientOptionsInternal {
    idle_service_sleep: Duration
}

const DEFAULT_IDLE_SLEEP_MILLIS : u64 = 20;

fn create_internal_options(options: &ThreadedOptions) -> ThreadedClientOptionsInternal {
    let idle_service_sleep = options.idle_service_sleep;

    ThreadedClientOptionsInternal {
        idle_service_sleep: idle_service_sleep.unwrap_or(Duration::from_millis(DEFAULT_IDLE_SLEEP_MILLIS))
    }
}

pub(crate) struct ClientRuntimeState  {
    connection_factory: SyncClientConnectionFactory,
    threaded_config: ThreadedClientOptionsInternal,
    operation_receiver: std::sync::mpsc::Receiver<OperationOptions>,
    stream: Option<StreamHandle>,
}

pub(crate) struct SyncResultSender<T> {
    result_lock: Arc<Mutex<Option<T>>>,
    result_signal: Arc<Condvar>
}

impl<T> Clone for SyncResultSender<T> {
    fn clone(&self) -> Self {
        SyncResultSender {
            result_lock: self.result_lock.clone(),
            result_signal: self.result_signal.clone()
        }
    }
}

impl<T> SyncResultSender<T> {

    pub(crate) fn new(result_lock: Arc<Mutex<Option<T>>>, result_signal: Arc<Condvar>) -> SyncResultSender<T> {
        SyncResultSender {
            result_lock,
            result_signal
        }
    }

    pub(crate) fn apply(&self, value: T) {
        let mut current_value = self.result_lock.lock().unwrap();

        if current_value.is_some() {
            panic!("Cannot set operation result twice!");
        }

        *current_value = Some(value);

        self.result_signal.notify_all();
    }
}

pub(crate) struct SyncResultReceiverImpl<T> {
    result_lock: Arc<Mutex<Option<T>>>,
    result_signal: Arc<Condvar>
}

impl<T> SyncResultReceiverImpl<T> {

    pub(crate) fn new(result_lock: Arc<Mutex<Option<T>>>, result_signal: Arc<Condvar>) -> SyncResultReceiverImpl<T> {
        SyncResultReceiverImpl {
            result_lock,
            result_signal
        }
    }
}

impl <T> SyncResultReceiver<T> for SyncResultReceiverImpl<T> {

    /// Blocking.  Waits for a result from a synchronous client MQTT operation.
    fn recv(&self) -> T {
        let mut current_value = self.result_lock.lock().unwrap();
        while current_value.is_none() {
            current_value = self.result_signal.wait(current_value).unwrap();
        }

        current_value.take().unwrap()
    }

    /// Non-blocking.  Checks if a synchronous client MQTT operation has produced a result yet.
    /// Returns the result value if so.
    fn try_recv(&self) -> Option<T> {
        let mut current_value = self.result_lock.lock().unwrap();
        if current_value.is_none() {
            None
        } else {
            current_value.take()
        }
    }
}

pub(crate) fn new_sync_result_pair<T>() -> (Arc<dyn SyncResultReceiver<T>>, SyncResultSender<T>) where T: 'static {
    let lock = Arc::new(Mutex::new(None));
    let signal = Arc::new(Condvar::new());

    (Arc::new(SyncResultReceiverImpl::new(lock.clone(), signal.clone())), SyncResultSender::new(lock.clone(), signal.clone()))
}

use log::*;
use gneiss_mqtt::client::{is_connection_established, ClientImplState, MqttClientImpl};
use std::time::Instant;
use std::cmp::min;

impl ClientRuntimeState {
    pub(crate) fn process_stopped(&mut self, client: &mut MqttClientImpl) -> GneissResult<ClientImplState> {
        loop {
            trace!("threaded - process_stopped loop");

            let mut sleep_duration = Some(self.threaded_config.idle_service_sleep);

            let operation_result = self.operation_receiver.try_recv();
            if let Ok(operation_options) = operation_result {
                debug!("threaded - process_stopped - user operation received");
                client.handle_incoming_operation(operation_options);
                sleep_duration = None;
            }

            if let Some(transition_state) = client.compute_optional_state_transition() {
                return Ok(transition_state);
            } else if let Some(sleep_duration) = sleep_duration {
                debug!("threaded - process_stopped - sleeping for {:?}", sleep_duration);
                std::thread::sleep(sleep_duration);
            } else {
                debug!("threaded - process_stopped - skipping sleep");
            }
        }
    }

    pub(crate) fn process_connecting(&mut self, client: &mut MqttClientImpl) -> GneissResult<ClientImplState> {
        // let mut connect = (self.threaded_config.connection_factory)();
        let timeout_timepoint = Instant::now() + *client.connect_timeout();

        let connection_factory = self.connection_factory.clone();
        let (connection_recv, connection_send) = new_sync_result_pair::<GneissResult<StreamHandle>>();
        std::thread::spawn(move || {
            connection_send.apply(connection_factory.connect());
        });

        loop {
            trace!("threaded - process_connecting loop");

            // check client control channel
            let mut sleep_duration = Some(self.threaded_config.idle_service_sleep);

            let operation_result = self.operation_receiver.try_recv();
            if let Ok(operation_options) = operation_result {
                debug!("threaded - process_connecting - user operation received");
                client.handle_incoming_operation(operation_options);
                sleep_duration = None;
            }

            // check connection completion
            if let Some(connection_result) = connection_recv.try_recv() {
                return
                    match connection_result {
                        Ok(stream) => {
                            info!("threaded - process_connecting - transport connection established successfully");
                            self.stream = Some(stream);
                            Ok(ClientImplState::Connected)
                        }
                        Err(error) => {
                            info!("threaded - process_connecting - transport connection establishment failed");
                            client.apply_error(GneissError::new_connection_establishment_failure(error));
                            Ok(ClientImplState::PendingReconnect)
                        }
                    }
            }

            // check timeout
            let now = Instant::now();
            if now >= timeout_timepoint {
                info!("threaded - process_connecting - connection establishment timeout exceeded");
                client.apply_error(GneissError::new_connection_establishment_failure("connection establishment timeout reached"));
                return Ok(ClientImplState::PendingReconnect);
            } else {
                let until_timeout = timeout_timepoint - now;
                if let Some(dur) = sleep_duration {
                    sleep_duration = Some(min(dur, until_timeout));
                }
            }

            if let Some(transition_state) = client.compute_optional_state_transition() {
                return Ok(transition_state);
            } else if let Some(sleep_duration) = sleep_duration {
                debug!("threaded - process_connecting - sleeping for {:?}", sleep_duration);
                std::thread::sleep(sleep_duration);
            } else {
                debug!("threaded - process_connecting - skipping sleep");
            }
        }
    }

    pub(crate) fn process_connected(&mut self, client: &mut MqttClientImpl) -> GneissResult<ClientImplState> {
        let mut outbound_data: Vec<u8> = Vec::with_capacity(4096);
        let mut cumulative_bytes_written : usize = 0;

        let mut inbound_data: [u8; 4096] = [0; 4096];

        let mut stream = self.stream.take().unwrap();

        let mut write_directive : Option<&[u8]>;

        let mut next_state = None;
        while next_state.is_none() {
            trace!("threaded - process_connected loop");

            let mut sleep_duration = Some(self.threaded_config.idle_service_sleep);

            // incoming user operations
            let operation_result = self.operation_receiver.try_recv();
            if let Ok(operation_options) = operation_result {
                debug!("threaded - process_pending_reconnect - user operation received");
                client.handle_incoming_operation(operation_options);
                sleep_duration = None;
            }

            // incoming data on the socket
            let mut connection_fatal_read_error = None;
            let read_result = stream.read(inbound_data.as_mut_slice());
            match read_result {
                Ok(bytes_read) => {
                    debug!("threaded - process_connected - read {} bytes from connection stream", bytes_read);

                    if bytes_read == 0 {
                        connection_fatal_read_error = Some(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
                    } else if let Err(error) = client.handle_incoming_bytes(&inbound_data[..bytes_read]) {
                        info!("threaded - process_connected - error handling incoming bytes: {:?}", error);
                        client.apply_error(error);
                        next_state = Some(ClientImplState::PendingReconnect);
                        continue;
                    }

                    sleep_duration = None;
                }
                Err(error) => {
                    match error.kind() {
                        std::io::ErrorKind::WouldBlock => {
                            trace!("threaded - process_connected - no data available to read");
                        }
                        _ => {
                            connection_fatal_read_error = Some(error);
                        }
                    }
                }
            }

            if let Some(read_error) = connection_fatal_read_error {
                info!("threaded - process_connected - connection stream read failed: {:?}", read_error);
                if is_connection_established(client.get_protocol_state()) {
                    client.apply_error(GneissError::new_connection_closed(read_error));
                } else {
                    client.apply_error(GneissError::new_connection_establishment_failure(read_error));
                }
                next_state = Some(ClientImplState::PendingReconnect);
                continue;
            }

            // client service (if relevant)
            let next_service_time_option = client.get_next_connected_service_time();
            if let Some(next_service_time) = next_service_time_option {
                if next_service_time <= Instant::now() {
                    if let Err(error) = client.handle_service(&mut outbound_data) {
                        client.apply_error(error);
                        next_state = Some(ClientImplState::PendingReconnect);
                        continue;
                    }
                }
            }

            let outbound_slice_option: Option<&[u8]> =
                if cumulative_bytes_written < outbound_data.len() {
                    Some(&outbound_data[cumulative_bytes_written..])
                } else {
                    None
                };

            if let Some(outbound_slice) = outbound_slice_option {
                debug!("threaded - process_connected - {} bytes to write", outbound_slice.len());
                write_directive = Some(outbound_slice)
            } else {
                debug!("threaded - process_connected - nothing to write");
                write_directive = None;
            }

            let mut connection_fatal_write_error = None;
            if let Some(write_bytes) = write_directive {
                let mut should_flush : bool = false;
                let bytes_written_result = stream.write(write_bytes);
                match bytes_written_result {
                    Ok(bytes_written) => {
                        if bytes_written > 0 {
                            debug!("threaded - process_connected - wrote {} bytes to connection stream", bytes_written);
                            cumulative_bytes_written += bytes_written;
                            if cumulative_bytes_written == outbound_data.len() {
                                outbound_data.clear();
                                cumulative_bytes_written = 0;
                                should_flush = true;
                            }
                        } else {
                            connection_fatal_write_error = Some(std::io::Error::from(std::io::ErrorKind::WriteZero));
                        }
                    }
                    Err(error) => {
                        match error.kind() {
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted => {
                                trace!("threaded - process_connected - no progress made writing data to socket");
                            }
                            _ => {
                                connection_fatal_write_error = Some(error);
                            }
                        }
                    }
                }

                if let Some(write_error) = connection_fatal_write_error {
                    info!("threaded - process_connected - connection stream write failed: {:?}", write_error);
                    if is_connection_established(client.get_protocol_state()) {
                        client.apply_error(GneissError::new_connection_closed(write_error));
                    } else {
                        client.apply_error(GneissError::new_connection_establishment_failure(write_error));
                    }
                    next_state = Some(ClientImplState::PendingReconnect);
                    continue;
                }

                if should_flush {
                    let flush_result = stream.flush();
                    match flush_result {
                        Ok(()) => {
                            if let Err(error) = client.handle_write_completion() {
                                info!("threaded - process_connected - stream write completion handler failed: {:?}", error);
                                client.apply_error(error);
                                next_state = Some(ClientImplState::PendingReconnect);
                                continue;
                            }
                        }
                        Err(error) => {
                            info!("threaded - process_connected - connection stream flush failed: {:?}", error);
                            if is_connection_established(client.get_protocol_state()) {
                                client.apply_error(GneissError::new_connection_closed(error));
                            } else {
                                client.apply_error(GneissError::new_connection_establishment_failure(error));
                            }
                            next_state = Some(ClientImplState::PendingReconnect);
                            continue;
                        }
                    }
                }
            }

            let next_service_time_option = client.get_next_connected_service_time();
            if let Some(next_service_time) = next_service_time_option {
                if let Some(current_duration) = sleep_duration {
                    sleep_duration = Some(min(current_duration, next_service_time - Instant::now()));
                }
            }

            if next_state.is_none() {
                next_state = client.compute_optional_state_transition();
            } else if let Some(sleep_duration) = sleep_duration {
                debug!("threaded - process_connected - sleeping for {:?}", sleep_duration);
                std::thread::sleep(sleep_duration);
            } else {
                debug!("threaded - process_connected - skipping sleep");
            }
        }

        Ok(next_state.unwrap())
    }

    pub(crate) fn process_pending_reconnect(&mut self, client: &mut MqttClientImpl, wait: Duration) -> GneissResult<ClientImplState> {
        let timeout_timepoint = Instant::now() + wait;

        loop {
            trace!("threaded - process_pending_reconnect loop");

            let mut sleep_duration = Some(self.threaded_config.idle_service_sleep);

            let operation_result = self.operation_receiver.try_recv();
            if let Ok(operation_options) = operation_result {
                debug!("threaded - process_pending_reconnect - user operation received");
                client.handle_incoming_operation(operation_options);
                sleep_duration = None;
            }

            let now = Instant::now();
            if now >= timeout_timepoint {
                info!("threaded - process_pending_reconnect - reconnect timer exceeded");
                return Ok(ClientImplState::Connecting);
            } else {
                let until_timeout = timeout_timepoint - now;
                if let Some(dur) = sleep_duration {
                    sleep_duration = Some(min(dur, until_timeout));
                }
            }

            if let Some(transition_state) = client.compute_optional_state_transition() {
                return Ok(transition_state);
            } else if let Some(sleep_duration) = sleep_duration {
                debug!("threaded - process_pending_reconnect - sleeping for {:?}", sleep_duration);
                std::thread::sleep(sleep_duration);
            } else {
                debug!("threaded - process_pending_reconnect - skipping sleep");
            }
        }
    }
}