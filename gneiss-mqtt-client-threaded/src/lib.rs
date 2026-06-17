/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]
#![cfg_attr(feature = "strict", deny(warnings))]

/*!
Implementation of an MQTT client that uses one or more background threads for processing.
 */

/// Configuration types specific to creating a threaded client
pub mod config;

/// Implementation of a client event wait for the threaded client
pub mod waiter;

use std::io::{Read, Write};
use std::time::Instant;
use std::cmp::min;
use std::time::Duration;
use std::sync::{Arc, Condvar, Mutex};

use gneiss_mqtt::error::{GneissError, GneissResult};
use gneiss_mqtt::client::config::{ConnectOptions, MqttClientOptions};
use gneiss_mqtt::client::*;
use gneiss_mqtt::mqtt::*;
use gneiss_mqtt_client_sync::{StreamHandle, SyncClientConnectionFactory, SyncStreamTransform};
use log::*;

use crate::config::*;


/// Builder type used to configure and create thread-based clients
pub struct ThreadedClientBuilder {
    connection_factory: SyncClientConnectionFactory,
    threaded_options: Option<ThreadedOptions>,
    client_options: Option<MqttClientOptions>,
    connect_options: Option<ConnectOptions>
}

impl ThreadedClientBuilder {

    /// Constructor function for a new threaded client builder.  By default contains no options
    /// overrides and connects directly via TCP to the broker.
    pub fn new(endpoint: &str, port: u16) -> ThreadedClientBuilder {
        ThreadedClientBuilder {
            connection_factory: SyncClientConnectionFactory::new(endpoint, port),
            threaded_options: None,
            client_options: None,
            connect_options: None
        }
    }

    /// Applies a stream transformation to the connection establishment process.  Typical
    /// transformations include wrappers for websockets and TLS.
    pub fn apply_connection_transform(&mut self, transform: &SyncStreamTransform) -> &mut Self {
        self.connection_factory.apply_transform(transform.transform());

        self
    }

    /// Applies configuration options related to the thread-based client implementation
    pub fn with_threaded_options(&mut self, options: ThreadedOptions) -> &mut Self {
        self.threaded_options = Some(options);

        self
    }

    /// Applies configuration options related to general MQTT client behavior
    pub fn with_client_options(&mut self, options: MqttClientOptions) -> &mut Self {
        self.client_options = Some(options);

        self
    }

    /// Applies configuration options related to the initial connect packet sent by the client
    /// each time a connection to the broker is successfully established.
    pub fn with_connect_options(&mut self, options: ConnectOptions) -> &mut Self {
        self.connect_options = Some(options);

        self
    }

    /// Builds a new thread-based MQTT client
    pub fn build(&self) -> SyncClientHandle {
        let connect_options =
            if let Some(options) = &self.connect_options {
                options.clone()
            } else {
                ConnectOptions::builder().build()
            };

        let client_options =
            if let Some(options) = &self.client_options {
                options.clone()
            } else {
                MqttClientOptions::builder().build()
            };

        let threaded_options =
            if let Some(options) = &self.threaded_options {
                options.clone()
            } else {
                ThreadedOptions::builder().build()
            };

        new_threaded_client(client_options, connect_options, threaded_options, self.connection_factory.clone())
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

fn client_event_loop(mut client_impl: MqttClientImpl, mut threaded_state: ClientRuntimeState) {
    let mut done = false;
    while !done {
        let current_state = client_impl.get_current_state();
        let next_state_result =
            match current_state {
                ClientImplState::Stopped => { threaded_state.process_stopped(&mut client_impl) }
                ClientImplState::Connecting => { threaded_state.process_connecting(&mut client_impl) }
                ClientImplState::Connected => { threaded_state.process_connected(&mut client_impl) }
                ClientImplState::PendingReconnect => {
                    let reconnect_wait = client_impl.advance_reconnect_period();
                    threaded_state.process_pending_reconnect(&mut client_impl, reconnect_wait)
                }
                _ => { Ok(ClientImplState::Shutdown) }
            };

        done = true;
        if let Ok(next_state) = next_state_result {
            if client_impl.transition_to_state(next_state).is_ok() && (next_state != ClientImplState::Shutdown) {
                done = false;
            }
        }
    }

    info!("Threaded client loop exiting");
}

pub(crate) fn spawn_client_impl(client_impl: MqttClientImpl, runtime_state: ClientRuntimeState) {
    std::thread::spawn(move || {
        client_event_loop(client_impl, runtime_state);
    });
}

macro_rules! submit_threaded_operation {
    ($self:ident, $validation_function:ident, $operation_type:ident, $options_internal_type: ident, $options_value: expr, $packet_value: expr) => ({

        let (result_recv, result_send) = new_sync_result_pair();

        let late_sender = result_send.clone();

        if let Err(error) = $validation_function(&$packet_value) {
            result_send.apply(Err(error));
            return result_recv;
        }

        let response_handler = Box::new(move |res| {
            result_send.apply(res);
            Ok(())
        });

        let internal_options = $options_internal_type {
            options : $options_value.unwrap_or_default(),
            response_handler : Some(response_handler)
        };

        let submit_result = $self.operation_sender.send(OperationOptions::$operation_type($packet_value, internal_options));
        if let Err(submit_error) = submit_result {
            late_sender.apply(Err(GneissError::new_operation_channel_failure(submit_error)));
        }

        result_recv
    })
}

macro_rules! submit_threaded_operation_with_callback {
    ($self:ident, $validation_function:ident, $operation_type:ident, $options_internal_type: ident, $options_value: expr, $packet_value: expr, $completion_callback: expr) => ({
        $validation_function(&$packet_value)?;

        let response_handler = Box::new(move |res| {
            $completion_callback(res);
            Ok(())
        });

        let internal_options = $options_internal_type {
            options : $options_value.unwrap_or_default(),
            response_handler : Some(response_handler)
        };

        let submit_result = $self.operation_sender.send(OperationOptions::$operation_type($packet_value, internal_options));
        if let Err(submit_error) = submit_result {
            return Err(GneissError::new_operation_channel_failure(submit_error));
        }

        Ok(())
    })
}

struct ThreadedClient {
    pub(crate) operation_sender: Arc<std::sync::mpsc::Sender<OperationOptions>>,

    pub(crate) listener_id_allocator: Mutex<u64>
}

impl SyncClient for ThreadedClient {

    fn start(&self, default_listener: Option<Arc<ClientEventListenerCallback>>) -> GneissResult<()> {
        info!("threaded client start invoked");
        if let Err(send_error) = self.operation_sender.send(OperationOptions::Start(default_listener)) {
            return Err(GneissError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }

    fn stop(&self, options: Option<StopOptions>) -> GneissResult<()> {
        info!("threaded client stop invoked {} a disconnect packet", if options.as_ref().is_some_and(|opts| { opts.disconnect().is_some()}) { "with" } else { "without" });
        let options = options.unwrap_or_default();

        if let Some(disconnect) = &options.disconnect() {
            validate_disconnect_packet_outbound(disconnect)?;
        }

        let stop_options_internal = StopOptionsInternal {
            options
        };

        if let Err(send_error) = self.operation_sender.send(OperationOptions::Stop(stop_options_internal)) {
            return Err(GneissError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }

    fn close(&self) -> GneissResult<()> {
        info!("threaded client close invoked; no further operations allowed");
        if let Err(send_error) = self.operation_sender.send(OperationOptions::Shutdown()) {
            return Err(GneissError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }

    fn publish(&self, packet: PublishPacket, options: Option<PublishOptions>) -> SyncPublishResult {
        debug!("threaded client - publish operation submitted");

        submit_threaded_operation!(self, validate_publish_packet_outbound, Publish, PublishOptionsInternal, options, packet)
    }

    fn publish_with_callback(&self, packet: PublishPacket, options: Option<PublishOptions>, completion_callback: SyncPublishResultCallback) -> GneissResult<()> {
        debug!("threaded client - publish operation with callback submitted");

        submit_threaded_operation_with_callback!(self, validate_publish_packet_outbound, Publish, PublishOptionsInternal, options, packet, completion_callback)
    }

    fn subscribe(&self, packet: SubscribePacket, options: Option<SubscribeOptions>) -> SyncSubscribeResult {
        debug!("threaded client - subscribe operation submitted");

        submit_threaded_operation!(self, validate_subscribe_packet_outbound, Subscribe, SubscribeOptionsInternal, options, packet)
    }

    fn subscribe_with_callback(&self, packet: SubscribePacket, options: Option<SubscribeOptions>, completion_callback: SyncSubscribeResultCallback) -> GneissResult<()> {
        debug!("threaded client - subscribe operation with callback submitted");

        submit_threaded_operation_with_callback!(self, validate_subscribe_packet_outbound, Subscribe, SubscribeOptionsInternal, options, packet, completion_callback)
    }

    fn unsubscribe(&self, packet: UnsubscribePacket, options: Option<UnsubscribeOptions>) -> SyncUnsubscribeResult {
        debug!("threaded client - unsubscribe operation submitted");

        submit_threaded_operation!(self, validate_unsubscribe_packet_outbound, Unsubscribe, UnsubscribeOptionsInternal, options, packet)
    }

    fn unsubscribe_with_callback(&self, packet: UnsubscribePacket, options: Option<UnsubscribeOptions>, completion_callback: SyncUnsubscribeResultCallback) -> GneissResult<()> {
        debug!("threaded client - unsubscribe operation with callback submitted");

        submit_threaded_operation_with_callback!(self, validate_unsubscribe_packet_outbound, Unsubscribe, UnsubscribeOptionsInternal, options, packet, completion_callback)
    }

    fn add_event_listener(&self, listener: ClientEventListener) -> GneissResult<Arc<dyn ListenerHandle>> {
        debug!("threaded client - add listener operation submitted");
        let mut current_id = self.listener_id_allocator.lock().unwrap();
        let listener_id = *current_id;
        *current_id += 1;

        if let Err(send_error) = self.operation_sender.send(OperationOptions::AddListener(listener_id, listener)) {
            return Err(GneissError::new_operation_channel_failure(send_error));
        }

        Ok(Arc::new(ThreadedListenerHandle::new(self.operation_sender.clone(), listener_id)))
    }
}

struct ThreadedListenerHandle {
    pub(crate) operation_sender: Arc<std::sync::mpsc::Sender<OperationOptions>>,
    pub(crate) id: u64
}

impl ThreadedListenerHandle {
    pub fn new(operation_sender: Arc<std::sync::mpsc::Sender<OperationOptions>>, id: u64) -> ThreadedListenerHandle {
        ThreadedListenerHandle {
            operation_sender,
            id
        }
    }
}

impl ListenerHandle for ThreadedListenerHandle {
}

impl Drop for ThreadedListenerHandle {
    fn drop(&mut self) {
        let _ = self.operation_sender.send(OperationOptions::RemoveListener(self.id));
    }
}

pub(crate) fn create_runtime_states(threaded_config: ThreadedOptions, connection_factory: SyncClientConnectionFactory) -> (std::sync::mpsc::Sender<OperationOptions>, ClientRuntimeState) {
    let (sender, receiver) = std::sync::mpsc::channel();

    let impl_state = ClientRuntimeState {
        connection_factory,
        threaded_config: create_internal_options(&threaded_config),
        operation_receiver: receiver,
        stream: None
    };

    (sender, impl_state)
}

/// Creates a new sync MQTT client that will use background threads for the client and connection attempts.
pub(crate) fn new_threaded_client(client_config: MqttClientOptions, connect_config: ConnectOptions, threaded_config: ThreadedOptions, connection_factory: SyncClientConnectionFactory) -> SyncClientHandle {
    let (operation_sender, internal_state) = create_runtime_states(threaded_config, connection_factory);

    let callback_spawner : CallbackSpawnerFunction = Box::new(|event, callback| {
        (callback)(event)
    });

    let client_impl = MqttClientImpl::new(client_config, connect_config, callback_spawner);

    spawn_client_impl(client_impl, internal_state);

    SyncClientHandle::new(
        Arc::new(ThreadedClient{
            operation_sender: Arc::new(operation_sender),
            listener_id_allocator: Mutex::new(1),
        })
    )
}

/////////////////////////////

#[cfg(feature = "testing")]
pub(crate) mod testing {
    use crate::*;
    use crate::waiter::*;
    use gneiss_mqtt::client::*;
    use gneiss_mqtt::client::config::*;
    use gneiss_mqtt::client::waiter::*;
    use assert_matches::assert_matches;

    pub(crate) type ThreadedTestFactory = Box<dyn Fn(ThreadedClientBuilder) -> GneissResult<()>>;

    pub(crate) fn start_sync_client(client: &SyncClientHandle) -> GneissResult<()> {
        let connection_attempt_waiter = ThreadedClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionAttempt);
        let connection_success_waiter = ThreadedClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionSuccess);

        client.start(None)?;

        let connection_attempt_events = connection_attempt_waiter.wait()?;
        assert_eq!(1, connection_attempt_events.len());
        let connection_attempt_event = &connection_attempt_events[0].event;
        assert_matches!(**connection_attempt_event, ClientEvent::ConnectionAttempt(_));

        let connection_success_events = connection_success_waiter.wait()?;
        assert_eq!(1, connection_success_events.len());
        let connection_success_event = &connection_success_events[0].event;
        assert_matches!(**connection_success_event, ClientEvent::ConnectionSuccess(_));
        if let ClientEvent::ConnectionSuccess(success_event) = &**connection_success_event {
            assert_eq!(ConnectReasonCode::Success, success_event.connack.reason_code());
        } else {
            panic!("impossible");
        }

        Ok(())
    }

    pub(crate) fn stop_sync_client(client: &SyncClientHandle) -> GneissResult<()> {
        let disconnection_waiter = ThreadedClientEventWaiter::new_single(client.clone(), ClientEventType::Disconnection);
        let stopped_waiter = ThreadedClientEventWaiter::new_single(client.clone(), ClientEventType::Stopped);

        client.stop(None)?;

        let disconnect_events = disconnection_waiter.wait()?;
        assert_eq!(1, disconnect_events.len());
        let disconnect_event = &disconnect_events[0].event;
        assert_matches!(**disconnect_event, ClientEvent::Disconnection(_));
        if let ClientEvent::Disconnection(event) = &**disconnect_event {
            assert_matches!(event.error, GneissError::UserInitiatedDisconnect(_));
        } else {
            panic!("impossible");
        }

        stopped_waiter.wait()?;

        Ok(())
    }

    pub(crate) fn create_good_threaded_client_builder() -> ThreadedClientBuilder {
        let connect_options = ConnectOptions::builder()
            .with_rejoin_session_policy(RejoinSessionPolicy::PostSuccess)
            .with_session_expiry_interval_seconds(3600)
            .build();

        let client_config = MqttClientOptions::builder()
            .with_connect_timeout(Duration::from_secs(5))
            .with_offline_queue_policy(OfflineQueuePolicy::PreserveAll)
            .build();

        let endpoint = std::env::var("GNEISS_MQTT_TEST_DIRECT_PLAINTEXT_ENDPOINT").unwrap();
        let port = std::env::var("GNEISS_MQTT_TEST_DIRECT_PLAINTEXT_PORT").unwrap();

        let mut builder = ThreadedClientBuilder::new(&endpoint, port.parse().unwrap());
        builder.with_connect_options(connect_options);
        builder.with_client_options(client_config);

        builder
    }

    fn do_good_client_test(test_factory: ThreadedTestFactory) {
        assert!((*test_factory)(create_good_threaded_client_builder()).is_ok());
    }

    fn threaded_connect_disconnect_test(builder: ThreadedClientBuilder) -> GneissResult<()> {
        let client = builder.build();

        start_sync_client(&client)?;
        stop_sync_client(&client)?;

        Ok(())
    }

    #[test]
    fn client_connect_disconnect() {
        do_good_client_test(Box::new(|builder|{
            threaded_connect_disconnect_test(builder)
        }));
    }

    /*
    fn threaded_subscribe_unsubscribe_test(builder: ThreadedClientBuilder) -> GneissResult<()> {
        sync_subscribe_unsubscribe_test(builder.build()?)
    }

    #[test]
    fn client_subscribe_unsubscribe() {
        do_good_client_test(Box::new(|builder|{
            threaded_subscribe_unsubscribe_test(builder)
        }));
    }

    fn threaded_subscribe_publish_test(builder: ThreadedClientBuilder, qos: QualityOfService) -> GneissResult<()> {
        let client = builder.build()?;
        sync_subscribe_publish_test(client, qos)
    }

    #[test]
    fn client_subscribe_publish_qos0() {
        do_good_client_test(TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            threaded_subscribe_publish_test(builder, QualityOfService::AtMostOnce)
        }));
    }

    #[test]
    fn client_subscribe_publish_qos1() {
        do_good_client_test(TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            threaded_subscribe_publish_test(builder, QualityOfService::AtLeastOnce)
        }));
    }

    #[test]
    fn client_subscribe_publish_qos2() {
        do_good_client_test(TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            threaded_subscribe_publish_test(builder, QualityOfService::ExactlyOnce)
        }));
    }

    fn build_threaded_client(builder: ThreadedClientBuilder,) -> SyncClientHandle {
        builder.build().unwrap()
    }

    // This primarily tests that the will configuration works.  Will functionality is mostly broker-side.
    fn threaded_will_test(builder: ThreadedClientBuilder) -> GneissResult<()> {
        sync_will_test(builder, build_threaded_client)
    }

    #[test]
    fn client_will_sent() {
        do_good_client_test(TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            threaded_will_test(builder)
        }));
    }

    fn threaded_connect_disconnect_cycle_session_rejoin_test(builder: ThreadedClientBuilder) -> GneissResult<()> {
        let client = builder.build()?;
        sync_connect_disconnect_cycle_session_rejoin_test(client)
    }

    #[test]
    fn connect_disconnect_cycle_session_rejoin() {
        do_good_client_test(TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            threaded_connect_disconnect_cycle_session_rejoin_test(builder)
        }));
    }

    pub(crate) fn do_builder_test(test_factory: ThreadedTestFactory, builder: ThreadedClientBuilder) {
        (*test_factory)(builder).unwrap();
    }

    fn connection_failure_test(builder : ThreadedClientBuilder) -> GneissResult<()> {
        let client = builder.build()?;
        let connection_failure_waiter = ThreadedClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionFailure);

        client.start(None)?;

        let connection_failure_results = connection_failure_waiter.wait()?;
        assert_eq!(1, connection_failure_results.len());

        Ok(())
    }

    #[cfg(any(feature = "threaded-websockets", feature="threaded-rustls", feature="threaded-native-tls"))]
    fn create_mismatch_builder(tls_config: TlsUsage, ws_config: WebsocketUsage, tls_endpoint: TlsUsage, ws_endpoint: WebsocketUsage) -> ThreadedClientBuilder {
        assert!(tls_config != tls_endpoint || ws_config != ws_endpoint);

        let connect_options = ConnectOptions::builder().build();

        let mut builder = create_threaded_client_builder_internal(connect_options, tls_config, ProxyUsage::None, tls_endpoint, ws_endpoint);
        apply_mismatch_sync_client_options(&mut builder, ws_config);
        builder
    }

    #[cfg(any(feature = "threaded-websockets", feature="threaded-rustls", feature="threaded-native-tls"))]
    #[cfg_attr(not(feature = "threaded-websockets"), allow(unused_mut, unused_variables))]
    fn apply_mismatch_sync_client_options(builder: &mut ThreadedClientBuilder, _ws_config: WebsocketUsage) {
        #[cfg(feature = "threaded-websockets")]
        {
            let websocket_config_option = create_websocket_options_sync(_ws_config);
            if let Some(websocket_options) = websocket_config_option {
                builder.with_websocket_options(websocket_options);
            } else {
                builder.clear_websocket_options();
            }
        }
    }

    #[test]
    #[cfg(feature = "threaded-rustls")]
    fn connection_failure_direct_rustls_tls_config_direct_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::None, TlsUsage::None, WebsocketUsage::None);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(feature = "threaded-native-tls")]
    fn connection_failure_direct_native_tls_tls_config_direct_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Nativetls, WebsocketUsage::None, TlsUsage::None, WebsocketUsage::None);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "threaded-rustls", feature = "threaded-websockets"))]
    fn connection_failure_direct_rustls_tls_config_websocket_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::None, TlsUsage::None, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "threaded-native-tls", feature = "threaded-websockets"))]
    fn connection_failure_direct_native_tls_tls_config_websocket_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Nativetls, WebsocketUsage::None, TlsUsage::None, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "threaded-rustls", feature = "threaded-websockets"))]
    fn connection_failure_direct_rustls_tls_config_websocket_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::None, TlsUsage::Rustls, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "threaded-native-tls", feature = "threaded-websockets"))]
    fn connection_failure_direct_native_tls_tls_config_websocket_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Nativetls, WebsocketUsage::None, TlsUsage::Nativetls, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(feature = "threaded-rustls")]
    fn connection_failure_direct_plaintext_config_direct_rustls_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::None, TlsUsage::Rustls, WebsocketUsage::None);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(feature = "threaded-native-tls")]
    fn connection_failure_direct_plaintext_config_direct_native_tls_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::None, TlsUsage::Nativetls, WebsocketUsage::None);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(feature = "threaded-websockets")]
    fn connection_failure_direct_plaintext_config_websocket_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::None, TlsUsage::None, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "threaded-rustls", feature = "threaded-websockets"))]
    fn connection_failure_direct_plaintext_config_websocket_rustls_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::None, TlsUsage::Rustls, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "threaded-native-tls", feature = "threaded-websockets"))]
    fn connection_failure_direct_plaintext_config_websocket_native_tls_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::None, TlsUsage::Nativetls, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "threaded-rustls", feature = "threaded-websockets"))]
    fn connection_failure_websocket_rustls_tls_config_direct_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::Tungstenite, TlsUsage::None, WebsocketUsage::None);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "threaded-native-tls", feature = "threaded-websockets"))]
    fn connection_failure_websocket_native_tls_tls_config_direct_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Nativetls, WebsocketUsage::Tungstenite, TlsUsage::None, WebsocketUsage::None);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "threaded-rustls", feature = "threaded-websockets"))]
    fn connection_failure_websocket_rustls_tls_config_websocket_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::Tungstenite, TlsUsage::None, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "threaded-native-tls", feature = "threaded-websockets"))]
    fn connection_failure_websocket_native_tls_tls_config_websocket_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Nativetls, WebsocketUsage::Tungstenite, TlsUsage::None, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "threaded-rustls", feature = "threaded-websockets"))]
    fn connection_failure_websocket_rustls_tls_config_direct_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::Tungstenite, TlsUsage::Rustls, WebsocketUsage::None);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "threaded-native-tls", feature = "threaded-websockets"))]
    fn connection_failure_websocket_native_tls_tls_config_direct_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Nativetls, WebsocketUsage::Tungstenite, TlsUsage::Nativetls, WebsocketUsage::None);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(feature="threaded-websockets")]
    fn connection_failure_websocket_plaintext_config_direct_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::Tungstenite, TlsUsage::None, WebsocketUsage::None);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "threaded-rustls", feature = "threaded-websockets"))]
    fn connection_failure_websocket_plaintext_config_websocket_rustls_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::Tungstenite, TlsUsage::Rustls, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "threaded-native-tls", feature = "threaded-websockets"))]
    fn connection_failure_websocket_plaintext_config_websocket_native_tls_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::Tungstenite, TlsUsage::Nativetls, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "threaded-rustls", feature = "threaded-websockets"))]
    fn connection_failure_websocket_plaintext_config_direct_rustls_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::Tungstenite, TlsUsage::Rustls, WebsocketUsage::None);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "threaded-native-tls", feature = "threaded-websockets"))]
    fn connection_failure_websocket_plaintext_config_direct_native_tls_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::Tungstenite, TlsUsage::Nativetls, WebsocketUsage::None);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    fn connection_failure_invalid_endpoint() {
        let client_options = MqttClientOptionsBuilder::new()
            .with_connect_timeout(Duration::from_secs(3))
            .build();

        let mut builder = ThreadedClientBuilder::new("example.com", 8000);
        builder.with_client_options(client_options);

        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }

    #[test]
    fn connection_failure_invalid_endpoint_http() {
        let builder = ThreadedClientBuilder::new("amazon.com", 443);
        do_builder_test(Box::new(move |builder| {
            connection_failure_test(builder)
        }), builder);
    }*/
}