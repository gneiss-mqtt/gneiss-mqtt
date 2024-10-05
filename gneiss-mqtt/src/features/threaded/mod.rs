/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Implementation of an MQTT client that uses one or more background threads for processing.
 */

#[cfg(feature="threaded-websockets")]
mod ws_stream;

use std::cmp::min;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use log::{debug, error, info, trace};
use crate::client::*;
use crate::client::waiter::{client_event_matches, ClientEventRecord, ClientEventType, ClientEventWaiterOptions, ClientEventWaitType, SyncClientEventWaiter};
use crate::config::*;
use crate::features::threaded::ws_stream::WebsocketStreamWrapper;
use crate::error::{MqttError, MqttResult};
use crate::mqtt::{MqttPacket, PublishPacket, SubscribePacket, UnsubscribePacket};
use crate::mqtt::disconnect::validate_disconnect_packet_outbound;
use crate::protocol::is_connection_established;
use crate::validate::validate_packet_outbound;

/// Factory function for creating the final connection object based on all the various
/// configuration options and features.  It might be a TcpStream, it might be a TlsStream,
/// it might be a WebsocketStream, it might be some nested combination.
///
/// Ultimately, the type must implement Read and Write.
pub type ConnectionFactory<T> = Arc<dyn Fn() -> MqttResult<T> + Send + Sync>;



#[derive(Copy, Clone)]
struct ThreadedClientOptionsInternal {
    idle_service_sleep: Duration
}

const DEFAULT_IDLE_SLEEP_MILLIS : u64 = 20;

fn create_internal_options(options: &ThreadedClientOptions) -> ThreadedClientOptionsInternal {
    let idle_service_sleep = options.idle_service_sleep;

    return ThreadedClientOptionsInternal {
        idle_service_sleep: idle_service_sleep.unwrap_or(Duration::from_millis(DEFAULT_IDLE_SLEEP_MILLIS))
    }
}

pub(crate) struct ClientRuntimeState<T> where T : Read + Write + Send + Sync + 'static {
    connection_factory: ConnectionFactory<T>,
    threaded_config: ThreadedClientOptionsInternal,
    operation_receiver: std::sync::mpsc::Receiver<OperationOptions>,
    stream: Option<T>
}

impl<T> ClientRuntimeState<T> where T : Read + Write + Send + Sync {
    pub(crate) fn process_stopped(&mut self, client: &mut MqttClientImpl) -> MqttResult<ClientImplState> {
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

    pub(crate) fn process_connecting(&mut self, client: &mut MqttClientImpl) -> MqttResult<ClientImplState> {
        // let mut connect = (self.threaded_config.connection_factory)();
        let timeout_timepoint = Instant::now() + *client.connect_timeout();

        let connection_factory = self.connection_factory.clone();
        let (connection_recv, connection_send) = new_sync_result_pair::<MqttResult<T>>();
        std::thread::spawn(move || {
            connection_send.apply(connection_factory());
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
                            client.apply_error(MqttError::new_connection_establishment_failure(error));
                            Ok(ClientImplState::PendingReconnect)
                        }
                    }
            }

            // check timeout
            let now = Instant::now();
            if now >= timeout_timepoint {
                info!("threaded - process_connecting - connection establishment timeout exceeded");
                client.apply_error(MqttError::new_connection_establishment_failure("connection establishment timeout reached"));
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

    pub(crate) fn process_connected(&mut self, client: &mut MqttClientImpl) -> MqttResult<ClientImplState> {
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
                    client.apply_error(MqttError::new_connection_closed(read_error));
                } else {
                    client.apply_error(MqttError::new_connection_establishment_failure(read_error));
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
                        client.apply_error(MqttError::new_connection_closed(write_error));
                    } else {
                        client.apply_error(MqttError::new_connection_establishment_failure(write_error));
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
                                client.apply_error(MqttError::new_connection_closed(error));
                            } else {
                                client.apply_error(MqttError::new_connection_establishment_failure(error));
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

    pub(crate) fn process_pending_reconnect(&mut self, client: &mut MqttClientImpl, wait: Duration) -> MqttResult<ClientImplState> {
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

fn client_event_loop<T>(mut client_impl: MqttClientImpl, mut threaded_state: ClientRuntimeState<T>) where T : Read + Write + Send + Sync {
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

pub(crate) fn spawn_client_impl<T>(
    client_impl: MqttClientImpl,
    runtime_state: ClientRuntimeState<T>
) where T : Read + Write + Send + Sync + 'static {
    std::thread::spawn(move || {
        client_event_loop(client_impl, runtime_state);
    });
}

macro_rules! submit_threaded_operation {
    ($self:ident, $packet_type:ident, $operation_type:ident, $options_internal_type: ident, $options_value: expr, $packet_value: expr) => ({

        let (result_recv, result_send) = new_sync_result_pair();

        let late_sender = result_send.clone();

        let boxed_packet = Box::new(MqttPacket::$packet_type($packet_value));
        if let Err(error) = validate_packet_outbound(&boxed_packet) {
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

        let submit_result = $self.operation_sender.send(OperationOptions::$operation_type(boxed_packet, internal_options));
        if let Err(submit_error) = submit_result {
            late_sender.apply(Err(MqttError::new_operation_channel_failure(submit_error)));
        }

        result_recv
    })
}

macro_rules! submit_threaded_operation_with_callback {
    ($self:ident, $packet_type:ident, $operation_type:ident, $options_internal_type: ident, $options_value: expr, $packet_value: expr, $completion_callback: expr) => ({
        let boxed_packet = Box::new(MqttPacket::$packet_type($packet_value));
        validate_packet_outbound(&boxed_packet)?;

        let response_handler = Box::new(move |res| {
            $completion_callback(res);
            Ok(())
        });

        let internal_options = $options_internal_type {
            options : $options_value.unwrap_or_default(),
            response_handler : Some(response_handler)
        };

        let submit_result = $self.operation_sender.send(OperationOptions::$operation_type(boxed_packet, internal_options));
        if let Err(submit_error) = submit_result {
            return Err(MqttError::new_operation_channel_failure(submit_error));
        }

        Ok(())
    })
}

struct ThreadedClient {
    pub(crate) operation_sender: std::sync::mpsc::Sender<OperationOptions>,

    pub(crate) listener_id_allocator: Mutex<u64>
}

impl SyncMqttClient for ThreadedClient {

    /// Signals the client that it should attempt to recurrently maintain a connection to
    /// the broker endpoint it has been configured with.
    fn start(&self, default_listener: Option<Arc<ClientEventListenerCallback>>) -> MqttResult<()> {
        info!("threaded client start invoked");
        if let Err(send_error) = self.operation_sender.send(OperationOptions::Start(default_listener)) {
            return Err(MqttError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }

    /// Signals the client that it should close any current connection it has and enter the
    /// Stopped state, where it does nothing.
    fn stop(&self, options: Option<StopOptions>) -> MqttResult<()> {
        info!("threaded client stop invoked {} a disconnect packet", if options.as_ref().is_some_and(|opts| { opts.disconnect.is_some()}) { "with" } else { "without" });
        let options = options.unwrap_or_default();

        if let Some(disconnect) = &options.disconnect {
            validate_disconnect_packet_outbound(disconnect)?;
        }

        let mut stop_options = StopOptionsInternal {
            disconnect : None,
        };

        if options.disconnect.is_some() {
            stop_options.disconnect = Some(Box::new(MqttPacket::Disconnect(options.disconnect.unwrap())));
        }

        if let Err(send_error) = self.operation_sender.send(OperationOptions::Stop(stop_options)) {
            return Err(MqttError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }

    /// Signals the client that it should clean up all internal resources (connection, channels,
    /// runtime tasks, etc...) and enter a terminal state that cannot be escaped.  Useful to ensure
    /// a full resource wipe.  If just `stop()` is used then the client will continue to track
    /// MQTT session state internally.
    fn close(&self) -> MqttResult<()> {
        info!("threaded client close invoked; no further operations allowed");
        if let Err(send_error) = self.operation_sender.send(OperationOptions::Shutdown()) {
            return Err(MqttError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }

    /// Submits a Publish operation to the client's operation queue.  The publish packet will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.
    fn publish(&self, packet: PublishPacket, options: Option<PublishOptions>) -> SyncPublishResult {
        debug!("threaded client - publish operation submitted");

        submit_threaded_operation!(self, Publish, Publish, PublishOptionsInternal, options, packet)
    }

    fn publish_with_callback(&self, packet: PublishPacket, options: Option<PublishOptions>, completion_callback: SyncPublishResultCallback) -> MqttResult<()> {
        debug!("threaded client - publish operation with callback submitted");

        submit_threaded_operation_with_callback!(self, Publish, Publish, PublishOptionsInternal, options, packet, completion_callback)
    }

    /// Submits a Subscribe operation to the client's operation queue.  The subscribe packet will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.
    fn subscribe(&self, packet: SubscribePacket, options: Option<SubscribeOptions>) -> SyncSubscribeResult {
        debug!("threaded client - subscribe operation submitted");

        submit_threaded_operation!(self, Subscribe, Subscribe, SubscribeOptionsInternal, options, packet)
    }

    fn subscribe_with_callback(&self, packet: SubscribePacket, options: Option<SubscribeOptions>, completion_callback: SyncSubscribeResultCallback) -> MqttResult<()> {
        debug!("threaded client - subscribe operation with callback submitted");

        submit_threaded_operation_with_callback!(self, Subscribe, Subscribe, SubscribeOptionsInternal, options, packet, completion_callback)
    }

    /// Submits an Unsubscribe operation to the client's operation queue.  The unsubscribe packet will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.
    fn unsubscribe(&self, packet: UnsubscribePacket, options: Option<UnsubscribeOptions>) -> SyncUnsubscribeResult {
        debug!("threaded client - unsubscribe operation submitted");

        submit_threaded_operation!(self, Unsubscribe, Unsubscribe, UnsubscribeOptionsInternal, options, packet)
    }

    fn unsubscribe_with_callback(&self, packet: UnsubscribePacket, options: Option<UnsubscribeOptions>, completion_callback: SyncUnsubscribeResultCallback) -> MqttResult<()> {
        debug!("threaded client - unsubscribe operation with callback submitted");

        submit_threaded_operation_with_callback!(self, Unsubscribe, Unsubscribe, UnsubscribeOptionsInternal, options, packet, completion_callback)
    }

    /// Adds an additional listener to the events emitted by this client.  This is useful when
    /// multiple higher-level constructs are sharing the same MQTT client.
    fn add_event_listener(&self, listener: ClientEventListener) -> MqttResult<ListenerHandle> {
        debug!("threaded client - add listener operation submitted");
        let mut current_id = self.listener_id_allocator.lock().unwrap();
        let listener_id = *current_id;
        *current_id += 1;

        if let Err(send_error) = self.operation_sender.send(OperationOptions::AddListener(listener_id, listener)) {
            return Err(MqttError::new_operation_channel_failure(send_error));
        }

        Ok(ListenerHandle {
            id: listener_id
        })
    }

    /// Removes a listener from this client's set of event listeners.
    fn remove_event_listener(&self, listener: ListenerHandle) -> MqttResult<()> {
        debug!("threaded client - remove listener operation submitted");
        if let Err(send_error) = self.operation_sender.send(OperationOptions::RemoveListener(listener.id)) {
            return Err(MqttError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }
}

pub(crate) fn create_runtime_states<T>(threaded_config: ThreadedClientOptions, connection_factory: ConnectionFactory<T>) -> (std::sync::mpsc::Sender<OperationOptions>, ClientRuntimeState<T>) where T : Read + Write + Send + Sync + 'static {
    let (sender, receiver) = std::sync::mpsc::channel();

    let impl_state = ClientRuntimeState {
        connection_factory,
        threaded_config: create_internal_options(&threaded_config),
        operation_receiver: receiver,
        stream: None
    };

    (sender, impl_state)
}

/// Creates a new sync MQTT5 client that will use background threads for the client and connection attempts
pub fn new_threaded<T>(client_config: MqttClientOptions, connect_config: ConnectOptions, threaded_config: ThreadedClientOptions, connection_factory: ConnectionFactory<T>) -> SyncGneissClient where T: Read + Write + Send + Sync + 'static {
    let (operation_sender, internal_state) = create_runtime_states(threaded_config, connection_factory);

    let callback_spawner : CallbackSpawnerFunction = Box::new(|event, callback| {
        (callback)(event)
    });

    let client_impl = MqttClientImpl::new(client_config, connect_config, callback_spawner);

    spawn_client_impl(client_impl, internal_state);

    Arc::new(ThreadedClient{
        operation_sender,
        listener_id_allocator: Mutex::new(1),
    })
}

pub(crate) fn make_client_threaded(tls_impl: TlsConfiguration, endpoint: String, port: u16, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, threaded_config: ThreadedClientOptions) -> MqttResult<SyncGneissClient> {
    #[cfg(feature="threaded-websockets")]
    if threaded_config.websocket_options.is_some() {
        return make_websocket_client_threaded(tls_impl, endpoint, port, tls_options, client_options, connect_options, http_proxy_options, threaded_config);
    }

    make_direct_client_threaded(tls_impl, endpoint, port, tls_options, client_options, connect_options, http_proxy_options, threaded_config)
}

#[allow(clippy::too_many_arguments)]
fn make_direct_client_threaded(tls_impl: TlsConfiguration, endpoint: String, port: u16, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, threaded_config: ThreadedClientOptions) -> MqttResult<SyncGneissClient> {
    match tls_impl {
        TlsConfiguration::None => { make_direct_client_no_tls(endpoint, port, client_options, connect_options, http_proxy_options, threaded_config) }
        #[cfg(feature = "threaded-rustls")]
        TlsConfiguration::Rustls => { make_direct_client_rustls(endpoint, port, tls_options, client_options, connect_options, http_proxy_options, threaded_config) }
        #[cfg(feature = "threaded-native-tls")]
        TlsConfiguration::Nativetls => { make_direct_client_native_tls(endpoint, port, tls_options, client_options, connect_options, http_proxy_options, threaded_config) }
        _ => { panic!("Illegal state"); }
    }
}

fn make_direct_client_no_tls(endpoint: String, port: u16, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, threaded_config: ThreadedClientOptions) -> MqttResult<SyncGneissClient> {
    info!("threaded make_direct_client_no_tls - creating connection establishment closure");
    let (stream_endpoint, http_connect_endpoint) = compute_endpoints(endpoint, port, &http_proxy_options);

    if http_connect_endpoint.is_some() {
        let connection_factory = Arc::new(move || {
            let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
            let tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
            let proxy_stream = apply_proxy_connect_to_stream(tcp_stream, http_connect_endpoint.clone())?;
            proxy_stream.set_nonblocking(true)?;
            Ok(proxy_stream)
        });

        info!("threaded make_direct_client_no_tls - plaintext-to-proxy -> plaintext-to-broker");
        Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
    } else {
        let connection_factory = Arc::new(move || {
            let tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
            tcp_stream.set_nonblocking(true)?;
            Ok(tcp_stream)
        });

        info!("threaded make_direct_client_no_tls - plaintext-to-broker");
        Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
    }
}

#[cfg(feature = "threaded-rustls")]
fn make_direct_client_rustls(endpoint: String, port: u16, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, threaded_config: ThreadedClientOptions) -> MqttResult<SyncGneissClient> {
    info!("threaded make_direct_client_rustls - creating connection establishment closure");

    let (stream_endpoint, http_connect_endpoint) = compute_endpoints(endpoint.clone(), port, &http_proxy_options);

    if let Some(tls_options) = tls_options {
        if let Some(http_proxy_options) = http_proxy_options {
            if let Some(proxy_tls_options) = http_proxy_options.tls_options {
                let connection_factory = Arc::new(move || {
                    let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                    let proxy_tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
                    let proxy_tls_stream = wrap_stream_with_tls_rustls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone())?;
                    let connect_stream = apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone())?;
                    let tls_connect_stream = wrap_stream_with_tls_rustls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone())?;
                    tls_connect_stream.sock.sock.set_nonblocking(true)?;
                    Ok(tls_connect_stream)
                });

                info!("threaded make_direct_client_rustls - tls-to-proxy -> tls-to-broker");
                Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
            } else {
                let connection_factory = Arc::new(move || {
                    let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                    let proxy_tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
                    let connect_stream = apply_proxy_connect_to_stream(proxy_tcp_stream, http_connect_endpoint.clone())?;
                    let tls_connect_stream = wrap_stream_with_tls_rustls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone())?;
                    tls_connect_stream.sock.set_nonblocking(true)?;
                    Ok(tls_connect_stream)
                });

                info!("threaded make_direct_client_rustls - plaintext-to-proxy -> tls-to-broker");
                Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
            }
        } else {
            let connection_factory = Arc::new(move || {
                let tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
                let tls_stream = wrap_stream_with_tls_rustls(tcp_stream, endpoint.clone(), tls_options.clone())?;
                tls_stream.sock.set_nonblocking(true)?;
                Ok(tls_stream)
            });

            info!("threaded make_direct_client_rustls - tls-to-broker");
            Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
        }
    } else if let Some(http_proxy_options) = http_proxy_options {
        if let Some(proxy_tls_options) = http_proxy_options.tls_options {
            let connection_factory = Arc::new(move || {
                let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                let proxy_tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
                let proxy_tls_stream = wrap_stream_with_tls_rustls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone())?;
                let connect_stream = apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone())?;
                connect_stream.sock.set_nonblocking(true)?;
                Ok(connect_stream)
            });

            info!("threaded make_direct_client_rustls - tls-to-proxy -> plaintext-to-broker");
            Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
        } else {
            panic!("threaded make_direct_client_rustls - invoked without tls configuration")
        }
    } else {
        panic!("threaded make_direct_client_rustls - invoked without tls configuration")
    }
}

#[cfg(feature = "threaded-native-tls")]
fn make_direct_client_native_tls(endpoint: String, port: u16, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>,  threaded_config: ThreadedClientOptions) -> MqttResult<SyncGneissClient> {
    info!("threaded make_direct_client_native_tls - creating async connection establishment closure");

    let (stream_endpoint, http_connect_endpoint) = compute_endpoints(endpoint.clone(), port, &http_proxy_options);

    if let Some(tls_options) = tls_options {
        if let Some(http_proxy_options) = http_proxy_options {
            if let Some(proxy_tls_options) = http_proxy_options.tls_options {
                let connection_factory = Arc::new(move || {
                    let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                    let proxy_tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
                    let proxy_tls_stream = wrap_stream_with_tls_native_tls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone())?;
                    let connect_stream = apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone())?;
                    let tls_connect_stream = wrap_stream_with_tls_native_tls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone())?;
                    tls_connect_stream.get_ref().get_ref().set_nonblocking(true)?;
                    Ok(tls_connect_stream)
                });

                info!("threaded make_direct_client_native_tls - tls-to-proxy -> tls-to-broker");
                Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
            } else {
                let connection_factory = Arc::new(move || {
                    let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                    let proxy_tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
                    let connect_stream = apply_proxy_connect_to_stream(proxy_tcp_stream, http_connect_endpoint.clone())?;
                    let tls_connect_stream = wrap_stream_with_tls_native_tls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone())?;
                    tls_connect_stream.get_ref().set_nonblocking(true)?;
                    Ok(tls_connect_stream)
                });

                info!("threaded make_direct_client_native_tls - plaintext-to-proxy -> tls-to-broker");
                Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
            }
        } else {
            let connection_factory = Arc::new(move || {
                let tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
                let tls_stream = wrap_stream_with_tls_native_tls(tcp_stream, endpoint.clone(), tls_options.clone())?;
                tls_stream.get_ref().set_nonblocking(true)?;
                Ok(tls_stream)
            });

            info!("threaded make_direct_client_native_tls - tls-to-broker");
            Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
        }
    } else if let Some(http_proxy_options) = http_proxy_options {
        if let Some(proxy_tls_options) = http_proxy_options.tls_options {
            let connection_factory = Arc::new(move || {
                let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                let proxy_tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
                let proxy_tls_stream = wrap_stream_with_tls_native_tls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone())?;
                let connect_stream = apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone())?;
                connect_stream.get_ref().set_nonblocking(true)?;
                Ok(connect_stream)
            });

            info!("threaded make_direct_client_native_tls - tls-to-proxy -> plaintext-to-broker");
            Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
        } else {
            panic!("threaded make_direct_client_native_tls - invoked without tls configuration")
        }
    } else {
        panic!("threaded make_direct_client_native_tls - invoked without tls configuration")
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg(feature="threaded-websockets")]
pub(crate) fn make_websocket_client_threaded(tls_impl: TlsConfiguration, endpoint: String, port: u16, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, threaded_config: ThreadedClientOptions) -> MqttResult<SyncGneissClient> {
    match tls_impl {
        TlsConfiguration::None => { make_websocket_client_no_tls(endpoint, port, client_options, connect_options, http_proxy_options, threaded_config) }
        /*
        #[cfg(feature = "threaded-rustls")]
        TlsConfiguration::Rustls => { make_websocket_client_rustls(endpoint, port, tls_options, client_options, connect_options, http_proxy_options, threaded_config) }
        #[cfg(feature = "threaded-native-tls")]
        TlsConfiguration::Nativetls => { make_websocket_client_native_tls(endpoint, port, tls_options, client_options, connect_options, http_proxy_options, threaded_config) }

         */
        _ => { panic!("Illegal state"); }
    }
}

#[cfg(feature="threaded-websockets")]
fn make_websocket_client_no_tls(endpoint: String, port: u16, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, mut threaded_config: ThreadedClientOptions) -> MqttResult<SyncGneissClient> {
    info!("threaded make_websocket_client_no_tls - creating connection establishment closure");
    let (stream_endpoint, http_connect_endpoint) = compute_endpoints(endpoint, port, &http_proxy_options);
    let ws_options = threaded_config.websocket_options.clone().unwrap();

    if http_connect_endpoint.is_some() {
        let connection_factory = Arc::new(move || {
            let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
            let tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
            let proxy_stream = apply_proxy_connect_to_stream(tcp_stream, http_connect_endpoint.clone())?;
            let mut ws_stream = wrap_stream_with_websockets(proxy_stream, http_connect_endpoint.endpoint.clone(), "ws", ws_options.clone())?;
            ws_stream.get_mut().set_nonblocking(true)?;
            Ok(ws_stream)
        });

        info!("threaded make_websocket_client_no_tls - plaintext-to-proxy -> plaintext-to-broker");
        Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
    } else {
        let connection_factory = Arc::new(move || {
            let tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
            let mut ws_stream = wrap_stream_with_websockets(tcp_stream, stream_endpoint.endpoint.clone(), "ws", ws_options.clone())?;
            ws_stream.get_mut().set_nonblocking(true)?;
            Ok(ws_stream)
        });

        info!("threaded make_websocket_client_no_tls - plaintext-to-broker");
        Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
    }
}
/*
#[cfg(all(feature = "threaded-rustls", feature = "threaded-websockets"))]
fn make_websocket_client_rustls(endpoint: String, port: u16, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, mut threaded_config: ThreadedClientOptions) -> MqttResult<SyncGneissClient> {
    info!("threaded make_websocket_client_rustls - creating connection establishment closure");

    let (stream_endpoint, http_connect_endpoint) = compute_endpoints(endpoint.clone(), port, &http_proxy_options);

    if let Some(tls_options) = tls_options {
        if let Some(http_proxy_options) = http_proxy_options {
            if let Some(proxy_tls_options) = http_proxy_options.tls_options {
                let connection_factory = Arc::new(move || {
                    let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                    let proxy_tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
                    let proxy_tls_stream = wrap_stream_with_tls_rustls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone())?;
                    let connect_stream = apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone())?;
                    let tls_connect_stream = wrap_stream_with_tls_rustls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone())?;
                    let ws_options = threaded_config.websocket_options.take();
                    let mut ws_stream = wrap_stream_with_websockets(tls_connect_stream, http_connect_endpoint.endpoint.clone(), "ws", ws_options.unwrap())?;
                    ws_stream.get_mut().sock.sock.set_nonblocking(true)?;

                    Ok(ws_stream)
                });

                info!("threaded make_websocket_client_rustls - tls-to-proxy -> tls-to-broker");
                Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
            } else {
                let connection_factory = Arc::new(move || {
                    let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                    let proxy_tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
                    let connect_stream = apply_proxy_connect_to_stream(proxy_tcp_stream, http_connect_endpoint.clone())?;
                    let tls_connect_stream = wrap_stream_with_tls_rustls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone())?;
                    let ws_options = threaded_config.websocket_options.take();
                    let mut ws_stream = wrap_stream_with_websockets(tls_connect_stream, http_connect_endpoint.endpoint.clone(), "ws", ws_options.unwrap())?;
                    ws_stream.get_mut().sock.set_nonblocking(true)?;

                    Ok(ws_stream)
                });

                info!("threaded make_websocket_client_rustls - plaintext-to-proxy -> tls-to-broker");
                Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
            }
        } else {
            let connection_factory = Arc::new(move || {
                let tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
                let tls_stream = wrap_stream_with_tls_rustls(tcp_stream, endpoint.clone(), tls_options.clone())?;
                let ws_options = threaded_config.websocket_options.take();
                let mut ws_stream = wrap_stream_with_websockets(tls_stream, endpoint.clone(), "ws", ws_options.unwrap())?;
                ws_stream.get_mut().sock.set_nonblocking(true)?;

                Ok(ws_stream)
            });

            info!("threaded make_websocket_client_rustls - tls-to-broker");
            Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
        }
    } else if let Some(http_proxy_options) = http_proxy_options {
        if let Some(proxy_tls_options) = http_proxy_options.tls_options {
            let connection_factory = Arc::new(move || {
                let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                let proxy_tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
                let proxy_tls_stream = wrap_stream_with_tls_rustls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone())?;
                let connect_stream = apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone())?;
                let ws_options = threaded_config.websocket_options.take();
                let mut ws_stream = wrap_stream_with_websockets(connect_stream, http_connect_endpoint.endpoint.clone(), "ws", ws_options.unwrap())?;
                ws_stream.get_mut().sock.set_nonblocking(true)?;

                Ok(ws_stream)
            });

            info!("threaded make_websocket_client_rustls - tls-to-proxy -> plaintext-to-broker");
            Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
        } else {
            panic!("threaded make_websocket_client_rustls - invoked without tls configuration")
        }
    } else {
        panic!("threaded make_websocket_client_rustls - invoked without tls configuration")
    }
}

#[cfg(all(feature = "threaded-native-tls", feature = "threaded-websockets"))]
fn make_websocket_client_native_tls(endpoint: String, port: u16, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>,  mut threaded_config: ThreadedClientOptions) -> MqttResult<SyncGneissClient> {
    info!("threaded make_websocket_client_native_tls - creating async connection establishment closure");

    let (stream_endpoint, http_connect_endpoint) = compute_endpoints(endpoint.clone(), port, &http_proxy_options);

    if let Some(tls_options) = tls_options {
        if let Some(http_proxy_options) = http_proxy_options {
            if let Some(proxy_tls_options) = http_proxy_options.tls_options {
                let connection_factory = Arc::new(move || {
                    let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                    let proxy_tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
                    let proxy_tls_stream = wrap_stream_with_tls_native_tls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone())?;
                    let connect_stream = apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone())?;
                    let tls_connect_stream = wrap_stream_with_tls_native_tls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone())?;
                    let ws_options = threaded_config.websocket_options.take();
                    let mut ws_stream = wrap_stream_with_websockets(tls_connect_stream, http_connect_endpoint.endpoint.clone(), "ws", ws_options.unwrap())?;
                    ws_stream.get_mut().get_ref().get_ref().set_nonblocking(true)?;

                    Ok(ws_stream)
                });

                info!("threaded make_websocket_client_native_tls - tls-to-proxy -> tls-to-broker");
                Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
            } else {
                let connection_factory = Arc::new(move || {
                    let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                    let proxy_tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
                    let connect_stream = apply_proxy_connect_to_stream(proxy_tcp_stream, http_connect_endpoint.clone())?;
                    let tls_connect_stream = wrap_stream_with_tls_native_tls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone())?;
                    let ws_options = threaded_config.websocket_options.take();
                    let mut ws_stream = wrap_stream_with_websockets(tls_connect_stream, http_connect_endpoint.endpoint.clone(), "ws", ws_options.unwrap())?;
                    ws_stream.get_mut().get_ref().set_nonblocking(true)?;

                    Ok(ws_stream)
                });

                info!("threaded make_websocket_client_native_tls - plaintext-to-proxy -> tls-to-broker");
                Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
            }
        } else {
            let connection_factory = Arc::new(move || {
                let tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
                let tls_stream = wrap_stream_with_tls_native_tls(tcp_stream, endpoint.clone(), tls_options.clone())?;
                let ws_options = threaded_config.websocket_options.take();
                let mut ws_stream = wrap_stream_with_websockets(tls_stream, endpoint.clone(), "ws", ws_options.unwrap())?;
                ws_stream.get_mut().get_ref().set_nonblocking(true)?;

                Ok(ws_stream)
            });

            info!("threaded make_websocket_client_native_tls - tls-to-broker");
            Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
        }
    } else if let Some(http_proxy_options) = http_proxy_options {
        if let Some(proxy_tls_options) = http_proxy_options.tls_options {
            let connection_factory = Arc::new(move || {
                let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                let proxy_tcp_stream = make_leaf_stream(stream_endpoint.clone())?;
                let proxy_tls_stream = wrap_stream_with_tls_native_tls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone())?;
                let connect_stream = apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone())?;
                let ws_options = threaded_config.websocket_options.take();
                let mut ws_stream = wrap_stream_with_websockets(connect_stream, http_connect_endpoint.endpoint.clone(), "ws", ws_options.unwrap())?;
                ws_stream.get_mut().get_ref().set_nonblocking(true)?;

                Ok(ws_stream)
            });

            info!("threaded make_websocket_client_native_tls - tls-to-proxy -> plaintext-to-broker");
            Ok(new_threaded(client_options, connect_options, threaded_config, connection_factory))
        } else {
            panic!("threaded make_websocket_client_native_tls - invoked without tls configuration")
        }
    } else {
        panic!("threaded make_websocket_client_native_tls - invoked without tls configuration")
    }
}
*/

fn make_leaf_stream(endpoint: Endpoint) -> MqttResult<TcpStream> {
    let addr = make_addr(endpoint.endpoint.as_str(), endpoint.port)?;
    debug!("make_leaf_stream - opening TCP stream");
    let stream = TcpStream::connect(&addr)?;
    debug!("make_leaf_stream - TCP stream successfully established");

    // we don't set non-blocking here.  The tls and websocket wrapper layers often need the
    // socket to be blocking to work correctly.  We set the socket non-blocking once the
    // transport has been fully established and we're ready to speak MQTT.
    stream.set_nodelay(true)?;

    Ok(stream)
}

#[cfg(feature = "threaded-rustls")]
fn wrap_stream_with_tls_rustls<S>(stream : S, endpoint: String, tls_options: TlsOptions) -> MqttResult<rustls::StreamOwned<rustls::client::ClientConnection, S>> where S : Read + Write {
    let domain = rustls_pki_types::ServerName::try_from(endpoint)?
        .to_owned();

    if let TlsData::Rustls(config) = tls_options.options {
        let client_connection = rustls::client::ClientConnection::new(config, domain)?;
        let mut tls_stream = rustls::StreamOwned::new(client_connection, stream);
        debug!("wrap_stream_with_tls_rustls - performing tls handshake");
        let result = tls_stream.flush();
        if result.is_err() {
            return Err(MqttError::new_connection_establishment_failure(result.err().unwrap()));
        }
        debug!("wrap_stream_with_tls_rustls - tls handshake successfully completed");
        Ok(tls_stream)
    } else {
        panic!("Rustls stream wrapper invoked without Rustls configuration");
    }
}

#[cfg(feature = "threaded-native-tls")]
fn wrap_stream_with_tls_native_tls<S>(stream : S, endpoint: String, tls_options: TlsOptions) -> MqttResult<native_tls::TlsStream<S>> where S : Read + Write {
    if let TlsData::NativeTls(ntls_builder) = tls_options.options {
        let connector = ntls_builder.build()?;
        debug!("threaded wrap_stream_with_tls_native_tls - performing tls handshake");
        let tls_stream = connector.connect(endpoint.as_str(), stream)?;
        debug!("threaded wrap_stream_with_tls_native_tls - tls handshake successfully completed");
        Ok(tls_stream)
    } else {
        panic!("Native-tls stream wrapper invoked without native-tls configuration");
    }
}


#[cfg(feature="threaded-websockets")]
fn wrap_stream_with_websockets<S>(stream : S, endpoint: String, scheme: &str, websocket_options: SyncWebsocketOptions) -> MqttResult<WebsocketStreamWrapper<S>> where S : Read + Write {

    let uri = format!("{}://{}/mqtt", scheme, endpoint); // scheme needs to be present but value irrelevant
    let handshake_builder = crate::config::create_default_websocket_handshake_request(uri)?;

    debug!("threaded wrap_stream_with_websockets - performing websocket upgrade request transform");
    let transformed_handshake_builder =
        if let Some(transform) = &*websocket_options.handshake_transform {
            transform(handshake_builder)?
        } else {
            handshake_builder
        };
    debug!("threaded wrap_stream_with_websockets - successfully transformed websocket upgrade request");

    debug!("threaded wrap_stream_with_websockets - upgrading stream to websockets");

    let websocket_result = tungstenite::client::client(HandshakeRequest { handshake_builder: transformed_handshake_builder }, stream)?;
    let ws_stream = WebsocketStreamWrapper::new(websocket_result.0);
    debug!("threaded wrap_stream_with_websockets - successfully upgraded stream to websockets");

    Ok(ws_stream)
}

fn apply_proxy_connect_to_stream<T>(mut stream : T, http_connect_endpoint: Endpoint) -> MqttResult<T> where T : Read + Write {

    debug!("apply_proxy_connect_to_stream - writing CONNECT request to connection stream");
    let request_bytes = build_connect_request(&http_connect_endpoint);
    stream.write_all(request_bytes.as_slice())?;
    debug!("apply_proxy_connect_to_stream - successfully wrote CONNECT request to stream");

    let mut inbound_data: [u8; 4096] = [0; 4096];
    let mut response_bytes = Vec::new();

    loop {
        let bytes_read = stream.read(&mut inbound_data)?;
        if bytes_read == 0 {
            info!("apply_proxy_connect_to_stream - proxy connect stream closed with zero byte read");
            return Err(MqttError::new_connection_establishment_failure("proxy connect stream closed"));
        }

        response_bytes.extend_from_slice(&inbound_data[..bytes_read]);

        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut response = httparse::Response::new(&mut headers);

        let parse_result = response.parse(response_bytes.as_slice());
        match parse_result {
            Err(e) => {
                error!("apply_proxy_connect_to_stream - failed to parse proxy response to CONNECT request: {:?}", e);
                return Err(MqttError::new_connection_establishment_failure(e));
            }
            Ok(httparse::Status::Complete(bytes_parsed)) => {
                if bytes_parsed < response_bytes.len() {
                    error!("apply_proxy_connect_to_stream - stream incoming data contains more data than the CONNECT response");
                    return Err(MqttError::new_connection_establishment_failure("proxy connect response too long"));
                }

                if let Some(response_code) = response.code {
                    if (200..300).contains(&response_code) {
                        return Ok(stream);
                    }
                }

                error!("apply_proxy_connect_to_stream - CONNECT request was failed, with http code: {:?}", response.code);
                return Err(MqttError::new_connection_establishment_failure("proxy connect request unsuccessful"));
            }
            Ok(httparse::Status::Partial) => {}
        }
    }
}


/// Simple debug type that uses the client listener framework to allow tests to asynchronously wait for
/// configurable client event sequences.  May be useful outside of tests.  May need polish.  Currently public
/// because we use it across crates.  May eventually go internal.
///
/// Requires the client to be Arc-wrapped.
pub struct ThreadedClientEventWaiter {
    event_count: usize,

    client: SyncGneissClient,

    listener: Option<ListenerHandle>,

    events: Arc<Mutex<Option<Vec<ClientEventRecord>>>>,

    signal: Arc<Condvar>,
}

impl ThreadedClientEventWaiter {

    /// Creates a new ClientEventWaiter instance from full configuration
    pub fn new(client: SyncGneissClient, config: ClientEventWaiterOptions, event_count: usize) -> Self {
        let lock = Arc::new(Mutex::new(Some(Vec::new())));
        let signal = Arc::new(Condvar::new());

        let mut waiter = ThreadedClientEventWaiter {
            event_count,
            client: client.clone(),
            listener: None,
            events: lock.clone(),
            signal: signal.clone(),
        };

        let listener_fn = move |event: Arc<ClientEvent>| {
            match &config.wait_type {
                ClientEventWaitType::Type(event_type) => {
                    if !client_event_matches(&event, *event_type) {
                        return;
                    }
                }
                ClientEventWaitType::Predicate(event_predicate) => {
                    if !(*event_predicate)(&event) {
                        return;
                    }
                }
            }

            let event_record = ClientEventRecord {
                event: event.clone(),
                timestamp: Instant::now(),
            };

            let mut events_guard = lock.lock().unwrap();
            let events_option = events_guard.as_mut();
            if let Some(events) = events_option {
                events.push(event_record);

                if events.len() >= event_count {
                    signal.notify_all();
                }
            }
        };

        waiter.listener = Some(client.add_event_listener(Arc::new(listener_fn)).unwrap());
        waiter
    }

    /// Creates a new ClientEventWaiter instance that will wait for a single occurrence of a single event type
    pub fn new_single(client: SyncGneissClient, event_type: ClientEventType) -> Self {
        let config = ClientEventWaiterOptions {
            wait_type: ClientEventWaitType::Type(event_type),
        };

        Self::new(client, config, 1)
    }
}

impl SyncClientEventWaiter for ThreadedClientEventWaiter {
    fn wait(self) -> MqttResult<Vec<ClientEventRecord>> {
        let mut current_events_option = self.events.lock().unwrap();
        loop {
            match &*current_events_option {
                Some(current_events) => {
                    if current_events.len() >= self.event_count {
                        return Ok(current_events_option.take().unwrap());
                    }
                }
                None => {
                    return Err(MqttError::new_other_error("Client event waiter result already taken"));
                }
            }

            current_events_option = self.signal.wait(current_events_option).unwrap();
        }
    }
}

impl Drop for ThreadedClientEventWaiter {
    fn drop(&mut self) {
        let listener_handler = self.listener.take().unwrap();

        let _ = self.client.remove_event_listener(listener_handler);
    }
}

#[cfg(all(test, feature = "testing"))]
pub(crate) mod testing {
    use crate::config::GenericClientBuilder;
    use crate::error::MqttResult;
    use crate::features::threaded::*;
    use crate::testing::integration::{create_good_client_builder, ProxyUsage, start_sync_client, stop_sync_client, SyncTestFactory, TlsUsage, WebsocketUsage};

    fn threaded_connect_disconnect_test(builder: GenericClientBuilder) -> MqttResult<()> {
        let threaded_options = ThreadedClientOptionsBuilder::new().build();
        let client = builder.build_threaded(threaded_options).unwrap();

        start_sync_client(&client, ThreadedClientEventWaiter::new_single)?;
        stop_sync_client(&client, ThreadedClientEventWaiter::new_single)?;

        Ok(())
    }

    fn do_good_client_test(tls: TlsUsage, ws: WebsocketUsage, proxy: ProxyUsage, test_factory: SyncTestFactory) {
        assert!((*test_factory)(create_good_client_builder(tls, ws, proxy)).is_ok());
    }

    #[test]
    #[cfg(feature = "threaded")]
    fn client_connect_disconnect_direct_plaintext_no_proxy() {
        do_good_client_test(TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            threaded_connect_disconnect_test(builder)
        }));
    }

    #[test]
    #[cfg(feature = "threaded-rustls")]
    fn client_connect_disconnect_direct_rustls_no_proxy() {
        do_good_client_test(TlsUsage::Rustls, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            threaded_connect_disconnect_test(builder)
        }));
    }

    #[test]
    #[cfg(feature = "threaded-native-tls")]
    fn client_connect_disconnect_direct_native_tls_no_proxy() {
        do_good_client_test(TlsUsage::Nativetls, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            threaded_connect_disconnect_test(builder)
        }));
    }

    #[test]
    #[cfg(feature="threaded-websockets")]
    fn client_connect_disconnect_websocket_plaintext_no_proxy() {
        do_good_client_test(TlsUsage::None, WebsocketUsage::Tungstenite, ProxyUsage::None, Box::new(|builder|{
            threaded_connect_disconnect_test(builder)
        }));
    }
}