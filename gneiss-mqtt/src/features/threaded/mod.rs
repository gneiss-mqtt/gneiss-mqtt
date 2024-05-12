/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Implementation of an MQTT client that uses one or more background threads for processing.
 */


use std::cmp::min;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant};
use log::{debug, info, trace};
use crate::client::*;
use crate::error::{MqttError, MqttResult};
use crate::mqtt::{MqttPacket, PublishPacket, SubscribePacket, UnsubscribePacket};
use crate::mqtt::disconnect::validate_disconnect_packet_outbound;
use crate::protocol::is_connection_established;
use crate::validate::validate_packet_outbound;

/// Thread-specific client configuration
pub struct ThreadedClientOptions<T> where T : Read + Write + Send + Sync + 'static {

    pub idle_service_sleep: Duration,

    /// Factory function for creating the final connection object based on all the various
    /// configuration options and features.  It might be a TcpStream, it might be a TlsStream,
    /// it might be a WebsocketStream, it might be some nested combination.
    ///
    /// Ultimately, the type must implement Read and Write.
    pub connection_factory: Arc<dyn Fn() -> MqttResult<T> + Send + Sync>,
}

pub(crate) struct ClientRuntimeState<T> where T : Read + Write + Send + Sync + 'static {
    threaded_config: ThreadedClientOptions<T>,
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
                sleep(sleep_duration);
            } else {
                debug!("threaded - process_stopped - skipping sleep");
            }
        }
    }

    pub(crate) fn process_connecting(&mut self, client: &mut MqttClientImpl) -> MqttResult<ClientImplState> {
        // let mut connect = (self.threaded_config.connection_factory)();
        let timeout_timepoint = Instant::now() + *client.connect_timeout();

        let connection_factory = self.threaded_config.connection_factory.clone();
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
                sleep(sleep_duration);
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
            let read_result = stream.read(inbound_data.as_mut_slice());
            match read_result {
                Ok(bytes_read) => {
                    debug!("threaded - process_connected - read {} bytes from connection stream", bytes_read);

                    if bytes_read == 0 {
                        info!("threaded - process_connected - connection closed for read (0 bytes)");
                        client.apply_error(MqttError::new_connection_closed("network stream closed"));
                        next_state = Some(ClientImplState::PendingReconnect);
                        continue;
                    } else if let Err(error) = client.handle_incoming_bytes(&inbound_data[..bytes_read]) {
                        info!("threaded - process_connected - error handling incoming bytes: {:?}", error);
                        client.apply_error(error);
                        next_state = Some(ClientImplState::PendingReconnect);
                        continue;
                    }

                    sleep_duration = None;
                }
                Err(error) => {
                    // TODO: handle would block/again cases as non-error

                    info!("threaded - process_connected - connection stream read failed: {:?}", error);
                    if is_connection_established(client.get_protocol_state()) {
                        client.apply_error(MqttError::new_connection_closed(error));
                    } else {
                        client.apply_error(MqttError::new_connection_establishment_failure(error));
                    }
                    next_state = Some(ClientImplState::PendingReconnect);
                    continue;
                }
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

            if let Some(write_bytes) = write_directive {
                let mut should_flush : bool = false;
                let bytes_written_result = stream.write(write_bytes);
                match bytes_written_result {
                    Ok(bytes_written) => {
                        // TODO: handle 0-length writes?

                        debug!("threaded - process_connected - wrote {} bytes to connection stream", bytes_written);
                        cumulative_bytes_written += bytes_written;
                        if cumulative_bytes_written == outbound_data.len() {
                            outbound_data.clear();
                            cumulative_bytes_written = 0;
                            should_flush = true;
                        }
                    }
                    Err(error) => {
                        // TODO: handle blocking errors?

                        info!("threaded - process_connected - connection stream write failed: {:?}", error);
                        if is_connection_established(client.get_protocol_state()) {
                            client.apply_error(MqttError::new_connection_closed(error));
                        } else {
                            client.apply_error(MqttError::new_connection_establishment_failure(error));
                        }
                        next_state = Some(ClientImplState::PendingReconnect);
                        continue;
                    }
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
                sleep(sleep_duration);
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
                sleep(sleep_duration);
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