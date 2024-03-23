/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Functionality for using [`async-std`](https://crates.io/crates/async-std) as an MQTT client's async
runtime implementation.
 */

use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use log::{debug, error, info, trace};
use async_std::channel::{Receiver, Sender};
use async_std::net::TcpStream;
use async_std::io::{Read, Write};
use async_std::task;
use async_std::prelude::*;
use async_std::task::sleep;

use crate::client::*;
use crate::config::*;
use crate::error::{MqttError, MqttResult};
use crate::protocol::is_connection_established;

use futures::{select, FutureExt, pin_mut};
use crate::mqtt::disconnect::validate_disconnect_packet_outbound;
use crate::mqtt::{MqttPacket, PublishPacket, SubscribePacket, UnsubscribePacket};
use crate::validate::validate_packet_outbound;

pub(crate) struct ClientRuntimeState<T> where T : Read + Write + Send + Sync + 'static {
    async_std_config: AsyncStdClientOptions<T>,
    operation_receiver: Receiver<OperationOptions>,
    stream: Option<T>
}

impl<T> ClientRuntimeState<T> where T : Read + Write + Send + Sync + 'static {
    pub(crate) async fn process_stopped(&mut self, client: &mut MqttClientImpl) -> MqttResult<ClientImplState> {
        loop {
            trace!("async-std - process_stopped loop");

            select! {
                operation_result = self.operation_receiver.recv().fuse() => {
                    match operation_result {
                        Ok(operation_options) => {
                            debug!("async-std - process_stopped - user operation received");
                            client.handle_incoming_operation(operation_options);
                        }
                        Err(error) => {
                            client.apply_error(MqttError::new_operation_channel_failure(error));
                        }
                    }
                }
            }

            if let Some(transition_state) = client.compute_optional_state_transition() {
                return Ok(transition_state);
            }
        }
    }

    pub(crate) async fn process_connecting(&mut self, client: &mut MqttClientImpl) -> MqttResult<ClientImplState> {
        let mut connection_future = Box::pin((self.async_std_config.connection_factory)().fuse());
        let mut timeout = Box::pin(sleep(*client.connect_timeout()).fuse());

        loop {
            select! {
                operation_result = self.operation_receiver.recv().fuse() => {
                    match operation_result {
                        Ok(operation_options) => {
                            debug!("async-std - process_connecting - user operation received");
                            client.handle_incoming_operation(operation_options);
                        }
                        Err(error) => {
                            client.apply_error(MqttError::new_operation_channel_failure(error));
                        }
                    }
                },
                () = &mut timeout => {
                    info!("async-std - process_connecting - connection establishment timeout exceeded");
                    client.apply_error(MqttError::new_connection_establishment_failure("connection establishment timeout reached"));
                    return Ok(ClientImplState::PendingReconnect);
                },
                connection_result = &mut connection_future => {
                    return match connection_result {
                        Ok(stream) => {
                            info!("async-std - process_connecting - transport connection established successfully");
                            self.stream = Some(stream);
                            Ok(ClientImplState::Connected)
                        }
                        Err(error) => {
                            info!("async-std - process_connecting - transport connection establishment failed");
                            client.apply_error(MqttError::new_connection_establishment_failure(error));
                            Ok(ClientImplState::PendingReconnect)
                        }
                    }
                }
            }

            if let Some(transition_state) = client.compute_optional_state_transition() {
                return Ok(transition_state);
            }
        }
    }

    pub(crate) async fn process_connected(&mut self, client: &mut MqttClientImpl) -> MqttResult<ClientImplState> {
        let mut outbound_data: Vec<u8> = Vec::with_capacity(4096);
        let mut cumulative_bytes_written : usize = 0;

        let mut inbound_data: [u8; 4096] = [0; 4096];

        let stream = self.stream.take().unwrap();
        let (mut reader, mut writer) = (&stream, &stream);

        let mut write_directive : Option<&[u8]>;

        let mut next_state = None;
        while next_state.is_none() {
            trace!("async-std - process_connected loop");

            let next_service_time_option = client.get_next_connected_service_time();
            let service_wait = next_service_time_option.map(|next_service_time| sleep(next_service_time - Instant::now()));

            let outbound_slice_option: Option<&[u8]> =
                if cumulative_bytes_written < outbound_data.len() {
                    Some(&outbound_data[cumulative_bytes_written..])
                } else {
                    None
                };

            let mut should_flush = false;
            if let Some(outbound_slice) = outbound_slice_option {
                debug!("async-std - process_connected - {} bytes to write", outbound_slice.len());
                write_directive = Some(outbound_slice)
            } else {
                debug!("async-std - process_connected - nothing to write");
                write_directive = None;
            }

            select! {
                operation_result = self.operation_receiver.recv() => {
                    match operation_result {
                        Ok(operation_options) => {
                            debug!("async-std - process_connected - user operation received");
                            client.handle_incoming_operation(operation_options);
                        }
                        Err(error) => {
                            client.apply_error(MqttError::new_operation_channel_failure(error));
                        }
                    }
                },
                // incoming data on the socket future
                read_result = reader.read(inbound_data.as_mut_slice()) => {
                    match read_result {
                        Ok(bytes_read) => {
                            debug!("async-std - process_connected - read {} bytes from connection stream", bytes_read);

                            if bytes_read == 0 {
                                info!("async-std - process_connected - connection closed for read (0 bytes)");
                                client.apply_error(MqttError::new_connection_closed("network stream closed"));
                                next_state = Some(ClientImplState::PendingReconnect);
                            } else if let Err(error) = client.handle_incoming_bytes(&inbound_data[..bytes_read]) {
                                info!("async-std - process_connected - error handling incoming bytes: {:?}", error);
                                client.apply_error(error);
                                next_state = Some(ClientImplState::PendingReconnect);
                            }
                        }
                        Err(error) => {
                            info!("async-std - process_connected - connection stream read failed: {:?}", error);
                            if is_connection_established(client.get_protocol_state()) {
                                client.apply_error(MqttError::new_connection_closed(error));
                            } else {
                                client.apply_error(MqttError::new_connection_establishment_failure(error));
                            }
                            next_state = Some(ClientImplState::PendingReconnect);
                        }
                    }
                },
                // client service future (if relevant)
                Some(_) = conditional_wait(service_wait) => {
                    debug!("async-std - process_connected - running client service task");
                    if let Err(error) = client.handle_service(&mut outbound_data) {
                        client.apply_error(error);
                        next_state = Some(ClientImplState::PendingReconnect);
                    }
                },
                // outbound data future (if relevant)
                Some(bytes_written_result) = conditional_write(write_directive, &mut writer).fuse() => {
                    match bytes_written_result {
                        Ok(bytes_written) => {
                            debug!("async-std - process_connected - wrote {} bytes to connection stream", bytes_written);
                            cumulative_bytes_written += bytes_written;
                            if cumulative_bytes_written == outbound_data.len() {
                                outbound_data.clear();
                                cumulative_bytes_written = 0;
                                should_flush = true;
                            }
                        }
                        Err(error) => {
                            info!("async-std - process_connected - connection stream write failed: {:?}", error);
                            if is_connection_established(client.get_protocol_state()) {
                                client.apply_error(MqttError::new_connection_closed(error));
                            } else {
                                client.apply_error(MqttError::new_connection_establishment_failure(error));
                            }
                            next_state = Some(ClientImplState::PendingReconnect);
                        }
                    }
                }
            }

            if should_flush {
                let flush_result = writer.flush().await;
                match flush_result {
                    Ok(()) => {
                        if let Err(error) = client.handle_write_completion() {
                            info!("async-std - process_connected - stream write completion handler failed: {:?}", error);
                            client.apply_error(error);
                            next_state = Some(ClientImplState::PendingReconnect);
                        }
                    }
                    Err(error) => {
                        info!("async-std - process_connected - connection stream flush failed: {:?}", error);
                        if is_connection_established(client.get_protocol_state()) {
                            client.apply_error(MqttError::new_connection_closed(error));
                        } else {
                            client.apply_error(MqttError::new_connection_establishment_failure(error));
                        }
                        next_state = Some(ClientImplState::PendingReconnect);
                    }
                }
            }

            if next_state.is_none() {
                next_state = client.compute_optional_state_transition();
            }
        }

        Ok(next_state.unwrap())
    }

    /*
   pub(crate) async fn process_connected(&mut self, client: &mut MqttClientImpl) -> MqttResult<ClientImplState> {
        let mut outbound_data: Vec<u8> = Vec::with_capacity(4096);
        let mut cumulative_bytes_written : usize = 0;

        let mut inbound_data: [u8; 4096] = [0; 4096];

        let stream = self.stream.take().unwrap();
        let (stream_reader, mut stream_writer) = split(stream);
        tokio::pin!(stream_reader);

        let mut write_directive : Option<&[u8]>;

        let mut next_state = None;
        while next_state.is_none() {
            trace!("tokio - process_connected loop");

            let next_service_time_option = client.get_next_connected_service_time();
            let service_wait: Option<tokio::time::Sleep> = next_service_time_option.map(|next_service_time| sleep(next_service_time - Instant::now()));

            let outbound_slice_option: Option<&[u8]> =
                if cumulative_bytes_written < outbound_data.len() {
                    Some(&outbound_data[cumulative_bytes_written..])
                } else {
                    None
                };

            let mut should_flush = false;
            if let Some(outbound_slice) = outbound_slice_option {
                debug!("tokio - process_connected - {} bytes to write", outbound_slice.len());
                write_directive = Some(outbound_slice)
            } else {
                debug!("tokio - process_connected - nothing to write");
                write_directive = None;
            }

            tokio::select! {
                // incoming user operations future
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        debug!("tokio - process_connected - user operation received");
                        client.handle_incoming_operation(operation_options);
                    }
                }
                // incoming data on the socket future
                read_result = stream_reader.read(inbound_data.as_mut_slice()) => {
                    match read_result {
                        Ok(bytes_read) => {
                            debug!("tokio - process_connected - read {} bytes from connection stream", bytes_read);

                            if bytes_read == 0 {
                                info!("tokio - process_connected - connection closed for read (0 bytes)");
                                client.apply_error(MqttError::new_connection_closed("network stream closed"));
                                next_state = Some(ClientImplState::PendingReconnect);
                            } else if let Err(error) = client.handle_incoming_bytes(&inbound_data[..bytes_read]) {
                                info!("tokio - process_connected - error handling incoming bytes: {:?}", error);
                                client.apply_error(error);
                                next_state = Some(ClientImplState::PendingReconnect);
                            }
                        }
                        Err(error) => {
                            info!("tokio - process_connected - connection stream read failed: {:?}", error);
                            if is_connection_established(client.get_protocol_state()) {
                                client.apply_error(MqttError::new_connection_closed(error));
                            } else {
                                client.apply_error(MqttError::new_connection_establishment_failure(error));
                            }
                            next_state = Some(ClientImplState::PendingReconnect);
                        }
                    }
                }
                // client service future (if relevant)
                Some(_) = conditional_wait(service_wait) => {
                    debug!("tokio - process_connected - running client service task");
                    if let Err(error) = client.handle_service(&mut outbound_data) {
                        client.apply_error(error);
                        next_state = Some(ClientImplState::PendingReconnect);
                    }
                }
                // outbound data future (if relevant)
                Some(bytes_written_result) = conditional_write(write_directive, &mut stream_writer) => {
                    match bytes_written_result {
                        Ok(bytes_written) => {
                            debug!("tokio - process_connected - wrote {} bytes to connection stream", bytes_written);
                            cumulative_bytes_written += bytes_written;
                            if cumulative_bytes_written == outbound_data.len() {
                                outbound_data.clear();
                                cumulative_bytes_written = 0;
                                should_flush = true;
                            }
                        }
                        Err(error) => {
                            info!("tokio - process_connected - connection stream write failed: {:?}", error);
                            if is_connection_established(client.get_protocol_state()) {
                                client.apply_error(MqttError::new_connection_closed(error));
                            } else {
                                client.apply_error(MqttError::new_connection_establishment_failure(error));
                            }
                            next_state = Some(ClientImplState::PendingReconnect);
                        }
                    }
                }
            }

            if should_flush {
                let flush_result = stream_writer.flush().await;
                match flush_result {
                    Ok(()) => {
                        if let Err(error) = client.handle_write_completion() {
                            info!("tokio - process_connected - stream write completion handler failed: {:?}", error);
                            client.apply_error(error);
                            next_state = Some(ClientImplState::PendingReconnect);
                        }
                    }
                    Err(error) => {
                        info!("tokio - process_connected - connection stream flush failed: {:?}", error);
                        if is_connection_established(client.get_protocol_state()) {
                            client.apply_error(MqttError::new_connection_closed(error));
                        } else {
                            client.apply_error(MqttError::new_connection_establishment_failure(error));
                        }
                        next_state = Some(ClientImplState::PendingReconnect);
                    }
                }
            }

            if next_state.is_none() {
                next_state = client.compute_optional_state_transition();
            }
        }

        info!("tokio - process_connected - shutting down stream");
        let _ = stream_writer.shutdown().await;
        info!("tokio - process_connected - stream fully closed");

        Ok(next_state.unwrap())
    }

     */
    pub(crate) async fn process_pending_reconnect(&mut self, client: &mut MqttClientImpl, wait: Duration) -> MqttResult<ClientImplState> {

        let derp = sleep(wait);

        let mut reconnect_timer = Box::pin(sleep(wait).fuse());

        loop {
            select! {
                operation_result = self.operation_receiver.recv().fuse() => {
                    match operation_result {
                        Ok(operation_options) => {
                            debug!("async-std - process_pending_reconnect - user operation received");
                            client.handle_incoming_operation(operation_options);
                        }
                        Err(error) => {
                            client.apply_error(MqttError::new_operation_channel_failure(error));
                        }
                    }
                },
                () = &mut reconnect_timer => {
                    info!("async-std - process_pending_reconnect - reconnect timer exceeded");
                    return Ok(ClientImplState::Connecting);
                },
            }

            if let Some(transition_state) = client.compute_optional_state_transition() {
                return Ok(transition_state);
            }
        }
    }
}

async fn conditional_wait(wait_option: Option<impl Future<Output=()> + Sized>) -> Option<()> {
    match wait_option {
        Some(timer) => {
            timer.await;
            Some(())
        },
        None => None,
    }
}

async fn conditional_write<T>(data: Option<&[u8]>, writer: &mut T) -> Option<std::io::Result<usize>> where T : Write + Unpin {
    match data {
        Some(bytes) => {
            Some(writer.write(bytes).await)
        }
        _ => { None }
    }
}

type AsyncStdConnectionFactoryReturnType<T> = Pin<Box<dyn Future<Output = MqttResult<T>> + Send>>;

/// Tokio-specific client configuration
pub struct AsyncStdClientOptions<T> where T : Read + Write + Send + Sync {

    /// Factory function for creating the final connection object based on all the various
    /// configuration options and features.  It might be a TcpStream, it might be a TlsStream,
    /// it might be a WebsocketStream, it might be some nested combination.
    ///
    /// Ultimately, the type must implement Read and Write.
    pub connection_factory: Box<dyn Fn() -> AsyncStdConnectionFactoryReturnType<T> + Send + Sync>,
}

pub(crate) fn create_runtime_states<T>(async_std_config: AsyncStdClientOptions<T>) -> (Sender<OperationOptions>, ClientRuntimeState<T>) where T : Read + Write + Send + Sync + 'static {
    let (sender, receiver) = async_std::channel::unbounded();

    let impl_state = ClientRuntimeState {
        async_std_config,
        operation_receiver: receiver,
        stream: None
    };

    (sender, impl_state)
}

struct AsyncStdClient {
    pub(crate) operation_sender: Sender<OperationOptions>,

    pub(crate) listener_id_allocator: Mutex<u64>
}


impl AsyncMqttClient for AsyncStdClient {

    /// Signals the client that it should attempt to recurrently maintain a connection to
    /// the broker endpoint it has been configured with.
    fn start(&self, default_listener: Option<Arc<ClientEventListenerCallback>>) -> MqttResult<()> {
        info!("client start invoked");
        if let Err(send_error) = self.operation_sender.try_send(OperationOptions::Start(default_listener)) {
            return Err(MqttError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }

    /// Signals the client that it should close any current connection it has and enter the
    /// Stopped state, where it does nothing.
    fn stop(&self, options: Option<StopOptions>) -> MqttResult<()> {
        info!("client stop invoked {} a disconnect packet", if options.as_ref().is_some_and(|opts| { opts.disconnect.is_some()}) { "with" } else { "without" });
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

        if let Err(send_error) = self.operation_sender.try_send(OperationOptions::Stop(stop_options)) {
            return Err(MqttError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }

    /// Signals the client that it should clean up all internal resources (connection, channels,
    /// runtime tasks, etc...) and enter a terminal state that cannot be escaped.  Useful to ensure
    /// a full resource wipe.  If just `stop()` is used then the client will continue to track
    /// MQTT session state internally.
    fn close(&self) -> MqttResult<()> {
        info!("client close invoked; no further operations allowed");
        if let Err(send_error) = self.operation_sender.try_send(OperationOptions::Shutdown()) {
            return Err(MqttError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }

    /// Submits a Publish operation to the client's operation queue.  The publish will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.
    fn publish(&self, packet: PublishPacket, options: Option<PublishOptions>) -> Pin<Box<PublishResultFuture>> {
        debug!("Publish operation submitted");
        let boxed_packet = Box::new(MqttPacket::Publish(packet));
        if let Err(error) = validate_packet_outbound(&boxed_packet) {
            return Box::pin(async move { Err(error) });
        }

        return Box::pin(async move { Err(MqttError::new_unimplemented("async-std publish NYI")) });
    }

    /// Submits a Subscribe operation to the client's operation queue.  The subscribe will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.
    fn subscribe(&self, packet: SubscribePacket, options: Option<SubscribeOptions>) -> Pin<Box<SubscribeResultFuture>> {
        debug!("Subscribe operation submitted");
        let boxed_packet = Box::new(MqttPacket::Subscribe(packet));
        if let Err(error) = validate_packet_outbound(&boxed_packet) {
            return Box::pin(async move { Err(error) });
        }

        return Box::pin(async move { Err(MqttError::new_unimplemented("async-std subscribe NYI")) });
    }

    /// Submits an Unsubscribe operation to the client's operation queue.  The unsubscribe will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.
    fn unsubscribe(&self, packet: UnsubscribePacket, options: Option<UnsubscribeOptions>) -> Pin<Box<UnsubscribeResultFuture>> {
        debug!("Unsubscribe operation submitted");
        let boxed_packet = Box::new(MqttPacket::Unsubscribe(packet));
        if let Err(error) = validate_packet_outbound(&boxed_packet) {
            return Box::pin(async move { Err(error) });
        }

        return Box::pin(async move { Err(MqttError::new_unimplemented("async-std unsubscribe NYI")) });
    }

    /// Adds an additional listener to the events emitted by this client.  This is useful when
    /// multiple higher-level constructs are sharing the same MQTT client.
    fn add_event_listener(&self, listener: ClientEventListener) -> MqttResult<ListenerHandle> {
        debug!("AddListener operation submitted");
        let mut current_id = self.listener_id_allocator.lock().unwrap();
        let listener_id = *current_id;
        *current_id += 1;

        if let Err(send_error) = self.operation_sender.try_send(OperationOptions::AddListener(listener_id, listener)) {
            return Err(MqttError::new_operation_channel_failure(send_error));
        }

        Ok(ListenerHandle {
            id: listener_id
        })
    }

    /// Removes a listener from this client's set of event listeners.
    fn remove_event_listener(&self, listener: ListenerHandle) -> MqttResult<()> {
        debug!("RemoveListener operation submitted");
        if let Err(send_error) = self.operation_sender.try_send(OperationOptions::RemoveListener(listener.id)) {
            return Err(MqttError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }
}

async fn client_event_loop<T>(client_impl: &mut MqttClientImpl, async_state: &mut ClientRuntimeState<T>) where T : Read + Write + Send + Sync + 'static {
    let mut done = false;
    while !done {
        let current_state = client_impl.get_current_state();
        let next_state_result =
            match current_state {
                ClientImplState::Stopped => { async_state.process_stopped(client_impl).await }
                ClientImplState::Connecting => { async_state.process_connecting(client_impl).await }
                ClientImplState::Connected => { async_state.process_connected(client_impl).await }
                ClientImplState::PendingReconnect => {
                    let reconnect_wait = client_impl.advance_reconnect_period();
                    async_state.process_pending_reconnect(client_impl, reconnect_wait).await
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

    info!("Async client loop exiting");
}

pub(crate) fn spawn_client_impl<T>(
    mut client_impl: MqttClientImpl,
    mut runtime_state: ClientRuntimeState<T>,
) where T : Read + Write + Send + Sync + 'static {
    task::spawn(async move {
        client_event_loop(&mut client_impl, &mut runtime_state).await;
    });
}

pub(crate) fn spawn_event_callback(event: Arc<ClientEvent>, callback: Arc<ClientEventListenerCallback>) {
    task::spawn(async move {
        (callback)(event)
    });
}

/// Creates a new async MQTT5 client that will use the async-std async runtime
pub fn new_with_async_std<T>(client_config: MqttClientOptions, connect_config: ConnectOptions, async_std_config: AsyncStdClientOptions<T>) -> AsyncGneissClient where T: Read + Write + Send + Sync + 'static {
    let (operation_sender, internal_state) = create_runtime_states(async_std_config);

    let callback_spawner : CallbackSpawnerFunction = Box::new(|event, callback| {
        spawn_event_callback(event, callback)
    });

    let client_impl = MqttClientImpl::new(client_config, connect_config, callback_spawner);

    spawn_client_impl(client_impl, internal_state);

    Arc::new(AsyncStdClient {
        operation_sender,
        listener_id_allocator: Mutex::new(1),
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn make_direct_client_async_std(endpoint: String, port: u16, tls_impl: TlsConfiguration, _tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>) -> MqttResult<AsyncGneissClient> {
    match tls_impl {
        TlsConfiguration::None => { make_direct_client_no_tls(endpoint, port, client_options, connect_options, http_proxy_options) }
        #[cfg(feature = "async-std-rustls")]
        TlsConfiguration::Rustls => { make_direct_client_rustls(endpoint, port, _tls_options, client_options, connect_options, http_proxy_options) }
        #[cfg(feature = "async-std-native-tls")]
        TlsConfiguration::Nativetls => { make_direct_client_native_tls(endpoint, port, _tls_options, client_options, connect_options, http_proxy_options) }
        _ => { panic!("Illegal state"); }
    }
}

fn make_direct_client_no_tls(endpoint: String, port: u16, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>,) -> MqttResult<AsyncGneissClient> {
    info!("make_direct_client_no_tls - creating async connection establishment closure");
    let (stream_endpoint, http_connect_endpoint) = compute_endpoints(endpoint, port, &http_proxy_options);

    if http_connect_endpoint.is_some() {
        let async_std_options = AsyncStdClientOptions {
            connection_factory: Box::new(move || {
                let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                let tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                Box::pin(apply_proxy_connect_to_stream(tcp_stream, http_connect_endpoint.clone()))
            }),
        };

        info!("make_direct_client_no_tls - plaintext-to-proxy -> plaintext-to-broker");
        Ok(new_with_async_std(client_options, connect_options, async_std_options))
    } else {
        let async_std_options = AsyncStdClientOptions {
            connection_factory: Box::new(move || {
                Box::pin(make_leaf_stream(stream_endpoint.clone()))
            }),
        };

        info!("make_direct_client_no_tls - plaintext-to-broker");
        Ok(new_with_async_std(client_options, connect_options, async_std_options))
    }
}

#[cfg(feature = "async-std-rustls")]
fn make_direct_client_rustls(endpoint: String, port: u16, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>) -> MqttResult<AsyncGneissClient> {
    info!("make_direct_client_rustls - creating async connection establishment closure");

    Err(MqttError::new_unimplemented("Async-std rustls support NYI"))
}

#[cfg(feature = "async-std-native-tls")]
fn make_direct_client_native_tls(endpoint: String, port: u16, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>) -> MqttResult<AsyncGneissClient> {
    info!("make_direct_client_native_tls - creating async connection establishment closure");

    Err(MqttError::new_unimplemented("Async-std native-tls support NYI"))
}

#[allow(clippy::too_many_arguments)]
#[cfg(feature="async-std-websockets")]
pub(crate) fn make_websocket_client_tokio(tls_impl: crate::config::TlsConfiguration, endpoint: String, port: u16, websocket_options: WebsocketOptions, _tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>) -> MqttResult<AsyncGneissClient> {
    match tls_impl {
        crate::config::TlsConfiguration::None => { make_websocket_client_no_tls(endpoint, port, websocket_options, client_options, connect_options, http_proxy_options, runtime) }
        #[cfg(feature = "tokio-rustls")]
        crate::config::TlsConfiguration::Rustls => { make_websocket_client_rustls(endpoint, port, websocket_options, _tls_options, client_options, connect_options, http_proxy_options, runtime) }
        #[cfg(feature = "tokio-native-tls")]
        crate::config::TlsConfiguration::Nativetls => { make_websocket_client_native_tls(endpoint, port, websocket_options, _tls_options, client_options, connect_options, http_proxy_options, runtime) }
        _ => { panic!("Illegal state"); }
    }
}

async fn make_leaf_stream(endpoint: Endpoint) -> MqttResult<TcpStream> {
    let addr = make_addr(endpoint.endpoint.as_str(), endpoint.port)?;
    debug!("make_leaf_stream - opening TCP stream");
    let stream = TcpStream::connect(&addr).await?;
    debug!("make_leaf_stream - TCP stream successfully established");

    Ok(stream)
}

fn build_connect_request(http_connect_endpoint: &Endpoint) -> Vec<u8> {
    let request_as_string = format!("CONNECT {}:{} HTTP/1.1\r\nHost: {}:{}\r\nConnection: keep-alive\r\n\r\n", http_connect_endpoint.endpoint, http_connect_endpoint.port, http_connect_endpoint.endpoint, http_connect_endpoint.port);

    return request_as_string.as_bytes().to_vec();
}

async fn apply_proxy_connect_to_stream<S>(stream : Pin<Box<impl Future<Output=MqttResult<S>>+Sized>>, http_connect_endpoint: Endpoint) -> MqttResult<S> where S : Read + Write + Unpin {
    let mut inner_stream = stream.await?;

    debug!("apply_proxy_connect_to_stream - writing CONNECT request to connection stream");
    let request_bytes = build_connect_request(&http_connect_endpoint);
    inner_stream.write_all(request_bytes.as_slice()).await?;
    debug!("apply_proxy_connect_to_stream - successfully wrote CONNECT request to stream");

    let mut inbound_data: [u8; 4096] = [0; 4096];
    let mut response_bytes = Vec::new();

    loop {
        let bytes_read = inner_stream.read(&mut inbound_data).await?;
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
                        return Ok(inner_stream);
                    }
                }

                error!("apply_proxy_connect_to_stream - CONNECT request was failed, with http code: {:?}", response.code);
                return Err(MqttError::new_connection_establishment_failure("proxy connect request unsuccessful"));
            }
            Ok(httparse::Status::Partial) => {}
        }
    }
}