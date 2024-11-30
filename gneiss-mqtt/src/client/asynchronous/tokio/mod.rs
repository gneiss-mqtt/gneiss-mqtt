/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Functionality for using [`tokio`](https://crates.io/crates/tokio) as an MQTT client's async
runtime implementation.
 */

#[cfg(all(feature = "testing", test))]
mod longtests;
pub(crate) mod builder;

use crate::client::*;
use crate::client::config::*;
use crate::client::waiter::*;
use crate::error::{GneissError, GneissResult};
use crate::mqtt::*;
use crate::mqtt::disconnect::validate_disconnect_packet_outbound;
use crate::protocol::is_connection_established;
use crate::validate::validate_packet_outbound;
use super::{AsyncClientHandle, AsyncClient, AsyncPublishResult, AsyncSubscribeResult, AsyncUnsubscribeResult};

use log::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Instant, Duration};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, split, WriteHalf};
use tokio::net::TcpStream;
use tokio::{runtime, runtime::Handle};
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{sleep};

#[cfg(feature="tokio-websockets")]
use stream_ws::{tungstenite::WsMessageHandler, WsMessageHandle, WsByteStream};
#[cfg(feature="tokio-websockets")]
use tokio_tungstenite::{client_async, WebSocketStream};
#[cfg(feature="tokio-websockets")]
use tungstenite::Message;

pub(crate) struct ClientRuntimeState<T> where T : AsyncRead + AsyncWrite + Send + Sync + 'static {
    tokio_config: TokioClientOptionsInternal<T>,
    operation_receiver: tokio::sync::mpsc::UnboundedReceiver<OperationOptions>,
    stream: Option<T>
}

impl<T> ClientRuntimeState<T> where T : AsyncRead + AsyncWrite + Send + Sync + 'static {
    pub(crate) async fn process_stopped(&mut self, client: &mut MqttClientImpl) -> GneissResult<ClientImplState> {
        loop {
            trace!("tokio - process_stopped loop");

            tokio::select! {
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        debug!("tokio - process_stopped - user operation received");
                        client.handle_incoming_operation(operation_options);
                    }
                }
            }

            if let Some(transition_state) = client.compute_optional_state_transition() {
                return Ok(transition_state);
            }
        }
    }

    pub(crate) async fn process_connecting(&mut self, client: &mut MqttClientImpl) -> GneissResult<ClientImplState> {
        let mut connect = (self.tokio_config.connection_factory)();

        let timeout = sleep(*client.connect_timeout());
        tokio::pin!(timeout);

        loop {
            trace!("tokio - process_connecting loop");

            tokio::select! {
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        debug!("tokio - process_connecting - user operation received");
                        client.handle_incoming_operation(operation_options);
                    }
                }
                () = &mut timeout => {
                    info!("tokio - process_connecting - connection establishment timeout exceeded");
                    client.apply_error(GneissError::new_connection_establishment_failure("connection establishment timeout reached"));
                    return Ok(ClientImplState::PendingReconnect);
                }
                connection_result = &mut connect => {
                    match connection_result {
                        Ok(stream) => {
                            info!("tokio - process_connecting - transport connection established successfully");
                            self.stream = Some(stream);
                            return Ok(ClientImplState::Connected);
                        }
                        Err(error) => {
                            info!("tokio - process_connecting - transport connection establishment failed");
                            client.apply_error(GneissError::new_connection_establishment_failure(error));
                            return Ok(ClientImplState::PendingReconnect);
                        }
                    }
                }
            }

            if let Some(transition_state) = client.compute_optional_state_transition() {
                return Ok(transition_state);
            }
        }
    }

    pub(crate) async fn process_connected(&mut self, client: &mut MqttClientImpl) -> GneissResult<ClientImplState> {
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
                                client.apply_error(GneissError::new_connection_closed("network stream closed"));
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
                                client.apply_error(GneissError::new_connection_closed(error));
                            } else {
                                client.apply_error(GneissError::new_connection_establishment_failure(error));
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
                                client.apply_error(GneissError::new_connection_closed(error));
                            } else {
                                client.apply_error(GneissError::new_connection_establishment_failure(error));
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
                            client.apply_error(GneissError::new_connection_closed(error));
                        } else {
                            client.apply_error(GneissError::new_connection_establishment_failure(error));
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

    pub(crate) async fn process_pending_reconnect(&mut self, client: &mut MqttClientImpl, wait: Duration) -> GneissResult<ClientImplState> {
        let reconnect_timer = sleep(wait);
        tokio::pin!(reconnect_timer);

        loop {
            trace!("tokio - process_pending_reconnect loop");

            tokio::select! {
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        debug!("tokio - process_pending_reconnect - user operation received");
                        client.handle_incoming_operation(operation_options);
                    }
                }
                () = &mut reconnect_timer => {
                    info!("tokio - process_pending_reconnect - reconnect timer exceeded");
                    return Ok(ClientImplState::Connecting);
                }
            }

            if let Some(transition_state) = client.compute_optional_state_transition() {
                return Ok(transition_state);
            }
        }
    }
}

async fn conditional_wait(wait_option: Option<tokio::time::Sleep>) -> Option<()> {
    match wait_option {
        Some(timer) => {
            timer.await;
            Some(())
        },
        None => None,
    }
}

async fn conditional_write<T>(data: Option<&[u8]>, writer: &mut WriteHalf<T>) -> Option<std::io::Result<usize>> where T : AsyncRead + AsyncWrite {
    match data {
        Some(bytes) => {
            Some(writer.write(bytes).await)
        }
        _ => { None }
    }
}

async fn client_event_loop<T>(client_impl: &mut MqttClientImpl, async_state: &mut ClientRuntimeState<T>) where T : AsyncRead + AsyncWrite + Send + Sync + 'static {
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

    info!("Tokio client loop exiting");
}

pub(crate) fn spawn_client_impl<T>(
    mut client_impl: MqttClientImpl,
    mut runtime_state: ClientRuntimeState<T>,
    runtime_handle: runtime::Handle,
) where T : AsyncRead + AsyncWrite + Send + Sync + 'static {
    runtime_handle.spawn(async move {
        client_event_loop(&mut client_impl, &mut runtime_state).await;
    });
}

pub(crate) fn spawn_event_callback(event: Arc<ClientEvent>, callback: Arc<ClientEventListenerCallback>) {
    tokio::spawn(async move {
        (callback)(event)
    });
}

type TokioConnectionFactoryReturnType<T> = Pin<Box<dyn Future<Output = GneissResult<T>> + Send>>;

/// Tokio-specific client configuration
pub struct TokioClientOptionsInternal<T> where T : AsyncRead + AsyncWrite + Send + Sync {

    /// Factory function for creating the final connection object based on all the various
    /// configuration options and features.  It might be a TcpStream, it might be a TlsStream,
    /// it might be a WebsocketStream, it might be some nested combination.
    ///
    /// Ultimately, the type must implement AsyncRead and AsyncWrite.
    pub connection_factory: Box<dyn Fn() -> TokioConnectionFactoryReturnType<T> + Send + Sync>,

    /// Handle of the tokio runtime that the client will run all of its logic on.
    pub runtime_handle: Handle,
}

struct TokioClient {
    pub(crate) operation_sender: UnboundedSender<OperationOptions>,

    pub(crate) listener_id_allocator: Mutex<u64>
}

macro_rules! submit_tokio_operation {
    ($self:ident, $operation_type:ident, $options_internal_type: ident, $options_value: expr, $packet_value: expr) => ({

        let (response_sender, rx) = tokio::sync::oneshot::channel();
        let response_handler = Box::new(move |res| {
            if response_sender.send(res).is_err() {
                return Err(GneissError::new_operation_channel_failure("Failed to send operation result on result channel"));
            }

            Ok(())
        });
        let internal_options = $options_internal_type {
            options : $options_value.unwrap_or_default(),
            response_handler : Some(response_handler)
        };
        let send_result = $self.operation_sender.send(OperationOptions::$operation_type($packet_value, internal_options));
        Box::pin(async move {
            match send_result {
                Err(error) => {
                    Err(GneissError::new_operation_channel_failure(error))
                }
                _ => {
                    rx.await?
                }
            }
        })
    })
}

impl AsyncClient for TokioClient {
    /// Signals the client that it should attempt to recurrently maintain a connection to
    /// the broker endpoint it has been configured with.
    fn start(&self, default_listener: Option<Arc<ClientEventListenerCallback>>) -> GneissResult<()> {
        info!("tokio client start invoked");
        if let Err(send_error) = self.operation_sender.send(OperationOptions::Start(default_listener)) {
            return Err(GneissError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }

    /// Signals the client that it should close any current connection it has and enter the
    /// Stopped state, where it does nothing.
    fn stop(&self, options: Option<StopOptions>) -> GneissResult<()> {
        info!("tokio client stop invoked {} a disconnect packet", if options.as_ref().is_some_and(|opts| { opts.disconnect.is_some()}) { "with" } else { "without" });
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
            return Err(GneissError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }

    /// Signals the client that it should clean up all internal resources (connection, channels,
    /// runtime tasks, etc...) and enter a terminal state that cannot be escaped.  Useful to ensure
    /// a full resource wipe.  If just `stop()` is used then the client will continue to track
    /// MQTT session state internally.
    fn close(&self) -> GneissResult<()> {
        info!("tokio client close invoked; no further operations allowed");
        if let Err(send_error) = self.operation_sender.send(OperationOptions::Shutdown()) {
            return Err(GneissError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }

    /// Submits a Publish operation to the client's operation queue.  The publish will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.
    fn publish(&self, packet: PublishPacket, options: Option<PublishOptions>) -> AsyncPublishResult {
        debug!("tokio client - publish operation submitted");
        let boxed_packet = Box::new(MqttPacket::Publish(packet));
        if let Err(error) = validate_packet_outbound(&boxed_packet) {
            return Box::pin(async move { Err(error) });
        }

        submit_tokio_operation!(self, Publish, PublishOptionsInternal, options, boxed_packet)
    }

    /// Submits a Subscribe operation to the client's operation queue.  The subscribe will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.
    fn subscribe(&self, packet: SubscribePacket, options: Option<SubscribeOptions>) -> AsyncSubscribeResult {
        debug!("tokio client - subscribe operation submitted");
        let boxed_packet = Box::new(MqttPacket::Subscribe(packet));
        if let Err(error) = validate_packet_outbound(&boxed_packet) {
            return Box::pin(async move { Err(error) });
        }

        submit_tokio_operation!(self, Subscribe, SubscribeOptionsInternal, options, boxed_packet)
    }

    /// Submits an Unsubscribe operation to the client's operation queue.  The unsubscribe will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.
    fn unsubscribe(&self, packet: UnsubscribePacket, options: Option<UnsubscribeOptions>) -> AsyncUnsubscribeResult {
        debug!("tokio client - unsubscribe operation submitted");
        let boxed_packet = Box::new(MqttPacket::Unsubscribe(packet));
        if let Err(error) = validate_packet_outbound(&boxed_packet) {
            return Box::pin(async move { Err(error) });
        }

        submit_tokio_operation!(self, Unsubscribe, UnsubscribeOptionsInternal, options, boxed_packet)
    }

    /// Adds an additional listener to the events emitted by this client.  This is useful when
    /// multiple higher-level constructs are sharing the same MQTT client.
    fn add_event_listener(&self, listener: ClientEventListener) -> GneissResult<ListenerHandle> {
        debug!("tokio client - add listener operation submitted");
        let mut current_id = self.listener_id_allocator.lock().unwrap();
        let listener_id = *current_id;
        *current_id += 1;

        if let Err(send_error) = self.operation_sender.send(OperationOptions::AddListener(listener_id, listener)) {
            return Err(GneissError::new_operation_channel_failure(send_error));
        }

        Ok(ListenerHandle {
            id: listener_id
        })
    }

    /// Removes a listener from this client's set of event listeners.
    fn remove_event_listener(&self, listener: ListenerHandle) -> GneissResult<()> {
        debug!("tokio client - remove listener operation submitted");
        if let Err(send_error) = self.operation_sender.send(OperationOptions::RemoveListener(listener.id)) {
            return Err(GneissError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }
}

pub(crate) fn create_runtime_states<T>(tokio_config: TokioClientOptionsInternal<T>) -> (UnboundedSender<OperationOptions>, ClientRuntimeState<T>) where T : AsyncRead + AsyncWrite + Send + Sync + 'static {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

    let impl_state = ClientRuntimeState {
        tokio_config,
        operation_receiver: receiver,
        stream: None
    };

    (sender, impl_state)
}


/// Creates a new async MQTT5 client that will use the tokio async runtime
pub fn new_tokio_client<T>(client_config: MqttClientOptions, connect_config: ConnectOptions, tokio_config: TokioClientOptionsInternal<T>) -> AsyncClientHandle
where T: AsyncRead + AsyncWrite + Send + Sync + 'static {
    let handle = tokio_config.runtime_handle.clone();
    let (operation_sender, internal_state) = create_runtime_states(tokio_config);

    let callback_spawner : CallbackSpawnerFunction = Box::new(|event, callback| {
        spawn_event_callback(event, callback)
    });

    let client_impl = MqttClientImpl::new(client_config, connect_config, callback_spawner);

    spawn_client_impl(client_impl, internal_state, handle);

    AsyncClientHandle::new(
        Arc::new(TokioClient{
            operation_sender,
            listener_id_allocator: Mutex::new(1),
        })
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn make_client_tokio(tls_impl: TlsConfiguration, endpoint: String, port: u16, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, _ws_options: Option<AsyncWebsocketOptions>, tokio_options: TokioOptions) -> GneissResult<AsyncClientHandle> {
    #[cfg(feature="tokio-websockets")]
    if let Some(ws_options) = _ws_options {
        return make_websocket_client_tokio(tls_impl, endpoint, port, tls_options, client_options, connect_options, http_proxy_options, ws_options, tokio_options)
    }

    make_direct_client_tokio(tls_impl, endpoint, port, tls_options, client_options, connect_options, http_proxy_options, tokio_options)

}

#[allow(unused_variables)]
#[allow(clippy::too_many_arguments)]
fn make_direct_client_tokio(tls_impl: TlsConfiguration, endpoint: String, port: u16, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, tokio_options: TokioOptions) -> GneissResult<AsyncClientHandle> {
    match tls_impl {
        TlsConfiguration::None => { make_direct_client_no_tls(endpoint, port, client_options, connect_options, http_proxy_options, tokio_options) }
        #[cfg(feature = "tokio-rustls")]
        TlsConfiguration::Rustls => { make_direct_client_rustls(endpoint, port, tls_options, client_options, connect_options, http_proxy_options, tokio_options) }
        #[cfg(feature = "tokio-native-tls")]
        TlsConfiguration::Nativetls => { make_direct_client_native_tls(endpoint, port, tls_options, client_options, connect_options, http_proxy_options, tokio_options) }
        _ => { panic!("Illegal state"); }
    }
}

fn make_direct_client_no_tls(endpoint: String, port: u16, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, tokio_options: TokioOptions) -> GneissResult<AsyncClientHandle> {
    info!("make_direct_client_no_tls - creating async connection establishment closure");
    let (stream_endpoint, http_connect_endpoint) = compute_endpoints(endpoint, port, &http_proxy_options);

    if http_connect_endpoint.is_some() {
        let tokio_options_internal = TokioClientOptionsInternal {
            connection_factory: Box::new(move || {
                let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                let tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                Box::pin(apply_proxy_connect_to_stream(tcp_stream, http_connect_endpoint.clone()))
            }),
            runtime_handle: tokio_options.runtime
        };

        info!("make_direct_client_no_tls - plaintext-to-proxy -> plaintext-to-broker");
        Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
    } else {
        let tokio_options_internal = TokioClientOptionsInternal {
            connection_factory: Box::new(move || {
                Box::pin(make_leaf_stream(stream_endpoint.clone()))
            }),
            runtime_handle: tokio_options.runtime
        };

        info!("make_direct_client_no_tls - plaintext-to-broker");
        Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
    }
}

#[cfg(feature = "tokio-rustls")]
#[allow(clippy::too_many_arguments)]
fn make_direct_client_rustls(endpoint: String, port: u16, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, tokio_options: TokioOptions) -> GneissResult<AsyncClientHandle> {
    info!("make_direct_client_rustls - creating async connection establishment closure");
    let handle = tokio_options.runtime.clone();
    let (stream_endpoint, http_connect_endpoint) = compute_endpoints(endpoint.clone(), port, &http_proxy_options);

    if let Some(tls_options) = tls_options {
        if let Some(http_proxy_options) = http_proxy_options {
            if let Some(proxy_tls_options) = http_proxy_options.tls_options {
                let tokio_options_internal = TokioClientOptionsInternal {
                    connection_factory: Box::new(move || {
                        let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                        let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                        let proxy_tls_stream = Box::pin(wrap_stream_with_tls_rustls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone()));
                        let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone()));
                        Box::pin(wrap_stream_with_tls_rustls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone()))
                    }),
                    runtime_handle: handle
                };

                info!("make_direct_client_rustls - tls-to-proxy -> tls-to-broker");
                Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
            } else {
                let tokio_options_internal = TokioClientOptionsInternal {
                    connection_factory: Box::new(move || {
                        let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                        let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                        let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tcp_stream, http_connect_endpoint.clone()));
                        Box::pin(wrap_stream_with_tls_rustls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone()))
                    }),
                    runtime_handle: handle,
                };

                info!("make_direct_client_rustls - plaintext-to-proxy -> tls-to-broker");
                Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
            }
        } else {
            let tokio_options_internal = TokioClientOptionsInternal {
                connection_factory: Box::new(move || {
                    let tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                    Box::pin(wrap_stream_with_tls_rustls(tcp_stream, endpoint.clone(), tls_options.clone()))
                }),
                runtime_handle: handle,
            };

            info!("make_direct_client_rustls - tls-to-broker");
            Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
        }
    } else if let Some(http_proxy_options) = http_proxy_options {
        if let Some(proxy_tls_options) = http_proxy_options.tls_options {
            let tokio_options_internal = TokioClientOptionsInternal {
                connection_factory: Box::new(move || {
                    let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                    let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                    let proxy_tls_stream = Box::pin(wrap_stream_with_tls_rustls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone()));
                    Box::pin(apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone()))
                }),
                runtime_handle: handle,
            };

            info!("make_direct_client_rustls - tls-to-proxy -> plaintext-to-broker");
            Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
        } else {
            panic!("Tls direct client creation invoked without tls configuration")
        }
    } else {
        panic!("Tls direct client creation invoked without tls configuration")
    }
}

#[cfg(feature = "tokio-native-tls")]
#[allow(clippy::too_many_arguments)]
fn make_direct_client_native_tls(endpoint: String, port: u16, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, tokio_options: TokioOptions) -> GneissResult<AsyncClientHandle> {
    info!("make_direct_client_native_tls - creating async connection establishment closure");

    let handle = tokio_options.runtime.clone();
    let (stream_endpoint, http_connect_endpoint) = compute_endpoints(endpoint.clone(), port, &http_proxy_options);

    if let Some(tls_options) = tls_options {
        if let Some(http_proxy_options) = http_proxy_options {
            if let Some(proxy_tls_options) = http_proxy_options.tls_options {
                let tokio_options_internal = TokioClientOptionsInternal {
                    connection_factory: Box::new(move || {
                        let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                        let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                        let proxy_tls_stream = Box::pin(wrap_stream_with_tls_native_tls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone()));
                        let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone()));
                        Box::pin(wrap_stream_with_tls_native_tls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone()))
                    }),
                    runtime_handle: handle,
                };

                info!("make_direct_client_native_tls - tls-to-proxy -> tls-to-broker");
                Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
            } else {
                let tokio_options_internal = TokioClientOptionsInternal {
                    connection_factory: Box::new(move || {
                        let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                        let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                        let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tcp_stream, http_connect_endpoint.clone()));
                        Box::pin(wrap_stream_with_tls_native_tls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone()))
                    }),
                    runtime_handle: handle,
                };

                info!("make_direct_client_native_tls - plaintext-to-proxy -> tls-to-broker");
                Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
            }
        } else {
            let tokio_options_internal = TokioClientOptionsInternal {
                connection_factory: Box::new(move || {
                    let tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                    Box::pin(wrap_stream_with_tls_native_tls(tcp_stream, endpoint.clone(), tls_options.clone()))
                }),
                runtime_handle: handle,
            };

            info!("make_direct_client_native_tls - tls-to-broker");
            Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
        }
    } else if let Some(http_proxy_options) = http_proxy_options {
        if let Some(proxy_tls_options) = http_proxy_options.tls_options {
            let tokio_options_internal = TokioClientOptionsInternal {
                connection_factory: Box::new(move || {
                    let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                    let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                    let proxy_tls_stream = Box::pin(wrap_stream_with_tls_native_tls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone()));
                    Box::pin(apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone()))
                }),
                runtime_handle: handle,
            };

            info!("make_direct_client_native_tls - tls-to-proxy -> plaintext-to-broker");
            Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
        } else {
            panic!("Tls direct client creation invoked without tls configuration")
        }
    } else {
        panic!("Tls direct client creation invoked without tls configuration")
    }
}

#[allow(unused_variables)]
#[allow(clippy::too_many_arguments)]
#[cfg(feature="tokio-websockets")]
fn make_websocket_client_tokio(tls_impl: TlsConfiguration, endpoint: String, port: u16, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, ws_options: AsyncWebsocketOptions, tokio_options: TokioOptions) -> GneissResult<AsyncClientHandle> {
    match tls_impl {
        TlsConfiguration::None => { make_websocket_client_no_tls(endpoint, port, client_options, connect_options, http_proxy_options, ws_options, tokio_options) }
        #[cfg(feature = "tokio-rustls")]
        TlsConfiguration::Rustls => { make_websocket_client_rustls(endpoint, port, tls_options, client_options, connect_options, http_proxy_options, ws_options, tokio_options) }
        #[cfg(feature = "tokio-native-tls")]
        TlsConfiguration::Nativetls => { make_websocket_client_native_tls(endpoint, port, tls_options, client_options, connect_options, http_proxy_options, ws_options, tokio_options) }
        _ => { panic!("Illegal state"); }
    }
}

#[cfg(feature="tokio-websockets")]
fn make_websocket_client_no_tls(endpoint: String, port: u16, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, ws_options: AsyncWebsocketOptions, tokio_options: TokioOptions) -> GneissResult<AsyncClientHandle> {
    info!("make_websocket_client_no_tls - creating async connection establishment closure");
    let (stream_endpoint, http_connect_endpoint) = compute_endpoints(endpoint, port, &http_proxy_options);
    let websocket_options = ws_options.clone();
    let handle = tokio_options.runtime.clone();

    if http_connect_endpoint.is_some() {
        let tokio_options_internal = TokioClientOptionsInternal {
            connection_factory: Box::new(move || {
                let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tcp_stream, http_connect_endpoint.clone()));
                Box::pin(wrap_stream_with_websockets(connect_stream, http_connect_endpoint.endpoint.clone(), "ws", websocket_options.clone()))
            }),
            runtime_handle: handle,
        };

        info!("create_websocket_client_plaintext_to_proxy_plaintext_to_broker");
        Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
    } else {
        let tokio_options_internal = TokioClientOptionsInternal {
            connection_factory: Box::new(move || {
                let tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                Box::pin(wrap_stream_with_websockets(tcp_stream, stream_endpoint.endpoint.clone(), "ws", websocket_options.clone()))
            }),
            runtime_handle: handle,
        };

        info!("create_websocket_client_plaintext_to_broker");
        Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg(all(feature = "tokio-rustls", feature = "tokio-websockets"))]
fn make_websocket_client_rustls(endpoint: String, port: u16, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, ws_options: AsyncWebsocketOptions, tokio_options: TokioOptions) -> GneissResult<AsyncClientHandle> {
    info!("make_websocket_client_rustls - creating async connection establishment closure");
    let (stream_endpoint, http_connect_endpoint) = compute_endpoints(endpoint.clone(), port, &http_proxy_options);
    let websocket_options = ws_options.clone();
    let handle = tokio_options.runtime.clone();

    if let Some(tls_options) = tls_options {
        if let Some(http_proxy_options) = http_proxy_options {
            if let Some(proxy_tls_options) = http_proxy_options.tls_options {
                let tokio_options_internal = TokioClientOptionsInternal {
                    connection_factory: Box::new(move || {
                        let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                        let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                        let proxy_tls_stream = Box::pin(wrap_stream_with_tls_rustls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone()));
                        let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone()));
                        let tls_stream = Box::pin(wrap_stream_with_tls_rustls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone()));
                        Box::pin(wrap_stream_with_websockets(tls_stream, http_connect_endpoint.endpoint.clone(), "wss", websocket_options.clone()))
                    }),
                    runtime_handle: handle,
                };

                info!("make_websocket_client_rustls - tls-to-proxy -> tls-to-broker");
                Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
            } else {
                let tokio_options_internal = TokioClientOptionsInternal {
                    connection_factory: Box::new(move || {
                        let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                        let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                        let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tcp_stream, http_connect_endpoint.clone()));
                        let tls_stream = Box::pin(wrap_stream_with_tls_rustls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone()));
                        Box::pin(wrap_stream_with_websockets(tls_stream, http_connect_endpoint.endpoint.clone(), "wss", websocket_options.clone()))
                    }),
                    runtime_handle: handle,
                };

                info!("make_websocket_client_rustls - plaintext-to-proxy -> tls-to-broker");
                Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
            }
        } else {
            let tokio_options_internal = TokioClientOptionsInternal {
                connection_factory: Box::new(move || {
                    let tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                    let tls_stream = Box::pin(wrap_stream_with_tls_rustls(tcp_stream, stream_endpoint.endpoint.clone(), tls_options.clone()));
                    Box::pin(wrap_stream_with_websockets(tls_stream, stream_endpoint.endpoint.clone(), "wss", websocket_options.clone()))
                }),
                runtime_handle: handle,
            };

            info!("make_websocket_client_rustls - tls-to-broker");
            Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
        }
    } else if let Some(http_proxy_options) = http_proxy_options {
        if let Some(proxy_tls_options) = http_proxy_options.tls_options {
            let tokio_options_internal = TokioClientOptionsInternal {
                connection_factory: Box::new(move || {
                    let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                    let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                    let proxy_tls_stream = Box::pin(wrap_stream_with_tls_rustls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone()));
                    let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone()));
                    Box::pin(wrap_stream_with_websockets(connect_stream, http_connect_endpoint.endpoint.clone(), "ws", websocket_options.clone()))
                }),
                runtime_handle: handle,
            };

            info!("make_websocket_client_rustls - tls-to-proxy -> plaintext-to-broker");
            Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
        } else {
            panic!("Tls websocket client creation invoked without tls configuration")
        }
    } else {
        panic!("Tls websocket client creation invoked without tls configuration")
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg(all(feature = "tokio-native-tls", feature = "tokio-websockets"))]
fn make_websocket_client_native_tls(endpoint: String, port: u16, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, ws_options: AsyncWebsocketOptions, tokio_options: TokioOptions) -> GneissResult<AsyncClientHandle> {
    info!("make_websocket_client_native_tls - creating async connection establishment closure");
    let (stream_endpoint, http_connect_endpoint) = compute_endpoints(endpoint.clone(), port, &http_proxy_options);
    let websocket_options = ws_options.clone();
    let handle = tokio_options.runtime.clone();

    if let Some(tls_options) = tls_options {
        if let Some(http_proxy_options) = http_proxy_options {
            if let Some(proxy_tls_options) = http_proxy_options.tls_options {
                let tokio_options_internal = TokioClientOptionsInternal {
                    connection_factory: Box::new(move || {
                        let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                        let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                        let proxy_tls_stream = Box::pin(wrap_stream_with_tls_native_tls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone()));
                        let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone()));
                        let tls_stream = Box::pin(wrap_stream_with_tls_native_tls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone()));
                        Box::pin(wrap_stream_with_websockets(tls_stream, http_connect_endpoint.endpoint.clone(), "wss", websocket_options.clone()))
                    }),
                    runtime_handle: handle,
                };

                info!("make_websocket_client_native_tls - tls-to-proxy -> tls-to-broker");
                Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
            } else {
                let tokio_options_internal = TokioClientOptionsInternal {
                    connection_factory: Box::new(move || {
                        let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                        let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                        let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tcp_stream, http_connect_endpoint.clone()));
                        let tls_stream = Box::pin(wrap_stream_with_tls_native_tls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone()));
                        Box::pin(wrap_stream_with_websockets(tls_stream, http_connect_endpoint.endpoint.clone(), "wss", websocket_options.clone()))
                    }),
                    runtime_handle: handle,
                };

                info!("make_websocket_client_native_tls - plaintext-to-proxy -> tls-to-broker");
                Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
            }
        } else {
            let tokio_options_internal = TokioClientOptionsInternal {
                connection_factory: Box::new(move || {
                    let tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                    let tls_stream = Box::pin(wrap_stream_with_tls_native_tls(tcp_stream, stream_endpoint.endpoint.clone(), tls_options.clone()));
                    Box::pin(wrap_stream_with_websockets(tls_stream, stream_endpoint.endpoint.clone(), "wss", websocket_options.clone()))
                }),
                runtime_handle: handle,
            };

            info!("make_websocket_client_native_tls - tls-to-broker");
            Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
        }
    } else if let Some(http_proxy_options) = http_proxy_options {
        if let Some(proxy_tls_options) = http_proxy_options.tls_options {
            let tokio_options_internal = TokioClientOptionsInternal {
                connection_factory: Box::new(move || {
                    let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                    let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                    let proxy_tls_stream = Box::pin(wrap_stream_with_tls_native_tls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone()));
                    let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone()));
                    Box::pin(wrap_stream_with_websockets(connect_stream, http_connect_endpoint.endpoint.clone(), "ws", websocket_options.clone()))
                }),
                runtime_handle: handle,
            };

            info!("make_websocket_client_native_tls - tls-to-proxy -> plaintext-to-broker");
            Ok(new_tokio_client(client_options, connect_options, tokio_options_internal))
        } else {
            panic!("Tls websocket client creation invoked without tls configuration")
        }
    } else {
        panic!("Tls websocket client creation invoked without tls configuration")
    }
}

async fn make_leaf_stream(endpoint: Endpoint) -> GneissResult<TcpStream> {
    let addr = make_addr(endpoint.endpoint.as_str(), endpoint.port)?;
    debug!("make_leaf_stream - opening TCP stream");
    let stream = TcpStream::connect(&addr).await?;
    debug!("make_leaf_stream - TCP stream successfully established");

    Ok(stream)
}

#[cfg(feature = "tokio-rustls")]
async fn wrap_stream_with_tls_rustls<S>(stream : Pin<Box<impl Future<Output=GneissResult<S>>+Sized>>, endpoint: String, tls_options: TlsOptions) -> GneissResult<tokio_rustls::client::TlsStream<S>> where S : AsyncRead + AsyncWrite + Unpin {
    let domain = rustls_pki_types::ServerName::try_from(endpoint)?
        .to_owned();

    let connector =
        match tls_options.options {
            TlsData::Rustls(config) => { tokio_rustls::TlsConnector::from(config.clone()) }
            _ => { panic!("Rustls stream wrapper invoked without Rustls configuration"); }
        };

    debug!("wrap_stream_with_tls_rustls - performing tls handshake");
    let inner_stream= stream.await?;
    let tls_stream = connector.connect(domain, inner_stream).await?;
    debug!("wrap_stream_with_tls_rustls - tls handshake successfully completed");

    Ok(tls_stream)
}

#[cfg(feature = "tokio-native-tls")]
async fn wrap_stream_with_tls_native_tls<S>(stream : Pin<Box<impl Future<Output=GneissResult<S>>+Sized>>, endpoint: String, tls_options: TlsOptions) -> GneissResult<tokio_native_tls::TlsStream<S>> where S : AsyncRead + AsyncWrite + Unpin {

    let connector =
        match tls_options.options {
            TlsData::NativeTls(ntls_builder) => {
                let cx = ntls_builder.build()?;
                tokio_native_tls::TlsConnector::from(cx)
            }
            _ => { panic!("Native-tls stream wrapper invoked without Native-tls configuration"); }
        };

    debug!("wrap_stream_with_tls_native_tls - performing tls handshake");
    let inner_stream = stream.await?;
    let tls_stream = connector.connect(endpoint.as_str(), inner_stream).await?;
    debug!("wrap_stream_with_tls_native_tls - tls handshake successfully completed");

    Ok(tls_stream)
}

#[cfg(feature="tokio-websockets")]
async fn wrap_stream_with_websockets<S>(stream : Pin<Box<impl Future<Output=GneissResult<S>>+Sized>>, endpoint: String, scheme: &str, websocket_options: AsyncWebsocketOptions) -> GneissResult<WsByteStream<WebSocketStream<S>, Message, tungstenite::Error, WsMessageHandler>> where S : AsyncRead + AsyncWrite + Unpin {

    let uri = format!("{}://{}/mqtt", scheme, endpoint); // scheme needs to be present but value irrelevant
    let handshake_builder = create_default_websocket_handshake_request(uri)?;

    debug!("wrap_stream_with_websockets - performing websocket upgrade request transform");
    let transformed_handshake_builder =
        if let Some(transform) = &*websocket_options.handshake_transform {
            transform(handshake_builder).await?
        } else {
            handshake_builder
        };
    debug!("wrap_stream_with_websockets - successfully transformed websocket upgrade request");

    debug!("wrap_stream_with_websockets - upgrading stream to websockets");
    let inner_stream= stream.await?;
    let (message_stream, _) = client_async(HandshakeRequest { handshake_builder: transformed_handshake_builder }, inner_stream).await?;
    let byte_stream = WsMessageHandler::wrap_stream(message_stream);
    debug!("wrap_stream_with_websockets - successfully upgraded stream to websockets");

    Ok(byte_stream)
}

async fn apply_proxy_connect_to_stream<S>(stream : Pin<Box<impl Future<Output=GneissResult<S>>+Sized>>, http_connect_endpoint: Endpoint) -> GneissResult<S> where S : AsyncRead + AsyncWrite + Unpin {
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
            return Err(GneissError::new_connection_establishment_failure("proxy connect stream closed"));
        }

        response_bytes.extend_from_slice(&inbound_data[..bytes_read]);

        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut response = httparse::Response::new(&mut headers);

        let parse_result = response.parse(response_bytes.as_slice());
        match parse_result {
            Err(e) => {
                error!("apply_proxy_connect_to_stream - failed to parse proxy response to CONNECT request: {:?}", e);
                return Err(GneissError::new_connection_establishment_failure(e));
            }
            Ok(httparse::Status::Complete(bytes_parsed)) => {
                if bytes_parsed < response_bytes.len() {
                    error!("apply_proxy_connect_to_stream - stream incoming data contains more data than the CONNECT response");
                    return Err(GneissError::new_connection_establishment_failure("proxy connect response too long"));
                }

                if let Some(response_code) = response.code {
                    if (200..300).contains(&response_code) {
                        return Ok(inner_stream);
                    }
                }

                error!("apply_proxy_connect_to_stream - CONNECT request was failed, with http code: {:?}", response.code);
                return Err(GneissError::new_connection_establishment_failure("proxy connect request unsuccessful"));
            }
            Ok(httparse::Status::Partial) => {}
        }
    }
}

/// Result type for calling wait() on an async client event waiter
pub type ClientEventWaitFuture = dyn Future<Output=GneissResult<Vec<ClientEventRecord>>> + Send;

/// Simple debug type that uses the client listener framework to allow logic to wait for
/// configurable client event sequences.
pub struct TokioClientEventWaiter {
    event_count: usize,

    client: AsyncClientHandle,

    listener: Option<ListenerHandle>,

    event_receiver: ::tokio::sync::mpsc::UnboundedReceiver<ClientEventRecord>,

    events: Vec<ClientEventRecord>,
}

impl TokioClientEventWaiter {

    fn new_internal(client: AsyncClientHandle, config: ClientEventWaiterOptions, event_count: usize) -> Self {
        let (tx, rx) = ::tokio::sync::mpsc::unbounded_channel();

        let mut waiter = TokioClientEventWaiter {
            event_count,
            client: client.clone(),
            listener: None,
            event_receiver: rx,
            events: Vec::new(),
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

            let _ = tx.send(event_record);
        };

        waiter.listener = Some(client.add_event_listener(Arc::new(listener_fn)).unwrap());
        waiter
    }

    /// Creates a new ClientEventWaiter instance from full configuration
    #[cfg(feature = "testing")]
    pub fn new(client: AsyncClientHandle, config: ClientEventWaiterOptions, event_count: usize) -> Self {
        Self::new_internal(client, config, event_count)
    }

    /// Creates a new ClientEventWaiter instance that will wait for a single occurrence of a single event type
    pub fn new_single(client: AsyncClientHandle, event_type: ClientEventType) -> Self {
        let config = ClientEventWaiterOptions {
            wait_type: ClientEventWaitType::Type(event_type),
        };

        Self::new_internal(client, config, 1)
    }

    /// Waits for and returns an event sequence that matches the original configuration
    pub fn wait(mut self) -> Pin<Box<ClientEventWaitFuture>> {
        Box::pin(async move {
            while self.events.len() < self.event_count {
                match self.event_receiver.recv().await {
                    None => {
                        return Err(GneissError::new_other_error("Channel closed"));
                    }
                    Some(event) => {
                        self.events.push(event);
                    }
                }
            }

            Ok(self.events.clone())
        })
    }
}

impl Drop for TokioClientEventWaiter {
    fn drop(&mut self) {
        let listener_handler = self.listener.take().unwrap();

        let _ = self.client.remove_event_listener(listener_handler);
    }
}

#[cfg(all(test, feature = "testing"))]
pub(crate) mod testing {
    use std::time::Duration;
    use crate::error::*;
    use crate::testing::integration::*;
    use super::*;

    fn build_tokio_client(builder: TokioClientBuilder) -> AsyncClientHandle {
        builder.build().unwrap()
    }

    fn do_good_client_test(handle: Handle, tls: TlsUsage, ws: WebsocketUsage, proxy: ProxyUsage, test_factory: TokioTestFactory) {
        let tokio_options = TokioOptions::builder(handle.clone()).build();

        let mut tokio_builder = create_good_tokio_client_builder(tls, ws, proxy);
        tokio_builder.with_tokio_options(tokio_options);

        let test_future = (*test_factory)(tokio_builder);

        handle.block_on(test_future).unwrap();
    }

    async fn tokio_connect_disconnect_test(builder: TokioClientBuilder) -> GneissResult<()> {
        let client = builder.build()?;

        start_async_client(&client).await?;
        stop_async_client(&client).await?;

        Ok(())
    }

    #[test]
    fn client_connect_disconnect_direct_plaintext_no_proxy() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        do_good_client_test(handle.clone(), TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(move |builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(feature = "tokio-rustls")]
    fn client_connect_disconnect_direct_rustls_no_proxy() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        do_good_client_test(handle.clone(), TlsUsage::Rustls, WebsocketUsage::None, ProxyUsage::None, Box::new(move |builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(feature = "tokio-native-tls")]
    fn client_connect_disconnect_direct_native_tls_no_proxy() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        do_good_client_test(handle.clone(), TlsUsage::Nativetls, WebsocketUsage::None, ProxyUsage::None, Box::new(move |builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(feature="tokio-websockets")]
    fn client_connect_disconnect_websocket_plaintext_no_proxy() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        do_good_client_test(handle.clone(), TlsUsage::None, WebsocketUsage::Tungstenite, ProxyUsage::None, Box::new(move |builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(all(feature = "tokio-rustls", feature = "tokio-websockets"))]
    fn client_connect_disconnect_websocket_rustls_no_proxy() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        do_good_client_test(handle.clone(), TlsUsage::Rustls, WebsocketUsage::Tungstenite, ProxyUsage::None, Box::new(move |builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(all(feature = "tokio-native-tls", feature = "tokio-websockets"))]
    fn client_connect_disconnect_websocket_native_tls_no_proxy() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        do_good_client_test(handle.clone(), TlsUsage::Nativetls, WebsocketUsage::Tungstenite, ProxyUsage::None, Box::new(move |builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    fn client_connect_disconnect_direct_plaintext_with_proxy() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        do_good_client_test(handle.clone(), TlsUsage::None, WebsocketUsage::None, ProxyUsage::Plaintext, Box::new(move |builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(feature = "tokio-rustls")]
    fn client_connect_disconnect_direct_rustls_with_proxy() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        do_good_client_test(handle.clone(), TlsUsage::Rustls, WebsocketUsage::None, ProxyUsage::Plaintext, Box::new(move |builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(feature = "tokio-native-tls")]
    fn client_connect_disconnect_direct_native_tls_with_proxy() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        do_good_client_test(handle.clone(), TlsUsage::Nativetls, WebsocketUsage::None, ProxyUsage::Plaintext, Box::new(move |builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(feature="tokio-websockets")]
    fn client_connect_disconnect_websocket_plaintext_with_proxy() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        do_good_client_test(handle.clone(), TlsUsage::None, WebsocketUsage::Tungstenite, ProxyUsage::Plaintext, Box::new(move |builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(all(feature = "tokio-rustls", feature="tokio-websockets"))]
    fn client_connect_disconnect_websocket_rustls_with_proxy() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        do_good_client_test(handle.clone(), TlsUsage::Rustls, WebsocketUsage::Tungstenite, ProxyUsage::Plaintext, Box::new(move |builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(all(feature = "tokio-native-tls", feature = "tokio-websockets"))]
    fn client_connect_disconnect_websocket_native_tls_with_proxy() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        do_good_client_test(handle.clone(), TlsUsage::Nativetls, WebsocketUsage::Tungstenite, ProxyUsage::Plaintext, Box::new(move |builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    async fn tokio_subscribe_unsubscribe_test(builder: TokioClientBuilder) -> GneissResult<()> {
        async_subscribe_unsubscribe_test(builder.build()?).await
    }

    #[test]
    fn client_subscribe_unsubscribe() {
        let runtime = ::tokio::runtime::Runtime::new().unwrap();
        do_good_client_test(runtime.handle().clone(), TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_subscribe_unsubscribe_test(builder))
        }));
    }

    async fn tokio_subscribe_publish_test(builder: TokioClientBuilder, qos: QualityOfService) -> GneissResult<()> {
        let client = builder.build()?;
        async_subscribe_publish_test(client, qos).await
    }

    #[test]
    fn client_subscribe_publish_qos0() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        do_good_client_test(runtime.handle().clone(), TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_subscribe_publish_test(builder, QualityOfService::AtMostOnce))
        }));
    }

    #[test]
    fn client_subscribe_publish_qos1() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        do_good_client_test(runtime.handle().clone(), TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_subscribe_publish_test(builder, QualityOfService::AtLeastOnce))
        }));
    }

    #[test]
    fn client_subscribe_publish_qos2() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        do_good_client_test(runtime.handle().clone(), TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_subscribe_publish_test(builder, QualityOfService::ExactlyOnce))
        }));
    }

    // This primarily tests that the will configuration works.  Will functionality is mostly broker-side.
    async fn tokio_will_test(builder: TokioClientBuilder) -> GneissResult<()> {
        async_will_test(builder, build_tokio_client).await
    }

    #[test]
    fn client_will_sent() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        do_good_client_test(runtime.handle().clone(), TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_will_test(builder))
        }));
    }

    async fn tokio_connect_disconnect_cycle_session_rejoin_test(builder: TokioClientBuilder) -> GneissResult<()> {
        let client = builder.build()?;
        async_connect_disconnect_cycle_session_rejoin_test(client).await
    }

    #[test]
    fn connect_disconnect_cycle_session_rejoin() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        do_good_client_test(runtime.handle().clone(), TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_cycle_session_rejoin_test(builder))
        }));
    }

    pub(crate) fn do_builder_test(handle: Handle, test_factory: TokioTestFactory, mut builder: TokioClientBuilder) {
        let tokio_options = TokioOptions::builder(handle.clone()).build();
        builder.with_tokio_options(tokio_options);

        let test_future = (*test_factory)(builder);

        handle.block_on(test_future).unwrap();
    }

    async fn connection_failure_test(builder : TokioClientBuilder) -> GneissResult<()> {
        let client = builder.build()?;
        let connection_failure_waiter = TokioClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionFailure);

        client.start(None)?;

        let connection_failure_results = connection_failure_waiter.wait().await?;
        assert_eq!(1, connection_failure_results.len());

        Ok(())
    }

    #[cfg(any(feature = "tokio-rustls", feature = "tokio-native-tls", feature = "tokio-websockets"))]
    fn create_mismatch_builder(tls_config: TlsUsage, ws_config: WebsocketUsage, tls_endpoint: TlsUsage, ws_endpoint: WebsocketUsage) -> TokioClientBuilder {
        assert!(tls_config != tls_endpoint || ws_config != ws_endpoint);

        let connect_options = ConnectOptions::builder().build();

        let mut builder = create_tokio_client_builder_internal(connect_options, tls_config, ProxyUsage::None, tls_endpoint, ws_endpoint);
        apply_mismatch_async_client_options(&mut builder, ws_config);
        builder
    }

    #[cfg(any(feature = "tokio-rustls", feature = "tokio-native-tls", feature = "tokio-websockets"))]
    #[cfg_attr(not(feature = "tokio-websockets"), allow(unused_mut, unused_variables))]
    fn apply_mismatch_async_client_options(builder: &mut TokioClientBuilder, _ws_config: WebsocketUsage) {
        #[cfg(feature = "tokio-websockets")]
        {
            let websocket_config_option = create_websocket_options_async(_ws_config);
            if let Some(websocket_options) = websocket_config_option {
                builder.with_websocket_options(websocket_options);
            } else {
                builder.clear_websocket_options();
            }
        }
    }

    #[test]
    #[cfg(feature = "tokio-rustls")]
    fn connection_failure_direct_rustls_tls_config_direct_plaintext_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::None, TlsUsage::None, WebsocketUsage::None);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(feature = "tokio-native-tls")]
    fn connection_failure_direct_native_tls_tls_config_direct_plaintext_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::Nativetls, WebsocketUsage::None, TlsUsage::None, WebsocketUsage::None);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "tokio-rustls", feature = "tokio-websockets"))]
    fn connection_failure_direct_rustls_tls_config_websocket_plaintext_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::None, TlsUsage::None, WebsocketUsage::Tungstenite);

        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "tokio-native-tls", feature = "tokio-websockets"))]
    fn connection_failure_direct_native_tls_tls_config_websocket_plaintext_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::Nativetls, WebsocketUsage::None, TlsUsage::None, WebsocketUsage::Tungstenite);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "tokio-rustls", feature = "tokio-websockets"))]
    fn connection_failure_direct_rustls_tls_config_websocket_tls_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::None, TlsUsage::Rustls, WebsocketUsage::Tungstenite);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "tokio-native-tls", feature = "tokio-websockets"))]
    fn connection_failure_direct_native_tls_tls_config_websocket_tls_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::Nativetls, WebsocketUsage::None, TlsUsage::Nativetls, WebsocketUsage::Tungstenite);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(feature = "tokio-rustls")]
    fn connection_failure_direct_plaintext_config_direct_rustls_tls_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::None, TlsUsage::Rustls, WebsocketUsage::None);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(feature = "tokio-native-tls")]
    fn connection_failure_direct_plaintext_config_direct_native_tls_tls_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::None, TlsUsage::Nativetls, WebsocketUsage::None);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(feature="tokio-websockets")]
    fn connection_failure_direct_plaintext_config_websocket_plaintext_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::None, TlsUsage::None, WebsocketUsage::Tungstenite);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "tokio-rustls", feature = "tokio-websockets"))]
    fn connection_failure_direct_plaintext_config_websocket_rustls_tls_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::None, TlsUsage::Rustls, WebsocketUsage::Tungstenite);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "tokio-native-tls", feature = "tokio-websockets"))]
    fn connection_failure_direct_plaintext_config_websocket_native_tls_tls_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::None, TlsUsage::Nativetls, WebsocketUsage::Tungstenite);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }


    #[test]
    #[cfg(all(feature = "tokio-rustls", feature = "tokio-websockets"))]
    fn connection_failure_websocket_rustls_tls_config_direct_plaintext_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::Tungstenite, TlsUsage::None, WebsocketUsage::None);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "tokio-native-tls", feature = "tokio-websockets"))]
    fn connection_failure_websocket_native_tls_tls_config_direct_plaintext_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::Nativetls, WebsocketUsage::Tungstenite, TlsUsage::None, WebsocketUsage::None);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "tokio-rustls", feature = "tokio-websockets"))]
    fn connection_failure_websocket_rustls_tls_config_websocket_plaintext_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::Tungstenite, TlsUsage::None, WebsocketUsage::Tungstenite);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "tokio-native-tls", feature = "tokio-websockets"))]
    fn connection_failure_websocket_native_tls_tls_config_websocket_plaintext_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::Nativetls, WebsocketUsage::Tungstenite, TlsUsage::None, WebsocketUsage::Tungstenite);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "tokio-rustls", feature = "tokio-websockets"))]
    fn connection_failure_websocket_rustls_tls_config_direct_tls_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::Tungstenite, TlsUsage::Rustls, WebsocketUsage::None);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "tokio-native-tls", feature = "tokio-websockets"))]
    fn connection_failure_websocket_native_tls_tls_config_direct_tls_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::Nativetls, WebsocketUsage::Tungstenite, TlsUsage::Nativetls, WebsocketUsage::None);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(feature="tokio-websockets")]
    fn connection_failure_websocket_plaintext_config_direct_plaintext_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::Tungstenite, TlsUsage::None, WebsocketUsage::None);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "tokio-rustls", feature = "tokio-websockets"))]
    fn connection_failure_websocket_plaintext_config_websocket_rustls_tls_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::Tungstenite, TlsUsage::Rustls, WebsocketUsage::Tungstenite);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "tokio-native-tls", feature = "tokio-websockets"))]
    fn connection_failure_websocket_plaintext_config_websocket_native_tls_tls_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::Tungstenite, TlsUsage::Nativetls, WebsocketUsage::Tungstenite);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "tokio-rustls", feature = "tokio-websockets"))]
    fn connection_failure_websocket_plaintext_config_direct_rustls_tls_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::Tungstenite, TlsUsage::Rustls, WebsocketUsage::None);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "tokio-native-tls", feature = "tokio-websockets"))]
    fn connection_failure_websocket_plaintext_config_direct_native_tls_tls_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::Tungstenite, TlsUsage::Nativetls, WebsocketUsage::None);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    fn connection_failure_invalid_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();

        let client_options = MqttClientOptionsBuilder::new()
            .with_connect_timeout(Duration::from_secs(3))
            .build();

        let mut builder = TokioClientBuilder::new("example.com", 8000);
        builder.with_client_options(client_options);

        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    fn connection_failure_invalid_endpoint_http() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = TokioClientBuilder::new("amazon.com", 443);
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }
}