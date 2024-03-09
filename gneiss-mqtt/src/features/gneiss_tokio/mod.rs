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

use crate::client::*;
use crate::config::*;
use crate::error::{MqttError, MqttResult};
use crate::protocol::is_connection_established;
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

#[cfg(feature="websockets")]
use stream_ws::{tungstenite::WsMessageHandler, WsMessageHandle, WsByteStream};
#[cfg(feature="websockets")]
use tokio_tungstenite::{client_async, WebSocketStream};
#[cfg(feature="websockets")]
use tungstenite::Message;

pub(crate) struct ClientRuntimeState<T> where T : AsyncRead + AsyncWrite + Send + Sync + 'static {
    tokio_config: TokioClientOptions<T>,
    operation_receiver: tokio::sync::mpsc::UnboundedReceiver<OperationOptions>,
    stream: Option<T>
}

impl<T> ClientRuntimeState<T> where T : AsyncRead + AsyncWrite + Send + Sync + 'static {
    pub(crate) async fn process_stopped(&mut self, client: &mut MqttClientImpl) -> MqttResult<ClientImplState> {
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

    pub(crate) async fn process_connecting(&mut self, client: &mut MqttClientImpl) -> MqttResult<ClientImplState> {
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
                    client.apply_error(MqttError::new_connection_establishment_failure("connection establishment timeout reached"));
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
                            client.apply_error(MqttError::new_connection_establishment_failure(error));
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

    pub(crate) async fn process_connected(&mut self, client: &mut MqttClientImpl) -> MqttResult<ClientImplState> {
        let mut outbound_data: Vec<u8> = Vec::with_capacity(4096);
        let mut cumulative_bytes_written : usize = 0;

        let mut inbound_data: [u8; 4096] = [0; 4096];

        let stream = self.stream.take().unwrap();
        let (stream_reader, mut stream_writer) = split(stream);
        tokio::pin!(stream_reader);

        let mut should_flush = false;
        let mut write_directive : Option<WriteDirective>;

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

            if should_flush {
                debug!("tokio - process_connected - flushing previous write");
                write_directive = Some(WriteDirective::Flush);
            } else if let Some(outbound_slice) = outbound_slice_option {
                debug!("tokio - process_connected - {} bytes to write", outbound_slice.len());
                write_directive = Some(WriteDirective::Bytes(outbound_slice))
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
                            if should_flush {
                                should_flush = false;
                                if let Err(error) = client.handle_write_completion() {
                                    info!("tokio - process_connected - stream write completion handler failed: {:?}", error);
                                    client.apply_error(error);
                                    next_state = Some(ClientImplState::PendingReconnect);
                                }
                            } else {
                                cumulative_bytes_written += bytes_written;
                                if cumulative_bytes_written == outbound_data.len() {
                                    outbound_data.clear();
                                    cumulative_bytes_written = 0;
                                    should_flush = true;
                                }
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

            if next_state.is_none() {
                next_state = client.compute_optional_state_transition();
            }
        }

        info!("tokio - process_connected - shutting down stream");
        let _ = stream_writer.shutdown().await;
        info!("tokio - process_connected - stream fully closed");

        Ok(next_state.unwrap())
    }

    pub(crate) async fn process_pending_reconnect(&mut self, client: &mut MqttClientImpl, wait: Duration) -> MqttResult<ClientImplState> {
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

enum WriteDirective<'a> {
    Bytes(&'a[u8]),
    Flush
}

async fn conditional_write<'a, T>(directive: Option<WriteDirective<'a>>, writer: &mut WriteHalf<T>) -> Option<std::io::Result<usize>> where T : AsyncRead + AsyncWrite {
    match directive {
        Some(WriteDirective::Bytes(bytes)) => {
            Some(writer.write(bytes).await)
        }
        Some(WriteDirective::Flush) => {
            if let Err(error) = writer.flush().await {
                Some(Err(error))
            } else {
                Some(Ok(0))
            }
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

    info!("Async client loop exiting");
}

pub(crate) fn spawn_client_impl<T>(
    mut client_impl: MqttClientImpl,
    mut runtime_state: ClientRuntimeState<T>,
    runtime_handle: &runtime::Handle,
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

type TokioConnectionFactoryReturnType<T> = Pin<Box<dyn Future<Output = MqttResult<T>> + Send>>;

/// Tokio-specific client configuration
pub struct TokioClientOptions<T> where T : AsyncRead + AsyncWrite + Send + Sync {

    /// Factory function for creating the final connection object based on all the various
    /// configuration options and features.  It might be a TcpStream, it might be a TlsStream,
    /// it might be a WebsocketStream, it might be some nested combination.
    ///
    /// Ultimately, the type must implement AsyncRead and AsyncWrite.
    pub connection_factory: Box<dyn Fn() -> TokioConnectionFactoryReturnType<T> + Send + Sync>,
}


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// WIP - Experimental

struct TokioClient {
    pub(crate) operation_sender: UnboundedSender<OperationOptions>,

    pub(crate) listener_id_allocator: Mutex<u64>
}

macro_rules! submit_tokio_operation {
    ($self:ident, $operation_type:ident, $options_internal_type: ident, $options_value: expr, $packet_value: expr) => ({

        let (response_sender, rx) = tokio::sync::oneshot::channel();
        let response_handler = Box::new(move |res| {
            if response_sender.send(res).is_err() {
                return Err(MqttError::new_operation_channel_failure("Failed to submit operation to client channel"));
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
                    Err(MqttError::new_operation_channel_failure(error))
                }
                _ => {
                    rx.await?
                }
            }
        })
    })
}

impl AsyncMqttClient for TokioClient {
    /// Signals the client that it should attempt to recurrently maintain a connection to
    /// the broker endpoint it has been configured with.
    fn start(&self, default_listener: Option<Arc<ClientEventListenerCallback>>) -> MqttResult<()> {
        info!("client start invoked");
        if let Err(send_error) = self.operation_sender.send(OperationOptions::Start(default_listener)) {
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
        info!("client close invoked; no further operations allowed");
        if let Err(send_error) = self.operation_sender.send(OperationOptions::Shutdown()) {
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

        submit_tokio_operation!(self, Publish, PublishOptionsInternal, options, boxed_packet)
    }

    /// Submits a Subscribe operation to the client's operation queue.  The subscribe will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.
    fn subscribe(&self, packet: SubscribePacket, options: Option<SubscribeOptions>) -> Pin<Box<SubscribeResultFuture>> {
        debug!("Subscribe operation submitted");
        let boxed_packet = Box::new(MqttPacket::Subscribe(packet));
        if let Err(error) = validate_packet_outbound(&boxed_packet) {
            return Box::pin(async move { Err(error) });
        }

        submit_tokio_operation!(self, Subscribe, SubscribeOptionsInternal, options, boxed_packet)
    }

    /// Submits an Unsubscribe operation to the client's operation queue.  The unsubscribe will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.
    fn unsubscribe(&self, packet: UnsubscribePacket, options: Option<UnsubscribeOptions>) -> Pin<Box<UnsubscribeResultFuture>> {
        debug!("Unsubscribe operation submitted");
        let boxed_packet = Box::new(MqttPacket::Unsubscribe(packet));
        if let Err(error) = validate_packet_outbound(&boxed_packet) {
            return Box::pin(async move { Err(error) });
        }

        submit_tokio_operation!(self, Unsubscribe, UnsubscribeOptionsInternal, options, boxed_packet)
    }

    /// Adds an additional listener to the events emitted by this client.  This is useful when
    /// multiple higher-level constructs are sharing the same MQTT client.
    fn add_event_listener(&self, listener: ClientEventListener) -> MqttResult<ListenerHandle> {
        debug!("AddListener operation submitted");
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
        debug!("RemoveListener operation submitted");
        if let Err(send_error) = self.operation_sender.send(OperationOptions::RemoveListener(listener.id)) {
            return Err(MqttError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }
}

pub(crate) fn create_runtime_states<T>(tokio_config: TokioClientOptions<T>) -> (UnboundedSender<OperationOptions>, ClientRuntimeState<T>) where T : AsyncRead + AsyncWrite + Send + Sync + 'static {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

    let impl_state = ClientRuntimeState {
        tokio_config,
        operation_receiver: receiver,
        stream: None
    };

    (sender, impl_state)
}


/// Creates a new async MQTT5 client that will use the tokio async runtime
pub fn new_with_tokio<T>(client_config: MqttClientOptions, connect_config: ConnectOptions, tokio_config: TokioClientOptions<T>, runtime_handle: &runtime::Handle) -> AsyncGneissClient where T: AsyncRead + AsyncWrite + Send + Sync + 'static {
    let (operation_sender, internal_state) = create_runtime_states(tokio_config);

    let callback_spawner : CallbackSpawnerFunction = Box::new(|event, callback| {
        spawn_event_callback(event, callback)
    });

    let client_impl = MqttClientImpl::new(client_config, connect_config, callback_spawner);

    spawn_client_impl(client_impl, internal_state, runtime_handle);

    Arc::new(TokioClient{
        operation_sender,
        listener_id_allocator: Mutex::new(1),
    })
}


#[allow(clippy::too_many_arguments)]
pub(crate) fn make_direct_client(tls_impl: TlsConfiguration, endpoint: String, port: u16, _tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, runtime: &Handle) -> MqttResult<AsyncGneissClient> {
    match tls_impl {
        TlsConfiguration::None => { make_direct_client_no_tls(endpoint, port, client_options, connect_options, http_proxy_options, runtime) }
        #[cfg(feature = "rustls")]
        TlsConfiguration::Rustls => { make_direct_client_rustls(endpoint, port, _tls_options, client_options, connect_options, http_proxy_options, runtime) }
        #[cfg(feature = "native-tls")]
        TlsConfiguration::Nativetls => { make_direct_client_native_tls(endpoint, port, _tls_options, client_options, connect_options, http_proxy_options, runtime) }
        _ => { panic!("Illegal state"); }
    }
}

fn make_direct_client_no_tls(endpoint: String, port: u16, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, runtime: &Handle) -> MqttResult<AsyncGneissClient> {
    info!("make_direct_client_no_tls - creating async connection establishment closure");
    let (stream_endpoint, http_connect_endpoint) = compute_endpoints(endpoint, port, &http_proxy_options);

    if http_connect_endpoint.is_some() {
        let tokio_options = TokioClientOptions {
            connection_factory: Box::new(move || {
                let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                let tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                Box::pin(apply_proxy_connect_to_stream(tcp_stream, http_connect_endpoint.clone()))
            }),
        };

        info!("make_direct_client_no_tls - plaintext-to-proxy -> plaintext-to-broker");
        Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
    } else {
        let tokio_options = TokioClientOptions {
            connection_factory: Box::new(move || {
                Box::pin(make_leaf_stream(stream_endpoint.clone()))
            }),
        };

        info!("make_direct_client_no_tls - plaintext-to-broker");
        Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
    }
}

#[cfg(feature = "rustls")]
fn make_direct_client_rustls(endpoint: String, port: u16, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, runtime: &Handle) -> MqttResult<AsyncGneissClient> {
    info!("make_direct_client_rustls - creating async connection establishment closure");

    let (stream_endpoint, http_connect_endpoint) = compute_endpoints(endpoint.clone(), port, &http_proxy_options);

    if let Some(tls_options) = tls_options {
        if let Some(http_proxy_options) = http_proxy_options {
            if let Some(proxy_tls_options) = http_proxy_options.tls_options {
                let tokio_options = TokioClientOptions {
                    connection_factory: Box::new(move || {
                        let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                        let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                        let proxy_tls_stream = Box::pin(wrap_stream_with_tls_rustls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone()));
                        let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone()));
                        Box::pin(wrap_stream_with_tls_rustls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone()))
                    }),
                };

                info!("make_direct_client_rustls - tls-to-proxy -> tls-to-broker");
                Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
            } else {
                let tokio_options = TokioClientOptions {
                    connection_factory: Box::new(move || {
                        let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                        let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                        let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tcp_stream, http_connect_endpoint.clone()));
                        Box::pin(wrap_stream_with_tls_rustls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone()))
                    }),
                };

                info!("make_direct_client_rustls - plaintext-to-proxy -> tls-to-broker");
                Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
            }
        } else {
            let tokio_options = TokioClientOptions {
                connection_factory: Box::new(move || {
                    let tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                    Box::pin(wrap_stream_with_tls_rustls(tcp_stream, endpoint.clone(), tls_options.clone()))
                }),
            };

            info!("make_direct_client_rustls - tls-to-broker");
            Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
        }
    } else if let Some(http_proxy_options) = http_proxy_options {
        if let Some(proxy_tls_options) = http_proxy_options.tls_options {
            let tokio_options = TokioClientOptions {
                connection_factory: Box::new(move || {
                    let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                    let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                    let proxy_tls_stream = Box::pin(wrap_stream_with_tls_rustls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone()));
                    Box::pin(apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone()))
                }),
            };

            info!("make_direct_client_rustls - tls-to-proxy -> plaintext-to-broker");
            Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
        } else {
            panic!("Tls direct client creation invoked without tls configuration")
        }
    } else {
        panic!("Tls direct client creation invoked without tls configuration")
    }
}

#[cfg(feature = "native-tls")]
fn make_direct_client_native_tls(endpoint: String, port: u16, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, runtime: &Handle) -> MqttResult<AsyncGneissClient> {
    info!("make_direct_client_native_tls - creating async connection establishment closure");

    let (stream_endpoint, http_connect_endpoint) = compute_endpoints(endpoint.clone(), port, &http_proxy_options);

    if let Some(tls_options) = tls_options {
        if let Some(http_proxy_options) = http_proxy_options {
            if let Some(proxy_tls_options) = http_proxy_options.tls_options {
                let tokio_options = TokioClientOptions {
                    connection_factory: Box::new(move || {
                        let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                        let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                        let proxy_tls_stream = Box::pin(wrap_stream_with_tls_native_tls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone()));
                        let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone()));
                        Box::pin(wrap_stream_with_tls_native_tls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone()))
                    }),
                };

                info!("make_direct_client_native_tls - tls-to-proxy -> tls-to-broker");
                Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
            } else {
                let tokio_options = TokioClientOptions {
                    connection_factory: Box::new(move || {
                        let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                        let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                        let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tcp_stream, http_connect_endpoint.clone()));
                        Box::pin(wrap_stream_with_tls_native_tls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone()))
                    }),
                };

                info!("make_direct_client_native_tls - plaintext-to-proxy -> tls-to-broker");
                Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
            }
        } else {
            let tokio_options = TokioClientOptions {
                connection_factory: Box::new(move || {
                    let tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                    Box::pin(wrap_stream_with_tls_native_tls(tcp_stream, endpoint.clone(), tls_options.clone()))
                }),
            };

            info!("make_direct_client_native_tls - tls-to-broker");
            Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
        }
    } else if let Some(http_proxy_options) = http_proxy_options {
        if let Some(proxy_tls_options) = http_proxy_options.tls_options {
            let tokio_options = TokioClientOptions {
                connection_factory: Box::new(move || {
                    let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                    let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                    let proxy_tls_stream = Box::pin(wrap_stream_with_tls_native_tls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone()));
                    Box::pin(apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone()))
                }),
            };

            info!("make_direct_client_native_tls - tls-to-proxy -> plaintext-to-broker");
            Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
        } else {
            panic!("Tls direct client creation invoked without tls configuration")
        }
    } else {
        panic!("Tls direct client creation invoked without tls configuration")
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg(feature="websockets")]
pub(crate) fn make_websocket_client(tls_impl: crate::config::TlsConfiguration, endpoint: String, port: u16, websocket_options: WebsocketOptions, _tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, runtime: &Handle) -> MqttResult<AsyncGneissClient> {
    match tls_impl {
        crate::config::TlsConfiguration::None => { make_websocket_client_no_tls(endpoint, port, websocket_options, client_options, connect_options, http_proxy_options, runtime) }
        #[cfg(feature = "rustls")]
        crate::config::TlsConfiguration::Rustls => { make_websocket_client_rustls(endpoint, port, websocket_options, _tls_options, client_options, connect_options, http_proxy_options, runtime) }
        #[cfg(feature = "native-tls")]
        crate::config::TlsConfiguration::Nativetls => { make_websocket_client_native_tls(endpoint, port, websocket_options, _tls_options, client_options, connect_options, http_proxy_options, runtime) }
        _ => { panic!("Illegal state"); }
    }
}

#[cfg(feature="websockets")]
fn make_websocket_client_no_tls(endpoint: String, port: u16, websocket_options: WebsocketOptions, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, runtime: &Handle) -> MqttResult<AsyncGneissClient> {
    info!("make_websocket_client_no_tls - creating async connection establishment closure");
    let (stream_endpoint, http_connect_endpoint) = compute_endpoints(endpoint, port, &http_proxy_options);

    if http_connect_endpoint.is_some() {
        let tokio_options = TokioClientOptions {
            connection_factory: Box::new(move || {
                let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tcp_stream, http_connect_endpoint.clone()));
                Box::pin(wrap_stream_with_websockets(connect_stream, http_connect_endpoint.endpoint.clone(), "ws", websocket_options.clone()))
            }),
        };

        info!("create_websocket_client_plaintext_to_proxy_plaintext_to_broker");
        Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
    } else {
        let tokio_options = TokioClientOptions {
            connection_factory: Box::new(move || {
                let tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                Box::pin(wrap_stream_with_websockets(tcp_stream, stream_endpoint.endpoint.clone(), "ws", websocket_options.clone()))
            }),
        };

        info!("create_websocket_client_plaintext_to_broker");
        Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg(all(feature = "rustls", feature = "websockets"))]
fn make_websocket_client_rustls(endpoint: String, port: u16, websocket_options: WebsocketOptions, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, runtime: &Handle) -> MqttResult<AsyncGneissClient> {
    info!("make_websocket_client_rustls - creating async connection establishment closure");
    let (stream_endpoint, http_connect_endpoint) = compute_endpoints(endpoint.clone(), port, &http_proxy_options);

    if let Some(tls_options) = tls_options {
        if let Some(http_proxy_options) = http_proxy_options {
            if let Some(proxy_tls_options) = http_proxy_options.tls_options {
                let tokio_options = TokioClientOptions {
                    connection_factory: Box::new(move || {
                        let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                        let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                        let proxy_tls_stream = Box::pin(wrap_stream_with_tls_rustls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone()));
                        let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone()));
                        let tls_stream = Box::pin(wrap_stream_with_tls_rustls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone()));
                        Box::pin(wrap_stream_with_websockets(tls_stream, http_connect_endpoint.endpoint.clone(), "wss", websocket_options.clone()))
                    }),
                };

                info!("make_websocket_client_rustls - tls-to-proxy -> tls-to-broker");
                Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
            } else {
                let tokio_options = TokioClientOptions {
                    connection_factory: Box::new(move || {
                        let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                        let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                        let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tcp_stream, http_connect_endpoint.clone()));
                        let tls_stream = Box::pin(wrap_stream_with_tls_rustls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone()));
                        Box::pin(wrap_stream_with_websockets(tls_stream, http_connect_endpoint.endpoint.clone(), "wss", websocket_options.clone()))
                    }),
                };

                info!("make_websocket_client_rustls - plaintext-to-proxy -> tls-to-broker");
                Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
            }
        } else {
            let tokio_options = TokioClientOptions {
                connection_factory: Box::new(move || {
                    let tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                    let tls_stream = Box::pin(wrap_stream_with_tls_rustls(tcp_stream, stream_endpoint.endpoint.clone(), tls_options.clone()));
                    Box::pin(wrap_stream_with_websockets(tls_stream, stream_endpoint.endpoint.clone(), "wss", websocket_options.clone()))
                }),
            };

            info!("make_websocket_client_rustls - tls-to-broker");
            Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
        }
    } else if let Some(http_proxy_options) = http_proxy_options {
        if let Some(proxy_tls_options) = http_proxy_options.tls_options {
            let tokio_options = TokioClientOptions {
                connection_factory: Box::new(move || {
                    let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                    let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                    let proxy_tls_stream = Box::pin(wrap_stream_with_tls_rustls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone()));
                    let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone()));
                    Box::pin(wrap_stream_with_websockets(connect_stream, http_connect_endpoint.endpoint.clone(), "ws", websocket_options.clone()))
                }),
            };

            info!("make_websocket_client_rustls - tls-to-proxy -> plaintext-to-broker");
            Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
        } else {
            panic!("Tls websocket client creation invoked without tls configuration")
        }
    } else {
        panic!("Tls websocket client creation invoked without tls configuration")
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg(all(feature = "native-tls", feature = "websockets"))]
fn make_websocket_client_native_tls(endpoint: String, port: u16, websocket_options: WebsocketOptions, tls_options: Option<TlsOptions>, client_options: MqttClientOptions, connect_options: ConnectOptions, http_proxy_options: Option<HttpProxyOptions>, runtime: &Handle) -> MqttResult<AsyncGneissClient> {
    info!("make_websocket_client_native_tls - creating async connection establishment closure");
    let (stream_endpoint, http_connect_endpoint) = compute_endpoints(endpoint.clone(), port, &http_proxy_options);

    if let Some(tls_options) = tls_options {
        if let Some(http_proxy_options) = http_proxy_options {
            if let Some(proxy_tls_options) = http_proxy_options.tls_options {
                let tokio_options = TokioClientOptions {
                    connection_factory: Box::new(move || {
                        let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                        let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                        let proxy_tls_stream = Box::pin(wrap_stream_with_tls_native_tls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone()));
                        let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone()));
                        let tls_stream = Box::pin(wrap_stream_with_tls_native_tls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone()));
                        Box::pin(wrap_stream_with_websockets(tls_stream, http_connect_endpoint.endpoint.clone(), "wss", websocket_options.clone()))
                    }),
                };

                info!("make_websocket_client_native_tls - tls-to-proxy -> tls-to-broker");
                Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
            } else {
                let tokio_options = TokioClientOptions {
                    connection_factory: Box::new(move || {
                        let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                        let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                        let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tcp_stream, http_connect_endpoint.clone()));
                        let tls_stream = Box::pin(wrap_stream_with_tls_native_tls(connect_stream, http_connect_endpoint.endpoint.clone(), tls_options.clone()));
                        Box::pin(wrap_stream_with_websockets(tls_stream, http_connect_endpoint.endpoint.clone(), "wss", websocket_options.clone()))
                    }),
                };

                info!("make_websocket_client_native_tls - plaintext-to-proxy -> tls-to-broker");
                Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
            }
        } else {
            let tokio_options = TokioClientOptions {
                connection_factory: Box::new(move || {
                    let tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                    let tls_stream = Box::pin(wrap_stream_with_tls_native_tls(tcp_stream, stream_endpoint.endpoint.clone(), tls_options.clone()));
                    Box::pin(wrap_stream_with_websockets(tls_stream, stream_endpoint.endpoint.clone(), "wss", websocket_options.clone()))
                }),
            };

            info!("make_websocket_client_native_tls - tls-to-broker");
            Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
        }
    } else if let Some(http_proxy_options) = http_proxy_options {
        if let Some(proxy_tls_options) = http_proxy_options.tls_options {
            let tokio_options = TokioClientOptions {
                connection_factory: Box::new(move || {
                    let http_connect_endpoint = http_connect_endpoint.clone().unwrap();
                    let proxy_tcp_stream = Box::pin(make_leaf_stream(stream_endpoint.clone()));
                    let proxy_tls_stream = Box::pin(wrap_stream_with_tls_native_tls(proxy_tcp_stream, stream_endpoint.endpoint.clone(), proxy_tls_options.clone()));
                    let connect_stream = Box::pin(apply_proxy_connect_to_stream(proxy_tls_stream, http_connect_endpoint.clone()));
                    Box::pin(wrap_stream_with_websockets(connect_stream, http_connect_endpoint.endpoint.clone(), "ws", websocket_options.clone()))
                }),
            };

            info!("make_websocket_client_native_tls - tls-to-proxy -> plaintext-to-broker");
            Ok(new_with_tokio(client_options, connect_options, tokio_options, runtime))
        } else {
            panic!("Tls websocket client creation invoked without tls configuration")
        }
    } else {
        panic!("Tls websocket client creation invoked without tls configuration")
    }
}

async fn make_leaf_stream(endpoint: Endpoint) -> MqttResult<TcpStream> {
    let addr = make_addr(endpoint.endpoint.as_str(), endpoint.port)?;
    debug!("make_leaf_stream - opening TCP stream");
    let stream = TcpStream::connect(&addr).await?;
    debug!("make_leaf_stream - TCP stream successfully established");

    Ok(stream)
}

#[cfg(feature = "rustls")]
async fn wrap_stream_with_tls_rustls<S>(stream : Pin<Box<impl Future<Output=MqttResult<S>>+Sized>>, endpoint: String, tls_options: TlsOptions) -> MqttResult<tokio_rustls::client::TlsStream<S>> where S : AsyncRead + AsyncWrite + Unpin {
    let domain = rustls_pki_types::ServerName::try_from(endpoint)?
        .to_owned();

    let connector =
        match tls_options.options {
            TlsData::Rustls(_, config) => { tokio_rustls::TlsConnector::from(config.clone()) }
            _ => { panic!("Rustls stream wrapper invoked without Rustls configuration"); }
        };

    debug!("wrap_stream_with_tls_rustls - performing tls handshake");
    let inner_stream= stream.await?;
    let tls_stream = connector.connect(domain, inner_stream).await?;
    debug!("wrap_stream_with_tls_rustls - tls handshake successfully completed");

    Ok(tls_stream)
}

#[cfg(feature = "native-tls")]
async fn wrap_stream_with_tls_native_tls<S>(stream : Pin<Box<impl Future<Output=MqttResult<S>>+Sized>>, endpoint: String, tls_options: TlsOptions) -> MqttResult<tokio_native_tls::TlsStream<S>> where S : AsyncRead + AsyncWrite + Unpin {

    let connector =
        match tls_options.options {
            TlsData::NativeTls(_, ntls_builder) => {
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

#[cfg(feature="websockets")]
async fn wrap_stream_with_websockets<S>(stream : Pin<Box<impl Future<Output=MqttResult<S>>+Sized>>, endpoint: String, scheme: &str, websocket_options: WebsocketOptions) -> MqttResult<WsByteStream<WebSocketStream<S>, Message, tungstenite::Error, WsMessageHandler>> where S : AsyncRead + AsyncWrite + Unpin {

    let uri = format!("{}://{}/mqtt", scheme, endpoint); // scheme needs to be present but value irrelevant
    let handshake_builder = crate::config::create_default_websocket_handshake_request(uri)?;

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
    let (message_stream, _) = client_async(crate::config::HandshakeRequest { handshake_builder: transformed_handshake_builder }, inner_stream).await?;
    let byte_stream = WsMessageHandler::wrap_stream(message_stream);
    debug!("wrap_stream_with_websockets - successfully upgraded stream to websockets");

    Ok(byte_stream)
}

fn build_connect_request(http_connect_endpoint: &Endpoint) -> Vec<u8> {
    let request_as_string = format!("CONNECT {}:{} HTTP/1.1\r\nHost: {}:{}\r\nConnection: keep-alive\r\n\r\n", http_connect_endpoint.endpoint, http_connect_endpoint.port, http_connect_endpoint.endpoint, http_connect_endpoint.port);

    return request_as_string.as_bytes().to_vec();
}

async fn apply_proxy_connect_to_stream<S>(stream : Pin<Box<impl Future<Output=MqttResult<S>>+Sized>>, http_connect_endpoint: Endpoint) -> MqttResult<S> where S : AsyncRead + AsyncWrite + Unpin {
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



//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use crate::client::waiter::*;
use crate::mqtt::disconnect::validate_disconnect_packet_outbound;
use crate::mqtt::{MqttPacket, PublishPacket, SubscribePacket, UnsubscribePacket};
use crate::validate::validate_packet_outbound;

#[derive(Clone)]
/// Timestamped client event record
pub struct ClientEventRecord {

    /// The event emitted by the client
    pub event : Arc<ClientEvent>,

    /// What time the event occurred at
    pub timestamp: Instant
}

/// Simple debug type that uses the client listener framework to allow tests to asynchronously wait for
/// configurable client event sequences.  May be useful outside of tests.  May need polish.  Currently public
/// because we use it across crates.  May eventually go internal.
///
/// Requires the client to be Arc-wrapped.
pub struct ClientEventWaiter {
    event_count: usize,

    client: AsyncGneissClient,

    listener: Option<ListenerHandle>,

    event_receiver: tokio::sync::mpsc::UnboundedReceiver<ClientEventRecord>,

    events: Vec<ClientEventRecord>,
}

impl ClientEventWaiter {

    /// Creates a new ClientEventWaiter instance from full configuration
    pub fn new(client: AsyncGneissClient, config: ClientEventWaiterOptions, event_count: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let mut waiter = ClientEventWaiter {
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

    /// Creates a new ClientEventWaiter instance that will wait for a single occurrence of a single event type
    pub fn new_single(client: AsyncGneissClient, event_type: ClientEventType) -> Self {
        let config = ClientEventWaiterOptions {
            wait_type: ClientEventWaitType::Type(event_type),
        };

        Self::new(client, config, 1)
    }

    /// Waits for and returns an event sequence that matches the original configuration
    pub async fn wait(&mut self) -> MqttResult<Vec<ClientEventRecord>> {
        while self.events.len() < self.event_count {
            match self.event_receiver.recv().await {
                None => {
                    return Err(MqttError::new_other_error("Channel closed"));
                }
                Some(event) => {
                    self.events.push(event);
                }
            }
        }

        Ok(self.events.clone())
    }
}

impl Drop for ClientEventWaiter {
    fn drop(&mut self) {
        let listener_handler = self.listener.take().unwrap();

        let _ = self.client.remove_event_listener(listener_handler);
    }
}

#[cfg(all(test, feature = "testing"))]
pub(crate) mod testing {
    use assert_matches::assert_matches;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::time::Duration;
    use crate::client::waiter::*;
    use crate::config::*;
    use crate::error::*;
    use crate::mqtt::*;
    use super::*;
    use crate::testing::integration::*;

    fn create_client_builder_internal(connect_options: ConnectOptions, _tls_usage: TlsUsage, ws_config: WebsocketUsage, proxy_config: ProxyUsage, tls_endpoint: TlsUsage, ws_endpoint: WebsocketUsage) -> GenericClientBuilder {
        let client_config = MqttClientOptionsBuilder::new()
            .with_connect_timeout(Duration::from_secs(5))
            .with_offline_queue_policy(OfflineQueuePolicy::PreserveAll)
            .build();

        let endpoint = get_broker_endpoint(tls_endpoint, ws_endpoint);
        let port = get_broker_port(tls_endpoint, ws_endpoint);

        let mut builder = GenericClientBuilder::new(&endpoint, port);
        builder.with_connect_options(connect_options);
        builder.with_client_options(client_config);

        #[cfg(feature = "rustls")]
        if _tls_usage == TlsUsage::Rustls {
            let mut tls_options_builder = TlsOptionsBuilder::new();
            tls_options_builder.with_verify_peer(false);
            tls_options_builder.with_root_ca_from_path(&get_ca_path()).unwrap();

            builder.with_tls_options(tls_options_builder.build_rustls().unwrap());
        }

        #[cfg(feature = "native-tls")]
        if _tls_usage == TlsUsage::Nativetls {
            let mut tls_options_builder = TlsOptionsBuilder::new();
            tls_options_builder.with_verify_peer(false);
            tls_options_builder.with_root_ca_from_path(&get_ca_path()).unwrap();

            builder.with_tls_options(tls_options_builder.build_native_tls().unwrap());
        }

        #[cfg(feature="websockets")]
        if ws_config != WebsocketUsage::None {
            let websocket_options = WebsocketOptionsBuilder::new().build();
            builder.with_websocket_options(websocket_options);
        }

        if proxy_config != ProxyUsage::None {
            let proxy_endpoint = get_proxy_endpoint();
            let proxy_port = get_proxy_port();
            let proxy_options = HttpProxyOptionsBuilder::new(&proxy_endpoint, proxy_port).build();
            builder.with_http_proxy_options(proxy_options);
        }

        builder
    }

    fn create_good_client_builder(tls: TlsUsage, ws: WebsocketUsage, proxy: ProxyUsage) -> GenericClientBuilder {
        let connect_options = ConnectOptionsBuilder::new()
            .with_rejoin_session_policy(RejoinSessionPolicy::PostSuccess)
            .with_session_expiry_interval_seconds(3600)
            .build();

        create_client_builder_internal(connect_options, tls, ws, proxy, tls, ws)
    }

    type AsyncTestFactoryReturnType = Pin<Box<dyn Future<Output = MqttResult<()>> + Send>>;
    type AsyncTestFactory = Box<dyn Fn(GenericClientBuilder) -> AsyncTestFactoryReturnType + Send + Sync>;

    fn do_good_client_test(tls: TlsUsage, ws: WebsocketUsage, proxy: ProxyUsage, test_factory: AsyncTestFactory) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let test_future = (*test_factory)(create_good_client_builder(tls, ws, proxy));

        runtime.block_on(test_future).unwrap();
    }

    pub(crate) fn do_builder_test(test_factory: AsyncTestFactory, builder: GenericClientBuilder) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let test_future = (*test_factory)(builder);

        runtime.block_on(test_future).unwrap();
    }

    async fn start_client(client: &AsyncGneissClient) -> MqttResult<()> {
        let mut connection_attempt_waiter = ClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionAttempt);
        let mut connection_success_waiter = ClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionSuccess);

        client.start(None)?;

        connection_attempt_waiter.wait().await?;
        let connection_success_events = connection_success_waiter.wait().await?;
        assert_eq!(1, connection_success_events.len());
        let connection_success_event = &connection_success_events[0].event;
        assert_matches!(**connection_success_event, ClientEvent::ConnectionSuccess(_));
        if let ClientEvent::ConnectionSuccess(success_event) = &**connection_success_event {
            assert_eq!(ConnectReasonCode::Success, success_event.connack.reason_code);
        } else {
            panic!("impossible");
        }

        Ok(())
    }

    async fn stop_client(client: &AsyncGneissClient) -> MqttResult<()> {
        let mut disconnection_waiter = ClientEventWaiter::new_single(client.clone(), ClientEventType::Disconnection);
        let mut stopped_waiter = ClientEventWaiter::new_single(client.clone(), ClientEventType::Stopped);

        client.stop(None)?;

        let disconnect_events = disconnection_waiter.wait().await?;
        assert_eq!(1, disconnect_events.len());
        let disconnect_event = &disconnect_events[0].event;
        assert_matches!(**disconnect_event, ClientEvent::Disconnection(_));
        if let ClientEvent::Disconnection(event) = &**disconnect_event {
            assert_matches!(event.error, MqttError::UserInitiatedDisconnect(_));
        } else {
            panic!("impossible");
        }

        stopped_waiter.wait().await?;

        Ok(())
    }

    async fn tokio_connect_disconnect_test(builder: GenericClientBuilder) -> MqttResult<()> {
        let client = builder.build(&tokio::runtime::Handle::current()).unwrap();

        start_client(&client).await?;
        stop_client(&client).await?;

        Ok(())
    }

    #[test]
    fn client_connect_disconnect_direct_plaintext_no_proxy() {
        do_good_client_test(TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(feature = "rustls")]
    fn client_connect_disconnect_direct_rustls_no_proxy() {
        do_good_client_test(TlsUsage::Rustls, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(feature = "native-tls")]
    fn client_connect_disconnect_direct_native_tls_no_proxy() {
        do_good_client_test(TlsUsage::Nativetls, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(feature="websockets")]
    fn client_connect_disconnect_websocket_plaintext_no_proxy() {
        do_good_client_test(TlsUsage::None, WebsocketUsage::Tungstenite, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(all(feature = "rustls", feature = "websockets"))]
    fn client_connect_disconnect_websocket_rustls_no_proxy() {
        do_good_client_test(TlsUsage::Rustls, WebsocketUsage::Tungstenite, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(all(feature = "native-tls", feature = "websockets"))]
    fn client_connect_disconnect_websocket_native_tls_no_proxy() {
        do_good_client_test(TlsUsage::Nativetls, WebsocketUsage::Tungstenite, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    fn client_connect_disconnect_direct_plaintext_with_proxy() {
        do_good_client_test(TlsUsage::None, WebsocketUsage::None, ProxyUsage::Plaintext, Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(feature = "rustls")]
    fn client_connect_disconnect_direct_rustls_with_proxy() {
        do_good_client_test(TlsUsage::Rustls, WebsocketUsage::None, ProxyUsage::Plaintext, Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(feature = "native-tls")]
    fn client_connect_disconnect_direct_native_tls_with_proxy() {
        do_good_client_test(TlsUsage::Nativetls, WebsocketUsage::None, ProxyUsage::Plaintext, Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(feature="websockets")]
    fn client_connect_disconnect_websocket_plaintext_with_proxy() {
        do_good_client_test(TlsUsage::None, WebsocketUsage::Tungstenite, ProxyUsage::Plaintext, Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(all(feature = "rustls", feature="websosckets"))]
    fn client_connect_disconnect_websocket_rustls_with_proxy() {
        do_good_client_test(TlsUsage::Rustls, WebsocketUsage::Tungstenite, ProxyUsage::Plaintext, Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    #[cfg(all(feature = "native-tls", feature = "websockets"))]
    fn client_connect_disconnect_websocket_native_tls_with_proxy() {
        do_good_client_test(TlsUsage::Nativetls, WebsocketUsage::Tungstenite, ProxyUsage::Plaintext, Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    async fn tokio_subscribe_unsubscribe_test(builder: GenericClientBuilder) -> MqttResult<()> {
        let client = builder.build(&tokio::runtime::Handle::current()).unwrap();
        start_client(&client).await?;

        let subscribe = SubscribePacket::builder()
            .with_subscription_simple("hello/world".to_string(), QualityOfService::AtLeastOnce)
            .build();

        let subscribe_result = client.subscribe(subscribe, None).await;
        assert!(subscribe_result.is_ok());
        let suback = subscribe_result.unwrap();
        assert_eq!(1, suback.reason_codes.len());
        assert_eq!(SubackReasonCode::GrantedQos1, suback.reason_codes[0]);

        let unsubscribe = UnsubscribePacket::builder()
            .with_topic_filter("hello/world".to_string())
            .with_topic_filter("not/subscribed".to_string())
            .build();

        let unsubscribe_result = client.unsubscribe(unsubscribe, None).await;
        assert!(unsubscribe_result.is_ok());
        let unsuback = unsubscribe_result.unwrap();
        assert_eq!(2, unsuback.reason_codes.len());
        assert_eq!(UnsubackReasonCode::Success, unsuback.reason_codes[0]);
        // broker may or may not give us a not subscribed reason code, so don't verify

        stop_client(&client).await?;

        Ok(())
    }

    #[test]
    fn client_subscribe_unsubscribe() {
        do_good_client_test(TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_subscribe_unsubscribe_test(builder))
        }));
    }

    fn verify_successful_publish_result(result: &PublishResponse, qos: QualityOfService) {
        match result {
            PublishResponse::Qos0 => {
                assert_eq!(qos, QualityOfService::AtMostOnce);
            }
            PublishResponse::Qos1(puback) => {
                assert_eq!(qos, QualityOfService::AtLeastOnce);
                assert_eq!(PubackReasonCode::Success, puback.reason_code);
            }
            PublishResponse::Qos2(qos2_result) => {
                assert_eq!(qos, QualityOfService::ExactlyOnce);
                assert_matches!(qos2_result, Qos2Response::Pubcomp(_));
                if let Qos2Response::Pubcomp(pubcomp) = qos2_result {
                    assert_eq!(PubcompReasonCode::Success, pubcomp.reason_code);
                }
            }
        }
    }

    fn verify_publish_received(event: &PublishReceivedEvent, expected_topic: &str, expected_qos: QualityOfService, expected_payload: &[u8]) {
        let publish = &event.publish;

        assert_eq!(expected_qos, publish.qos);
        assert_eq!(expected_topic, &publish.topic);
        assert_eq!(expected_payload, publish.payload.as_ref().unwrap().as_slice());
    }

    async fn tokio_subscribe_publish_test(builder: GenericClientBuilder, qos: QualityOfService) -> MqttResult<()> {
        let client = builder.build(&tokio::runtime::Handle::current()).unwrap();
        start_client(&client).await?;

        let payload = "derp".as_bytes().to_vec();

        // tests are running in parallel, need a unique topic
        let uuid = uuid::Uuid::new_v4();
        let topic = format!("hello/world/{}", uuid.to_string());
        let subscribe = SubscribePacket::builder()
            .with_subscription_simple(topic.clone(), QualityOfService::ExactlyOnce)
            .build();

        let _ = client.subscribe(subscribe, None).await?;

        let mut publish_received_waiter = ClientEventWaiter::new_single(client.clone(), ClientEventType::PublishReceived);

        let publish = PublishPacket::builder(topic.clone(), qos)
            .with_payload(payload.clone())
            .build();

        let publish_result = client.publish(publish, None).await?;
        verify_successful_publish_result(&publish_result, qos);

        let publish_received_events = publish_received_waiter.wait().await?;
        assert_eq!(1, publish_received_events.len());
        let publish_received_event = &publish_received_events[0].event;
        assert_matches!(**publish_received_event, ClientEvent::PublishReceived(_));
        if let ClientEvent::PublishReceived(event) = &**publish_received_event {
            verify_publish_received(event, &topic, qos, payload.as_slice());
        } else {
            panic!("impossible");
        }

        stop_client(&client).await?;

        Ok(())
    }

    #[test]
    fn client_subscribe_publish_qos0() {
        do_good_client_test(TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_subscribe_publish_test(builder, QualityOfService::AtMostOnce))
        }));
    }

    #[test]
    fn client_subscribe_publish_qos1() {
        do_good_client_test(TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_subscribe_publish_test(builder, QualityOfService::AtLeastOnce))
        }));
    }

    #[test]
    fn client_subscribe_publish_qos2() {
        do_good_client_test(TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_subscribe_publish_test(builder, QualityOfService::ExactlyOnce))
        }));
    }

    // This primarily tests that the will configuration works.  Will functionality is mostly broker-side.
    async fn tokio_will_test(builder: GenericClientBuilder) -> MqttResult<()> {
        let client = builder.build(&tokio::runtime::Handle::current()).unwrap();
        let payload = "Onsecondthought".as_bytes().to_vec();

        // tests are running in parallel, need a unique topic
        let uuid = uuid::Uuid::new_v4();
        let will_topic = format!("goodbye/cruel/world/{}", uuid.to_string());

        let will = PublishPacket::builder(will_topic.clone(), QualityOfService::AtLeastOnce)
            .with_payload(payload.clone())
            .build();

        let connect_options = ConnectOptionsBuilder::new()
            .with_rejoin_session_policy(RejoinSessionPolicy::PostSuccess)
            .with_will(will)
            .build();

        let will_builder = create_client_builder_internal(connect_options, TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, TlsUsage::None, WebsocketUsage::None);
        let will_client = will_builder.build(&tokio::runtime::Handle::current()).unwrap();

        start_client(&client).await?;
        start_client(&will_client).await?;

        let subscribe = SubscribePacket::builder()
            .with_subscription_simple(will_topic.clone(), QualityOfService::ExactlyOnce)
            .build();
        let _ = client.subscribe(subscribe, None).await?;

        let mut publish_received_waiter = ClientEventWaiter::new_single(client.clone(), ClientEventType::PublishReceived);

        // no stop options, so we just close the socket locally; the broker should send the will
        stop_client(&will_client).await?;

        let publish_received_events = publish_received_waiter.wait().await?;
        assert_eq!(1, publish_received_events.len());
        let publish_received_event = &publish_received_events[0].event;
        assert_matches!(**publish_received_event, ClientEvent::PublishReceived(_));
        if let ClientEvent::PublishReceived(event) = &**publish_received_event {
            verify_publish_received(event, &will_topic, QualityOfService::AtLeastOnce, payload.as_slice());
        } else {
            panic!("impossible");
        }

        stop_client(&client).await?;

        Ok(())
    }

    #[test]
    fn client_will_sent() {
        do_good_client_test(TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_will_test(builder))
        }));
    }

    async fn tokio_connect_disconnect_cycle_session_rejoin_test(builder: GenericClientBuilder) -> MqttResult<()> {
        let client = builder.build(&tokio::runtime::Handle::current()).unwrap();
        start_client(&client).await?;
        stop_client(&client).await?;

        for _ in 0..5 {
            let waiter_config = ClientEventWaiterOptions {
                wait_type: ClientEventWaitType::Predicate(Box::new(|ev| {
                    if let ClientEvent::ConnectionSuccess(success_event) = &**ev {
                        return success_event.connack.session_present && success_event.settings.rejoined_session;
                    }

                    false
                })),
            };
            let mut connection_success_waiter = ClientEventWaiter::new(client.clone(), waiter_config, 1);

            client.start(None)?;

            let connection_success_events = connection_success_waiter.wait().await?;
            assert_eq!(1, connection_success_events.len());

            stop_client(&client).await?;
        }

        Ok(())
    }

    #[test]
    fn connect_disconnect_cycle_session_rejoin() {
        do_good_client_test(TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_cycle_session_rejoin_test(builder))
        }));
    }

    async fn connection_failure_test(builder : GenericClientBuilder) -> MqttResult<()> {
        let client = builder.build(&tokio::runtime::Handle::current()).unwrap();
        let mut connection_failure_waiter = ClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionFailure);

        client.start(None)?;

        let connection_failure_results = connection_failure_waiter.wait().await?;
        assert_eq!(1, connection_failure_results.len());

        Ok(())
    }

    fn create_mismatch_builder(tls_config: TlsUsage, ws_config: WebsocketUsage, tls_endpoint: TlsUsage, ws_endpoint: WebsocketUsage) -> GenericClientBuilder {
        assert!(tls_config != tls_endpoint || ws_config != ws_endpoint);

        let connect_options = ConnectOptionsBuilder::new().build();

        create_client_builder_internal(connect_options, tls_config, ws_config, ProxyUsage::None, tls_endpoint, ws_endpoint)
    }

    #[test]
    #[cfg(feature = "rustls")]
    fn connection_failure_direct_rustls_tls_config_direct_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::None, TlsUsage::None, WebsocketUsage::None);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(feature = "native-tls")]
    fn connection_failure_direct_native_tls_tls_config_direct_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Nativetls, WebsocketUsage::None, TlsUsage::None, WebsocketUsage::None);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "rustls", feature = "websockets"))]
    fn connection_failure_direct_rustls_tls_config_websocket_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::None, TlsUsage::None, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "native-tls", feature = "websockets"))]
    fn connection_failure_direct_native_tls_tls_config_websocket_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Nativetls, WebsocketUsage::None, TlsUsage::None, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "rustls", feature = "websockets"))]
    fn connection_failure_direct_rustls_tls_config_websocket_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::None, TlsUsage::Rustls, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "native-tls", feature = "websockets"))]
    fn connection_failure_direct_native_tls_tls_config_websocket_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Nativetls, WebsocketUsage::None, TlsUsage::Nativetls, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(feature = "rustls")]
    fn connection_failure_direct_plaintext_config_direct_rustls_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::None, TlsUsage::Rustls, WebsocketUsage::None);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(feature = "native-tls")]
    fn connection_failure_direct_plaintext_config_direct_native_tls_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::None, TlsUsage::Nativetls, WebsocketUsage::None);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(feature="websockets")]
    fn connection_failure_direct_plaintext_config_websocket_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::None, TlsUsage::None, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "rustls", feature = "websockets"))]
    fn connection_failure_direct_plaintext_config_websocket_rustls_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::None, TlsUsage::Rustls, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "native-tls", feature = "websockets"))]
    fn connection_failure_direct_plaintext_config_websocket_native_tls_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::None, TlsUsage::Nativetls, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "rustls", feature = "websockets"))]
    fn connection_failure_websocket_rustls_tls_config_direct_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::Tungstenite, TlsUsage::None, WebsocketUsage::None);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "native-tls", feature = "websockets"))]
    fn connection_failure_websocket_native_tls_tls_config_direct_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Nativetls, WebsocketUsage::Tungstenite, TlsUsage::None, WebsocketUsage::None);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "rustls", feature = "websockets"))]
    fn connection_failure_websocket_rustls_tls_config_websocket_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::Tungstenite, TlsUsage::None, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "native-tls", feature = "websockets"))]
    fn connection_failure_websocket_native_tls_tls_config_websocket_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Nativetls, WebsocketUsage::Tungstenite, TlsUsage::None, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "rustls", feature = "websockets"))]
    fn connection_failure_websocket_rustls_tls_config_direct_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::Tungstenite, TlsUsage::Rustls, WebsocketUsage::None);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "native-tls", feature = "websockets"))]
    fn connection_failure_websocket_native_tls_tls_config_direct_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Nativetls, WebsocketUsage::Tungstenite, TlsUsage::Nativetls, WebsocketUsage::None);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(feature="websockets")]
    fn connection_failure_websocket_plaintext_config_direct_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::Tungstenite, TlsUsage::None, WebsocketUsage::None);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "rustls", feature = "websockets"))]
    fn connection_failure_websocket_plaintext_config_websocket_rustls_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::Tungstenite, TlsUsage::Rustls, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "native-tls", feature = "websockets"))]
    fn connection_failure_websocket_plaintext_config_websocket_native_tls_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::Tungstenite, TlsUsage::Nativetls, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "rustls", feature = "websockets"))]
    fn connection_failure_websocket_plaintext_config_direct_rustls_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::Tungstenite, TlsUsage::Rustls, WebsocketUsage::None);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    #[cfg(all(feature = "native-tls", feature = "websockets"))]
    fn connection_failure_websocket_plaintext_config_direct_native_tls_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::Tungstenite, TlsUsage::Nativetls, WebsocketUsage::None);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    fn connection_failure_invalid_endpoint() {
        let client_options = MqttClientOptionsBuilder::new()
            .with_connect_timeout(Duration::from_secs(3))
            .build();

        let mut builder = GenericClientBuilder::new("example.com", 8000);
        builder.with_client_options(client_options);

        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    fn connection_failure_invalid_endpoint_http() {
        let builder = GenericClientBuilder::new("amazon.com", 443);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }


}