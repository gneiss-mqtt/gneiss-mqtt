/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Functionality for using [`tokio`](https://crates.io/crates/tokio) as an MQTT client's async
runtime implementation.
 */


use crate::client::*;
use crate::config::*;
use crate::error::{MqttError, MqttResult};
use crate::protocol::is_connection_established;
use log::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Instant, Duration};
use tokio::runtime;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, split, WriteHalf};
use tokio::sync::oneshot;
use tokio::time::{sleep};


impl From<oneshot::error::RecvError> for MqttError {
    fn from(err: oneshot::error::RecvError) -> Self {
        MqttError::new_operation_channel_failure(err)
    }
}

macro_rules! submit_async_client_operation {
    ($self:ident, $operation_type:ident, $options_internal_type: ident, $options_value: expr, $packet_value: expr) => ({

        let (response_sender, rx) = AsyncOperationChannel::new().split();
        let internal_options = $options_internal_type {
            options : $options_value.unwrap_or_default(),
            response_sender : Some(response_sender)
        };
        let send_result = $self.user_state.try_send(OperationOptions::$operation_type($packet_value, internal_options));
        Box::pin(async move {
            match send_result {
                Err(error) => {
                    Err(error)
                }
                _ => {
                    rx.recv().await?
                }
            }
        })
    })
}

pub(crate) use submit_async_client_operation;

pub(crate) struct AsyncOperationChannel<T> {
    sender: AsyncOperationSender<T>,
    receiver: AsyncOperationReceiver<T>
}

impl <T> AsyncOperationChannel<T> {
    pub fn new() -> Self {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        AsyncOperationChannel {
            sender: AsyncOperationSender::new(sender),
            receiver: AsyncOperationReceiver::new(receiver)
        }
    }

    pub fn split(self) -> (AsyncOperationSender<T>, AsyncOperationReceiver<T>) {
        (self.sender, self.receiver)
    }
}

pub(crate) struct AsyncOperationSender<T> {
    sender: tokio::sync::oneshot::Sender<T>
}

impl <T> AsyncOperationSender<T> {

    fn new(sender: oneshot::Sender<T>) -> Self {
        AsyncOperationSender {
            sender
        }
    }

    pub(crate) fn send(self, operation_options: T) -> MqttResult<()> {
        if self.sender.send(operation_options).is_err() {
            return Err(MqttError::new_operation_channel_failure("failed to submit MQTT operation result to operation result channel"));
        }

        Ok(())
    }
}

/// tokio-specific client event channel receiver
pub struct AsyncOperationReceiver<T> {
    receiver: oneshot::Receiver<T>
}

impl <T> AsyncOperationReceiver<T> {

    fn new(receiver: oneshot::Receiver<T>) -> Self {
        AsyncOperationReceiver {
            receiver
        }
    }

    /// Async function that waits for the next client event sent to this receiver
    pub async fn recv(self) -> MqttResult<T> {
        match self.receiver.await {
            Err(_) => { Err(MqttError::new_operation_channel_failure("no result pending on operation result channel")) }
            Ok(val) => { Ok(val) }
        }
    }

    #[cfg(test)]
    pub fn try_recv(&mut self) -> MqttResult<T> {
        match self.receiver.try_recv() {
            Err(_) => {
                Err(MqttError::new_operation_channel_failure("no operation result pending on operation result channel"))
            }
            Ok(val) => {
                Ok(val)
            }

        }
    }

    #[cfg(test)]
    pub fn blocking_recv(self) -> MqttResult<T> {
        match self.receiver.blocking_recv() {
            Err(_) => {
                Err(MqttError::new_operation_channel_failure("no operation result pending on operation result channel"))
            }
            Ok(val) => {
                Ok(val)
            }

        }
    }
}


pub(crate) struct UserRuntimeState {
    operation_sender: tokio::sync::mpsc::Sender<OperationOptions>
}

impl UserRuntimeState {
    pub(crate) fn try_send(&self, operation_options: OperationOptions) -> MqttResult<()> {
        if self.operation_sender.try_send(operation_options).is_err() {
            return Err(MqttError::new_operation_channel_failure("failed to submit MQTT operation to client operation channel"));
        }

        Ok(())
    }
}

pub(crate) struct ClientRuntimeState<T> where T : AsyncRead + AsyncWrite + Send + Sync + 'static {
    tokio_config: TokioClientOptions<T>,
    operation_receiver: tokio::sync::mpsc::Receiver<OperationOptions>,
    stream: Option<T>
}

impl<T> ClientRuntimeState<T> where T : AsyncRead + AsyncWrite + Send + Sync + 'static {
    pub(crate) async fn process_stopped(&mut self, client: &mut Mqtt5ClientImpl) -> MqttResult<ClientImplState> {
        loop {
            trace!("tokio - process_stopped loop");

            tokio::select! {
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        debug!("tokio - process_stopped - user operation received");
                        client.handle_incoming_operation(operation_options, Instant::now());
                    }
                }
            }

            if let Some(transition_state) = client.compute_optional_state_transition() {
                return Ok(transition_state);
            }
        }
    }

    pub(crate) async fn process_connecting(&mut self, client: &mut Mqtt5ClientImpl) -> MqttResult<ClientImplState> {
        let mut connect = (self.tokio_config.connection_factory)();

        let timeout = sleep(*client.connect_timeout());
        tokio::pin!(timeout);

        loop {
            trace!("tokio - process_connecting loop");

            tokio::select! {
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        debug!("tokio - process_connecting - user operation received");
                        client.handle_incoming_operation(operation_options, Instant::now());
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

    pub(crate) async fn process_connected(&mut self, client: &mut Mqtt5ClientImpl) -> MqttResult<ClientImplState> {
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

            let next_service_time_option = client.get_next_connected_service_time(Instant::now());
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
                        client.handle_incoming_operation(operation_options, Instant::now());
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
                            } else if let Err(error) = client.handle_incoming_bytes(&inbound_data[..bytes_read], Instant::now()) {
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
                    if let Err(error) = client.handle_service(&mut outbound_data, Instant::now()) {
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
                                if let Err(error) = client.handle_write_completion(Instant::now()) {
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

    pub(crate) async fn process_pending_reconnect(&mut self, client: &mut Mqtt5ClientImpl, wait: Duration) -> MqttResult<ClientImplState> {
        let reconnect_timer = sleep(wait);
        tokio::pin!(reconnect_timer);

        loop {
            trace!("tokio - process_pending_reconnect loop");

            tokio::select! {
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        debug!("tokio - process_pending_reconnect - user operation received");
                        client.handle_incoming_operation(operation_options, Instant::now());
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

pub(crate) fn create_runtime_states<T>(tokio_config: TokioClientOptions<T>) -> (UserRuntimeState, ClientRuntimeState<T>) where T : AsyncRead + AsyncWrite + Send + Sync + 'static {
    let (sender, receiver) = tokio::sync::mpsc::channel(100);

    let user_state = UserRuntimeState {
        operation_sender: sender
    };

    let impl_state = ClientRuntimeState {
        tokio_config,
        operation_receiver: receiver,
        stream: None
    };

    (user_state, impl_state)
}

async fn client_event_loop<T>(client_impl: &mut Mqtt5ClientImpl, async_state: &mut ClientRuntimeState<T>) where T : AsyncRead + AsyncWrite + Send + Sync + 'static {
    let mut done = false;
    while !done {
        let current_state = client_impl.get_current_state();
        let next_state_result =
            match current_state {
                ClientImplState::Stopped => { async_state.process_stopped(client_impl).await }
                ClientImplState::Connecting => { async_state.process_connecting(client_impl).await }
                ClientImplState::Connected => { async_state.process_connected(client_impl).await }
                ClientImplState::PendingReconnect => {
                    let reconnect_wait = client_impl.compute_reconnect_period();
                    async_state.process_pending_reconnect(client_impl, reconnect_wait).await
                }
                _ => { Ok(ClientImplState::Shutdown) }
            };

        done = true;
        if let Ok(next_state) = next_state_result {
            if client_impl.transition_to_state(next_state, Instant::now()).is_ok() && (next_state != ClientImplState::Shutdown) {
                done = false;
            }
        }
    }

    info!("Async client loop exiting");
}

pub(crate) fn spawn_client_impl<T>(
    mut client_impl: Mqtt5ClientImpl,
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

impl Mqtt5Client {

    /// Creates a new async MQTT5 client that will use the tokio async runtime
    pub fn new_with_tokio<T>(client_config: Mqtt5ClientOptions, connect_config: ConnectOptions, tokio_config: TokioClientOptions<T>, runtime_handle: &runtime::Handle) -> Mqtt5Client where T: AsyncRead + AsyncWrite + Send + Sync + 'static {
        let (user_state, internal_state) = create_runtime_states(tokio_config);

        let client_impl = Mqtt5ClientImpl::new(client_config, connect_config, Instant::now());

        spawn_client_impl(client_impl, internal_state, runtime_handle);

        Mqtt5Client {
            user_state,
            listener_id_allocator: Mutex::new(1),
        }
    }
}

use crate::client::waiter::*;

/// Simple debug type that uses the client listener framework to allow tests to asynchronously wait for
/// configurable client event sequences.  May be useful outside of tests.  May need polish.  Currently public
/// because we use it across crates.  May eventually go internal.
///
/// Requires the client to be Arc-wrapped.
pub struct ClientEventWaiter {
    event_count: usize,

    client: Arc<Mqtt5Client>,

    listener: Option<ListenerHandle>,

    event_receiver: tokio::sync::mpsc::UnboundedReceiver<Arc<ClientEvent>>,

    events: Vec<Arc<ClientEvent>>,
}

impl ClientEventWaiter {

    /// Creates a new ClientEventWaiter instance from full configuration
    pub fn new(client: Arc<Mqtt5Client>, config: ClientEventWaiterOptions, event_count: usize) -> Self {
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

            let _ = tx.send(event.clone());
        };

        waiter.listener = Some(client.add_event_listener(Arc::new(listener_fn)).unwrap());
        waiter
    }

    /// Creates a new ClientEventWaiter instance that will wait for a single occurrence of a single event type
    pub fn new_single(client: Arc<Mqtt5Client>, event_type: ClientEventType) -> Self {
        let config = ClientEventWaiterOptions {
            wait_type: ClientEventWaitType::Type(event_type),
        };

        Self::new(client, config, 1)
    }

    /// Waits for and returns an event sequence that matches the original configuration
    pub async fn wait(&mut self) -> MqttResult<Vec<Arc<ClientEvent>>> {
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

#[cfg(test)]
pub(crate) mod testing {
    use assert_matches::assert_matches;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::time::Duration;
    use crate::client::*;
    use crate::client::waiter::*;
    use crate::config::*;
    use crate::error::{MqttError, MqttResult};
    use crate::mqtt::*;
    use super::*;
    use crate::testing::integration::*;

    fn create_client_builder_internal(connect_options: ConnectOptions, tls_config: TlsUsage, ws_config: WebsocketUsage, proxy_config: ProxyUsage, tls_endpoint: TlsUsage, ws_endpoint: WebsocketUsage) -> GenericClientBuilder {
        let client_config = Mqtt5ClientOptionsBuilder::new()
            .with_connect_timeout(Duration::from_secs(5))
            .with_offline_queue_policy(OfflineQueuePolicy::PreserveAll)
            .build();

        let endpoint = get_broker_endpoint(tls_endpoint, ws_endpoint);
        let port = get_broker_port(tls_endpoint, ws_endpoint);

        let mut builder = GenericClientBuilder::new(&endpoint, port);
        builder.with_connect_options(connect_options);
        builder.with_client_options(client_config);

        if tls_config != TlsUsage::None {
            let mut tls_options_builder = TlsOptionsBuilder::new();
            tls_options_builder.with_verify_peer(false);
            tls_options_builder.with_root_ca_from_path(&get_ca_path()).unwrap();

            builder.with_tls_options(tls_options_builder.build_rustls().unwrap());
        }

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

    fn do_builder_test(test_factory: AsyncTestFactory, builder: GenericClientBuilder) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let test_future = (*test_factory)(builder);

        runtime.block_on(test_future).unwrap();
    }

    async fn start_client(client: &Arc<Mqtt5Client>) -> MqttResult<()> {
        let mut connection_attempt_waiter = ClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionAttempt);
        let mut connection_success_waiter = ClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionSuccess);

        client.start(None)?;

        connection_attempt_waiter.wait().await?;
        let connection_success_events = connection_success_waiter.wait().await?;
        assert_eq!(1, connection_success_events.len());
        let connection_success_event = connection_success_events[0].clone();
        assert_matches!(*connection_success_event, ClientEvent::ConnectionSuccess(_));
        if let ClientEvent::ConnectionSuccess(success_event) = &*connection_success_event {
            assert_eq!(ConnectReasonCode::Success, success_event.connack.reason_code);
        } else {
            panic!("impossible");
        }

        Ok(())
    }

    async fn stop_client(client: &Arc<Mqtt5Client>) -> MqttResult<()> {
        let mut disconnection_waiter = ClientEventWaiter::new_single(client.clone(), ClientEventType::Disconnection);
        let mut stopped_waiter = ClientEventWaiter::new_single(client.clone(), ClientEventType::Stopped);

        client.stop(None)?;

        let disconnect_events = disconnection_waiter.wait().await?;
        assert_eq!(1, disconnect_events.len());
        let disconnect_event = disconnect_events[0].clone();
        assert_matches!(*disconnect_event, ClientEvent::Disconnection(_));
        if let ClientEvent::Disconnection(event) = &*disconnect_event {
            assert_matches!(event.error, MqttError::UserInitiatedDisconnect(_));
        } else {
            panic!("impossible");
        }

        stopped_waiter.wait().await?;

        Ok(())
    }

    async fn tokio_connect_disconnect_test(builder: GenericClientBuilder) -> MqttResult<()> {
        let client = Arc::new(builder.build(&tokio::runtime::Handle::current()).unwrap());

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
    fn client_connect_disconnect_direct_rustls_no_proxy() {
        do_good_client_test(TlsUsage::Rustls, WebsocketUsage::None, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    fn client_connect_disconnect_websocket_plaintext_no_proxy() {
        do_good_client_test(TlsUsage::None, WebsocketUsage::Tungstenite, ProxyUsage::None, Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    fn client_connect_disconnect_websocket_rustls_no_proxy() {
        do_good_client_test(TlsUsage::Rustls, WebsocketUsage::Tungstenite, ProxyUsage::None, Box::new(|builder|{
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
    fn client_connect_disconnect_direct_rustls_with_proxy() {
        do_good_client_test(TlsUsage::Rustls, WebsocketUsage::None, ProxyUsage::Plaintext, Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    fn client_connect_disconnect_websocket_plaintext_with_proxy() {
        do_good_client_test(TlsUsage::None, WebsocketUsage::Tungstenite, ProxyUsage::Plaintext, Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    #[test]
    fn client_connect_disconnect_websocket_rustls_with_proxy() {
        do_good_client_test(TlsUsage::Rustls, WebsocketUsage::Tungstenite, ProxyUsage::Plaintext, Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    async fn tokio_subscribe_unsubscribe_test(builder: GenericClientBuilder) -> MqttResult<()> {
        let client = Arc::new(builder.build(&tokio::runtime::Handle::current()).unwrap());
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
        let client = Arc::new(builder.build(&tokio::runtime::Handle::current()).unwrap());
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
        let publish_received_event = publish_received_events[0].clone();
        assert_matches!(*publish_received_event, ClientEvent::PublishReceived(_));
        if let ClientEvent::PublishReceived(event) = &*publish_received_event {
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
        let client = Arc::new(builder.build(&tokio::runtime::Handle::current()).unwrap());
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
        let will_client = Arc::new(will_builder.build(&tokio::runtime::Handle::current()).unwrap());

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
        let publish_received_event = publish_received_events[0].clone();
        assert_matches!(*publish_received_event, ClientEvent::PublishReceived(_));
        if let ClientEvent::PublishReceived(event) = &*publish_received_event {
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
        let client = Arc::new(builder.build(&tokio::runtime::Handle::current()).unwrap());
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
        let client = Arc::new(builder.build(&tokio::runtime::Handle::current()).unwrap());
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
    fn connection_failure_direct_tls_config_direct_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::None, TlsUsage::None, WebsocketUsage::None);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    fn connection_failure_direct_tls_config_websocket_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::None, TlsUsage::None, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    fn connection_failure_direct_tls_config_websocket_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::None, TlsUsage::Rustls, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    fn connection_failure_direct_plaintext_config_direct_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::None, TlsUsage::Rustls, WebsocketUsage::None);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    fn connection_failure_direct_plaintext_config_websocket_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::None, TlsUsage::None, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    fn connection_failure_direct_plaintext_config_websocket_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::None, TlsUsage::Rustls, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    fn connection_failure_websocket_tls_config_direct_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::Tungstenite, TlsUsage::None, WebsocketUsage::None);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    fn connection_failure_websocket_tls_config_websocket_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::Tungstenite, TlsUsage::None, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    fn connection_failure_websocket_tls_config_direct_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::Rustls, WebsocketUsage::Tungstenite, TlsUsage::Rustls, WebsocketUsage::None);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    //

    #[test]
    fn connection_failure_websocket_plaintext_config_direct_plaintext_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::Tungstenite, TlsUsage::None, WebsocketUsage::None);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    fn connection_failure_websocket_plaintext_config_websocket_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::Tungstenite, TlsUsage::Rustls, WebsocketUsage::Tungstenite);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    fn connection_failure_websocket_plaintext_config_direct_tls_endpoint() {
        let builder = create_mismatch_builder(TlsUsage::None, WebsocketUsage::Tungstenite, TlsUsage::Rustls, WebsocketUsage::None);
        do_builder_test(Box::new(|builder| {
            Box::pin(connection_failure_test(builder))
        }), builder);
    }

    #[test]
    fn connection_failure_invalid_endpoint() {
        let client_options = Mqtt5ClientOptionsBuilder::new()
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