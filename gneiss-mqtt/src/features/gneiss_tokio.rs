/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Functionality for using [`tokio`](https://crates.io/crates/tokio) as an MQTT client's async
runtime implementation.
 */


use crate::*;
use crate::client::*;
use crate::client::shared_impl::*;
use crate::config::*;
use crate::error::{MqttError, MqttResult};
use crate::protocol::is_connection_established;
use log::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
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

#[cfg(test)]
pub(crate) mod testing {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::time::Duration;
    use crate::client::{ClientEvent, ListenerHandle, Mqtt5Client};
    use crate::config::{ConnectOptionsBuilder, GenericClientBuilder, HttpProxyOptionsBuilder, Mqtt5ClientOptionsBuilder, OfflineQueuePolicy, RejoinSessionPolicy, TlsOptionsBuilder, WebsocketOptionsBuilder};
    use crate::error::{MqttError, MqttResult};
    use crate::testing::integration::*;

    struct TokioClientEventWaiter {
        event_count: usize,

        client: Arc<Mqtt5Client>,

        listener: Option<ListenerHandle>,

        event_receiver: tokio::sync::mpsc::UnboundedReceiver<Arc<ClientEvent>>,

        events: Vec<Arc<ClientEvent>>,
    }

    impl TokioClientEventWaiter {
        pub(crate) fn new(client: Arc<Mqtt5Client>, config: ClientEventWaiterOptions, event_count: usize) -> Self {
            let event_type = config.event_type;

            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

            let mut waiter = TokioClientEventWaiter {
                event_count,
                client: client.clone(),
                listener: None,
                event_receiver: rx,
                events: Vec::new(),
            };

            let listener_fn = move |event: Arc<ClientEvent>| {
                if !client_event_matches(&event, event_type) {
                    return;
                }

                if let Some(event_predicate) = &config.event_predicate {
                    if !(*event_predicate)(&event) {
                        return;
                    }
                }

                let _ = tx.send(event.clone());
            };

            waiter.listener = Some(client.add_event_listener(Arc::new(listener_fn)).unwrap());
            waiter
        }

        pub fn new_single(client: Arc<Mqtt5Client>, event_type: ClientEventType) -> Self {
            let config = ClientEventWaiterOptions {
                event_type,
                event_predicate: None
            };

            Self::new(client, config, 1)
        }

        pub(crate) async fn wait(&mut self) -> MqttResult<Vec<Arc<ClientEvent>>> {
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

    impl Drop for TokioClientEventWaiter {
        fn drop(&mut self) {
            let listener_handler = self.listener.take().unwrap();

            let _ = self.client.remove_event_listener(listener_handler);
        }
    }


    fn build_tokio_client(runtime: &tokio::runtime::Runtime, tls: TlsUsage, ws: WebsocketUsage, proxy: ProxyUsage) -> Mqtt5Client {
        let connect_options = ConnectOptionsBuilder::new()
            .with_rejoin_session_policy(RejoinSessionPolicy::PostSuccess)
            .build();

        let client_config = Mqtt5ClientOptionsBuilder::new()
            .with_connect_timeout(Duration::from_secs(5))
            .with_offline_queue_policy(OfflineQueuePolicy::PreserveAll)
            .build();

        let endpoint = get_broker_endpoint(tls, ws);
        let port = get_broker_port(tls, ws);

        let mut builder = GenericClientBuilder::new(&endpoint, port);
        builder.with_connect_options(connect_options);
        builder.with_client_options(client_config);

        if tls != TlsUsage::None {
            let mut tls_options_builder = TlsOptionsBuilder::new();
            tls_options_builder.with_verify_peer(false);
            tls_options_builder.with_root_ca_from_path(&get_ca_path()).unwrap();

            builder.with_tls_options(tls_options_builder.build_rustls().unwrap());
        }

        if ws != WebsocketUsage::None {
            let websocket_options = WebsocketOptionsBuilder::new().build();
            builder.with_websocket_options(websocket_options);
        }

        if proxy != ProxyUsage::None {
            let proxy_endpoint = get_proxy_endpoint();
            let proxy_port = get_proxy_port();
            let proxy_options = HttpProxyOptionsBuilder::new(&proxy_endpoint, proxy_port).build();
            builder.with_http_proxy_options(proxy_options);
        }

        builder.build(runtime.handle()).unwrap()
    }

    type AsyncClientTestFactoryReturnType = Pin<Box<dyn Future<Output = MqttResult<()>> + Send>>;
    type AsyncClientTestFactory = Box<dyn Fn(Arc<Mqtt5Client>) -> AsyncClientTestFactoryReturnType + Send + Sync>;

    fn do_tokio_client_test(tls: TlsUsage, ws: WebsocketUsage, proxy: ProxyUsage, test_factory: AsyncClientTestFactory) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let client = Arc::new(build_tokio_client(&runtime, tls, ws, proxy));
        let test_future = (*test_factory)(client);

        runtime.block_on(test_future).unwrap();
    }

    async fn tokio_connect_disconnect_test(client: Arc<Mqtt5Client>) -> MqttResult<()> {
        let mut connection_attempt_waiter = TokioClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionAttempt);
        let mut connection_success_waiter = TokioClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionSuccess);

        client.start(None).ok();

        connection_attempt_waiter.wait().await.ok();
        connection_success_waiter.wait().await.ok();

        let mut disconnection_waiter = TokioClientEventWaiter::new_single(client.clone(), ClientEventType::Disconnection);
        let mut stopped_waiter = TokioClientEventWaiter::new_single(client.clone(), ClientEventType::Stopped);

        client.stop(None).ok();

        disconnection_waiter.wait().await.ok();
        stopped_waiter.wait().await.ok();

        Ok(())
    }

    // subscribe-unsubscribe
    // subscribe-publish-qos0
    // subscribe-publish-qos1
    // subscribe-publish-qos2
    // will check


    #[test]
    fn tokio_client_connect_disconnect_direct_plaintext_no_proxy() {
        do_tokio_client_test(TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|client|{
            Box::pin(tokio_connect_disconnect_test(client))
        }));
    }

    #[test]
    fn tokio_client_connect_disconnect_direct_rustls_no_proxy() {
        do_tokio_client_test(TlsUsage::Rustls, WebsocketUsage::None, ProxyUsage::None, Box::new(|client|{
            Box::pin(tokio_connect_disconnect_test(client))
        }));
    }

    #[test]
    fn tokio_client_connect_disconnect_websocket_plaintext_no_proxy() {
        do_tokio_client_test(TlsUsage::None, WebsocketUsage::Tungstenite, ProxyUsage::None, Box::new(|client|{
            Box::pin(tokio_connect_disconnect_test(client))
        }));
    }

    #[test]
    fn tokio_client_connect_disconnect_websocket_rustls_no_proxy() {
        do_tokio_client_test(TlsUsage::Rustls, WebsocketUsage::Tungstenite, ProxyUsage::None, Box::new(|client|{
            Box::pin(tokio_connect_disconnect_test(client))
        }));
    }

    #[test]
    fn tokio_client_connect_disconnect_direct_plaintext_with_proxy() {
        do_tokio_client_test(TlsUsage::None, WebsocketUsage::None, ProxyUsage::Plaintext, Box::new(|client|{
            Box::pin(tokio_connect_disconnect_test(client))
        }));
    }

    #[test]
    fn tokio_client_connect_disconnect_direct_rustls_with_proxy() {
        do_tokio_client_test(TlsUsage::Rustls, WebsocketUsage::None, ProxyUsage::Plaintext, Box::new(|client|{
            Box::pin(tokio_connect_disconnect_test(client))
        }));
    }

    #[test]
    fn tokio_client_connect_disconnect_websocket_plaintext_with_proxy() {
        do_tokio_client_test(TlsUsage::None, WebsocketUsage::Tungstenite, ProxyUsage::Plaintext, Box::new(|client|{
            Box::pin(tokio_connect_disconnect_test(client))
        }));
    }

    #[test]
    fn tokio_client_connect_disconnect_websocket_rustls_with_proxy() {
        do_tokio_client_test(TlsUsage::Rustls, WebsocketUsage::Tungstenite, ProxyUsage::Plaintext, Box::new(|client|{
            Box::pin(tokio_connect_disconnect_test(client))
        }));
    }
}