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
use ::tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, split, WriteHalf};
use ::tokio::net::TcpStream;
use ::tokio::runtime::Handle;
use ::tokio::sync::mpsc::UnboundedSender;
use ::tokio::time::{sleep};

pub(crate) struct ClientRuntimeState<T> where T : AsyncRead + AsyncWrite + Send + Sync + 'static {
    connection_factory: TokioConnectionFactory<T>,
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
        let mut connect = (self.connection_factory)();

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
    runtime_handle: Handle,
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

pub type TokioConnectionFactoryReturnType<T> = Pin<Box<dyn Future<Output = GneissResult<T>> + Send>>;

pub type TokioConnectionFactory<T> = Box<dyn Fn() -> TokioConnectionFactoryReturnType<T> + Send + Sync>;

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

    fn start(&self, default_listener: Option<Arc<ClientEventListenerCallback>>) -> GneissResult<()> {
        info!("tokio client start invoked");
        if let Err(send_error) = self.operation_sender.send(OperationOptions::Start(default_listener)) {
            return Err(GneissError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }

    fn stop(&self, options: Option<StopOptions>) -> GneissResult<()> {
        info!("tokio client stop invoked {} a disconnect packet", if options.as_ref().is_some_and(|opts| { opts.disconnect.is_some()}) { "with" } else { "without" });
        let options = options.unwrap_or_default();

        if let Some(disconnect) = &options.disconnect {
            validate_disconnect_packet_outbound(disconnect)?;
        }

        let mut stop_options = StopOptionsInternal {
            disconnect : None,
        };

        if let Some(disconnect) = options.disconnect {
            stop_options.disconnect = Some(Box::new(MqttPacket::Disconnect(disconnect)));
        }

        if let Err(send_error) = self.operation_sender.send(OperationOptions::Stop(stop_options)) {
            return Err(GneissError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }

    fn close(&self) -> GneissResult<()> {
        info!("tokio client close invoked; no further operations allowed");
        if let Err(send_error) = self.operation_sender.send(OperationOptions::Shutdown()) {
            return Err(GneissError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }

    fn publish(&self, packet: PublishPacket, options: Option<PublishOptions>) -> AsyncPublishResult {
        debug!("tokio client - publish operation submitted");
        let boxed_packet = Box::new(MqttPacket::Publish(packet));
        if let Err(error) = validate_packet_outbound(&boxed_packet) {
            return Box::pin(async move { Err(error) });
        }

        submit_tokio_operation!(self, Publish, PublishOptionsInternal, options, boxed_packet)
    }

    fn subscribe(&self, packet: SubscribePacket, options: Option<SubscribeOptions>) -> AsyncSubscribeResult {
        debug!("tokio client - subscribe operation submitted");
        let boxed_packet = Box::new(MqttPacket::Subscribe(packet));
        if let Err(error) = validate_packet_outbound(&boxed_packet) {
            return Box::pin(async move { Err(error) });
        }

        submit_tokio_operation!(self, Subscribe, SubscribeOptionsInternal, options, boxed_packet)
    }

    fn unsubscribe(&self, packet: UnsubscribePacket, options: Option<UnsubscribeOptions>) -> AsyncUnsubscribeResult {
        debug!("tokio client - unsubscribe operation submitted");
        let boxed_packet = Box::new(MqttPacket::Unsubscribe(packet));
        if let Err(error) = validate_packet_outbound(&boxed_packet) {
            return Box::pin(async move { Err(error) });
        }

        submit_tokio_operation!(self, Unsubscribe, UnsubscribeOptionsInternal, options, boxed_packet)
    }

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

    fn remove_event_listener(&self, listener: ListenerHandle) -> GneissResult<()> {
        debug!("tokio client - remove listener operation submitted");
        if let Err(send_error) = self.operation_sender.send(OperationOptions::RemoveListener(listener.id)) {
            return Err(GneissError::new_operation_channel_failure(send_error));
        }

        Ok(())
    }
}

pub(crate) fn create_runtime_states<T>(connection_factory: TokioConnectionFactory<T>) -> (UnboundedSender<OperationOptions>, ClientRuntimeState<T>) where T : AsyncRead + AsyncWrite + Send + Sync + 'static {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

    let impl_state = ClientRuntimeState {
        connection_factory,
        operation_receiver: receiver,
        stream: None
    };

    (sender, impl_state)
}

fn new_tokio_client<T>(client_config: MqttClientOptions, connect_config: ConnectOptions, tokio_options: TokioOptions, connection_factory: TokioConnectionFactory<T>) -> AsyncClientHandle
where T: AsyncRead + AsyncWrite + Send + Sync + 'static {
    let (operation_sender, internal_state) = create_runtime_states(connection_factory);

    let callback_spawner : CallbackSpawnerFunction = Box::new(|event, callback| {
        spawn_event_callback(event, callback)
    });

    let client_impl = MqttClientImpl::new(client_config, connect_config, callback_spawner);

    spawn_client_impl(client_impl, internal_state, tokio_options.runtime);

    AsyncClientHandle::new(
        Arc::new(TokioClient{
            operation_sender,
            listener_id_allocator: Mutex::new(1),
        })
    )
}

/// Tokio client connection factory that creates a plaintext TCP stream to a specified
/// host and port
pub fn make_tokio_tcp_stream_connection_factory(endpoint: String, port: u16) -> TokioConnectionFactory<TcpStream> {
    Box::new(move || {
        Box::pin(make_tcp_stream(endpoint.clone(), port))
    })
}

async fn make_tcp_stream(endpoint: String, port: u16) -> GneissResult<TcpStream> {
    let addr = make_addr(endpoint.as_str(), port)?;
    debug!("make_leaf_stream - opening TCP stream");
    let stream = TcpStream::connect(&addr).await?;
    debug!("make_leaf_stream - TCP stream successfully established");

    Ok(stream)
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
    use crate::testing::integration::*;
    use super::*;

    fn create_plaintext_client_from_builder(builder: TokioClientBuilder) -> AsyncClientHandle {
        let connection_factory = make_tokio_tcp_stream_connection_factory(get_broker_endpoint(), get_broker_port());
        builder.build(connection_factory).unwrap()
    }

    fn do_good_client_test(handle: Handle, test_factory: TokioTestFactory) {
        let tokio_options = TokioOptions::builder(handle.clone()).build();

        let mut tokio_builder = create_good_tokio_client_builder();
        tokio_builder.with_tokio_options(tokio_options);

        let test_future = (test_factory)(tokio_builder);

        handle.block_on(test_future).unwrap();
    }

    async fn tokio_connect_disconnect_test(builder: TokioClientBuilder) -> GneissResult<()> {
        let client = create_plaintext_client_from_builder(builder);

        start_async_client(&client).await?;
        stop_async_client(&client).await?;

        Ok(())
    }

    #[test]
    fn client_connect_disconnect_direct_plaintext_no_proxy() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        do_good_client_test(handle.clone(), Box::new(move |builder|{
            Box::pin(tokio_connect_disconnect_test(builder))
        }));
    }

    async fn tokio_subscribe_unsubscribe_test(builder: TokioClientBuilder) -> GneissResult<()> {
        async_subscribe_unsubscribe_test(create_plaintext_client_from_builder(builder)).await
    }

    #[test]
    fn client_subscribe_unsubscribe() {
        let runtime = ::tokio::runtime::Runtime::new().unwrap();
        do_good_client_test(runtime.handle().clone(), Box::new(|builder|{
            Box::pin(tokio_subscribe_unsubscribe_test(builder))
        }));
    }

    async fn tokio_subscribe_publish_test(builder: TokioClientBuilder, qos: QualityOfService) -> GneissResult<()> {
        let client = create_plaintext_client_from_builder(builder);
        async_subscribe_publish_test(client, qos).await
    }

    #[test]
    fn client_subscribe_publish_qos0() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        do_good_client_test(runtime.handle().clone(), Box::new(|builder|{
            Box::pin(tokio_subscribe_publish_test(builder, QualityOfService::AtMostOnce))
        }));
    }

    #[test]
    fn client_subscribe_publish_qos1() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        do_good_client_test(runtime.handle().clone(), Box::new(|builder|{
            Box::pin(tokio_subscribe_publish_test(builder, QualityOfService::AtLeastOnce))
        }));
    }

    #[test]
    fn client_subscribe_publish_qos2() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        do_good_client_test(runtime.handle().clone(), Box::new(|builder|{
            Box::pin(tokio_subscribe_publish_test(builder, QualityOfService::ExactlyOnce))
        }));
    }

    // This primarily tests that the will configuration works.  Will functionality is mostly broker-side.
    async fn tokio_will_test(builder: TokioClientBuilder) -> GneissResult<()> {
        async_will_test(builder, create_plaintext_client_from_builder).await
    }

    #[test]
    fn client_will_sent() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        do_good_client_test(runtime.handle().clone(), Box::new(|builder|{
            Box::pin(tokio_will_test(builder))
        }));
    }

    async fn tokio_connect_disconnect_cycle_session_rejoin_test(builder: TokioClientBuilder) -> GneissResult<()> {
        let client = create_plaintext_client_from_builder(builder);
        async_connect_disconnect_cycle_session_rejoin_test(client).await
    }

    #[test]
    fn connect_disconnect_cycle_session_rejoin() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        do_good_client_test(runtime.handle().clone(), Box::new(|builder|{
            Box::pin(tokio_connect_disconnect_cycle_session_rejoin_test(builder))
        }));
    }

    pub(crate) fn do_builder_test(handle: Handle, test_factory: TokioTestFactory, mut builder: TokioClientBuilder) {
        let tokio_options = TokioOptions::builder(handle.clone()).build();
        builder.with_tokio_options(tokio_options);

        let test_future = (test_factory)(builder);

        handle.block_on(test_future).unwrap();
    }

    async fn connection_failure_test(builder : TokioClientBuilder, host: &str, port: u16) -> GneissResult<()> {
        let client = builder.build(make_tokio_tcp_stream_connection_factory(host.to_string(), port)).unwrap();
        let connection_failure_waiter = TokioClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionFailure);

        client.start(None)?;

        let connection_failure_results = connection_failure_waiter.wait().await?;
        assert_eq!(1, connection_failure_results.len());

        Ok(())
    }

    #[test]
    fn connection_failure_invalid_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();

        let client_options = MqttClientOptionsBuilder::new()
            .with_connect_timeout(Duration::from_secs(3))
            .build();

        let mut builder = TokioClientBuilder::new();
        builder.with_client_options(client_options);

        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder, "example.com", 8000))
        }), builder);
    }

    #[test]
    fn connection_failure_invalid_endpoint_http() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let handle = runtime.handle().clone();
        let builder = TokioClientBuilder::new();
        do_builder_test(handle.clone(), Box::new(move |builder| {
            Box::pin(connection_failure_test(builder, "amazon.com", 443))
        }), builder);
    }
}