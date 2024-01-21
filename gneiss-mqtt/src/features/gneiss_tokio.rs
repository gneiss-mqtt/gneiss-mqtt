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
            tokio::select! {
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        client.handle_incoming_operation(operation_options);
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

        let timeout = sleep(Duration::from_millis(30 * 1000));
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        client.handle_incoming_operation(operation_options);
                    }
                }
                () = &mut timeout => {
                    client.apply_error(MqttError::ConnectionTimeout);
                    return Ok(ClientImplState::PendingReconnect);
                }
                connection_result = &mut connect => {
                    if let Ok(stream) = connection_result {
                        self.stream = Some(stream);
                        return Ok(ClientImplState::Connected);
                    } else {
                        client.apply_error(MqttError::ConnectionEstablishmentFailure);
                        return Ok(ClientImplState::PendingReconnect);
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
            let next_service_time_option = client.get_next_connected_service_time();
            let service_wait: Option<tokio::time::Sleep> = next_service_time_option.map(|next_service_time| sleep(next_service_time - Instant::now()));

            let outbound_slice_option: Option<&[u8]> =
                if cumulative_bytes_written < outbound_data.len() {
                    Some(&outbound_data[cumulative_bytes_written..])
                } else {
                    None
                };

            if should_flush {
                write_directive = Some(WriteDirective::Flush);
            } else if outbound_slice_option.is_some() {
                write_directive = Some(WriteDirective::Bytes(outbound_slice_option.unwrap()))
            } else {
                write_directive = None;
            }

            tokio::select! {
                // incoming user operations future
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        client.handle_incoming_operation(operation_options);
                    }
                }
                // incoming data on the socket future
                read_result = stream_reader.read(inbound_data.as_mut_slice()) => {
                    match read_result {
                        Ok(bytes_read) => {
                            if bytes_read == 0 {
                                client.apply_error(MqttError::ConnectionClosed);
                                next_state = Some(ClientImplState::PendingReconnect);
                            } else if client.handle_incoming_bytes(&inbound_data[..bytes_read]).is_err() {
                                next_state = Some(ClientImplState::PendingReconnect);
                            }
                        }
                        Err(_) => {
                            client.apply_error(MqttError::StreamReadFailure);
                            next_state = Some(ClientImplState::PendingReconnect);
                        }
                    }
                }
                // client service future (if relevant)
                Some(_) = conditional_wait(service_wait) => {
                    if client.handle_service(&mut outbound_data).is_err() {
                        next_state = Some(ClientImplState::PendingReconnect);
                    }
                }
                // outbound data future (if relevant)
                Some(bytes_written_result) = conditional_write(write_directive, &mut stream_writer) => {
                    match bytes_written_result {
                        Ok(bytes_written) => {
                            if should_flush {
                                should_flush = false;
                                if client.handle_write_completion().is_err() {
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
                        Err(_) => {
                            client.apply_error(MqttError::StreamWriteFailure);
                            next_state = Some(ClientImplState::PendingReconnect);
                        }
                    }
                }
            }

            if next_state.is_none() {
                next_state = client.compute_optional_state_transition();
            }
        }

        let _ = stream_writer.shutdown().await;

        Ok(next_state.unwrap())
    }

    pub(crate) async fn process_pending_reconnect(&mut self, client: &mut Mqtt5ClientImpl, wait: Duration) -> MqttResult<ClientImplState> {
        let reconnect_timer = sleep(wait);
        tokio::pin!(reconnect_timer);

        loop {
            tokio::select! {
                operation_result = self.operation_receiver.recv() => {
                    if let Some(operation_options) = operation_result {
                        client.handle_incoming_operation(operation_options);
                    }
                }
                () = &mut reconnect_timer => {
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
            if client_impl.transition_to_state(next_state).is_ok() && (next_state != ClientImplState::Shutdown) {
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

type TokioConnectionFactoryReturnType<T> = Pin<Box<dyn Future<Output = std::io::Result<T>> + Send>>;

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

        let client_impl = Mqtt5ClientImpl::new(client_config, connect_config);

        spawn_client_impl(client_impl, internal_state, runtime_handle);

        Mqtt5Client {
            user_state,
            listener_id_allocator: Mutex::new(1),
        }
    }
}