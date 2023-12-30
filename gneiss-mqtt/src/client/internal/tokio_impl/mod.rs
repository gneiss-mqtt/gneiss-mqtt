/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate tokio;

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, split, WriteHalf};
use tokio::sync::oneshot;
use tokio::time::{sleep};

use crate::client::internal::*;

impl From<oneshot::error::RecvError> for Mqtt5Error {
    fn from(_: oneshot::error::RecvError) -> Self {
        Mqtt5Error::OperationChannelReceiveError
    }
}

macro_rules! submit_async_client_operation {
    ($self:ident, $operation_type:ident, $options_internal_type: ident, $options_value: expr, $packet_value: expr) => ({

        let (response_sender, rx) = AsyncOperationChannel::new().split();
        let internal_options = $options_internal_type {
            options : $options_value,
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

    pub(crate) fn send(self, operation_options: T) -> Mqtt5Result<()> {
        if self.sender.send(operation_options).is_err() {
            return Err(Mqtt5Error::OperationChannelSendError);
        }

        Ok(())
    }
}

pub struct AsyncOperationReceiver<T> {
    receiver: oneshot::Receiver<T>
}

impl <T> AsyncOperationReceiver<T> {

    fn new(receiver: oneshot::Receiver<T>) -> Self {
        AsyncOperationReceiver {
            receiver
        }
    }

    pub async fn recv(self) -> Mqtt5Result<T> {
        match self.receiver.await {
            Err(_) => { Err(Mqtt5Error::OperationChannelReceiveError) }
            Ok(val) => { Ok(val) }
        }
    }

    pub fn try_recv(&mut self) -> Mqtt5Result<T> {
        match self.receiver.try_recv() {
            Err(_) => {
                Err(Mqtt5Error::OperationChannelEmpty)
            }
            Ok(val) => {
                Ok(val)
            }

        }
    }

    pub fn blocking_recv(self) -> Mqtt5Result<T> {
        match self.receiver.blocking_recv() {
            Err(_) => {
                Err(Mqtt5Error::OperationChannelReceiveError)
            }
            Ok(val) => {
                Ok(val)
            }

        }
    }
}

pub struct AsyncClientEventChannel {
    sender: AsyncClientEventSender,
    receiver: AsyncClientEventReceiver
}

impl AsyncClientEventChannel {
    pub fn new() -> Self {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        AsyncClientEventChannel {
            sender: AsyncClientEventSender::new(sender),
            receiver: AsyncClientEventReceiver::new(receiver)
        }
    }

    pub fn split(self) -> (AsyncClientEventSender, AsyncClientEventReceiver) {
        (self.sender, self.receiver)
    }
}

impl Default for AsyncClientEventChannel {
    fn default() -> Self {
        Self::new()
    }
}

pub struct AsyncClientEventSender {
    sender: tokio::sync::mpsc::UnboundedSender<Arc<ClientEvent>>
}

impl AsyncClientEventSender {
    fn new(sender: tokio::sync::mpsc::UnboundedSender<Arc<ClientEvent>>) -> Self {
        AsyncClientEventSender {
            sender
        }
    }

    pub(crate) fn send(&self, event: Arc<ClientEvent>) -> Mqtt5Result<()> {
        if self.sender.send(event).is_err() {
            return Err(Mqtt5Error::OperationChannelSendError);
        }

        Ok(())
    }
}

pub struct AsyncClientEventReceiver {
    receiver: tokio::sync::mpsc::UnboundedReceiver<Arc<ClientEvent>>
}

impl AsyncClientEventReceiver {
    fn new(receiver: tokio::sync::mpsc::UnboundedReceiver<Arc<ClientEvent>>) -> Self {
        AsyncClientEventReceiver {
            receiver
        }
    }

    pub async fn recv(&mut self) -> Option<Arc<ClientEvent>> {
        self.receiver.recv().await
    }
}

pub(crate) struct UserRuntimeState {
    operation_sender: tokio::sync::mpsc::Sender<OperationOptions>
}

impl UserRuntimeState {
    pub(crate) fn try_send(&self, operation_options: OperationOptions) -> Mqtt5Result<()> {
        if self.operation_sender.try_send(operation_options).is_err() {
            return Err(Mqtt5Error::OperationChannelSendError);
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
    pub(crate) async fn process_stopped(&mut self, client: &mut Mqtt5ClientImpl) -> Mqtt5Result<ClientImplState> {
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

    pub(crate) async fn process_connecting(&mut self, client: &mut Mqtt5ClientImpl) -> Mqtt5Result<ClientImplState> {
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
                    client.apply_error(Mqtt5Error::ConnectionTimeout);
                    return Ok(ClientImplState::PendingReconnect);
                }
                connection_result = &mut connect => {
                    if let Ok(stream) = connection_result {
                        self.stream = Some(stream);
                        return Ok(ClientImplState::Connected);
                    } else {
                        client.apply_error(Mqtt5Error::ConnectionEstablishmentFailure);
                        return Ok(ClientImplState::PendingReconnect);
                    }
                }
            }

            if let Some(transition_state) = client.compute_optional_state_transition() {
                return Ok(transition_state);
            }
        }
    }

    pub(crate) async fn process_connected(&mut self, client: &mut Mqtt5ClientImpl) -> Mqtt5Result<ClientImplState> {
        let mut outbound_data: Vec<u8> = Vec::with_capacity(4096);
        let mut cumulative_bytes_written : usize = 0;

        let mut inbound_data: [u8; 4096] = [0; 4096];

        let stream = self.stream.take().unwrap();
        let (stream_reader, mut stream_writer) = split(stream);
        tokio::pin!(stream_reader);

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
                                client.apply_error(Mqtt5Error::ConnectionClosed);
                                next_state = Some(ClientImplState::PendingReconnect);
                            } else if client.handle_incoming_bytes(&inbound_data[..bytes_read]).is_err() {
                                next_state = Some(ClientImplState::PendingReconnect);
                            }
                        }
                        Err(_) => {
                            client.apply_error(Mqtt5Error::StreamReadFailure);
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
                Some(bytes_written_result) = conditional_write(outbound_slice_option, &mut stream_writer) => {
                    match bytes_written_result {
                        Ok(bytes_written) => {
                            cumulative_bytes_written += bytes_written;
                            if cumulative_bytes_written == outbound_data.len() {
                                outbound_data.clear();
                                cumulative_bytes_written = 0;
                                if client.handle_write_completion().is_err() {
                                    next_state = Some(ClientImplState::PendingReconnect);
                                }
                            }
                        }
                        Err(_) => {
                            client.apply_error(Mqtt5Error::StreamWriteFailure);
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

    pub(crate) async fn process_pending_reconnect(&mut self, client: &mut Mqtt5ClientImpl, wait: Duration) -> Mqtt5Result<ClientImplState> {
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

async fn conditional_write<T>(bytes_option: Option<&[u8]>, writer: &mut WriteHalf<T>) -> Option<std::io::Result<usize>> where T : AsyncRead + AsyncWrite {
    match bytes_option {
        Some(bytes) => Some(writer.write(bytes).await),
        None => None,
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