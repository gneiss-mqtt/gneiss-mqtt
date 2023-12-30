/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

pub mod tokio_impl;

extern crate log;
extern crate rand;

use crate::*;
use crate::client::*;
use crate::operation::*;
use crate::spec::*;

use std::collections::{HashMap, VecDeque};
use std::mem;
use std::time::{Duration, Instant};

use log::*;
use rand::Rng;

pub(crate) struct PublishOptionsInternal {
    pub options: PublishOptions,
    pub response_sender: Option<AsyncOperationSender<PublishResult>>,
}

pub(crate) struct SubscribeOptionsInternal {
    pub options: SubscribeOptions,
    pub response_sender: Option<AsyncOperationSender<SubscribeResult>>,
}

pub(crate) struct UnsubscribeOptionsInternal {
    pub options: UnsubscribeOptions,
    pub response_sender: Option<AsyncOperationSender<UnsubscribeResult>>,
}

#[derive(Debug, Default)]
pub(crate) struct StopOptionsInternal {
    pub disconnect: Option<Box<MqttPacket>>,
}

pub(crate) enum OperationOptions {
    Publish(Box<MqttPacket>, PublishOptionsInternal),
    Subscribe(Box<MqttPacket>, SubscribeOptionsInternal),
    Unsubscribe(Box<MqttPacket>, UnsubscribeOptionsInternal),
    Start(),
    Stop(StopOptionsInternal),
    Shutdown(),
    AddListener(u64, ClientEventListener),
    RemoveListener(u64)
}

#[derive(Eq, PartialEq, Copy, Clone)]
pub(crate) enum ClientImplState {
    Stopped,
    Connecting,
    Connected,
    PendingReconnect,
    Shutdown,
    // possibly need a pending stopped state for async connection shutdown
}

pub(crate) struct Mqtt5ClientImpl {
    operational_state: OperationalState,
    listeners: HashMap<u64, ClientEventListener>,

    current_state: ClientImplState,
    desired_state: ClientImplState,

    desired_stop_options: Option<StopOptionsInternal>,

    packet_events: VecDeque<PacketEvent>,

    last_connack: Option<ConnackPacket>,
    last_disconnect: Option<DisconnectPacket>,
    last_error: Option<Mqtt5Error>,

    successful_connect_time: Option<Instant>,
    next_reconnect_period: Duration,
    reconnect_options: ReconnectOptions
}


impl Mqtt5ClientImpl {

    pub(crate) fn new(mut config: Mqtt5ClientOptions) -> Self {
        let state_config = OperationalStateConfig {
            connect_options: config.connect_options.unwrap_or(ConnectOptions { ..Default::default() }),
            base_timestamp: Instant::now(),
            offline_queue_policy: config.offline_queue_policy,
            connack_timeout: config.connack_timeout,
            ping_timeout: config.ping_timeout,
            outbound_alias_resolver: config.outbound_alias_resolver.take(),
        };

        let default_listener = config.default_event_listener.take();

        let mut client_impl = Mqtt5ClientImpl {
            operational_state: OperationalState::new(state_config),
            listeners: HashMap::new(),
            current_state: ClientImplState::Stopped,
            desired_state: ClientImplState::Stopped,
            desired_stop_options: None,
            packet_events: VecDeque::new(),
            last_connack: None,
            last_disconnect: None,
            last_error: None,
            successful_connect_time: None,
            next_reconnect_period: config.reconnect_options.base_reconnect_period,
            reconnect_options: config.reconnect_options
        };

        client_impl.reconnect_options.normalize();

        if let Some(listener) = default_listener {
            client_impl.listeners.insert(0, listener);
        }

        client_impl
    }

    pub(crate) fn get_current_state(&self) -> ClientImplState {
        self.current_state
    }

    pub(crate) fn add_listener(&mut self, id: u64, listener: ClientEventListener) {
        self.listeners.insert(id, listener);
    }

    pub(crate) fn remove_listener(&mut self, id: u64) {
        self.listeners.remove(&id);
    }

    pub(crate) fn broadcast_event(&self, event: Arc<ClientEvent>) {
        for listener in self.listeners.values() {
            match listener {
                ClientEventListener::Channel(channel) => {
                    channel.send(event.clone()).unwrap();
                }
                ClientEventListener::Callback(callback) => {
                    spawn_event_callback(event.clone(), callback.clone());
                }
            }
        }
    }

    pub(crate) fn apply_error(&mut self, error: Mqtt5Error) {
        if self.last_error.is_none() {
            self.last_error = Some(error);
        }
    }

    pub(crate) fn handle_incoming_operation(&mut self, operation: OperationOptions) {
        match operation {
            OperationOptions::Publish(packet, internal_options) => {
                let user_event_context = UserEventContext {
                    event: UserEvent::Publish(packet, internal_options),
                    current_time: Instant::now()
                };

                self.operational_state.handle_user_event(user_event_context);
            }
            OperationOptions::Subscribe(packet, internal_options) => {
                let user_event_context = UserEventContext {
                    event: UserEvent::Subscribe(packet, internal_options),
                    current_time: Instant::now()
                };

                self.operational_state.handle_user_event(user_event_context);
            }
            OperationOptions::Unsubscribe(packet, internal_options) => {
                let user_event_context = UserEventContext {
                    event: UserEvent::Unsubscribe(packet, internal_options),
                    current_time: Instant::now()
                };

                self.operational_state.handle_user_event(user_event_context);
            }
            OperationOptions::Start() => {
                self.desired_stop_options = None;
                self.desired_state = ClientImplState::Connected;
            }
            OperationOptions::Stop(options) => {
                if let Some(disconnect) = &options.disconnect {
                    let disconnect_context = UserEventContext {
                        event: UserEvent::Disconnect(disconnect.clone()),
                        current_time: Instant::now()
                    };

                    self.operational_state.handle_user_event(disconnect_context);
                }

                self.desired_stop_options = Some(options);
                self.desired_state = ClientImplState::Stopped;
            }
            OperationOptions::Shutdown() => {
                self.operational_state.reset(&Instant::now());
                self.desired_state = ClientImplState::Shutdown;
            }
            OperationOptions::AddListener(id, listener) => {
                self.add_listener(id, listener);
            }
            OperationOptions::RemoveListener(id) => {
                self.remove_listener(id);
            }
        }
    }

    fn handle_packet_events(&mut self) {
        let mut events = VecDeque::new();
        mem::swap(&mut events, &mut self.packet_events);

        for event in events {
            match event {
                PacketEvent::Publish(publish) => {
                    let publish_event = PublishReceivedEvent {
                        publish,
                    };

                    let publish_client_event = Arc::new(ClientEvent::PublishReceived(publish_event));
                    self.broadcast_event(publish_client_event);
                }
                PacketEvent::Disconnect(disconnect) => {
                    self.last_disconnect = Some(disconnect);
                }
                PacketEvent::Connack(connack) => {
                    let reason_code = connack.reason_code;
                    self.last_connack = Some(connack);
                    if reason_code == ConnectReasonCode::Success {
                        self.successful_connect_time = Some(Instant::now());
                        self.emit_connection_success_event();
                    }
                }
            }
        }

        self.packet_events.clear();
    }

    pub(crate) fn handle_incoming_bytes(&mut self, bytes: &[u8]) -> Mqtt5Result<()> {
        let mut context = NetworkEventContext {
            event: NetworkEvent::IncomingData(bytes),
            current_time: Instant::now(),
            packet_events: &mut self.packet_events
        };

        let result = self.operational_state.handle_network_event(&mut context);
        if let Err(error) = result {
            self.apply_error(error);
        }

        self.handle_packet_events();

        result
    }

    pub(crate) fn handle_write_completion(&mut self) -> Mqtt5Result<()> {
        let mut context = NetworkEventContext {
            event: NetworkEvent::WriteCompletion,
            current_time: Instant::now(),
            packet_events: &mut self.packet_events
        };

        let result = self.operational_state.handle_network_event(&mut context);
        if let Err(error) = result {
            self.apply_error(error);
        }

        result
    }

    pub(crate) fn handle_service(&mut self, outbound_data: &mut Vec<u8>) -> Mqtt5Result<()> {
        let mut context = ServiceContext {
            to_socket: outbound_data,
            current_time: Instant::now(),
        };

        let result = self.operational_state.service(&mut context);
        if let Err(error) = result {
            self.apply_error(error);
        }

        result
    }

    fn clamp_reconnect_period(&self, mut reconnect_period: Duration) -> Duration {
        if reconnect_period > self.reconnect_options.max_reconnect_period {
            reconnect_period = self.reconnect_options.max_reconnect_period;
        }

        reconnect_period
    }

    fn compute_uniform_jitter_period(&self, max_nanos: u128) -> Duration {
        let mut rng = rand::thread_rng();
        let uniform_nanos = rng.gen_range(0..max_nanos);
        Duration::from_nanos(uniform_nanos as u64)
    }

    pub(crate) fn compute_reconnect_period(&mut self) -> Duration {
        let reconnect_period = self.next_reconnect_period;
        self.next_reconnect_period = self.clamp_reconnect_period(self.next_reconnect_period * 2);

        match self.reconnect_options.reconnect_period_jitter {
            ExponentialBackoffJitterType::None => {
                reconnect_period
            }
            ExponentialBackoffJitterType::Uniform => {
                self.compute_uniform_jitter_period(reconnect_period.as_nanos())
            }
        }
    }

    fn compute_optional_state_transition(&self) -> Option<ClientImplState> {
        match self.current_state {
            ClientImplState::Stopped => {
                match self.desired_state {
                    ClientImplState::Connected => {
                        return Some(ClientImplState::Connecting)
                    }
                    ClientImplState::Shutdown => {
                        return Some(ClientImplState::Shutdown)
                    }
                    _ => {}
                }
            }

            ClientImplState::Connecting | ClientImplState::PendingReconnect => {
                if self.desired_state != ClientImplState::Connected {
                    return Some(ClientImplState::Stopped)
                }
            }

            ClientImplState::Connected => {
                if self.desired_state != ClientImplState::Connected {
                    if let Some(stop_options) = &self.desired_stop_options {
                        if stop_options.disconnect.is_none() {
                            return Some(ClientImplState::Stopped);
                        }
                    } else {
                        return Some(ClientImplState::Stopped);
                    }
                }
            }

            _ => { }
        }

        None
    }

    fn get_next_connected_service_time(&mut self) -> Option<Instant> {
        if self.current_state == ClientImplState::Connected {
            return self.operational_state.get_next_service_timepoint(&Instant::now());
        }

        None
    }

    fn emit_connection_attempt_event(&self) {
        let connection_attempt_event = ConnectionAttemptEvent {
        };

        self.broadcast_event(Arc::new(ClientEvent::ConnectionAttempt(connection_attempt_event)));
    }

    fn emit_connection_success_event(&self) {
        let settings = self.operational_state.get_negotiated_settings().as_ref().unwrap();

        let connection_success_event = ConnectionSuccessEvent {
            connack: self.last_connack.as_ref().unwrap().clone(),
            settings: settings.clone(),
        };

        self.broadcast_event(Arc::new(ClientEvent::ConnectionSuccess(connection_success_event)));
    }

    fn emit_connection_failure_event(&self) {
        let mut connection_failure_event = ConnectionFailureEvent {
            error: self.last_error.unwrap_or(Mqtt5Error::Unknown),
            connack: None,
        };

        if let Some(connack) = &self.last_connack {
            connection_failure_event.connack = Some(connack.clone());
        }

        self.broadcast_event(Arc::new(ClientEvent::ConnectionFailure(connection_failure_event)));
    }

    fn emit_disconnection_event(&self) {
        let mut disconnection_event = DisconnectionEvent {
            error: self.last_error.unwrap_or(Mqtt5Error::Unknown),
            disconnect: None,
        };

        if let Some(disconnect) = &self.last_disconnect {
            disconnection_event.disconnect = Some(disconnect.clone());
        }

        self.broadcast_event(Arc::new(ClientEvent::Disconnection(disconnection_event)));
    }

    fn emit_stopped_event(&self) {
        let stopped_event = StoppedEvent {
        };

        self.broadcast_event(Arc::new(ClientEvent::Stopped(stopped_event)));
    }

    fn transition_to_state(&mut self, mut new_state: ClientImplState) -> Mqtt5Result<()> {
        let old_state = self.current_state;
        if old_state == new_state {
            return Ok(());
        }

        // Displeasing hacks to support state transition short-circuits.  We need two:
        //
        //  (1) PendingReconnect -> Stopped after a disconnect packet has been flushed
        //      We can't break out of
        //      connected until the disconnect is written to the socket, and so we suspend the
        //      desired != current check to support that since flushing a disconnect will halt
        //      the operational state.  But then we blindly transition to pending connect which isn't
        //      right, so correct that here.
        //  (2) Stopped -> Shutdown after a close operation has been received
        //      Stopped does not have a naturally exit point except operation receipt.  But we've
        //      received the last operation in theory, so we need to jump to shutdown immediately
        //      without waiting on a select
        //
        //  TODO: these indicate some flaws in the overall contract/model that should be corrected
        if new_state == ClientImplState::PendingReconnect && self.desired_state != ClientImplState::Connected {
            new_state = ClientImplState::Stopped;
        }

        if new_state == ClientImplState::Stopped && self.desired_state == ClientImplState::Shutdown {
            new_state = ClientImplState::Shutdown;
        }

        if new_state == ClientImplState::Connected {
            let mut connection_opened_context = NetworkEventContext {
                event: NetworkEvent::ConnectionOpened,
                current_time: Instant::now(),
                packet_events: &mut self.packet_events
            };

            self.operational_state.handle_network_event(&mut connection_opened_context)?;
        } else if old_state == ClientImplState::Connected {
            let mut connection_closed_context = NetworkEventContext {
                event: NetworkEvent::ConnectionClosed,
                current_time: Instant::now(),
                packet_events: &mut self.packet_events
            };

            self.operational_state.handle_network_event(&mut connection_closed_context)?;
        }

        if new_state == ClientImplState::Connecting {
            self.last_error = None;
            self.last_connack = None;
            self.last_disconnect = None;
            self.emit_connection_attempt_event();
        }

        if old_state == ClientImplState::Connecting && new_state != ClientImplState::Connected {
            self.emit_connection_failure_event();
        }

        if old_state == ClientImplState::Connected {
            if let Some(connack) = &self.last_connack {
                if connack.reason_code == ConnectReasonCode::Success {
                    self.emit_disconnection_event();
                } else {
                    self.emit_connection_failure_event();
                }
            } else {
                self.emit_connection_failure_event();
            }

            if let Some(successful_connect_timepoint) = self.successful_connect_time {
                let now = Instant::now();
                if (now - successful_connect_timepoint) > self.reconnect_options.reconnect_stability_reset_period {
                    self.next_reconnect_period = self.reconnect_options.base_reconnect_period;
                }
            }

            self.successful_connect_time = None;
        }

        if new_state == ClientImplState::Stopped {
            self.desired_stop_options = None;
            self.emit_stopped_event();
        }

        self.current_state = new_state;

        Ok(())
    }
}


