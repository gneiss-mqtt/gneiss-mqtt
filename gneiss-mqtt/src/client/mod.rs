/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Module containing the public MQTT client and associated types necessary to invoke operations on it.
 */

#![warn(missing_docs)]

pub(crate) mod shared_impl;

use crate::*;
use crate::client::shared_impl::*;
use crate::config::*;
use crate::spec::*;
use crate::spec::disconnect::validate_disconnect_packet_outbound;
use crate::spec::utils::*;
use crate::validate::*;

use std::fmt::{Debug, Display};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// async choice conditional
extern crate tokio;
use crate::features::gneiss_tokio::*;

#[derive(Debug, Default)]
pub struct PublishOptions {
    pub(crate) timeout: Option<Duration>,
}

#[derive(Default)]
pub struct PublishOptionsBuilder {
    options: PublishOptions
}

impl PublishOptionsBuilder {
    pub fn new() -> Self {
        PublishOptionsBuilder {
            ..Default::default()
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.options.timeout = Some(timeout);
        self
    }

    pub fn build(self) -> PublishOptions {
        self.options
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Qos2Response {
    Pubrec(PubrecPacket),
    Pubcomp(PubcompPacket),
}

impl Display for Qos2Response {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Qos2Response::Pubrec(pubrec) => {
                write!(f, "Pubrec ( {} )", pubrec)
            }
            Qos2Response::Pubcomp(pubcomp) => {
                write!(f, "Pubcomp ( {} )", pubcomp)
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum PublishResponse {
    Qos0,
    Qos1(PubackPacket),
    Qos2(Qos2Response),
}

impl Display for PublishResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PublishResponse::Qos0 => {
                write!(f, "PublishResponse Qos0")
            }
            PublishResponse::Qos1(puback) => {
                write!(f, "PublishResponse Qos1 ( {} )", puback)
            }
            PublishResponse::Qos2(qos2response) => {
                write!(f, "PublishResponse Qos2 ( {} )", qos2response)
            }
        }
    }
}

pub type PublishResult = MqttResult<PublishResponse>;

pub type PublishResultFuture = dyn Future<Output = PublishResult>;

#[derive(Debug, Default)]
pub struct SubscribeOptions {
    pub(crate) timeout: Option<Duration>,
}

#[derive(Default)]
pub struct SubscribeOptionsBuilder {
    options: SubscribeOptions
}

impl SubscribeOptionsBuilder {
    pub fn new() -> Self {
        SubscribeOptionsBuilder {
            ..Default::default()
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.options.timeout = Some(timeout);
        self
    }

    pub fn build(self) -> SubscribeOptions {
        self.options
    }
}

pub type SubscribeResult = MqttResult<SubackPacket>;

pub type SubscribeResultFuture = dyn Future<Output = SubscribeResult>;

#[derive(Debug, Default)]
pub struct UnsubscribeOptions {
    pub(crate) timeout: Option<Duration>,
}

#[derive(Default)]
pub struct UnsubscribeOptionsBuilder {
    options: UnsubscribeOptions
}

impl UnsubscribeOptionsBuilder {
    pub fn new() -> Self {
        UnsubscribeOptionsBuilder {
            ..Default::default()
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.options.timeout = Some(timeout);
        self
    }

    pub fn build(self) -> UnsubscribeOptions {
        self.options
    }
}

pub type UnsubscribeResult = MqttResult<UnsubackPacket>;

pub type UnsubscribeResultFuture = dyn Future<Output = UnsubscribeResult>;

#[derive(Debug, Default)]
pub struct StopOptions {
    pub(crate) disconnect: Option<DisconnectPacket>,
}

#[derive(Default)]
pub struct StopOptionsBuilder {
    options: StopOptions
}

impl StopOptionsBuilder {
    pub fn new() -> Self {
        StopOptionsBuilder {
            ..Default::default()
        }
    }

    pub fn with_disconnect_packet(mut self, disconnect: DisconnectPacket) -> Self {
        self.options.disconnect = Some(disconnect);
        self
    }

    pub fn build(self) -> StopOptions {
        self.options
    }
}

#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct NegotiatedSettings {

    /// The maximum QoS allowed between the server and client.
    pub maximum_qos : QualityOfService,

    /// The amount of time in seconds the server will retain the session after a disconnect.
    pub session_expiry_interval : u32,

    /// The number of QoS 1 and QoS2 publications the server is willing to process concurrently.
    pub receive_maximum_from_server : u16,

    /// The maximum packet size the server is willing to accept.
    pub maximum_packet_size_to_server : u32,

    /// The highest value that the server will accept as a Topic Alias sent by the client.
    pub topic_alias_maximum_to_server : u16,

    /// The amount of time in seconds before the server will disconnect the client for inactivity.
    pub server_keep_alive : u16,

    /// Whether or not the server supports retained messages.
    pub retain_available : bool,

    /// Whether or not the server supports wildcard subscriptions.
    pub wildcard_subscriptions_available : bool,

    /// Whether or not the server supports subscription identifiers.
    pub subscription_identifiers_available : bool,

    /// Whether or not the server supports shared subscriptions.
    pub shared_subscriptions_available : bool,

    /// Whether or not the client has rejoined an existing session.
    pub rejoined_session : bool,

    /// Client id in use for the current connection
    pub client_id : String
}

impl fmt::Display for NegotiatedSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "NegotiatedSettings {{")?;
        writeln!(f, "  maximum_qos: {}", quality_of_service_to_str(self.maximum_qos))?;
        writeln!(f, "  session_expiry_interval: {}", self.session_expiry_interval)?;
        writeln!(f, "  receive_maximum_from_server: {}", self.receive_maximum_from_server)?;
        writeln!(f, "  maximum_packet_size_to_server: {}", self.maximum_packet_size_to_server)?;
        writeln!(f, "  topic_alias_maximum_to_server: {}", self.topic_alias_maximum_to_server)?;
        writeln!(f, "  server_keep_alive: {}", self.server_keep_alive)?;
        writeln!(f, "  retain_available: {}", self.retain_available)?;
        writeln!(f, "  wildcard_subscriptions_available: {}", self.wildcard_subscriptions_available)?;
        writeln!(f, "  subscription_identifiers_available: {}", self.subscription_identifiers_available)?;
        writeln!(f, "  shared_subscriptions_available: {}", self.shared_subscriptions_available)?;
        writeln!(f, "  rejoined_session: {}", self.rejoined_session)?;
        writeln!(f, "  client_id: {}", self.client_id)?;
        write!(f, "}}")?;

        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ConnectionAttemptEvent {}

#[derive(Debug, Eq, PartialEq)]
pub struct ConnectionSuccessEvent {
    pub connack: ConnackPacket,
    pub settings: NegotiatedSettings
}

#[derive(Debug, Eq, PartialEq)]
pub struct ConnectionFailureEvent {
    pub error: MqttError,
    pub connack: Option<ConnackPacket>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct DisconnectionEvent {
    pub error: MqttError,
    pub disconnect: Option<DisconnectPacket>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct StoppedEvent {}

#[derive(Debug, Eq, PartialEq)]
pub struct PublishReceivedEvent {
    pub publish: PublishPacket
}

#[derive(Debug, Eq, PartialEq)]
pub enum ClientEvent {
    ConnectionAttempt(ConnectionAttemptEvent),
    ConnectionSuccess(ConnectionSuccessEvent),
    ConnectionFailure(ConnectionFailureEvent),
    Disconnection(DisconnectionEvent),
    Stopped(StoppedEvent),
    PublishReceived(PublishReceivedEvent),
}

#[derive(Debug, Eq, PartialEq)]
pub struct ListenerHandle {
    id: u64
}


pub struct Mqtt5Client where {
    pub(crate) user_state: UserRuntimeState,

    pub(crate) listener_id_allocator: Mutex<u64>,
}

impl Mqtt5Client {

    pub fn start(&self) -> MqttResult<()> {
        self.user_state.try_send(OperationOptions::Start())
    }

    pub fn stop(&self, options: Option<StopOptions>) -> MqttResult<()> {
        let options = options.unwrap_or_default();

        if let Some(disconnect) = &options.disconnect {
            validate_disconnect_packet_outbound(disconnect)?;
        }

        let mut stop_options_internal = StopOptionsInternal {
            ..Default::default()
        };

        if options.disconnect.is_some() {
            stop_options_internal.disconnect = Some(Box::new(MqttPacket::Disconnect(options.disconnect.unwrap())));
        }

        self.user_state.try_send(OperationOptions::Stop(stop_options_internal))
    }

    pub fn close(&self) -> MqttResult<()> {
        self.user_state.try_send(OperationOptions::Shutdown())
    }

    pub fn publish(&self, packet: PublishPacket, options: Option<PublishOptions>) -> Pin<Box<PublishResultFuture>> {
        let boxed_packet = Box::new(MqttPacket::Publish(packet));
        if let Err(error) = validate_packet_outbound(&boxed_packet) {
            return Box::pin(async move { Err(error) });
        }

        submit_async_client_operation!(self, Publish, PublishOptionsInternal, options, boxed_packet)
    }

    pub fn subscribe(&self, packet: SubscribePacket, options: Option<SubscribeOptions>) -> Pin<Box<SubscribeResultFuture>> {
        let boxed_packet = Box::new(MqttPacket::Subscribe(packet));
        if let Err(error) = validate_packet_outbound(&boxed_packet) {
            return Box::pin(async move { Err(error) });
        }

        submit_async_client_operation!(self, Subscribe, SubscribeOptionsInternal, options, boxed_packet)
    }

    pub fn unsubscribe(&self, packet: UnsubscribePacket, options: Option<UnsubscribeOptions>) -> Pin<Box<UnsubscribeResultFuture>> {
        let boxed_packet = Box::new(MqttPacket::Unsubscribe(packet));
        if let Err(error) = validate_packet_outbound(&boxed_packet) {
            return Box::pin(async move { Err(error) });
        }

        submit_async_client_operation!(self, Unsubscribe, UnsubscribeOptionsInternal, options, boxed_packet)
    }

    pub fn add_event_listener(&self, listener: ClientEventListener) -> MqttResult<ListenerHandle> {
        let mut current_id = self.listener_id_allocator.lock().unwrap();
        let listener_id = *current_id;
        *current_id += 1;

        self.user_state.try_send(OperationOptions::AddListener(listener_id, listener))?;

        Ok(ListenerHandle {
            id: listener_id
        })
    }

    pub fn remove_event_listener(&self, listener: ListenerHandle) -> MqttResult<()> {
        self.user_state.try_send(OperationOptions::RemoveListener(listener.id))
    }
}

