/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Module containing the public MQTT client and associated types necessary to invoke operations on it.
 */

pub(crate) mod shared_impl;

use crate::*;
use crate::client::shared_impl::*;
use crate::config::*;
use crate::error::{MqttError, MqttResult};
use crate::spec::*;
use crate::spec::disconnect::validate_disconnect_packet_outbound;
use crate::spec::utils::*;
use crate::validate::*;

use log::*;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// async choice conditional
use crate::features::gneiss_tokio::*;

/// Additional client options applicable to an MQTT Publish operation
#[derive(Debug, Default)]
pub struct PublishOptions {
    pub(crate) timeout: Option<Duration>,
}

/// Builder type for the set of additional client options applicable to an MQTT Publish operation
#[derive(Default)]
pub struct PublishOptionsBuilder {
    options: PublishOptions
}

impl PublishOptionsBuilder {

    /// Creates a new PublishOptionsBuilder with default values
    pub fn new() -> Self {
        PublishOptionsBuilder {
            ..Default::default()
        }
    }

    /// Sets the operation timeout for a Publish operation.  The operation timeout only applies
    /// to the time interval between when the operation's packet is written to the socket and when
    /// the corresponding ACK packet is received from the broker.  Has no effect on QoS0 publishes.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.options.timeout = Some(timeout);
        self
    }

    /// Creates a new PublishOptions object from what was configured on the builder.
    pub fn build(self) -> PublishOptions {
        self.options
    }
}

/// Wraps the two different ways a Qos2 publish might complete successfully via protocol definition.
/// Success is defined as "received a final Ack packet that signals the outcome of the operation."
#[derive(Debug, Eq, PartialEq)]
pub enum Qos2Response {

    /// The QoS2 publish was completed by receiving a Pubrec packet from the broker with a failing
    /// reason code.
    Pubrec(PubrecPacket),

    /// The Qos2 publish was completed by receiving a Pubcomp packet from the broker.
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

/// Union type that encapsulates the non-error ways that a Publish operation can complete with.
#[derive(Debug, Eq, PartialEq)]
pub enum PublishResponse {

    /// Indicates that a QoS0 Publish operation was successfully written to the wire.  This does
    /// not mean the Publish actually reached the broker.
    Qos0,

    /// Indicates that a QoS1 Publish operation was completed via Puback receipt.  Check the reason
    /// code in the Puback for protocol-level success/failure.
    Qos1(PubackPacket),

    /// Indicates that a QoS2 Publish operation was completed via Ack packet receipt.  Check the
    /// reason code in the packet for protocol-level success/failure.
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

/// Result type for the final outcome of a Publish operation
pub type PublishResult = MqttResult<PublishResponse>;

/// Return type of a Publish operation for the asynchronous client.  Await on this value to
/// receive the operation's result, but note that the operation will complete independently of
/// the use of `.await` (you don't need to await for the operation to be performed, you only need
/// to await to get the final result of performing it).
pub type PublishResultFuture = dyn Future<Output = PublishResult>;

/// Additional client options applicable to an MQTT Subscribe operation
#[derive(Debug, Default)]
pub struct SubscribeOptions {
    pub(crate) timeout: Option<Duration>,
}

/// Builder type for the set of additional client options applicable to an MQTT Subscribe operation
#[derive(Default)]
pub struct SubscribeOptionsBuilder {
    options: SubscribeOptions
}

impl SubscribeOptionsBuilder {

    /// Creates a new SubscribeOptionsBuilder with default values
    pub fn new() -> Self {
        SubscribeOptionsBuilder {
            ..Default::default()
        }
    }

    /// Sets the operation timeout for a Subscribe operation.  The operation timeout only applies
    /// to the time interval between when the operation's packet is written to the socket and when
    /// the corresponding Suback packet is received from the broker.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.options.timeout = Some(timeout);
        self
    }

    /// Creates a new SubscribeOptions object from what was configured on the builder.
    pub fn build(self) -> SubscribeOptions {
        self.options
    }
}

/// Result type for the final outcome of a Subscribe operation.  Check the reason
/// code vector on the Suback packet for individual success/failure indicators.
pub type SubscribeResult = MqttResult<SubackPacket>;

/// Return type of a Subscribe operation for the asynchronous client.  Await on this value to
/// receive the operation's result, but note that the operation will complete independently of
/// the use of `.await` (you don't need to await for the operation to be performed, you only need
/// to await to get the final result of performing it).
pub type SubscribeResultFuture = dyn Future<Output = SubscribeResult>;

/// Additional client options applicable to an MQTT Unsubscribe operation
#[derive(Debug, Default)]
pub struct UnsubscribeOptions {
    pub(crate) timeout: Option<Duration>,
}

/// Builder type for the set of additional client options applicable to an MQTT Unsubscribe operation
#[derive(Default)]
pub struct UnsubscribeOptionsBuilder {
    options: UnsubscribeOptions
}

impl UnsubscribeOptionsBuilder {

    /// Creates a new UnsubscribeOptionsBuilder with default values
    pub fn new() -> Self {
        UnsubscribeOptionsBuilder {
            ..Default::default()
        }
    }

    /// Sets the operation timeout for a Unsubscribe operation.  The operation timeout only applies
    /// to the time interval between when the operation's packet is written to the socket and when
    /// the corresponding Unsuback packet is received from the broker.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.options.timeout = Some(timeout);
        self
    }

    /// Creates a new UnsubscribeOptions object from what was configured on the builder.
    pub fn build(self) -> UnsubscribeOptions {
        self.options
    }
}

/// Result type for the final outcome of an Unsubscribe operation.  Check the reason
/// code vector on the Unsuback packet for individual success/failure indicators.
pub type UnsubscribeResult = MqttResult<UnsubackPacket>;

/// Return type of an Unsubscribe operation for the asynchronous client.  Await on this value to
/// receive the operation's result, but note that the operation will complete independently of
/// the use of `.await` (you don't need to await for the operation to be performed, you only need
/// to await to get the final result of performing it).
pub type UnsubscribeResultFuture = dyn Future<Output = UnsubscribeResult>;

/// Additional client options applicable to client Stop operation
#[derive(Debug, Default)]
pub struct StopOptions {

    /// MQTT Disconnect packet the client should send before closing the connection and entering
    /// the Stopped state.
    pub(crate) disconnect: Option<DisconnectPacket>,
}

/// Builder type for the set of additional client options applicable to a client Stop invocation
#[derive(Default)]
pub struct StopOptionsBuilder {
    options: StopOptions
}

impl StopOptionsBuilder {

    /// Creates a new StopOptionsBuilder with default values
    pub fn new() -> Self {
        StopOptionsBuilder {
            ..Default::default()
        }
    }

    /// Configures the stop invocation to send an MQTT Disconnect packet (if connected) before
    /// closing any connection and parking the client in the Stopped state.
    pub fn with_disconnect_packet(mut self, disconnect: DisconnectPacket) -> Self {
        self.options.disconnect = Some(disconnect);
        self
    }

    /// Creates a new StopOptions object from what was configured on the builder.
    pub fn build(self) -> StopOptions {
        self.options
    }
}

/// Structure containing all of the variable MQTT protocol settings that get negotiated as part of
/// each new network connection's Connect <-> Connack handshake on establishment.
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

impl Display for NegotiatedSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NegotiatedSettings {{")?;
        write!(f, " maximum_qos:{}", quality_of_service_to_str(self.maximum_qos))?;
        write!(f, " session_expiry_interval:{}", self.session_expiry_interval)?;
        write!(f, " receive_maximum_from_server:{}", self.receive_maximum_from_server)?;
        write!(f, " maximum_packet_size_to_server:{}", self.maximum_packet_size_to_server)?;
        write!(f, " topic_alias_maximum_to_server:{}", self.topic_alias_maximum_to_server)?;
        write!(f, " server_keep_alive:{}", self.server_keep_alive)?;
        write!(f, " retain_available:{}", self.retain_available)?;
        write!(f, " wildcard_subscriptions_available:{}", self.wildcard_subscriptions_available)?;
        write!(f, " subscription_identifiers_available:{}", self.subscription_identifiers_available)?;
        write!(f, " shared_subscriptions_available:{}", self.shared_subscriptions_available)?;
        write!(f, " rejoined_session:{}", self.rejoined_session)?;
        write!(f, " client_id:{}", self.client_id)?;
        write!(f, " }}")
    }
}

/// An event emitted by the client every time it attempts to establish a new network connection
/// to the broker.
#[derive(Debug)]
pub struct ConnectionAttemptEvent {}

impl Display for ConnectionAttemptEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConnectionAttemptEvent {{ }}")
    }
}

/// An event emitted by the client after successfully performing a Connect <-> Connack handshake
/// with the broker over a new network connection.
#[derive(Debug)]
pub struct ConnectionSuccessEvent {

    /// Connack packet sent by the broker as the final step of successful MQTT connection
    /// establishment
    pub connack: ConnackPacket,

    /// Set of protocol-related values that are negotiated by the Connect <-> Connack handshake
    pub settings: NegotiatedSettings
}

impl Display for ConnectionSuccessEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConnectionSuccessEvent {{ {}, {} }}", self.connack, self.settings)
    }
}

/// An event emitted by the client every time a connection attempt does not succeed.  The reason
/// for failure may be transport-related, protocol-related, or client-configuration-related.
#[derive(Debug)]
pub struct ConnectionFailureEvent {

    /// Error describing why the connection attempt failed
    pub error: MqttError,

    /// If the connection attempt was rejected by the broker with a Connack with
    /// failing reason code, that packet is found here.
    pub connack: Option<ConnackPacket>,
}

impl Display for ConnectionFailureEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(connack) = &self.connack {
            write!(f, "ConnectionFailureEvent {{ {}, {} }}", self.error, connack )
        } else {
            write!(f, "ConnectionFailureEvent {{ {}, None }}", self.error )
        }
    }
}

/// An event emitted by the client when a previously successfully-established connection is
/// shut down, for any reason.
#[derive(Debug)]
pub struct DisconnectionEvent {

    /// High-level reason for why the connection was shut down
    pub error: MqttError,

    /// If the connection was shut down due to the receipt of a broker-sent Disconnect packet,
    /// then that packet is found here.
    pub disconnect: Option<DisconnectPacket>,
}

impl Display for DisconnectionEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(disconnect) = &self.disconnect {
            write!(f, "DisconnectionEvent {{ {}, {} }}", self.error, disconnect)
        } else {
            write!(f, "DisconnectionEvent {{ {}, None }}", self.error)
        }

    }
}

/// An event emitted by the client when it enters the Stopped state, causing it to no longer
/// attempt to reconnect to the broker.
#[derive(Debug)]
pub struct StoppedEvent {}

impl Display for StoppedEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StoppedEvent {{ }}")
    }
}

/// An event emitted by the client whenever it receives a Publish packet from the broker.
/// This structure may expand in the future (pre-1.0.0) to support MQTT bridging.
#[derive(Debug)]
pub struct PublishReceivedEvent {

    /// Publish that was received from the broker.  Currently, the appropriate Ack is always
    /// sent by the client before this event is emitted.  In the future, bridging support
    /// may make the sending of Acks a user-controlled option.
    pub publish: PublishPacket
}

impl Display for PublishReceivedEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PublishReceivedEvent {{ {} }}", self.publish )
    }
}

/// Union of all the different events emitted by the client.
#[derive(Debug)]
pub enum ClientEvent {

    /// An event emitted by the client every time it attempts to establish a new network connection
    /// to the broker.
    ConnectionAttempt(ConnectionAttemptEvent),

    /// An event emitted by the client after successfully performing a Connect <-> Connack handshake
    /// with the broker over a new network connection.
    ConnectionSuccess(ConnectionSuccessEvent),

    /// An event emitted by the client every time a connection attempt does not succeed.
    ConnectionFailure(ConnectionFailureEvent),

    /// An event emitted by the client when a previously successfully-established connection is
    /// shut down, for any reason.
    Disconnection(DisconnectionEvent),

    /// An event emitted by the client when it enters the Stopped state, causing it to no longer
    /// attempt to reconnect to the broker.
    Stopped(StoppedEvent),

    /// An event emitted by the client whenever it receives a Publish packet from the broker.
    PublishReceived(PublishReceivedEvent),
}

impl Display for ClientEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientEvent::ConnectionAttempt(event) => { write!(f, "{}", event) }
            ClientEvent::ConnectionSuccess(event) => { write!(f, "{}", event) }
            ClientEvent::ConnectionFailure(event) => { write!(f, "{}", event) }
            ClientEvent::Disconnection(event) => { write!(f, "{}", event) }
            ClientEvent::Stopped(event) => { write!(f, "{}", event) }
            ClientEvent::PublishReceived(event) => { write!(f, "{}", event) }
        }
    }
}

/// Opaque structure that represents the identity of a client event listener.  Returned by
/// adding a listener and used to remove that same listener if needed.
#[derive(Debug, Eq, PartialEq)]
pub struct ListenerHandle {
    id: u64
}

/// A network client that functions as a thin wrapper over the MQTT5 protocol.
///
/// A client is always in one of two states:
/// * Stopped - the client is not connected and will perform no work
/// * Not Stopped - the client will continually attempt to maintain a connection to the configured broker.
///
/// The start() and stop() APIs toggle between these two states.
///
/// The client will use configurable exponential backoff with jitter when re-establishing connections.
///
/// Regardless of the client's state, you may always safely invoke MQTT operations on it, but
/// whether or not they are rejected (due to no connection) is a function of client configuration.
///
/// There are no mutable functions in the client API, so you can safely share it amongst threads,
/// runtimes/tasks, etc... by wrapping a newly-constructed client in an Arc.
///
/// Submitted operations are placed in a queue where they remain until they reach the head.  At
/// that point, the operation's packet is assigned a packet id (if appropriate) and encoded and
/// written to the socket.
///
/// Direct client construction is messy due to the different possibilities for TLS, async runtime,
/// etc...  We encourage you to use the various client builders in this crate, or in other crates,
/// to simplify this process.
pub struct Mqtt5Client {
    pub(crate) user_state: UserRuntimeState,

    pub(crate) listener_id_allocator: Mutex<u64>,
}

impl Mqtt5Client {

    /// Signals the client that it should attempt to recurrently maintain a connection to
    /// the broker endpoint it has been configured with.
    pub fn start(&self) -> MqttResult<()> {
        info!("client start invoked");
        self.user_state.try_send(OperationOptions::Start())
    }

    /// Signals the client that it should close any current connection it has and enter the
    /// Stopped state, where it does nothing.
    pub fn stop(&self, options: Option<StopOptions>) -> MqttResult<()> {
        info!("client stop invoked {} a disconnect packet", if options.as_ref().is_some_and(|opts| { opts.disconnect.is_some()}) { "with" } else { "without" });
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

    /// Signals the client that it should clean up all internal resources (connection, channels,
    /// runtime tasks, etc...) and enter a terminal state that cannot be escaped.  Useful to ensure
    /// a full resource wipe.  If just `stop()` is used then the client will continue to track
    /// MQTT session state internally.
    pub fn close(&self) -> MqttResult<()> {
        info!("client close invoked; no further operations allowed");
        self.user_state.try_send(OperationOptions::Shutdown())
    }

    /// Submits a Publish operation to the client's operation queue.  The publish will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.
    pub fn publish(&self, packet: PublishPacket, options: Option<PublishOptions>) -> Pin<Box<PublishResultFuture>> {
        debug!("Publish operation submitted");
        let boxed_packet = Box::new(MqttPacket::Publish(packet));
        if let Err(error) = validate_packet_outbound(&boxed_packet) {
            return Box::pin(async move { Err(error) });
        }

        submit_async_client_operation!(self, Publish, PublishOptionsInternal, options, boxed_packet)
    }

    /// Submits a Subscribe operation to the client's operation queue.  The subscribe will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.
    pub fn subscribe(&self, packet: SubscribePacket, options: Option<SubscribeOptions>) -> Pin<Box<SubscribeResultFuture>> {
        debug!("Subscribe operation submitted");
        let boxed_packet = Box::new(MqttPacket::Subscribe(packet));
        if let Err(error) = validate_packet_outbound(&boxed_packet) {
            return Box::pin(async move { Err(error) });
        }

        submit_async_client_operation!(self, Subscribe, SubscribeOptionsInternal, options, boxed_packet)
    }

    /// Submits an Unsubscribe operation to the client's operation queue.  The unsubscribe will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.
    pub fn unsubscribe(&self, packet: UnsubscribePacket, options: Option<UnsubscribeOptions>) -> Pin<Box<UnsubscribeResultFuture>> {
        debug!("Unsubscribe operation submitted");
        let boxed_packet = Box::new(MqttPacket::Unsubscribe(packet));
        if let Err(error) = validate_packet_outbound(&boxed_packet) {
            return Box::pin(async move { Err(error) });
        }

        submit_async_client_operation!(self, Unsubscribe, UnsubscribeOptionsInternal, options, boxed_packet)
    }

    /// Adds an additional listener to the events emitted by this client.  This is useful when
    /// multiple higher-level constructs are sharing the same MQTT client.
    pub fn add_event_listener(&self, listener: ClientEventListener) -> MqttResult<ListenerHandle> {
        debug!("AddListener operation submitted");
        let mut current_id = self.listener_id_allocator.lock().unwrap();
        let listener_id = *current_id;
        *current_id += 1;

        self.user_state.try_send(OperationOptions::AddListener(listener_id, listener))?;

        Ok(ListenerHandle {
            id: listener_id
        })
    }

    /// Removes a listener from this client's set of event listeners.
    pub fn remove_event_listener(&self, listener: ListenerHandle) -> MqttResult<()> {
        debug!("RemoveListener operation submitted");
        self.user_state.try_send(OperationOptions::RemoveListener(listener.id))
    }
}

