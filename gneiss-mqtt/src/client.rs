/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Module containing the public MQTT client and associated types necessary to invoke operations on it.
 */

use crate::config::*;
use crate::error::{MqttError, MqttResult};
use crate::mqtt::*;
use crate::mqtt::disconnect::validate_disconnect_packet_outbound;
use crate::mqtt::utils::*;
use crate::protocol::*;
use crate::validate::*;

use log::*;
use rand::Rng;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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
pub type PublishResultFuture = dyn Future<Output = PublishResult> + Send;

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
pub type SubscribeResultFuture = dyn Future<Output = SubscribeResult> + Send;

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
pub type UnsubscribeResultFuture = dyn Future<Output = UnsubscribeResult> + Send;

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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PublishReceivedEvent {{ {} }}", self.publish )
    }
}

/// Union of all the different events emitted by the client.
#[derive(Debug)]
#[non_exhaustive]
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

/// Callback function to be invoked with every emitted client event
pub type ClientEventListenerCallback = dyn Fn(Arc<ClientEvent>) + Send + Sync;

/// Basic client event listener type
pub type ClientEventListener = Arc<ClientEventListenerCallback>;

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
    pub fn start(&self, default_listener: Option<Arc<ClientEventListenerCallback>>) -> MqttResult<()> {
        info!("client start invoked");
        self.user_state.try_send(OperationOptions::Start(default_listener))
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
    Start(Option<ClientEventListener>),
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

impl Display for ClientImplState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientImplState::Stopped => { write!(f, "Stopped") }
            ClientImplState::Connecting => { write!(f, "Connecting") }
            ClientImplState::Connected => { write!(f, "Connected") }
            ClientImplState::PendingReconnect => { write!(f, "PendingReconnect") }
            ClientImplState::Shutdown => { write!(f, "Shutdown") }
        }
    }
}

pub(crate) struct Mqtt5ClientImpl {
    protocol_state: ProtocolState,
    listeners: HashMap<u64, ClientEventListener>,

    current_state: ClientImplState,
    desired_state: ClientImplState,

    desired_stop_options: Option<StopOptionsInternal>,

    packet_events: VecDeque<PacketEvent>,

    last_connack: Option<ConnackPacket>,
    last_disconnect: Option<DisconnectPacket>,
    last_error: Option<MqttError>,

    last_start_connect_time: Option<Instant>,
    successful_connect_time: Option<Instant>,
    next_reconnect_period: Duration,
    reconnect_options: ReconnectOptions,

    connect_timeout: Duration,

    current_time: Instant,
}


impl Mqtt5ClientImpl {

    pub(crate) fn new(client_config: Mqtt5ClientOptions, connect_config: ConnectOptions, current_time: Instant) -> Self {
        debug!("Creating new MQTT client - client options: {:?}", client_config);
        debug!("Creating new MQTT client - connect options: {:?}", connect_config);

        let state_config = ProtocolStateConfig {
            connect_options: connect_config,
            base_timestamp: current_time,
            offline_queue_policy: client_config.offline_queue_policy,
            ping_timeout: client_config.ping_timeout,
            outbound_alias_resolver: client_config.outbound_alias_resolver_factory.map(|f| { f() })
        };

        let mut client_impl = Mqtt5ClientImpl {
            protocol_state: ProtocolState::new(state_config),
            listeners: HashMap::new(),
            current_state: ClientImplState::Stopped,
            desired_state: ClientImplState::Stopped,
            desired_stop_options: None,
            packet_events: VecDeque::new(),
            last_connack: None,
            last_disconnect: None,
            last_error: None,
            last_start_connect_time: None,
            successful_connect_time: None,
            next_reconnect_period: client_config.reconnect_options.base_reconnect_period,
            reconnect_options: client_config.reconnect_options,
            connect_timeout: client_config.connect_timeout,
            current_time
        };

        client_impl.reconnect_options.normalize();

        client_impl
    }

    pub(crate) fn connect_timeout(&self) -> &Duration {
        &self.connect_timeout
    }

    pub(crate) fn get_current_state(&self) -> ClientImplState {
        self.current_state
    }

    pub(crate) fn get_protocol_state(&self) -> ProtocolStateType {
        self.protocol_state.state()
    }

    pub(crate) fn add_listener(&mut self, id: u64, listener: ClientEventListener) {
        self.listeners.insert(id, listener);
    }

    pub(crate) fn remove_listener(&mut self, id: u64) {
        self.listeners.remove(&id);
    }

    pub(crate) fn broadcast_event(&self, event: Arc<ClientEvent>) {
        debug!("Broadcasting client event: {}", *event);

        for listener in self.listeners.values() {
            spawn_event_callback(event.clone(), listener.clone());
        }
    }

    pub(crate) fn apply_error(&mut self, error: MqttError) {
        debug!("Applying error to client: {}", error);

        if self.last_error.is_none() {
            self.last_error = Some(error);
        }
    }

    fn set_current_time(&mut self, current_time: Instant) {
        self.current_time = current_time;
    }

    pub(crate) fn handle_incoming_operation(&mut self, operation: OperationOptions, current_time: Instant) {
        self.set_current_time(current_time);

        match operation {
            OperationOptions::Publish(packet, internal_options) => {
                debug!("Submitting publish operation to protocol state");
                let user_event_context = UserEventContext {
                    event: UserEvent::Publish(packet, internal_options),
                    current_time
                };

                self.protocol_state.handle_user_event(user_event_context);
            }
            OperationOptions::Subscribe(packet, internal_options) => {
                debug!("Submitting subscribe operation to protocol state");
                let user_event_context = UserEventContext {
                    event: UserEvent::Subscribe(packet, internal_options),
                    current_time
                };

                self.protocol_state.handle_user_event(user_event_context);
            }
            OperationOptions::Unsubscribe(packet, internal_options) => {
                debug!("Submitting unsubscribe operation to protocol state");
                let user_event_context = UserEventContext {
                    event: UserEvent::Unsubscribe(packet, internal_options),
                    current_time
                };

                self.protocol_state.handle_user_event(user_event_context);
            }
            OperationOptions::Start(listener_option) => {
                if let Some(listener) = listener_option {
                    self.listeners.insert(0, listener);
                }

                debug!("Updating desired state to Connected");
                self.desired_stop_options = None;
                self.desired_state = ClientImplState::Connected;
            }
            OperationOptions::Stop(options) => {

                if let Some(disconnect) = &options.disconnect {
                    debug!("Submitting disconnect operation to protocol state");
                    let disconnect_context = UserEventContext {
                        event: UserEvent::Disconnect(disconnect.clone()),
                        current_time
                    };

                    self.protocol_state.handle_user_event(disconnect_context);
                }

                debug!("Updating desired state to Stopped");
                self.desired_stop_options = Some(options);
                self.desired_state = ClientImplState::Stopped;
            }
            OperationOptions::Shutdown() => {
                debug!("Updating desired state to Shutdown");
                self.protocol_state.reset(&current_time);
                self.desired_state = ClientImplState::Shutdown;
            }
            OperationOptions::AddListener(id, listener) => {
                debug!("Adding listener {} to client events", id);
                self.add_listener(id, listener);
            }
            OperationOptions::RemoveListener(id) => {
                debug!("Removing listener {} from client events", id);
                self.remove_listener(id);
            }
        }
    }

    fn dispatch_packet_events(&mut self) {
        let mut events = VecDeque::new();
        mem::swap(&mut events, &mut self.packet_events);

        for event in events {
            match event {
                PacketEvent::Publish(publish) => {
                    debug!("dispatch_packet_events - publish packet");
                    let publish_event = PublishReceivedEvent {
                        publish,
                    };

                    let publish_client_event = Arc::new(ClientEvent::PublishReceived(publish_event));
                    self.broadcast_event(publish_client_event);
                }
                PacketEvent::Disconnect(disconnect) => {
                    debug!("dispatch_packet_events - server-side disconnect packet");
                    self.last_disconnect = Some(disconnect);
                }
                PacketEvent::Connack(connack) => {
                    debug!("dispatch_packet_events - connack packet");
                    let reason_code = connack.reason_code;
                    self.last_connack = Some(connack);
                    if reason_code == ConnectReasonCode::Success {
                        self.successful_connect_time = Some(self.current_time);
                        self.emit_connection_success_event();
                    }
                }
            }
        }

        self.packet_events.clear();
    }

    pub(crate) fn handle_incoming_bytes(&mut self, bytes: &[u8], current_time: Instant) -> MqttResult<()> {
        debug!("client impl - handle_incoming_bytes: {} bytes", bytes.len());
        self.set_current_time(current_time);

        let mut context = NetworkEventContext {
            event: NetworkEvent::IncomingData(bytes),
            current_time,
            packet_events: &mut self.packet_events
        };

        let result = self.protocol_state.handle_network_event(&mut context);
        self.dispatch_packet_events();
        result
    }

    pub(crate) fn handle_write_completion(&mut self, current_time: Instant) -> MqttResult<()> {
        debug!("client impl - handle_write_completion");
        self.set_current_time(current_time);

        let mut context = NetworkEventContext {
            event: NetworkEvent::WriteCompletion,
            current_time,
            packet_events: &mut self.packet_events
        };

        self.protocol_state.handle_network_event(&mut context)
    }

    pub(crate) fn handle_service(&mut self, outbound_data: &mut Vec<u8>, current_time: Instant) -> MqttResult<()> {
        debug!("client impl - handle_service");
        self.set_current_time(current_time);

        let mut context = ServiceContext {
            to_socket: outbound_data,
            current_time,
        };

        self.protocol_state.service(&mut context)
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

    pub(crate) fn compute_optional_state_transition(&self) -> Option<ClientImplState> {
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

    pub(crate) fn get_next_connected_service_time(&mut self, current_time: Instant) -> Option<Instant> {
        self.set_current_time(current_time);

        if self.current_state == ClientImplState::Connected {
            return self.protocol_state.get_next_service_timepoint(&current_time);
        }

        None
    }

    fn emit_connection_attempt_event(&self) {
        let connection_attempt_event = ConnectionAttemptEvent {
        };

        self.broadcast_event(Arc::new(ClientEvent::ConnectionAttempt(connection_attempt_event)));
    }

    fn emit_connection_success_event(&self) {
        let settings = self.protocol_state.get_negotiated_settings().as_ref().unwrap();

        let connection_success_event = ConnectionSuccessEvent {
            connack: self.last_connack.as_ref().unwrap().clone(),
            settings: settings.clone(),
        };

        self.broadcast_event(Arc::new(ClientEvent::ConnectionSuccess(connection_success_event)));
    }

    fn emit_connection_failure_event(&mut self) {
        let mut connection_failure_event = ConnectionFailureEvent {
            error: self.last_error.take().unwrap_or(MqttError::new_connection_establishment_failure("unknown failure source")),
            connack: None,
        };

        if let Some(connack) = &self.last_connack {
            connection_failure_event.connack = Some(connack.clone());
        }

        self.broadcast_event(Arc::new(ClientEvent::ConnectionFailure(connection_failure_event)));
    }

    fn emit_disconnection_event(&mut self) {
        let mut disconnection_event = DisconnectionEvent {
            error: self.last_error.take().unwrap_or(MqttError::new_connection_closed("disconnection with no source error")),
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

    pub(crate) fn transition_to_state(&mut self, mut new_state: ClientImplState, current_time: Instant) -> MqttResult<()> {
        self.set_current_time(current_time);

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
        //      the protocol state.  But then we blindly transition to pending connect which isn't
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

        debug!("client impl transition_to_state - old state: {}, new_state: {}", old_state, new_state);

        if new_state == ClientImplState::Connected {
            let establishment_timeout = self.last_start_connect_time.unwrap() + self.connect_timeout;
            let mut connection_opened_context = NetworkEventContext {
                event: NetworkEvent::ConnectionOpened(ConnectionOpenedContext{
                    establishment_timeout,
                }),
                current_time,
                packet_events: &mut self.packet_events
            };

            self.protocol_state.handle_network_event(&mut connection_opened_context)?;
        } else if old_state == ClientImplState::Connected {
            let mut connection_closed_context = NetworkEventContext {
                event: NetworkEvent::ConnectionClosed,
                current_time,
                packet_events: &mut self.packet_events
            };

            self.protocol_state.handle_network_event(&mut connection_closed_context)?;
        }

        if new_state == ClientImplState::Connecting {
            self.last_error = None;
            self.last_connack = None;
            self.last_disconnect = None;
            self.last_start_connect_time = Some(current_time);
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
                if (current_time - successful_connect_timepoint) > self.reconnect_options.reconnect_stability_reset_period {
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
