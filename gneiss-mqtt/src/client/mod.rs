/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

pub(crate) mod asyncstd_impl;
pub mod builder;
pub(crate) mod shared_impl;
pub(crate) mod thread_impl;
pub mod tokio_impl;

use crate::*;
use crate::alias::OutboundAliasResolver;
use crate::client::shared_impl::*;
use crate::spec::*;
use crate::spec::disconnect::validate_disconnect_packet_outbound;
use crate::spec::utils::*;
use crate::validate::*;

use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// async choice conditional
extern crate tokio;
use crate::client::tokio_impl::*;
use tokio::runtime;
use tokio::io::{AsyncRead, AsyncWrite};

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
                write!(f, "Qos0")
            }
            PublishResponse::Qos1(puback) => {
                write!(f, "Qos1 ( {} )", puback)
            }
            PublishResponse::Qos2(qos2response) => {
                write!(f, "Qos2 ( {} )", qos2response)
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

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub enum OfflineQueuePolicy {
    #[default]
    PreserveAll,
    PreserveAcknowledged,
    PreserveQos1PlusPublishes,
    PreserveNothing,
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub enum RejoinSessionPolicy {
    #[default]
    PostSuccess,
    Always,
    Never
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub enum ExponentialBackoffJitterType {
    None,
    #[default]
    Uniform
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

pub type ClientEventListenerCallback = dyn Fn(Arc<ClientEvent>) + Send + Sync;

pub enum ClientEventListener {
    Channel(AsyncClientEventSender),
    Callback(Arc<ClientEventListenerCallback>)
}

impl Debug for ClientEventListener {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ClientEventListener::Channel(_) => {
                write!(f, "ClientEventListener::Channel(...)")
            }
            ClientEventListener::Callback(_) => {
                write!(f, "ClientEventListener::Callback(...)")
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ListenerHandle {
    id: u64
}


/// Configuration options that will determine packet field values for the CONNECT packet sent out
/// by the client on each connection attempt.  Almost equivalent to ConnectPacket, but there are a
/// few differences that make exposing a ConnectPacket directly awkward and potentially misleading.
///
/// Auth-related fields are not yet exposed because we don't support authentication exchanges yet.
#[derive(Debug, Default)]
pub struct ConnectOptions {

    /// The maximum time interval, in seconds, that is permitted to elapse between the point at which the client
    /// finishes transmitting one MQTT packet and the point it starts sending the next.  The client will use
    /// PINGREQ packets to maintain this property.
    ///
    /// If the responding CONNACK contains a keep alive property value, then that is the negotiated keep alive value.
    /// Otherwise, the keep alive sent by the client is the negotiated value.
    ///
    /// See [MQTT5 Keep Alive](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901045)
    ///
    /// If the final negotiated value is 0, then that means no keep alive will be used.  Such a
    /// state is not advised due to scenarios where TCP connections can be invisibly dropped by
    /// routers/firewalls within the full connection circuit.
    pub(crate) keep_alive_interval_seconds: Option<u16>,

    /// Configuration value that determines how the client will attempt to rejoin sessions
    pub(crate) rejoin_session_policy: RejoinSessionPolicy,

    /// A unique string identifying the client to the server.  Used to restore session state between connections.
    ///
    /// If left empty, the broker will auto-assign a unique client id.  When reconnecting, the mqtt5 client will
    /// always use the auto-assigned client id.
    ///
    /// See [MQTT5 Client Identifier](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901059)
    pub(crate) client_id: Option<String>,

    /// A string value that the server may use for client authentication and authorization.
    ///
    /// See [MQTT5 User Name](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901071)
    username: Option<String>,

    /// Opaque binary data that the server may use for client authentication and authorization.
    ///
    /// See [MQTT5 Password](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901072)
    password: Option<Vec<u8>>,

    /// A time interval, in seconds, that the client requests the server to persist this connection's MQTT session state
    /// for.  Has no meaning if the client has not been configured to rejoin sessions.  Must be non-zero in order to
    /// successfully rejoin a session.
    ///
    /// If the responding CONNACK contains a session expiry property value, then that is the negotiated session expiry
    /// value.  Otherwise, the session expiry sent by the client is the negotiated value.
    ///
    /// See [MQTT5 Session Expiry Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901048)
    pub(crate) session_expiry_interval_seconds: Option<u32>,

    /// If set to true, requests that the server send response information in the subsequent CONNACK.  This response
    /// information may be used to set up request-response implementations over MQTT, but doing so is outside
    /// the scope of the MQTT5 spec and client.
    ///
    /// See [MQTT5 Request Response Information](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901052)
    request_response_information: Option<bool>,

    /// If set to true, requests that the server send additional diagnostic information (via response string or
    /// user properties) in DISCONNECT or CONNACK packets from the server.
    ///
    /// See [MQTT5 Request Problem Information](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901053)
    request_problem_information: Option<bool>,

    /// Notifies the server of the maximum number of in-flight Qos 1 and 2 messages the client is willing to handle.  If
    /// omitted, then no limit is requested.
    ///
    /// See [MQTT5 Receive Maximum](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901049)
    receive_maximum: Option<u16>,

    /// Maximum number of topic aliases that the client will accept for incoming publishes.  An inbound topic alias larger than
    /// this number is a protocol error.  If this value is not specified, the client does not support inbound topic
    /// aliasing.
    ///
    /// See [MQTT5 Topic Alias Maximum](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901051)
    pub(crate) topic_alias_maximum: Option<u16>,

    /// Notifies the server of the maximum packet size the client is willing to handle.  If
    /// omitted, then no limit beyond the natural limits of MQTT packet size is requested.
    ///
    /// See [MQTT5 Maximum Packet Size](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901050)
    pub(crate) maximum_packet_size_bytes: Option<u32>,

    /// A time interval, in seconds, that the server should wait (for a session reconnection) before sending the
    /// will message associated with the connection's session.  If omitted, the server will send the will when the
    /// associated session is destroyed.  If the session is destroyed before a will delay interval has elapsed, then
    /// the will must be sent at the time of session destruction.
    ///
    /// See [MQTT5 Will Delay Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901062)
    will_delay_interval_seconds: Option<u32>,

    /// The definition of a message to be published when the connection's session is destroyed by the server or when
    /// the will delay interval has elapsed, whichever comes first.  If undefined, then nothing will be sent.
    ///
    /// See [MQTT5 Will](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901040)
    ///
    /// TODO: consider making this a builder function to allow dynamic will construction
    will: Option<PublishPacket>,

    /// Set of MQTT5 user properties to include with all CONNECT packets.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901054)
    user_properties: Option<Vec<UserProperty>>,
}

impl ConnectOptions {
    pub(crate) fn to_connect_packet(&self, connected_previously: bool) -> ConnectPacket {
        let clean_start =
            match self.rejoin_session_policy {
                RejoinSessionPolicy::PostSuccess => {
                    !connected_previously
                }
                RejoinSessionPolicy::Always => {
                    false
                }
                RejoinSessionPolicy::Never => {
                    true
                }
            };

        ConnectPacket {
            keep_alive_interval_seconds: self.keep_alive_interval_seconds.unwrap_or(0),
            clean_start,
            client_id: self.client_id.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            session_expiry_interval_seconds: self.session_expiry_interval_seconds,
            request_response_information: self.request_response_information,
            request_problem_information: self.request_problem_information,
            receive_maximum: self.receive_maximum,
            topic_alias_maximum: self.topic_alias_maximum,
            maximum_packet_size_bytes: self.maximum_packet_size_bytes,
            authentication_method: None,
            authentication_data: None,
            will_delay_interval_seconds: self.will_delay_interval_seconds,
            will: self.will.clone(),
            user_properties: self.user_properties.clone(),
        }
    }

    pub fn set_username(&mut self, username: Option<&str>) {
        self.username = username.map(str::to_string);
    }

    pub fn set_password(&mut self, password: Option<&[u8]>) {
        self.password = password.map(|p| p.to_vec());
    }
}

#[derive(Debug, Default)]
pub struct ConnectOptionsBuilder {
    options: ConnectOptions
}

impl ConnectOptionsBuilder {
    pub fn new() -> Self {
        ConnectOptionsBuilder {
            ..Default::default()
        }
    }

    pub fn with_keep_alive_interval_seconds(mut self, keep_alive: u16) -> Self {
        self.options.keep_alive_interval_seconds = Some(keep_alive);
        self
    }

    pub fn with_rejoin_session_policy(mut self, policy: RejoinSessionPolicy) -> Self {
        self.options.rejoin_session_policy = policy;
        self
    }

    pub fn with_client_id(mut self, client_id: &str) -> Self {
        self.options.client_id = Some(client_id.to_string());
        self
    }

    pub fn with_username(mut self, username: &str) -> Self {
        self.options.username = Some(username.to_string());
        self
    }

    pub fn with_password(mut self, password: &[u8]) -> Self {
        self.options.password = Some(password.to_vec());
        self
    }

    pub fn with_session_expiry_interval_seconds(mut self, session_expiry_interval_seconds: u32) -> Self {
        self.options.session_expiry_interval_seconds = Some(session_expiry_interval_seconds);
        self
    }

    pub fn with_request_response_information(mut self, request_response_information: bool) -> Self {
        self.options.request_response_information = Some(request_response_information);
        self
    }

    pub fn with_request_problem_information(mut self, request_problem_information: bool) -> Self {
        self.options.request_problem_information = Some(request_problem_information);
        self
    }

    pub fn with_receive_maximum(mut self, receive_maximum: u16) -> Self {
        self.options.receive_maximum = Some(receive_maximum);
        self
    }

    pub fn with_topic_alias_maximum(mut self, topic_alias_maximum: u16) -> Self {
        self.options.topic_alias_maximum = Some(topic_alias_maximum);
        self
    }

    pub fn with_maximum_packet_size_bytes(mut self, maximum_packet_size_bytes: u32) -> Self {
        self.options.maximum_packet_size_bytes = Some(maximum_packet_size_bytes);
        self
    }

    pub fn with_will_delay_interval_seconds(mut self, will_delay_interval_seconds: u32) -> Self {
        self.options.will_delay_interval_seconds = Some(will_delay_interval_seconds);
        self
    }

    pub fn with_will(mut self, will: PublishPacket) -> Self {
        self.options.will = Some(will);
        self
    }

    pub fn with_user_properties(mut self, user_properties: Vec<UserProperty>) -> Self {
        self.options.user_properties = Some(user_properties);
        self
    }

    pub fn build(self) -> ConnectOptions {
        self.options
    }
}

#[derive(Copy, Clone, Debug)]
struct ReconnectOptions {
    reconnect_period_jitter: ExponentialBackoffJitterType,
    base_reconnect_period: Duration,
    max_reconnect_period: Duration,
    reconnect_stability_reset_period: Duration,
}

impl ReconnectOptions {
    fn normalize(&mut self) {
        if self.base_reconnect_period > self.max_reconnect_period {
            std::mem::swap(&mut self.base_reconnect_period, &mut self.max_reconnect_period)
        }

        if self.max_reconnect_period < Duration::from_secs(1) {
            self.max_reconnect_period = Duration::from_secs(1);
        }
    }
}

impl Default for ReconnectOptions {
    fn default() -> Self {
        ReconnectOptions {
            reconnect_period_jitter: ExponentialBackoffJitterType::default(),
            base_reconnect_period: Duration::from_secs(1),
            max_reconnect_period: Duration::from_secs(120),
            reconnect_stability_reset_period: Duration::from_secs(30),
        }
    }
}

#[derive(Default)]
pub struct Mqtt5ClientOptions {
    pub connect_options : ConnectOptions,

    offline_queue_policy: OfflineQueuePolicy,

    connack_timeout: Duration,
    ping_timeout: Duration,

    default_event_listener: Option<ClientEventListener>,

    outbound_alias_resolver: Option<Box<dyn OutboundAliasResolver + Send>>,

    reconnect_options: ReconnectOptions,
}

impl Debug for Mqtt5ClientOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Mqtt5ClientOptions {{ ")?;
        write!(f, "connect_options: {:?}, ", self.connect_options)?;
        write!(f, "offline_queue_policy: {:?}, ", self.offline_queue_policy)?;
        write!(f, "connack_timeout: {:?}, ", self.connack_timeout)?;
        write!(f, "ping_timeout: {:?}, ", self.ping_timeout)?;
        write!(f, "default_event_listener: {:?}, ", self.default_event_listener)?;
        if self.outbound_alias_resolver.is_some() {
            write!(f, "outbound_alias_resolver: Some(...), ")?;
        } else {
            write!(f, "outbound_alias_resolver: None, ")?;
        };
        write!(f, "reconnect_options: {:?}, ", self.reconnect_options)?;

        write!(f, "}}")
    }
}

#[derive(Debug, Default)]
pub struct Mqtt5ClientOptionsBuilder {
    options: Mqtt5ClientOptions
}

impl Mqtt5ClientOptionsBuilder {
    pub fn new() -> Self {
        Mqtt5ClientOptionsBuilder {
            options: Mqtt5ClientOptions {
                ..Default::default()
            }
        }
    }

    pub fn with_connect_options(mut self, connect_options: ConnectOptions) -> Self {
        self.options.connect_options = connect_options;
        self
    }

    pub fn set_connect_options(&mut self, connect_options: ConnectOptions) {
        self.options.connect_options = connect_options;
    }

    pub fn with_offline_queue_policy(mut self, offline_queue_policy: OfflineQueuePolicy) -> Self {
        self.options.offline_queue_policy = offline_queue_policy;
        self
    }

    pub fn set_offline_queue_policy(&mut self, offline_queue_policy: OfflineQueuePolicy) {
        self.options.offline_queue_policy = offline_queue_policy;
    }

    pub fn with_connack_timeout(mut self, connack_timeout: Duration) -> Self {
        self.options.connack_timeout = connack_timeout;
        self
    }

    pub fn set_connack_timeout(&mut self, connack_timeout: Duration) {
        self.options.connack_timeout = connack_timeout;
    }

    pub fn with_ping_timeout(mut self, ping_timeout: Duration) -> Self {
        self.options.ping_timeout = ping_timeout;
        self
    }

    pub fn set_ping_timeout(&mut self, ping_timeout: Duration) {
        self.options.ping_timeout = ping_timeout;
    }

    pub fn with_default_event_listener(mut self, default_event_listener: ClientEventListener) -> Self {
        self.options.default_event_listener = Some(default_event_listener);
        self
    }

    pub fn set_default_event_listener(&mut self, default_event_listener: ClientEventListener) {
        self.options.default_event_listener = Some(default_event_listener);
    }

    pub fn with_outbound_alias_resolver(mut self, outbound_alias_resolver: Box<dyn OutboundAliasResolver + Send>) -> Self {
        self.options.outbound_alias_resolver = Some(outbound_alias_resolver);
        self
    }

    pub fn set_outbound_alias_resolver(&mut self, outbound_alias_resolver: Box<dyn OutboundAliasResolver + Send>) {
        self.options.outbound_alias_resolver = Some(outbound_alias_resolver);
    }

    pub fn with_reconnect_period_jitter(mut self, reconnect_period_jitter: ExponentialBackoffJitterType) -> Self {
        self.options.reconnect_options.reconnect_period_jitter = reconnect_period_jitter;
        self
    }

    pub fn set_reconnect_period_jitter(&mut self, reconnect_period_jitter: ExponentialBackoffJitterType) {
        self.options.reconnect_options.reconnect_period_jitter = reconnect_period_jitter;
    }

    pub fn with_base_reconnect_period(mut self, base_reconnect_period: Duration) -> Self {
        self.options.reconnect_options.base_reconnect_period = base_reconnect_period;
        self
    }

    pub fn set_base_reconnect_period(&mut self, base_reconnect_period: Duration) {
        self.options.reconnect_options.base_reconnect_period = base_reconnect_period;
    }

    pub fn with_max_reconnect_period(mut self, max_reconnect_period: Duration) -> Self {
        self.options.reconnect_options.max_reconnect_period = max_reconnect_period;
        self
    }

    pub fn set_max_reconnect_period(&mut self, max_reconnect_period: Duration) {
        self.options.reconnect_options.max_reconnect_period = max_reconnect_period;
    }

    pub fn with_reconnect_stability_reset_period(mut self, reconnect_stability_reset_period: Duration) -> Self {
        self.options.reconnect_options.reconnect_stability_reset_period = reconnect_stability_reset_period;
        self
    }

    pub fn set_reconnect_stability_reset_period(&mut self, reconnect_stability_reset_period: Duration) {
        self.options.reconnect_options.reconnect_stability_reset_period = reconnect_stability_reset_period;
    }

    pub fn build(self) -> Mqtt5ClientOptions {
        self.options
    }
}

// Note to self: don't implement clone.  the interface is not &mut so sharing across threads just
// needs an Arc wrapper
pub struct Mqtt5Client where {
    user_state: UserRuntimeState,

    listener_id_allocator: Mutex<u64>,
}

// conditional on runtime selection
type TokioConnectionFactoryReturnType<T> = Pin<Box<dyn Future<Output = std::io::Result<T>> + Send + Sync>>;
pub struct TokioClientOptions<T> where T : AsyncRead + AsyncWrite + Send + Sync + 'static {
    pub connection_factory: Box<dyn Fn() -> TokioConnectionFactoryReturnType<T> + Send + Sync>
}

impl Mqtt5Client {

    // async choice conditional
    pub fn new<T>(config: Mqtt5ClientOptions, tokio_config: TokioClientOptions<T>, runtime_handle: &runtime::Handle) -> Mqtt5Client where T : AsyncRead + AsyncWrite + Send + Sync + 'static {
        let (user_state, internal_state) = create_runtime_states(tokio_config);

        let client_impl = Mqtt5ClientImpl::new(config);

        spawn_client_impl(client_impl, internal_state, runtime_handle);

        Mqtt5Client {
            user_state,
            listener_id_allocator: Mutex::new(1),
        }
    }

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

