/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Module containing types for configuring an MQTT client.
 */

use crate::alias::{OutboundAliasResolverFactoryFn};
use crate::error::*;
use crate::mqtt::*;

use log::*;
use std::fmt::{Debug, Formatter};
use std::time::Duration;

/// Controls how the client attempts to rejoin MQTT sessions.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub enum RejoinSessionPolicy {

    /// The client will not attempt to rejoin a session until it successfully connects for the
    /// very first time.
    ///
    /// After that point, it will always attempt to rejoin a session.
    #[default]
    PostSuccess,

    /// The client will always attempt to rejoin a session.
    ///
    /// Until persistence is supported, this
    /// is technically a spec-non-compliant setting because the client cannot possibly have the
    /// correct session state on its initial connection attempt.
    Always,

    /// The client will never attempt to rejoin a session.
    Never
}

pub(crate) const DEFAULT_KEEP_ALIVE_SECONDS : u16 = 1200;

/// Configuration options that will determine packet field values for the CONNECT packet sent out
/// by the client on each connection attempt.
///
/// Almost equivalent to ConnectPacket, but there are a
/// few differences that make exposing a ConnectPacket directly awkward and potentially misleading.
///
/// Auth-related fields are not yet exposed because we don't support authentication exchanges yet.
#[derive(Debug, Clone)]
pub struct ConnectOptions {

    pub(crate) keep_alive_interval_seconds: Option<u16>,

    pub(crate) rejoin_session_policy: RejoinSessionPolicy,

    pub(crate) client_id: Option<String>,

    pub(crate) username: Option<String>,

    pub(crate) password: Option<Vec<u8>>,

    pub(crate) session_expiry_interval_seconds: Option<u32>,

    pub(crate) request_response_information: Option<bool>,

    pub(crate) request_problem_information: Option<bool>,

    pub(crate) receive_maximum: Option<u16>,

    pub(crate) topic_alias_maximum: Option<u16>,

    pub(crate) maximum_packet_size_bytes: Option<u32>,

    pub(crate) will_delay_interval_seconds: Option<u32>,

    pub(crate) will: Option<PublishPacket>,

    pub(crate) user_properties: Option<Vec<UserProperty>>,
}

impl ConnectOptions {

    /// Creates a new builder for a ConnectOptions instances.
    pub fn builder() -> ConnectOptionsBuilder {
        ConnectOptionsBuilder::new()
    }

    /// Creates a new builder object for ConnectOptions using an existing ConnectOptions
    /// value as a starting point.
    ///
    /// Useful for internally tweaking user-supplied configuration.
    pub fn builder_from_existing(connect_options: ConnectOptions) -> ConnectOptionsBuilder {
        ConnectOptionsBuilder::new_from_existing(connect_options)
    }

    // TODO: implement as From<ConnectOptions>
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

    /// Returns the MQTT client id currently configured in these options
    pub fn client_id(&self) -> &Option<String> { &self.client_id }
}

/// Builder type for connection-related options on the client.
///
/// These options will determine packet field values for the CONNECT packet sent out
/// by the client on each connection attempt.
#[derive(Debug)]
pub struct ConnectOptionsBuilder {
    options: ConnectOptions
}

impl ConnectOptionsBuilder {

    pub(crate) fn new() -> Self {
        ConnectOptionsBuilder {
            options: ConnectOptions {
                keep_alive_interval_seconds: Some(DEFAULT_KEEP_ALIVE_SECONDS),
                rejoin_session_policy: RejoinSessionPolicy::PostSuccess,
                client_id: None,
                username: None,
                password: None,
                session_expiry_interval_seconds: None,
                request_response_information: None,
                request_problem_information: None,
                receive_maximum: None,
                topic_alias_maximum: None,
                maximum_packet_size_bytes: None,
                will_delay_interval_seconds: None,
                will: None,
                user_properties: None,
            }
        }
    }

    pub(crate) fn new_from_existing(options: ConnectOptions) -> Self {
        ConnectOptionsBuilder {
            options
        }
    }

    /// Sets the maximum time interval, in seconds, that is permitted to elapse between the point at which the client
    /// finishes transmitting one MQTT packet and the point it starts sending the next.
    ///
    /// The client will use
    /// PINGREQ packets to maintain this property.
    ///
    /// If the responding CONNACK contains a keep alive property value, then that is the negotiated keep alive value.
    /// Otherwise, the keep alive sent by the client is the negotiated value.
    ///
    /// See [MQTT Keep Alive](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901045)
    ///
    /// If the final negotiated value is 0, then that means no keep alive will be used.  Such a
    /// state is not advised due to scenarios where TCP connections can be invisibly dropped by
    /// routers/firewalls within the full connection circuit.
    pub fn with_keep_alive_interval_seconds(&mut self, keep_alive: Option<u16>) -> &mut Self {
        self.options.keep_alive_interval_seconds = keep_alive;
        self
    }

    /// Configures how the client will attempt to rejoin sessions
    pub fn with_rejoin_session_policy(&mut self, policy: RejoinSessionPolicy) -> &mut Self {
        self.options.rejoin_session_policy = policy;
        self
    }

    /// Sets a unique string identifying the client to the server.
    ///
    /// Used to restore session state between connections.
    ///
    /// If left empty, the broker will auto-assign a unique client id.
    ///
    /// See [MQTT Client Identifier](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901059)
    pub fn with_client_id(&mut self, client_id: &str) -> &mut Self {
        self.options.client_id = Some(client_id.to_string());
        self
    }

    /// Sets a string value that the server may use for client authentication and authorization.
    ///
    /// See [MQTT User Name](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901071)
    pub fn with_username(&mut self, username: &str) -> &mut Self {
        self.options.username = Some(username.to_string());
        self
    }

    /// Sets opaque binary data that the server may use for client authentication and authorization.
    ///
    /// See [MQTT Password](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901072)
    pub fn with_password(&mut self, password: &[u8]) -> &mut Self {
        self.options.password = Some(password.to_vec());
        self
    }

    /// Sets the time interval, in seconds, that the client requests the server to persist this connection's MQTT session state
    /// for.
    ///
    /// Has no meaning if the client has not been configured to rejoin sessions.  Must be non-zero in order to
    /// successfully rejoin a session.
    ///
    /// If the responding CONNACK contains a session expiry property value, then that is the negotiated session expiry
    /// value.  Otherwise, the session expiry sent by the client is the negotiated value.
    ///
    /// See [MQTT5 Session Expiry Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901048)
    pub fn with_session_expiry_interval_seconds(&mut self, session_expiry_interval_seconds: u32) -> &mut Self {
        self.options.session_expiry_interval_seconds = Some(session_expiry_interval_seconds);
        self
    }

    /// Sets whether or not the server should send response information in the subsequent CONNACK.
    ///
    /// This response
    /// information may be used to set up request-response implementations over MQTT, but doing so is outside
    /// the scope of the MQTT5 spec and client.
    ///
    /// See [MQTT5 Request Response Information](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901052)
    pub fn with_request_response_information(&mut self, request_response_information: bool) -> &mut Self {
        self.options.request_response_information = Some(request_response_information);
        self
    }

    /// Sets whether or not the server should send additional diagnostic information (via response string or
    /// user properties) in DISCONNECT or CONNACK packets from the server.
    ///
    /// See [MQTT5 Request Problem Information](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901053)
    pub fn with_request_problem_information(&mut self, request_problem_information: bool) -> &mut Self {
        self.options.request_problem_information = Some(request_problem_information);
        self
    }

    /// Sets a value that notifies the server of the maximum number of in-flight Qos 1 and 2
    /// messages the client is willing to handle.
    ///
    /// If omitted, then no limit is requested.
    ///
    /// See [MQTT5 Receive Maximum](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901049)
    pub fn with_receive_maximum(&mut self, receive_maximum: u16) -> &mut Self {
        self.options.receive_maximum = Some(receive_maximum);
        self
    }

    /// Sets a value that controls the maximum number of topic aliases that the client will accept
    /// for incoming publishes.
    ///
    /// An inbound topic alias larger than
    /// this number is a protocol error.  If this value is not specified, the client does not
    /// support inbound topic aliasing.
    ///
    /// See [MQTT5 Topic Alias Maximum](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901051)
    pub fn with_topic_alias_maximum(&mut self, topic_alias_maximum: u16) -> &mut Self {
        self.options.topic_alias_maximum = Some(topic_alias_maximum);
        self
    }

    /// A setting that notifies the server of the maximum packet size the client is willing to handle.
    ///
    /// If omitted, then no limit beyond the natural limits of MQTT packet size is requested.
    ///
    /// See [MQTT5 Maximum Packet Size](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901050)
    pub fn with_maximum_packet_size_bytes(&mut self, maximum_packet_size_bytes: u32) -> &mut Self {
        self.options.maximum_packet_size_bytes = Some(maximum_packet_size_bytes);
        self
    }

    /// Sets the time interval, in seconds, that the server should wait (for a session reconnection) before sending the
    /// will message associated with the connection's session.
    ///
    /// If omitted, the server will send the will when the
    /// associated session is destroyed.  If the session is destroyed before a will delay interval has elapsed, then
    /// the will must be sent at the time of session destruction.
    ///
    /// See [MQTT5 Will Delay Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901062)
    pub fn with_will_delay_interval_seconds(&mut self, will_delay_interval_seconds: u32) -> &mut Self {
        self.options.will_delay_interval_seconds = Some(will_delay_interval_seconds);
        self
    }

    /// Configures a message to be published when the connection's session is destroyed by the server or when
    /// the will delay interval has elapsed, whichever comes first.
    ///
    /// If omitted, then no will message will be sent.
    ///
    /// See [MQTT Will](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901040)
    pub fn with_will(&mut self, will: PublishPacket) -> &mut Self {
        self.options.will = Some(will);
        self
    }

    /// Sets the MQTT5 user properties to include with all CONNECT packets.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901054)
    pub fn with_user_properties(&mut self, user_properties: Vec<UserProperty>) -> &mut Self {
        self.options.user_properties = Some(user_properties);
        self
    }

    /// Builds a new ConnectOptions object for client construction
    pub fn build(&self) -> ConnectOptions {
        self.options.clone()
    }
}

/// Controls how the client treats existing and newly-submitted operations while it does not
/// have a valid connection to the broker.
///
/// Protocol requirements always override this setting as needed.  For example, even if set to
/// PreserveNothing, a client must, by specification, keep in-progress QoS1 and 2 operations
/// ready and available should a session by resumed later.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub enum OfflineQueuePolicy {

    /// Operations are never failed due to connection state
    #[default]
    PreserveAll,

    /// Qos0 Publishes are failed when there is no connection, all other operations are left alone.
    PreserveAcknowledged,

    /// Only QoS1 and QoS2 publishes are retained when there is no connection
    PreserveQos1PlusPublishes,

    /// Nothing is retained when there is no connection
    PreserveNothing,
}

/// Controls what kind of jitter, if any, the client will apply to the exponential backoff waiting
/// period in-between connection attempts.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExponentialBackoffJitterType {

    /// The client will not perform any jitter to the backoff, leading to a rigid doubling of
    /// the reconnect time period.
    ///
    /// Not recommended for real use; useful for correctness testing.
    None,

    /// The client will pick a wait duration uniformly between 0 and the current exponential
    /// backoff (which doubles each time up to the maximum).
    #[default]
    Uniform
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct ReconnectOptions {
    pub(crate) reconnect_period_jitter: ExponentialBackoffJitterType,
    pub(crate) base_reconnect_period: Duration,
    pub(crate) max_reconnect_period: Duration,
    pub(crate) reconnect_stability_reset_period: Duration,
}

impl ReconnectOptions {
    pub(crate) fn normalize(&mut self) {
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

/// Controls how the client selects what MQTT protocol to use
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub enum ProtocolMode {

    /// Use MQTT 5 as the client protocol
    #[default]
    Mqtt5,

    /// Use MQTT 311 as the client protocol
    Mqtt311,

    // Maybe some day we'll add an adaptive mode, Mqtt5Downgradable or the like
}

impl TryFrom<u32> for ProtocolMode {
    type Error = GneissError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            5 => { Ok(ProtocolMode::Mqtt5) }
            311 => { Ok(ProtocolMode::Mqtt311) }
            _ => {
                let message = format!("ProtocolMode::try_from - invalid protocol mode value ({})", value);
                error!("{}", message);
                Err(GneissError::new_other_error(message))
            }
        }
    }
}

/// Controls how the client resubmits (ack-based) operations that were interrupted by the preceding
/// disconnection.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
pub enum PostReconnectQueueDrainPolicy {

    /// Does not apply any flow control when resubmitting interrupted operations on a new
    /// connection.
    #[default]
    None,

    /// Resubmits previously interrupted operations one at a time.  The next operation will not
    /// be submitted until the previous one completes.
    OneAtATime,

}

/// A structure that holds client-level behavioral configuration
#[derive(Clone)]
pub struct MqttClientOptions {
    pub(crate) offline_queue_policy: OfflineQueuePolicy,

    pub(crate) connect_timeout: Duration,
    pub(crate) ping_timeout: Duration,

    pub(crate) outbound_alias_resolver_factory: Option<OutboundAliasResolverFactoryFn>,

    pub(crate) reconnect_options: ReconnectOptions,

    pub(crate) protocol_mode: ProtocolMode,

    // use an Option so that the AWS client builder can tell if this has been explicitly set or not
    pub(crate) post_reconnect_queue_drain_policy: Option<PostReconnectQueueDrainPolicy>,

    pub(crate) max_interrupted_retries: Option<u32>,
}

impl MqttClientOptions {

    /// Creates a new builder for MqttClientOptions instances.
    pub fn builder() -> MqttClientOptionsBuilder {
        MqttClientOptionsBuilder::new()
    }

    /// Creates a new builder from an existing MqttClientOptions instance
    pub fn to_builder(self) -> MqttClientOptionsBuilder {
        MqttClientOptionsBuilder::new_from_options(self)
    }

    #[doc(hidden)]
    pub fn protocol_mode(&self) -> ProtocolMode {
        self.protocol_mode
    }

    #[doc(hidden)]
    pub fn post_reconnect_queue_drain_policy(&self) -> Option<PostReconnectQueueDrainPolicy> {
        self.post_reconnect_queue_drain_policy
    }

    #[doc(hidden)]
    pub fn max_interrupted_retries(&self) -> Option<u32> {
        self.max_interrupted_retries
    }
}

impl Debug for MqttClientOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MqttClientOptions {{ ")?;
        write!(f, "offline_queue_policy: {:?}, ", self.offline_queue_policy)?;
        write!(f, "connect_timeout: {:?}, ", self.connect_timeout)?;
        write!(f, "ping_timeout: {:?}, ", self.ping_timeout)?;
        if self.outbound_alias_resolver_factory.is_some() {
            write!(f, "outbound_alias_resolver_factory: Some(...), ")?;
        } else {
            write!(f, "outbound_alias_resolver_factory: None, ")?;
        };
        write!(f, "reconnect_options: {:?}, ", self.reconnect_options)?;

        write!(f, "}}")
    }
}

/// A builder for client-level behavior configuration options
#[derive(Debug)]
pub struct MqttClientOptionsBuilder {
    options: MqttClientOptions
}

impl MqttClientOptionsBuilder {

    pub(crate) fn new() -> Self {
        MqttClientOptionsBuilder {
            options: MqttClientOptions {
                offline_queue_policy: OfflineQueuePolicy::PreserveAcknowledged,
                connect_timeout: Duration::from_secs(30),
                ping_timeout: Duration::from_secs(10),
                outbound_alias_resolver_factory: None,
                reconnect_options: ReconnectOptions::default(),
                protocol_mode: ProtocolMode::Mqtt5,
                post_reconnect_queue_drain_policy: None,
                max_interrupted_retries: None,
            }
        }
    }

    pub(crate) fn new_from_options(options: MqttClientOptions) -> Self {
        MqttClientOptionsBuilder {
            options
        }
    }

    /// Configures how the client should treat queued and newly-submitted operations while
    /// it does not have a connection to the broker.
    pub fn with_offline_queue_policy(&mut self, offline_queue_policy: OfflineQueuePolicy) -> &mut Self {
        self.options.offline_queue_policy = offline_queue_policy;
        self
    }

    /// Configures how long the client will wait for the client's transport connection to be fully
    /// established (such that the MQTT protocol can begin).
    pub fn with_connect_timeout(&mut self, connect_timeout: Duration) -> &mut Self {
        self.options.connect_timeout = connect_timeout;
        self
    }

    /// Configures how long, after sending a Pingreq, the client will wait for a Pingresp from the
    /// broker before giving up and shutting down the connection.
    pub fn with_ping_timeout(&mut self, ping_timeout: Duration) -> &mut Self {
        self.options.ping_timeout = ping_timeout;
        self
    }

    /// Configures an outbound topic alias resolver to be used when sending Publish packets to
    /// the broker.
    pub fn with_outbound_alias_resolver_factory(&mut self, outbound_alias_resolver_factory: OutboundAliasResolverFactoryFn) -> &mut Self {
        self.options.outbound_alias_resolver_factory = Some(outbound_alias_resolver_factory);
        self
    }

    /// Configures what kind of jitter, if any, should be applied to the waiting period between
    /// connection attempts.
    pub fn with_reconnect_period_jitter(&mut self, reconnect_period_jitter: ExponentialBackoffJitterType) -> &mut Self {
        self.options.reconnect_options.reconnect_period_jitter = reconnect_period_jitter;
        self
    }

    /// Configures the minimum amount of time to wait between connection attempts.
    ///
    /// Depending on
    /// jitter settings, the actual wait period may be shorter.  Defaults to one second if not
    /// specified.
    pub fn with_base_reconnect_period(&mut self, base_reconnect_period: Duration) -> &mut Self {
        self.options.reconnect_options.base_reconnect_period = base_reconnect_period;
        self
    }

    /// Configures the maximum amount of time to wait between connection attempts.
    ///
    /// Defaults to
    /// two minutes if not specified.
    pub fn with_max_reconnect_period(&mut self, max_reconnect_period: Duration) -> &mut Self {
        self.options.reconnect_options.max_reconnect_period = max_reconnect_period;
        self
    }

    /// Configures the interval of time that the client must remain successfully connected before
    /// the exponential backoff for connection attempts is reset.
    ///
    /// Defaults to thirty seconds if
    /// not specified.
    pub fn with_reconnect_stability_reset_period(&mut self, reconnect_stability_reset_period: Duration) -> &mut Self {
        self.options.reconnect_options.reconnect_stability_reset_period = reconnect_stability_reset_period;
        self
    }

    /// Configures how the client chooses an MQTT protocol version to communicate with.
    ///
    /// Defaults to MQTT5
    pub fn with_protocol_mode(&mut self, protocol_mode: ProtocolMode) -> &mut Self {
        self.options.protocol_mode = protocol_mode;
        self
    }

    /// Configures how the client resubmits (ack-based) operations that were interrupted by the preceding
    /// disconnection.
    ///
    /// Defaults to a policy that does not apply any throttling to resubmission
    pub fn with_post_reconnect_queue_drain_policy(&mut self, policy: PostReconnectQueueDrainPolicy) -> &mut Self {
        self.options.post_reconnect_queue_drain_policy = Some(policy);
        self
    }

    /// Configures how quickly, if at all, the client will give up on publish/subscribe/unsubscribe
    /// operations that are interrupted (ie the operation has been sent but not acknowledged) by
    /// disconnections.
    ///
    /// This setting is useful when you have a broker that disconnects you in response to
    /// certain valid (by the spec) operations.  In that case, if operations are always retried,
    /// the client enters a "death loop" where it continuously sends the operation on post-reconnect
    /// and immediately gets disconnected again.
    ///
    /// This setting works together with the post reconnect queue drain policy to isolate "poison"
    /// packets and eventually fail them rather than let them wreck the client.  It's expected that
    /// this is mostly useful when operating in MQTT311 mode, where error reporting is extremely
    /// limited and brokers may just close the connection when you exceed a service limit.
    ///
    /// Defaults to no limit.
    pub fn with_max_interrupted_retries(&mut self, max_retries: u32) -> &mut Self {
        self.options.max_interrupted_retries = Some(max_retries);
        self
    }

    /// Builds a new set of client options
    pub fn build(&self) -> MqttClientOptions {
        self.options.clone()
    }
}
