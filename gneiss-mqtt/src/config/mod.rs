/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Module containing types for configuring an MQTT client.
 */

extern crate http;
extern crate rustls;
extern crate rustls_pki_types;
extern crate tokio;
extern crate tokio_rustls;
extern crate tokio_tungstenite;
extern crate tungstenite;
extern crate stream_ws;


use crate::*;
use crate::alias::{OutboundAliasResolver, OutboundAliasResolverFactoryFn};
use crate::client::*;
use crate::features::gneiss_tokio::{TokioClientOptions};

use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::Read;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio_rustls::client::TlsStream;
use tokio_rustls::{TlsConnector};

use std::future::Future;
use std::pin::Pin;
use tungstenite::Message;
use tokio_tungstenite::{client_async, WebSocketStream};
use stream_ws::{tungstenite::WsMessageHandler, WsMessageHandle, WsByteStream};


/// Return type for a websocket handshake transformation function
pub type WebsocketHandshakeTransformReturnType = Pin<Box<dyn Future<Output = std::io::Result<http::request::Builder>> + Send + Sync >>;

/// Async websocket handshake transformation function type
pub type WebsocketHandshakeTransform = Box<dyn Fn(http::request::Builder) -> WebsocketHandshakeTransformReturnType + Send + Sync>;

/// Configuration options related to establishing an MQTT over websockets
#[derive(Default, Clone)]
pub struct WebsocketOptions {
    pub(crate) handshake_transform: Arc<Option<WebsocketHandshakeTransform>>
}

/// Builder type for constructing Websockets-related configuration.
pub struct WebsocketOptionsBuilder {
    options : WebsocketOptions
}

impl WebsocketOptionsBuilder {

    /// Creates a new builder object with default options.
    pub fn new() -> Self {
        WebsocketOptionsBuilder {
            options: WebsocketOptions {
                ..Default::default()
            }
        }
    }

    /// Configure an async transformation function that operates on the websocket handshake.  Useful
    /// for brokers that require some kind of signing algorithm to accept the upgrade request.
    pub fn with_handshake_transform(mut self, transform: WebsocketHandshakeTransform) -> Self {
        self.options.handshake_transform = Arc::new(Some(transform));
        self
    }

    /// Creates a new set of Websocket options
    pub fn build(self) -> WebsocketOptions {
        self.options
    }
}

#[derive(Eq, PartialEq, Clone)]
pub(crate) enum TlsMode {
    Standard,
    Mtls
}

#[derive(Clone)]
pub(crate) enum TlsData {
    Rustls(TlsMode, Arc<rustls::ClientConfig>),
}

/// Opaque struct containing TLS configuration data, assuming TLS has been enabled as a feature
#[derive(Clone)]
pub struct TlsOptions {
    pub(crate) options: TlsData
}

/// Builder type for constructing TLS configuration.
pub struct TlsOptionsBuilder {
    pub(crate) mode: TlsMode,
    pub(crate) root_ca_bytes: Option<Vec<u8>>,
    pub(crate) certificate_bytes: Option<Vec<u8>>,
    pub(crate) private_key_bytes: Option<Vec<u8>>,
    pub(crate) verify_peer: bool,
    pub(crate) alpn: Option<Vec<u8>> // one protocol only for now
}

impl TlsOptionsBuilder {

    /// Creates a new builder object with default options.  Defaults may be TLS-feature specific.
    /// Presently, this means standard TLS using 1.2 or higher and the system trust store.
    pub fn new() -> Self {
        TlsOptionsBuilder::default()
    }

    /// Configures the builder to create a mutual TLS context using an X509 certificate and a
    /// private key, by file path.
    pub fn new_with_mtls_from_path(certificate_path: &str, private_key_path: &str) -> std::io::Result<Self> {
        let certificate_bytes = load_file(certificate_path)?;
        let private_key_bytes = load_file(private_key_path)?;

        Ok(TlsOptionsBuilder {
            mode: TlsMode::Mtls,
            root_ca_bytes: None,
            certificate_bytes: Some(certificate_bytes),
            private_key_bytes: Some(private_key_bytes),
            verify_peer: true,
            alpn: None
        })
    }

    /// Configures the builder to create a mutual TLS context using an X509 certificate and a
    /// private key, from memory.
    pub fn new_with_mtls_from_memory(certificate_bytes: &[u8], private_key_bytes: &[u8]) -> Self {
        TlsOptionsBuilder {
            mode: TlsMode::Mtls,
            root_ca_bytes: None,
            certificate_bytes: Some(certificate_bytes.to_vec()),
            private_key_bytes: Some(private_key_bytes.to_vec()),
            verify_peer: true,
            alpn: None
        }
    }

    /// Configures the builder to use a trust store that *only* contains a single root certificate,
    /// supplied by file path.
    pub fn with_root_ca_from_path(mut self, root_ca_path: &str) -> std::io::Result<Self> {
        self.root_ca_bytes = Some(load_file(root_ca_path)?);
        Ok(self)
    }

    /// Configures the builder to use a trust store that *only* contains a single root certificate,
    /// supplied from memory.
    pub fn with_root_ca_from_memory(mut self, root_ca_bytes: &[u8]) -> Self {
        self.root_ca_bytes = Some(root_ca_bytes.to_vec());
        self
    }

    /// Controls whether or not SNI is used during the TLS handshake.  It is highly recommended
    /// to set this value to false only in testing environments.
    pub fn with_verify_peer(mut self, verify_peer: bool) -> Self {
        self.verify_peer = verify_peer;
        self
    }

    /// Sets an ALPN protocol to negotiate during the TLS handshake.  Should multiple protocols
    /// become a valid use case, new APIs will be added to manipulate the set of protocols.
    pub fn with_alpn(mut self, alpn: &[u8]) -> Self {
        self.alpn = Some(alpn.to_vec());
        self
    }
}

impl Default for TlsOptionsBuilder {
    fn default() -> Self {
        TlsOptionsBuilder {
            mode: TlsMode::Standard,
            root_ca_bytes: None,
            certificate_bytes: None,
            private_key_bytes: None,
            verify_peer: true,
            alpn: None
        }
    }
}

fn load_file(filename: &str) -> std::io::Result<Vec<u8>> {
    let mut bytes_vec = Vec::new();
    let mut bytes_file = File::open(filename)?;
    bytes_file.read_to_end(&mut bytes_vec)?;
    Ok(bytes_vec)
}

/// Controls how the client attempts to rejoin MQTT sessions.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub enum RejoinSessionPolicy {

    /// The client will not attempt to rejoin a session until it successfully connects for the
    /// very first time.  After that point, it will always attempt to rejoin a session.
    #[default]
    PostSuccess,

    /// The client will always attempt to rejoin a session.  Until persistence is supported, this
    /// is technically a spec-non-compliant setting because the client cannot possibly have the
    /// correct session state on its initial connection attempt.
    Always,

    /// The client will never attempt to rejoin a session.
    Never
}

pub(crate) const DEFAULT_KEEP_ALIVE_SECONDS : u16 = 1200;

/// Configuration options that will determine packet field values for the CONNECT packet sent out
/// by the client on each connection attempt.  Almost equivalent to ConnectPacket, but there are a
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

    /// Sets the username value to be used in the client's Connect packets.  Public because
    /// external builder crates need this.
    pub fn set_username(&mut self, username: Option<&str>) {
        self.username = username.map(str::to_string);
    }

    /// Sets the password value to be used in the client's Connect packets.  Public because
    /// external builder crates need this.
    pub fn set_password(&mut self, password: Option<&[u8]>) {
        self.password = password.map(|p| p.to_vec());
    }
}

impl Default for ConnectOptions {

    /// Creates a ConnectOptions object with default values.
    ///
    /// In particular, MQTT keep alive is set to a "reasonable" default value rather than
    /// set to zero, which means don't use keep alive.  It is strongly recommended to never set
    /// keep alive to zero.
    fn default() -> Self {
        ConnectOptions {
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

/// A builder for connection-related options on the client.
///
/// These options will determine packet field values for the CONNECT packet sent out
/// by the client on each connection attempt.
#[derive(Debug, Default)]
pub struct ConnectOptionsBuilder {
    options: ConnectOptions
}

impl ConnectOptionsBuilder {

    /// Creates a new builder object for ConnectOptions
    pub fn new() -> Self {
        ConnectOptionsBuilder {
            ..Default::default()
        }
    }

    /// Sets the maximum time interval, in seconds, that is permitted to elapse between the point at which the client
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
    pub fn with_keep_alive_interval_seconds(mut self, keep_alive: Option<u16>) -> Self {
        self.options.keep_alive_interval_seconds = keep_alive;
        self
    }

    /// Configures how the client will attempt to rejoin sessions
    pub fn with_rejoin_session_policy(mut self, policy: RejoinSessionPolicy) -> Self {
        self.options.rejoin_session_policy = policy;
        self
    }

    /// Sets a unique string identifying the client to the server.  Used to restore session state between connections.
    ///
    /// If left empty, the broker will auto-assign a unique client id.  When reconnecting, the mqtt5 client will
    /// always use the auto-assigned client id.
    ///
    /// See [MQTT5 Client Identifier](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901059)
    pub fn with_client_id(mut self, client_id: &str) -> Self {
        self.options.client_id = Some(client_id.to_string());
        self
    }

    /// Sets a string value that the server may use for client authentication and authorization.
    ///
    /// See [MQTT5 User Name](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901071)
    pub fn with_username(mut self, username: &str) -> Self {
        self.options.username = Some(username.to_string());
        self
    }

    /// Sets opaque binary data that the server may use for client authentication and authorization.
    ///
    /// See [MQTT5 Password](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901072)
    pub fn with_password(mut self, password: &[u8]) -> Self {
        self.options.password = Some(password.to_vec());
        self
    }

    /// Sets the time interval, in seconds, that the client requests the server to persist this connection's MQTT session state
    /// for.  Has no meaning if the client has not been configured to rejoin sessions.  Must be non-zero in order to
    /// successfully rejoin a session.
    ///
    /// If the responding CONNACK contains a session expiry property value, then that is the negotiated session expiry
    /// value.  Otherwise, the session expiry sent by the client is the negotiated value.
    ///
    /// See [MQTT5 Session Expiry Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901048)
    pub fn with_session_expiry_interval_seconds(mut self, session_expiry_interval_seconds: u32) -> Self {
        self.options.session_expiry_interval_seconds = Some(session_expiry_interval_seconds);
        self
    }

    /// Sets whether or not the server should send response information in the subsequent CONNACK.  This response
    /// information may be used to set up request-response implementations over MQTT, but doing so is outside
    /// the scope of the MQTT5 spec and client.
    ///
    /// See [MQTT5 Request Response Information](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901052)
    pub fn with_request_response_information(mut self, request_response_information: bool) -> Self {
        self.options.request_response_information = Some(request_response_information);
        self
    }

    /// Sets whether or not the server should send additional diagnostic information (via response string or
    /// user properties) in DISCONNECT or CONNACK packets from the server.
    ///
    /// See [MQTT5 Request Problem Information](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901053)
    pub fn with_request_problem_information(mut self, request_problem_information: bool) -> Self {
        self.options.request_problem_information = Some(request_problem_information);
        self
    }

    /// Sets a value that notifies the server of the maximum number of in-flight Qos 1 and 2
    /// messages the client is willing to handle.  If omitted, then no limit is requested.
    ///
    /// See [MQTT5 Receive Maximum](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901049)
    pub fn with_receive_maximum(mut self, receive_maximum: u16) -> Self {
        self.options.receive_maximum = Some(receive_maximum);
        self
    }

    /// Sets a value that controls the maximum number of topic aliases that the client will accept
    /// for incoming publishes.  An inbound topic alias larger than
    /// this number is a protocol error.  If this value is not specified, the client does not
    /// support inbound topic aliasing.
    ///
    /// See [MQTT5 Topic Alias Maximum](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901051)
    pub fn with_topic_alias_maximum(mut self, topic_alias_maximum: u16) -> Self {
        self.options.topic_alias_maximum = Some(topic_alias_maximum);
        self
    }

    /// A setting that notifies the server of the maximum packet size the client is willing to handle.  If
    /// omitted, then no limit beyond the natural limits of MQTT packet size is requested.
    ///
    /// See [MQTT5 Maximum Packet Size](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901050)
    pub fn with_maximum_packet_size_bytes(mut self, maximum_packet_size_bytes: u32) -> Self {
        self.options.maximum_packet_size_bytes = Some(maximum_packet_size_bytes);
        self
    }

    /// Sets the time interval, in seconds, that the server should wait (for a session reconnection) before sending the
    /// will message associated with the connection's session.  If omitted, the server will send the will when the
    /// associated session is destroyed.  If the session is destroyed before a will delay interval has elapsed, then
    /// the will must be sent at the time of session destruction.
    ///
    /// See [MQTT5 Will Delay Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901062)
    pub fn with_will_delay_interval_seconds(mut self, will_delay_interval_seconds: u32) -> Self {
        self.options.will_delay_interval_seconds = Some(will_delay_interval_seconds);
        self
    }

    /// Configures a message to be published when the connection's session is destroyed by the server or when
    /// the will delay interval has elapsed, whichever comes first.  If undefined, then nothing will be sent.
    ///
    /// See [MQTT5 Will](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901040)
    pub fn with_will(mut self, will: PublishPacket) -> Self {
        self.options.will = Some(will);
        self
    }

    /// Sets the MQTT5 user properties to include with all CONNECT packets.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901054)
    pub fn with_user_properties(mut self, user_properties: Vec<UserProperty>) -> Self {
        self.options.user_properties = Some(user_properties);
        self
    }

    /// Builds a new ConnectOptions object for client construction
    pub fn build(self) -> ConnectOptions {
        self.options
    }
}

/// Controls how the client treats existing and newly-submitted operations while it does not
/// have a valid connection to the broker.
///
/// Protocol requirements always override this setting as needed.  For example, even if set to
/// PreserveNothing, a client must, by specification, keep in-progress QoS1 and 2 operations
/// ready and available should a session by resumed later.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
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
pub enum ExponentialBackoffJitterType {

    /// The client will not perform any jitter to the backoff, leading to a rigid doubling of
    /// the reconnect time period.  Not recommended for real use; useful for correctness testing.
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

/// Type for a callback function to be invoked with every emitted client event
pub type ClientEventListenerCallback = dyn Fn(Arc<ClientEvent>) + Send + Sync;

/// Union type for all of the different ways a client event listener can be configured.
#[derive(Clone)]
pub enum ClientEventListener {

    /// A function that should be invoked with the events.
    ///
    /// Important Note: for async clients, this function is invoked from a spawned task on the
    /// client's runtime.  This means you can safely `.await` within the callback without blocking
    /// the client's forward progress.
    Callback(Arc<ClientEventListenerCallback>)
}

impl Debug for ClientEventListener {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ClientEventListener::Callback(_) => {
                write!(f, "ClientEventListener::Callback(...)")
            }
        }
    }
}

/// A structure that holds client-level behavioral configuration
#[derive(Default, Clone)]
pub struct Mqtt5ClientOptions {
    pub(crate) offline_queue_policy: OfflineQueuePolicy,

    pub(crate) connack_timeout: Duration,
    pub(crate) ping_timeout: Duration,

    pub(crate) default_event_listener: Option<ClientEventListener>,

    pub(crate) outbound_alias_resolver_factory: Option<OutboundAliasResolverFactoryFn>,

    pub(crate) reconnect_options: ReconnectOptions,
}

impl Debug for Mqtt5ClientOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Mqtt5ClientOptions {{ ")?;
        write!(f, "offline_queue_policy: {:?}, ", self.offline_queue_policy)?;
        write!(f, "connack_timeout: {:?}, ", self.connack_timeout)?;
        write!(f, "ping_timeout: {:?}, ", self.ping_timeout)?;
        write!(f, "default_event_listener: {:?}, ", self.default_event_listener)?;
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
#[derive(Debug, Default)]
pub struct Mqtt5ClientOptionsBuilder {
    options: Mqtt5ClientOptions
}

impl Mqtt5ClientOptionsBuilder {

    /// Creates a new builder object for Mqtt5ClientOptions
    pub fn new() -> Self {
        Mqtt5ClientOptionsBuilder {
            options: Mqtt5ClientOptions {
                ..Default::default()
            }
        }
    }

    /// Configures how the client should treat queued and newly-submitted operations while
    /// it does not have a connection to the broker.
    pub fn with_offline_queue_policy(mut self, offline_queue_policy: OfflineQueuePolicy) -> Self {
        self.options.offline_queue_policy = offline_queue_policy;
        self
    }

    /// Configures how long the client will wait for a Connack from the broker before giving up and
    /// failing the connection attempt.
    pub fn with_connack_timeout(mut self, connack_timeout: Duration) -> Self {
        self.options.connack_timeout = connack_timeout;
        self
    }

    /// Configures how long, after sending a Pingreq, the client will wait for a Pingresp from the
    /// broker before giving up and shutting down the connection.
    pub fn with_ping_timeout(mut self, ping_timeout: Duration) -> Self {
        self.options.ping_timeout = ping_timeout;
        self
    }

    /// Configures the default event listener for the client.
    pub fn with_default_event_listener(mut self, default_event_listener: ClientEventListener) -> Self {
        self.options.default_event_listener = Some(default_event_listener);
        self
    }

    /// Configures an outbound topic alias resolver to be used when sending Publish packets to
    /// the broker.
    pub fn with_outbound_alias_resolver_factory(mut self, outbound_alias_resolver_factory: OutboundAliasResolverFactoryFn) -> Self {
        self.options.outbound_alias_resolver_factory = Some(outbound_alias_resolver_factory);
        self
    }

    /// Configures what kind of jitter, if any, should be applied to the waiting period between
    /// connection attempts.
    pub fn with_reconnect_period_jitter(mut self, reconnect_period_jitter: ExponentialBackoffJitterType) -> Self {
        self.options.reconnect_options.reconnect_period_jitter = reconnect_period_jitter;
        self
    }

    /// Configures the minimum amount of time to wait between connection attempts.  Depending on
    /// jitter settings, the actual wait period may be shorter.  Defaults to one second if not
    /// specified.
    pub fn with_base_reconnect_period(mut self, base_reconnect_period: Duration) -> Self {
        self.options.reconnect_options.base_reconnect_period = base_reconnect_period;
        self
    }

    /// Configures the maximum amount of time to wait between connection attempts.  Defaults to
    /// two minutes if not specified.
    pub fn with_max_reconnect_period(mut self, max_reconnect_period: Duration) -> Self {
        self.options.reconnect_options.max_reconnect_period = max_reconnect_period;
        self
    }

    /// Configures the interval of time that the client must remain successfully connected before
    /// the exponential backoff for connection attempts is reset.  Defaults to thirty seconds if
    /// not specified.
    pub fn with_reconnect_stability_reset_period(mut self, reconnect_stability_reset_period: Duration) -> Self {
        self.options.reconnect_options.reconnect_stability_reset_period = reconnect_stability_reset_period;
        self
    }

    /// Builds a new set of client options, consuming the builder in the process.
    pub fn build(self) -> Mqtt5ClientOptions {
        self.options
    }
}

/// A basic builder for creating MQTT clients.  Specialized builders for particular brokers may
/// exist in other crates.
pub struct GenericClientBuilder {
    endpoint: String,
    port: u16,

    tls_options: Option<TlsOptions>,
    connect_options: Option<ConnectOptions>,
    client_options: Option<Mqtt5ClientOptions>,
    websocket_options: Option<WebsocketOptions>
}

impl GenericClientBuilder {

    /// Creates a new client builder attuned to a given host name and port.
    pub fn new(endpoint: &str, port: u16) -> Self {
        GenericClientBuilder {
            endpoint: endpoint.to_string(),
            port,
            tls_options: None,
            connect_options: None,
            client_options: None,
            websocket_options: None
        }
    }

    /// Configures what TLS options to use for the connection to the broker.  If not specified,
    /// then TLS will not be used.
    pub fn with_tls_options(mut self, tls_options: TlsOptions) -> Self {
        self.tls_options = Some(tls_options);
        self
    }

    /// Configures the Connect packet related options for the client.  If not specified, default
    /// values will be used.
    pub fn with_connect_options(mut self, connect_options: ConnectOptions) -> Self {
        self.connect_options = Some(connect_options);
        self
    }

    /// Configures the client behavioral options.  If not specified, default values will be used.
    pub fn with_client_options(mut self, client_options: Mqtt5ClientOptions) -> Self {
        self.client_options = Some(client_options);
        self
    }

    /// Configures the client to connect over websockets
    pub fn with_websocket_options(mut self, websocket_options: WebsocketOptions) -> Self {
        self.websocket_options = Some(websocket_options);
        self
    }

    /// Builds a new MQTT client according to all the configuration options given to the builder.
    /// Does not consume self; can be called multiple times
    pub fn build(&self, runtime: &Handle) -> MqttResult<Mqtt5Client> {
        let connect_options =
            if let Some(options) = &self.connect_options {
                options.clone()
            } else {
                ConnectOptionsBuilder::new().build()
            };

        let client_options =
            if let Some(options) = &self.client_options {
                options.clone()
            } else {
                Mqtt5ClientOptionsBuilder::new().build()
            };

        let tls_options = self.tls_options.clone();
        let websocket_options = self.websocket_options.clone();
        let endpoint = self.endpoint.clone();

        if websocket_options.is_some() {
            make_websocket_client(endpoint, self.port, websocket_options.unwrap(), tls_options, client_options, connect_options, runtime)
        } else {
            make_direct_client(endpoint, self.port, tls_options, client_options, connect_options, runtime)
        }
    }
}

fn make_direct_client(endpoint: String, port: u16, tls_options: Option<TlsOptions>, client_options: Mqtt5ClientOptions, connect_options: ConnectOptions, runtime: &Handle) -> MqttResult<Mqtt5Client> {
    let to_socket_addrs = (endpoint.clone(), port).to_socket_addrs();
    if to_socket_addrs.is_err()  {
        return Err(MqttError::Unknown);
    }

    let addr = to_socket_addrs.unwrap().next().unwrap();

    if let Some(tls_options) = tls_options {
        let connector =
            match tls_options.options {
                TlsData::Rustls(_, config) => { TlsConnector::from(config.clone()) }
            };

        let endpoint = endpoint.clone();

        let tokio_options = TokioClientOptions {
            connection_factory: Box::new(move || {
                Box::pin(make_tls_stream(addr, endpoint.clone(), connector.clone()))
            }),
        };

        Ok(Mqtt5Client::new_with_tokio(client_options, connect_options, tokio_options, runtime))
    } else {
        let tokio_options = TokioClientOptions {
            connection_factory: Box::new(move || { Box::pin(TcpStream::connect(addr)) }),
        };

        Ok(Mqtt5Client::new_with_tokio(client_options, connect_options, tokio_options, runtime))
    }
}

fn make_websocket_client(endpoint: String, port: u16, websocket_options: WebsocketOptions, tls_options: Option<TlsOptions>, client_options: Mqtt5ClientOptions, connect_options: ConnectOptions, runtime: &Handle) -> MqttResult<Mqtt5Client> {
    let handshake_transform = websocket_options.handshake_transform.clone();
    if let Some(tls_options) = tls_options {
        let connector =
            match tls_options.options {
                TlsData::Rustls(_, config) => { TlsConnector::from(config.clone()) }
            };

        let tokio_options = TokioClientOptions {
            connection_factory: Box::new(move || {
                Box::pin(make_wss_stream(endpoint.clone(), port, connector.clone(), handshake_transform.clone()))
            }),
        };

        Ok(Mqtt5Client::new_with_tokio(client_options, connect_options, tokio_options, runtime))
    } else {
        let tokio_options = TokioClientOptions {
            connection_factory: Box::new(move || {
                Box::pin(make_ws_stream(endpoint.clone(), port, handshake_transform.clone()))
            }),
        };

        Ok(Mqtt5Client::new_with_tokio(client_options, connect_options, tokio_options, runtime))
    }
}

// should be moved somewhere else
async fn make_tls_stream(addr: SocketAddr, endpoint: String, connector: TlsConnector) -> std::io::Result<TlsStream<TcpStream>> {
    let tcp_stream = TcpStream::connect(&addr).await?;

    let domain = rustls_pki_types::ServerName::try_from(endpoint)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid dnsname"))?
        .to_owned();

    let stream_result = connector.connect(domain, tcp_stream).await;
    if let Err(error) = &stream_result {
        println!("Error: {:?}", error);
    }

    stream_result
}

use std::str::FromStr;
use http::{Uri, Version};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tungstenite::{client::*, handshake::client::generate_key};

struct HandshakeRequest {
    handshake_builder: http::request::Builder,
}

impl IntoClientRequest for HandshakeRequest {
    fn into_client_request(self) -> tungstenite::Result<tungstenite::handshake::client::Request> {
        let final_request = self.handshake_builder.body(()).unwrap();
        Ok(tungstenite::handshake::client::Request::from(final_request))
    }
}

fn create_default_websocket_handshake_request(uri: String) -> std::io::Result<http::request::Builder> {
    let uri = Uri::from_str(uri.as_str()).unwrap();

    Ok(http::Request::builder()
        .uri(uri.to_string())
        .version(Version::HTTP_11)
        .header("Sec-WebSocket-Protocol", "mqtt")
        .header("Sec-WebSocket-Key", generate_key())
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", 13)
        .header("Host", uri.host().unwrap()))
}

async fn make_ws_stream(endpoint: String, port: u16, handshake_transform: Arc<Option<WebsocketHandshakeTransform>>) -> std::io::Result<WsByteStream<WebSocketStream<TcpStream>, Message, tungstenite::Error, WsMessageHandler>> {
    let mut to_socket_addrs = (endpoint.clone(), port).to_socket_addrs()?;
    let addr = to_socket_addrs.next().unwrap();

    let uri = format!("ws://{}:{}/mqtt", endpoint, port);
    let handshake_builder = create_default_websocket_handshake_request(uri)?;
    let transformed_handshake_builder =
        if let Some(transform) = &*handshake_transform {
            transform(handshake_builder).await?
        } else {
            handshake_builder
        };

    let tcp_stream = TcpStream::connect(&addr).await?;
    let handshake_result = client_async(HandshakeRequest{ handshake_builder: transformed_handshake_builder }, tcp_stream).await;
    let (message_stream, _) = handshake_result.map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Failed to perform websocket handshake"))?;
    let byte_stream = WsMessageHandler::wrap_stream(message_stream);

    Ok(byte_stream)
}


async fn make_wss_stream(endpoint: String, port: u16, connector: TlsConnector, handshake_transform: Arc<Option<WebsocketHandshakeTransform>>) -> std::io::Result<WsByteStream<WebSocketStream<TlsStream<TcpStream>>, Message, tungstenite::Error, WsMessageHandler>> {
    let mut to_socket_addrs = (endpoint.clone(), port).to_socket_addrs()?;
    let addr = to_socket_addrs.next().unwrap();

    let uri = format!("wss://{}:{}/mqtt", endpoint, port);
    let handshake_builder = create_default_websocket_handshake_request(uri)?;
    let transformed_handshake_builder =
        if let Some(transform) = &*handshake_transform {
            transform(handshake_builder).await?
        } else {
            handshake_builder
        };

    let tcp_stream = TcpStream::connect(&addr).await?;

    let domain = rustls_pki_types::ServerName::try_from(endpoint)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid dnsname"))?
        .to_owned();

    let tls_stream = connector.connect(domain, tcp_stream).await?;

    let handshake_result = client_async( HandshakeRequest { handshake_builder: transformed_handshake_builder }, tls_stream).await;
    let (message_stream, _) = handshake_result.map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Failed to perform websocket handshake"))?;

    Ok(WsMessageHandler::wrap_stream(message_stream))
}

async fn make_stream(addr: SocketAddr) -> std::io::Result<TcpStream> {
    TcpStream::connect(&addr).await
}

async fn wrap_stream_with_tls<S>(stream : S, endpoint: String, tls_options: TlsOptions) -> std::io::Result<TlsStream<S>> where S : AsyncRead + AsyncWrite + Unpin {
    let domain = rustls_pki_types::ServerName::try_from(endpoint)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid dnsname"))?
        .to_owned();

    let connector =
        match tls_options.options {
            TlsData::Rustls(_, config) => { TlsConnector::from(config.clone()) }
        };

    connector.connect(domain, stream).await
}

async fn wrap_stream_with_websockets<S>(stream : S, endpoint: String, websocket_options: WebsocketOptions) -> std::io::Result<WsByteStream<WebSocketStream<S>, Message, tungstenite::Error, WsMessageHandler>> where S : AsyncRead + AsyncWrite + Unpin {

    let uri = format!("{}/mqtt", endpoint);
    let handshake_builder = create_default_websocket_handshake_request(uri)?;
    let transformed_handshake_builder =
        if let Some(transform) = &*websocket_options.handshake_transform {
            transform(handshake_builder).await?
        } else {
            handshake_builder
        };

    let handshake_result = client_async( HandshakeRequest { handshake_builder: transformed_handshake_builder }, stream).await;
    let (message_stream, _) = handshake_result.map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Failed to perform websocket handshake"))?;
    let byte_stream = WsMessageHandler::wrap_stream(message_stream);

    Ok(byte_stream)
}

async fn wrap_stream_with_proxy_connect<S>(stream : S, endpoint: String, port: u16) -> std::io::Result<S> where S : AsyncRead + AsyncWrite + Unpin {
    Ok(stream)
}