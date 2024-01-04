/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Module containing types for configuring an MQTT client.
 */

extern crate rustls;
extern crate tokio;
extern crate tokio_rustls;

use std::fmt::{Debug, Formatter};
use crate::*;
use crate::client::*;

use std::sync::Arc;
use std::fs::File;
use std::io::Read;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio_rustls::client::TlsStream;
use tokio_rustls::{TlsConnector};
use crate::alias::OutboundAliasResolver;
use crate::features::gneiss_tokio::{AsyncClientEventSender, TokioClientOptions};

#[derive(Eq, PartialEq)]
pub(crate) enum TlsMode {
    Standard,
    Mtls
}

pub(crate) enum TlsData {
    Rustls(TlsMode, rustls::ClientConfig),
}

pub struct TlsOptions {
    pub(crate) options: TlsData
}

pub struct TlsOptionsBuilder {
    pub(crate) mode: TlsMode,
    pub(crate) root_ca_bytes: Option<Vec<u8>>,
    pub(crate) certificate_bytes: Option<Vec<u8>>,
    pub(crate) private_key_bytes: Option<Vec<u8>>,
    pub(crate) verify_peer: bool,
    pub(crate) alpn: Option<Vec<u8>> // one protocol only for now
}

impl TlsOptionsBuilder {
    pub fn new() -> Self {
        TlsOptionsBuilder::default()
    }

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

    pub fn with_root_ca_from_path(mut self, root_ca_path: &str) -> std::io::Result<Self> {
        self.root_ca_bytes = Some(load_file(root_ca_path)?);
        Ok(self)
    }

    pub fn with_root_ca_from_memory(mut self, root_ca_bytes: &[u8]) -> Self {
        self.root_ca_bytes = Some(root_ca_bytes.to_vec());
        self
    }

    pub fn with_verify_peer(mut self, verify_peer: bool) -> Self {
        self.verify_peer = verify_peer;
        self
    }

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

pub struct GenericClientBuilder {
    endpoint: String,
    port: u16,

    tls_options: Option<TlsOptions>,
    connect_options: Option<ConnectOptions>,
    client_options: Option<Mqtt5ClientOptions>
}

impl GenericClientBuilder {

    pub fn new(endpoint: &str, port: u16) -> Self {
        GenericClientBuilder {
            endpoint: endpoint.to_string(),
            port,
            tls_options: None,
            connect_options: None,
            client_options: None
        }
    }

    pub fn with_tls_options(mut self, tls_options: TlsOptions) -> Self {
        self.tls_options = Some(tls_options);
        self
    }

    pub fn with_connect_options(mut self, connect_options: ConnectOptions) -> Self {
        self.connect_options = Some(connect_options);
        self
    }

    pub fn with_client_options(mut self, client_options: Mqtt5ClientOptions) -> Self {
        self.client_options = Some(client_options);
        self
    }

    pub fn build(self, runtime: &Handle) -> MqttResult<Mqtt5Client> {
        let to_socket_addrs = (self.endpoint.clone(), self.port).to_socket_addrs();
        if to_socket_addrs.is_err()  {
            return Err(MqttError::Unknown);
        }

        let addr = to_socket_addrs.unwrap().next().unwrap();

        let connect_options =
            if let Some(options) = self.connect_options {
                options
            } else {
                ConnectOptionsBuilder::new().build()
            };

        let client_options =
            if let Some(options) = self.client_options {
                options
            } else {
                Mqtt5ClientOptionsBuilder::new().build()
            };

        if let Some(tls_options) = self.tls_options {
            let connector =
                match tls_options.options {
                    TlsData::Rustls(_, config) => { TlsConnector::from(Arc::new(config)) }
                };

            let endpoint = self.endpoint.clone();

            let tokio_options = TokioClientOptions {
                connection_factory: Box::new(move || {
                    Box::pin(make_tls_stream(addr, endpoint.clone(), connector.clone()))
                })
            };

            Ok(Mqtt5Client::new_with_tokio(client_options, connect_options, tokio_options, runtime))
        } else {
            let tokio_options = TokioClientOptions {
                connection_factory: Box::new(move || { Box::pin(TcpStream::connect(addr)) })
            };

            Ok(Mqtt5Client::new_with_tokio(client_options, connect_options, tokio_options, runtime))
        }
    }
}

async fn make_tls_stream(addr: SocketAddr, endpoint: String, connector: TlsConnector) -> std::io::Result<TlsStream<TcpStream>> {
    let tcp_stream = TcpStream::connect(&addr).await?;

    let domain = pki_types::ServerName::try_from(endpoint)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid dnsname"))?
        .to_owned();

    connector.connect(domain, tcp_stream).await
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

pub(crate) const DEFAULT_KEEP_ALIVE_SECONDS : u16 = 1200;

/// Configuration options that will determine packet field values for the CONNECT packet sent out
/// by the client on each connection attempt.  Almost equivalent to ConnectPacket, but there are a
/// few differences that make exposing a ConnectPacket directly awkward and potentially misleading.
///
/// Auth-related fields are not yet exposed because we don't support authentication exchanges yet.
#[derive(Debug, Clone)]
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
    pub(crate) username: Option<String>,

    /// Opaque binary data that the server may use for client authentication and authorization.
    ///
    /// See [MQTT5 Password](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901072)
    pub(crate) password: Option<Vec<u8>>,

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
    pub(crate) request_response_information: Option<bool>,

    /// If set to true, requests that the server send additional diagnostic information (via response string or
    /// user properties) in DISCONNECT or CONNACK packets from the server.
    ///
    /// See [MQTT5 Request Problem Information](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901053)
    pub(crate) request_problem_information: Option<bool>,

    /// Notifies the server of the maximum number of in-flight Qos 1 and 2 messages the client is willing to handle.  If
    /// omitted, then no limit is requested.
    ///
    /// See [MQTT5 Receive Maximum](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901049)
    pub(crate) receive_maximum: Option<u16>,

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
    pub(crate) will_delay_interval_seconds: Option<u32>,

    /// The definition of a message to be published when the connection's session is destroyed by the server or when
    /// the will delay interval has elapsed, whichever comes first.  If undefined, then nothing will be sent.
    ///
    /// See [MQTT5 Will](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901040)
    ///
    /// TODO: consider making this a builder function to allow dynamic will construction
    pub(crate) will: Option<PublishPacket>,

    /// Set of MQTT5 user properties to include with all CONNECT packets.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901054)
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

    pub fn set_username(&mut self, username: Option<&str>) {
        self.username = username.map(str::to_string);
    }

    pub fn set_password(&mut self, password: Option<&[u8]>) {
        self.password = password.map(|p| p.to_vec());
    }
}

impl Default for ConnectOptions {
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

    pub fn with_keep_alive_interval_seconds(mut self, keep_alive: Option<u16>) -> Self {
        self.options.keep_alive_interval_seconds = keep_alive;
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

#[derive(Default)]
pub struct Mqtt5ClientOptions {
    pub(crate) offline_queue_policy: OfflineQueuePolicy,

    pub(crate) connack_timeout: Duration,
    pub(crate) ping_timeout: Duration,

    pub(crate) default_event_listener: Option<ClientEventListener>,

    pub(crate) outbound_alias_resolver: Option<Box<dyn OutboundAliasResolver + Send>>,

    pub(crate) reconnect_options: ReconnectOptions,
}

impl Debug for Mqtt5ClientOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Mqtt5ClientOptions {{ ")?;
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
