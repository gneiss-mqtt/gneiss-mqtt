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
use std::fs::File;
use std::io::Read;
use std::net::{SocketAddr, ToSocketAddrs};
#[cfg(any(feature = "tokio-native-tls", feature = "threaded-native-tls", feature = "tokio-rustls", feature = "threaded-rustls", feature = "tokio-websockets", feature = "threaded-websockets"))]
use std::sync::Arc;
use std::time::Duration;

#[cfg(any(feature="tokio-websockets", feature="threaded-websockets"))]
use http::{Uri, Version};
#[cfg(any(feature="tokio-websockets", feature="threaded-websockets"))]
use std::str::FromStr;
#[cfg(feature="tokio-websockets")]
use std::{future::Future, pin::Pin,};
#[cfg(any(feature = "tokio-rustls", feature = "threaded-rustls"))]
use rustls_pki_types::PrivateKeyDer;
#[cfg(feature = "tokio")]
use ::tokio::runtime::Handle;
#[cfg(any(feature="tokio-websockets", feature="threaded-websockets"))]
use tungstenite::{client::*, handshake::client::generate_key};

/// Configuration options related to establishing connections through HTTP proxies
#[derive(Clone)]
pub struct HttpProxyOptions {
    pub(crate) endpoint: String,
    pub(crate) port: u16,
    pub(crate) tls_options: Option<TlsOptions>
}

impl HttpProxyOptions {
    /// Creates a new builder for an HttpProxyOptions instance.
    pub fn builder(endpoint: &str, port: u16) -> HttpProxyOptionsBuilder {
        HttpProxyOptionsBuilder::new(endpoint, port)
    }
}

/// Builder type for constructing HTTP-proxy-related configuration.
pub struct HttpProxyOptionsBuilder {
    options: HttpProxyOptions
}

impl HttpProxyOptionsBuilder {

    /// Creates a new builder object
    pub(crate) fn new(endpoint: &str, port: u16) -> Self {
        HttpProxyOptionsBuilder {
            options: HttpProxyOptions {
                endpoint: endpoint.to_string(),
                port,
                tls_options: None
            }
        }
    }

    /// Configures tls settings for the to-proxy connection.
    ///
    /// This is independent of any tls configuration to the broker.
    pub fn with_tls_options(mut self, tls_options: TlsOptions) -> Self {
        self.options.tls_options = Some(tls_options);
        self
    }

    /// Creates a new set of HTTP proxy options
    pub fn build(&self) -> HttpProxyOptions {
        self.options.clone()
    }
}

/// Return type for a synchronous websocket handshake transformation function
#[cfg(feature="threaded-websockets")]
pub type SyncWebsocketHandshakeTransformReturnType = GneissResult<http::request::Builder>;

/// Synchronous websocket handshake transformation function type
#[cfg(feature="threaded-websockets")]
pub type SyncWebsocketHandshakeTransform = Box<dyn Fn(http::request::Builder) -> SyncWebsocketHandshakeTransformReturnType + Send + Sync>;

/// Configuration options related to establishing a synchronous MQTT connection over websockets
#[derive(Default, Clone)]
#[cfg(feature="threaded-websockets")]
pub struct SyncWebsocketOptions {
    pub(crate) handshake_transform: Arc<Option<SyncWebsocketHandshakeTransform>>
}

/// Empty marker struct when synchronous websockets are not enabled
#[cfg(not(feature = "threaded-websockets"))]
#[derive(Default, Clone)]
pub struct SyncWebsocketOptions{}

#[cfg(feature="threaded-websockets")]
impl SyncWebsocketOptions {

    /// Creates a new builder for SyncWebsocketOptions instances
    pub fn builder() -> SyncWebsocketOptionsBuilder {
        SyncWebsocketOptionsBuilder::new()
    }
}

/// Builder type for constructing async Websockets-related configuration.
#[cfg(feature="threaded-websockets")]
pub struct SyncWebsocketOptionsBuilder {
    options : SyncWebsocketOptions
}

#[cfg(feature="threaded-websockets")]
impl SyncWebsocketOptionsBuilder {

    /// Creates a new builder object with default options.
    pub(crate) fn new() -> Self {
        SyncWebsocketOptionsBuilder {
            options: SyncWebsocketOptions {
                ..Default::default()
            }
        }
    }

    /// Configure a transformation function that operates on the websocket handshake.
    ///
    /// Useful for brokers that require some kind of signing algorithm to accept the upgrade request.
    pub fn with_handshake_transform(&mut self, transform: SyncWebsocketHandshakeTransform) -> &mut Self {
        self.options.handshake_transform = std::sync::Arc::new(Some(transform));
        self
    }

    /// Creates a new set of Websocket options
    pub fn build(&self) -> SyncWebsocketOptions {
        self.options.clone()
    }
}

/// Return type for an async websocket handshake transformation function
#[cfg(feature="tokio-websockets")]
pub type AsyncWebsocketHandshakeTransformReturnType = Pin<Box<dyn Future<Output = GneissResult<http::request::Builder>> + Send >>;

/// Async websocket handshake transformation function type
#[cfg(feature="tokio-websockets")]
pub type AsyncWebsocketHandshakeTransform = Box<dyn Fn(http::request::Builder) -> AsyncWebsocketHandshakeTransformReturnType + Send + Sync>;

/// Configuration options related to establishing an async MQTT connection over websockets
#[derive(Clone)]
#[cfg(feature="tokio-websockets")]
pub struct AsyncWebsocketOptions {
    pub(crate) handshake_transform: std::sync::Arc<Option<AsyncWebsocketHandshakeTransform>>
}

/// Empty marker struct when async websockets are not enabled
#[cfg(not(feature = "tokio-websockets"))]
#[derive(Default, Clone)]
pub struct AsyncWebsocketOptions{}

#[cfg(feature="tokio-websockets")]
impl AsyncWebsocketOptions {

    /// Creates a new builder for AsyncWebsocketOptions instances
    pub fn builder() -> AsyncWebsocketOptionsBuilder {
        AsyncWebsocketOptionsBuilder::new()
    }
}

/// Builder type for constructing async Websockets-related configuration.
#[cfg(feature="tokio-websockets")]
pub struct AsyncWebsocketOptionsBuilder {
    options : AsyncWebsocketOptions
}

#[cfg(feature="tokio-websockets")]
impl AsyncWebsocketOptionsBuilder {

    /// Creates a new builder object with default options.
    pub(crate) fn new() -> Self {
        AsyncWebsocketOptionsBuilder {
            options: AsyncWebsocketOptions {
                handshake_transform: Arc::new(None)
            }
        }
    }

    /// Configure an async transformation function that operates on the websocket handshake.
    ///
    /// Useful for brokers that require some kind of signing algorithm to accept the upgrade request.
    pub fn with_handshake_transform(&mut self, transform: AsyncWebsocketHandshakeTransform) -> &mut Self {
        self.options.handshake_transform = Arc::new(Some(transform));
        self
    }

    /// Creates a new set of Websocket options
    pub fn build(&self) -> AsyncWebsocketOptions {
        self.options.clone()
    }
}

#[derive(Eq, PartialEq, Clone, Copy)]
pub(crate) enum TlsMode {
    Standard,
    Mtls
}

#[derive(Clone)]
pub(crate) enum TlsData {
    #[allow(dead_code)]
    Invalid,

    #[cfg(any(feature = "tokio-rustls", feature = "threaded-rustls"))]
    Rustls(std::sync::Arc<rustls::ClientConfig>),

    #[cfg(any(feature = "tokio-native-tls", feature = "threaded-native-tls"))]
    NativeTls(std::sync::Arc<native_tls::TlsConnectorBuilder>)
}

/// TLS configuration options, assuming TLS has been enabled as a feature
#[derive(Clone)]
pub struct TlsOptions {
    pub(crate) options: TlsData
}

impl TlsOptions {

    /// Creates a new builder object with default options.
    ///
    /// Defaults may be specific to the TLS implementation.  Presently, this means standard TLS using 1.2 or
    /// higher and the system trust store.
    pub fn builder() -> TlsOptionsBuilder {
        TlsOptionsBuilder::new()
    }

    /// Creates a new builder object using mutual TLS and an X509 certificate and a
    /// private key, by file path.
    pub fn builder_with_mtls_from_path(certificate_path: &str, private_key_path: &str) -> GneissResult<TlsOptionsBuilder> {
        TlsOptionsBuilder::new_with_mtls_from_path(certificate_path, private_key_path)
    }

    /// Creates a new builder object using mutual TLS and an X509 certificate and a
    /// private key from memory.
    pub fn builder_with_mtls_from_memory(certificate_bytes: &[u8], private_key_bytes: &[u8]) -> TlsOptionsBuilder {
        TlsOptionsBuilder::new_with_mtls_from_memory(certificate_bytes, private_key_bytes)
    }

    #[cfg(any(feature = "tokio-rustls", feature = "threaded-rustls"))]
    /// Builds client TLS options directly from a Rustls ClientConfig instance
    ///
    /// This factory is useful when you need TLS properties that TlsOptionsBuilder does not support.
    pub fn new_rustls_from_client_config(config: rustls::ClientConfig) -> TlsOptions {
        TlsOptions {
            options: TlsData::Rustls(Arc::new(config))
        }
    }

    #[cfg(any(feature = "tokio-native-tls", feature = "threaded-native-tls"))]
    /// Builds client TLS options directly from a native-tls TlsConnectorBuilder instance
    ///
    /// This factory is useful when you need TLS properties that TlsOptionsBuilder does not support.
    pub fn new_native_tls_from_tls_connector_builder(builder: native_tls::TlsConnectorBuilder) -> TlsOptions {
        TlsOptions {
            options: TlsData::NativeTls(Arc::new(builder))
        }
    }
}
fn load_file(filename: &str) -> std::io::Result<Vec<u8>> {
    let mut bytes_vec = Vec::new();
    let mut bytes_file = File::open(filename)?;
    bytes_file.read_to_end(&mut bytes_vec)?;
    Ok(bytes_vec)
}


/// Builder type for constructing TLS configuration.
#[cfg_attr(not(any(feature = "tokio-rustls", feature = "tokio-native-tls", feature = "threaded-rustls", feature = "threaded-native-tls")), allow(dead_code))]
pub struct TlsOptionsBuilder {
    pub(crate) mode: TlsMode,
    pub(crate) root_ca_bytes: Option<Vec<u8>>,
    pub(crate) certificate_bytes: Option<Vec<u8>>,
    pub(crate) private_key_bytes: Option<Vec<u8>>,
    pub(crate) verify_peer: bool,
    pub(crate) alpn: Option<String> // one protocol only for now
}

impl TlsOptionsBuilder {

    pub(crate) fn new() -> Self {
        TlsOptionsBuilder {
            mode: TlsMode::Standard,
            root_ca_bytes: None,
            certificate_bytes: None,
            private_key_bytes: None,
            verify_peer: true,
            alpn: None
        }
    }

    pub(crate) fn new_with_mtls_from_path(certificate_path: &str, private_key_path: &str) -> GneissResult<Self> {
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

    pub(crate) fn new_with_mtls_from_memory(certificate_bytes: &[u8], private_key_bytes: &[u8]) -> Self {
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
    pub fn with_root_ca_from_path(&mut self, root_ca_path: &str) -> GneissResult<&mut Self> {
        self.root_ca_bytes = Some(load_file(root_ca_path)?);
        Ok(self)
    }

    /// Configures the builder to use a trust store that *only* contains a single root certificate,
    /// supplied from memory.
    pub fn with_root_ca_from_memory(&mut self, root_ca_bytes: &[u8]) -> &mut Self {
        self.root_ca_bytes = Some(root_ca_bytes.to_vec());
        self
    }

    /// Controls whether or not SNI is used during the TLS handshake.
    ///
    /// It is highly recommended to set this value to false only in testing environments.
    pub fn with_verify_peer(&mut self, verify_peer: bool) -> &mut Self {
        self.verify_peer = verify_peer;
        self
    }

    /// Sets an ALPN protocol to negotiate during the TLS handshake.
    ///
    /// Should multiple protocols become a valid use case, new APIs will be added to manipulate
    /// the set of protocols.
    pub fn with_alpn(&mut self, alpn: &str) -> &mut Self {
        self.alpn = Some(alpn.to_string());
        self
    }

    #[cfg(any(feature = "tokio-native-tls", feature = "threaded-native-tls"))]
    /// Builds client TLS options using the `native-tls` crate
    ///
    /// If using MTLS, native-tls only supports pkcs8 format private keys.  If your private key is in a different
    /// format, you must first convert it to pkcs8 and instead use that.
    pub fn build_native_tls(&self) -> Result<TlsOptions, GneissError> {
        let mut builder = native_tls::TlsConnector::builder();

        if let Some(root_ca_bytes) = &self.root_ca_bytes {
            builder.disable_built_in_roots(true);

            let root = native_tls::Certificate::from_pem(root_ca_bytes.as_slice())?;
            builder.add_root_certificate(root);
        }

        if self.mode == TlsMode::Mtls {
            let identity = native_tls::Identity::from_pkcs8(self.certificate_bytes.as_ref().unwrap(), self.private_key_bytes.as_ref().unwrap())?;
            builder.identity(identity);
        }

        if let Some(alpn) = &self.alpn {
            let protocols = vec!(alpn.as_str());
            builder.request_alpns(protocols.as_slice());
        }

        builder.use_sni(self.verify_peer);

        Ok(TlsOptions {
            options: TlsData::NativeTls(Arc::new(builder))
        })
    }

    #[cfg(any(feature = "tokio-rustls", feature = "threaded-rustls"))]
    /// Builds client TLS options using the `rustls` crate
    pub fn build_rustls(&self) -> Result<TlsOptions, GneissError> {
        let root_cert_store = build_root_ca_store(self.root_ca_bytes.as_deref())?;

        let mut config =
            match self.mode {
                TlsMode::Standard => {
                    rustls::ClientConfig::builder()
                        .with_root_certificates(root_cert_store)
                        .with_no_client_auth()
                }
                TlsMode::Mtls => {
                    let certs = build_certs(self.certificate_bytes.as_deref().unwrap());
                    let private_key = build_private_key(self.private_key_bytes.as_deref().unwrap())?;
                    rustls::ClientConfig::builder()
                        .with_root_certificates(root_cert_store)
                        .with_client_auth_cert(certs, private_key)?
                }
            };

        config.alpn_protocols = Vec::new();
        if let Some(alpn) = &self.alpn {
            config.alpn_protocols.push(alpn.as_bytes().to_vec());
        }

        config.enable_sni = self.verify_peer;

        Ok(TlsOptions {
            options: TlsData::Rustls(Arc::new(config))
        })
    }
}

#[cfg(any(feature = "tokio-rustls", feature = "threaded-rustls"))]
fn build_root_ca_store(root_ca_bytes: Option<&[u8]>) -> Result<rustls::RootCertStore, std::io::Error> {
    let mut root_cert_store = rustls::RootCertStore::empty();
    if let Some(root_ca_bytes) = root_ca_bytes {
        let mut pem = std::io::BufReader::new(root_ca_bytes);
        for cert in rustls_pemfile::certs(&mut pem) {
            root_cert_store.add(cert?).unwrap();
        }
    } else {
        let system_certs = rustls_native_certs::load_native_certs();
        for cert in system_certs.unwrap() {
            root_cert_store.add(cert).unwrap();
        }
    }

    Ok(root_cert_store)
}

#[cfg(any(feature = "tokio-rustls", feature = "threaded-rustls"))]
fn build_private_key(key_bytes: &[u8]) -> Result<PrivateKeyDer<'static>, rustls::Error> {
    let mut reader = std::io::BufReader::new(key_bytes);

    loop {
        let read_pem_result = rustls_pemfile::read_one(&mut reader);
        match read_pem_result {
            Ok(Some(rustls_pemfile::Item::Pkcs1Key(key))) => return Ok(key.into()),
            Ok(Some(rustls_pemfile::Item::Pkcs8Key(key))) => return Ok(key.into()),
            Ok(Some(rustls_pemfile::Item::Sec1Key(key))) => return Ok(key.into()),
            Ok(None) => { return Err(rustls::Error::General("no valid private keys found".to_string())); }
            Ok(_) => {}
            Err(_) => { return Err(rustls::Error::General("failed to parse private key pem file".to_string())); }
        }
    }
}

#[cfg(any(feature = "tokio-rustls", feature = "threaded-rustls"))]
fn build_certs(certificate_bytes: &[u8]) -> Vec<rustls_pki_types::CertificateDer<'static>> {
    let mut reader = std::io::BufReader::new(certificate_bytes);

    rustls_pemfile::certs(&mut reader)
        .flatten()
        .collect()
}

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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum TlsConfiguration {
    None,
    #[cfg(any(feature = "tokio-rustls", feature = "threaded-rustls"))]
    Rustls,
    #[cfg(any(feature = "tokio-native-tls", feature = "threaded-native-tls"))]
    Nativetls,
    Mixed
}

pub(crate) fn get_tls_impl_from_options(tls_options: Option<&TlsOptions>) -> TlsConfiguration {
    if let Some(tls_opts) = tls_options {
        return
            match &tls_opts.options {
                #[cfg(any(feature = "tokio-rustls", feature = "threaded-rustls"))]
                TlsData::Rustls(_) => { TlsConfiguration::Rustls }
                #[cfg(any(feature = "tokio-native-tls", feature = "threaded-native-tls"))]
                TlsData::NativeTls(_) => { TlsConfiguration::Nativetls }
                _ => { TlsConfiguration::None }
            };
    }

    TlsConfiguration::None
}

#[derive(Clone)]
pub(crate) struct Endpoint {
    pub(crate) endpoint: String,
    pub(crate) port: u16,
}

impl Endpoint {
    pub(crate) fn new(endpoint: &str, port: u16) -> Self {
        Endpoint {
            endpoint: endpoint.to_string(),
            port
        }
    }
}

pub(crate) fn make_addr(endpoint: &str, port: u16) -> std::io::Result<SocketAddr> {
    let mut to_socket_addrs = (endpoint.to_string(), port).to_socket_addrs()?;

    Ok(to_socket_addrs.next().unwrap())
}

pub(crate) fn compute_endpoints(endpoint: String, port: u16, http_proxy_options: &Option<HttpProxyOptions>) -> (Endpoint, Option<Endpoint>) {
    let broker_endpoint = Endpoint::new(endpoint.as_str(), port);
    let proxy_endpoint = http_proxy_options.as_ref().map(|val| { Endpoint::new( val.endpoint.as_str(), val.port )});
    info!("compute_endpoints - broker address - {}:{}", broker_endpoint.endpoint, broker_endpoint.port);
    if let Some(proxy_end) = &proxy_endpoint {
        info!("compute_endpoints - proxy address - {}:{}", proxy_end.endpoint, proxy_end.port);
    }

    if let Some(proxy_endpoint) = proxy_endpoint {
        (proxy_endpoint, Some(broker_endpoint))
    } else {
        (broker_endpoint, None)
    }
}

pub(crate) fn build_connect_request(http_connect_endpoint: &Endpoint) -> Vec<u8> {
    let request_as_string = format!("CONNECT {}:{} HTTP/1.1\r\nHost: {}:{}\r\nConnection: keep-alive\r\n\r\n", http_connect_endpoint.endpoint, http_connect_endpoint.port, http_connect_endpoint.endpoint, http_connect_endpoint.port);

    request_as_string.as_bytes().to_vec()
}

#[cfg(any(feature="tokio-websockets", feature="threaded-websockets"))]
pub(crate) struct HandshakeRequest {
    pub(crate) handshake_builder: http::request::Builder,
}

#[cfg(any(feature="tokio-websockets", feature="threaded-websockets"))]
impl IntoClientRequest for HandshakeRequest {
    fn into_client_request(self) -> tungstenite::Result<tungstenite::handshake::client::Request> {
        let final_request = self.handshake_builder.body(()).unwrap();
        Ok(tungstenite::handshake::client::Request::from(final_request))
    }
}

#[cfg(any(feature="tokio-websockets", feature="threaded-websockets"))]
pub(crate) fn create_default_websocket_handshake_request(uri: String) -> GneissResult<http::request::Builder> {
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

/// A structure that holds configuration related to how an asynchronous client should interact
/// with the Tokio async runtime.
#[cfg(feature = "tokio")]
#[derive(Clone)]
pub struct TokioOptions {
    pub(crate) runtime: Handle,
}

#[cfg(feature = "tokio")]
impl TokioOptions {

    /// Creates a new builder for TokioClientOptions instances.
    pub fn builder(runtime: Handle) -> TokioOptionsBuilder {
        TokioOptionsBuilder::new(runtime)
    }
}

/// Builder type for tokio-based client configuration
#[cfg(feature = "tokio")]
pub struct TokioOptionsBuilder {
    options: TokioOptions
}

#[cfg(feature = "tokio")]
impl TokioOptionsBuilder {

    pub(crate) fn new(runtime: Handle) -> Self {
        TokioOptionsBuilder {
            options: TokioOptions {
                runtime
            }
        }
    }

    /// Builds a new set of tokio client configuration options
    pub fn build(self) -> TokioOptions {
        self.options
    }
}

/// Threaded client specific configuration
#[cfg(feature = "threaded")]
#[derive(Default, Clone)]
pub struct ThreadedOptions {
    pub(crate) idle_service_sleep: Option<Duration>,
}

#[cfg(feature = "threaded")]
impl ThreadedOptions {

    /// Creates a new builder for ThreadedClientOptions instances
    pub fn builder() -> ThreadedOptionsBuilder {
        ThreadedOptionsBuilder::new()
    }
}

/// Builder type for threaded client configuration
#[cfg(feature = "threaded")]
pub struct ThreadedOptionsBuilder {
    config: ThreadedOptions
}

#[cfg(feature = "threaded")]
impl ThreadedOptionsBuilder {

    pub(crate) fn new() -> Self {
        ThreadedOptionsBuilder {
            config: ThreadedOptions {
                idle_service_sleep: None,
            }
        }
    }

    /// Configures the time interval to sleep the thread the client runs on between io
    /// processing events.
    ///
    /// Only used if no events occurred on the previous iteration.  If the
    /// client is handling significant work, it will not sleep, but if there's nothing
    /// happening, it will.
    ///
    /// If not set, defaults to 20 milliseconds.
    pub fn with_idle_service_sleep(&mut self, duration: Duration) {
        self.config.idle_service_sleep = Some(duration);
    }

    /// Builds a new set of threaded client configuration options
    pub fn build(self) -> ThreadedOptions {
        self.config
    }
}