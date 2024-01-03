/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate rustls;
extern crate tokio;
extern crate tokio_rustls;

use crate::*;
use crate::client::*;

use std::sync::Arc;
use std::fs::File;
use std::io::Read;
use std::net::{SocketAddr, ToSocketAddrs};
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio_rustls::client::TlsStream;
use tokio_rustls::{TlsConnector};

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

            Ok(Mqtt5Client::new(client_options, connect_options, tokio_options, runtime))
        } else {
            let tokio_options = TokioClientOptions {
                connection_factory: Box::new(move || { Box::pin(TcpStream::connect(addr)) })
            };

            Ok(Mqtt5Client::new(client_options, connect_options, tokio_options, runtime))
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
