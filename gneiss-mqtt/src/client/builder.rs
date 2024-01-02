/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate rustls;
extern crate rustls_pemfile;
extern crate tokio;
extern crate tokio_rustls;

use crate::*;
use crate::client::*;

use rustls::pki_types::{PrivateKeyDer};
use std::sync::Arc;
use std::fs::File;
use std::io::Read;
use std::net::{SocketAddr, ToSocketAddrs};
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio_rustls::client::TlsStream;
use tokio_rustls::{TlsConnector};



impl From<std::io::Error> for MqttError {
    fn from(_: std::io::Error) -> Self {
        MqttError::Unknown
    }
}

impl From<rustls::Error> for MqttError {
    fn from(_: rustls::Error) -> Self {
        MqttError::Unknown
    }
}

pub struct ClientBuilder {
    endpoint: String,
    port: u16,

    tls_config: Option<rustls::ClientConfig>,

    client_options: Option<Mqtt5ClientOptions>
}

impl ClientBuilder {

    pub fn new(endpoint: &str, port: u16) -> MqttResult<Self> {
        Ok(
            ClientBuilder {
                endpoint: endpoint.to_string(),
                port,
                tls_config: None,
                client_options: None
            }
        )
    }

    pub fn new_with_tls(endpoint: &str, port: u16, root_ca_path: Option<&str>) -> MqttResult<Self> {
        let root_cert_store = load_root_ca_store(root_ca_path)?;

        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();

        Ok(
            ClientBuilder {
                endpoint: endpoint.to_string(),
                port,
                tls_config: Some(tls_config),
                client_options: None
            }
        )
    }

    pub fn new_with_tls_config(endpoint: &str, port: u16, tls_config: rustls::ClientConfig) -> MqttResult<Self> {
        Ok(
            ClientBuilder {
                endpoint: endpoint.to_string(),
                port,
                tls_config: Some(tls_config),
                client_options: None
            }
        )
    }

    pub fn new_with_mtls_from_fs(endpoint: &str, port: u16, certificate_path: &str, private_key_path: &str, root_ca_path: Option<&str>) -> MqttResult<Self> {
        let root_cert_store = load_root_ca_store(root_ca_path)?;
        let certs = load_certs(certificate_path)?;
        let private_key = load_private_key(private_key_path)?;

        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_client_auth_cert(certs, private_key)?;

        Ok(
            ClientBuilder {
                endpoint: endpoint.to_string(),
                port,
                tls_config: Some(tls_config),
                client_options: None
            }
        )
    }

    pub fn new_with_mtls_from_memory(endpoint: &str, port: u16, certificate_bytes: &[u8], private_key_bytes: &[u8], root_ca_bytes: Option<&[u8]>) -> MqttResult<Self> {
        let root_cert_store = build_root_ca_store(root_ca_bytes)?;
        let certs = build_certs(certificate_bytes)?;
        let private_key = build_private_key(private_key_bytes)?;

        let tls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_client_auth_cert(certs, private_key)?;

        Ok(
            ClientBuilder {
                endpoint: endpoint.to_string(),
                port,
                tls_config: Some(tls_config),
                client_options: None
            }
        )
    }

    pub fn get_mut_tls_config(&mut self) -> Option<&mut rustls::ClientConfig> {
        self.tls_config.as_mut()
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

        let client_options =
            if let Some(options) = self.client_options {
                options
            } else {
                Mqtt5ClientOptionsBuilder::new()
                    .with_connect_options(ConnectOptionsBuilder::new().build())
                    .build()
            };

        if let Some(tls_config) = self.tls_config {
            let connector = TlsConnector::from(Arc::new(tls_config));
            let endpoint = self.endpoint.clone();

            let tokio_options = TokioClientOptions {
                connection_factory: Box::new(move || {
                    Box::pin(make_tls_stream(addr, endpoint.clone(), connector.clone()))
                })
            };

            Ok(Mqtt5Client::new(client_options, tokio_options, runtime))
        } else {
            Ok(Mqtt5Client::new(client_options,TokioClientOptions {
                connection_factory: Box::new(move || { Box::pin(TcpStream::connect(addr)) })
            }, runtime))
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

fn load_certs(certificate_path: &str) -> MqttResult<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let mut cert_bytes_vec = Vec::new();
    let mut cert_file = File::open(certificate_path)?;
    cert_file.read_to_end(&mut cert_bytes_vec)?;

    build_certs(cert_bytes_vec.as_slice())
}

fn build_certs(certificate_bytes: &[u8]) -> MqttResult<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let mut reader = std::io::BufReader::new(certificate_bytes);

    Ok(rustls_pemfile::certs(&mut reader)
        .filter(|cert| { cert.is_ok() })
        .map(|cert| cert.unwrap())
        .collect())
}

fn load_root_ca_store(root_ca_path: Option<&str>) -> MqttResult<rustls::RootCertStore> {
    let mut ca_bytes_vec = Vec::new();
    let ca_bytes =
        match root_ca_path {
            Some(path) => {
                let mut ca_file = File::open(path)?;
                ca_file.read_to_end(&mut ca_bytes_vec)?;
                Some(ca_bytes_vec.as_slice())
            }
            _ => { None }
        };

    build_root_ca_store(ca_bytes)
}

fn build_root_ca_store(root_ca_bytes: Option<&[u8]>) -> MqttResult<rustls::RootCertStore> {
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

fn load_private_key(key_path: &str) -> MqttResult<PrivateKeyDer<'static>> {
    let mut pk_bytes_vec = Vec::new();
    let mut pk_file = File::open(key_path)?;
    pk_file.read_to_end(&mut pk_bytes_vec)?;

    build_private_key(pk_bytes_vec.as_slice())
}

fn build_private_key(key_bytes: &[u8]) -> MqttResult<PrivateKeyDer<'static>> {
    let mut reader = std::io::BufReader::new(key_bytes);

    loop {
        match rustls_pemfile::read_one(&mut reader)? {
            Some(rustls_pemfile::Item::Pkcs1Key(key)) => return Ok(key.into()),
            Some(rustls_pemfile::Item::Pkcs8Key(key)) => return Ok(key.into()),
            Some(rustls_pemfile::Item::Sec1Key(key)) => return Ok(key.into()),
            None => { return Err(MqttError::Unknown); }
            _ => {}
        }
    }
}
