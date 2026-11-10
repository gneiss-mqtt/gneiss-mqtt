/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]
#![cfg_attr(feature = "strict", deny(warnings))]
#![cfg_attr(feature = "testing", allow(dead_code))]
#![cfg_attr(feature = "testing", allow(unused_macros))]
#![cfg_attr(feature = "testing", allow(unused_imports))]

/*!
Rustls-based TLS connector for thread-based MQTT clients
 */

use std::io::{Read, Write};
use std::sync::Arc;
use rustls::{ClientConnection, StreamOwned};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use gneiss_mqtt::error::*;
use gneiss_mqtt::client::synchronous::network::*;
use gneiss_mqtt_client_tls::*;
use log::debug;

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

fn build_certs(certificate_bytes: &[u8]) -> Vec<CertificateDer<'static>> {
    let mut reader = std::io::BufReader::new(certificate_bytes);

    rustls_pemfile::certs(&mut reader)
        .flatten()
        .collect()
}

fn build_rustls_client_config(options: &TlsOptions) -> GneissResult<rustls::ClientConfig> {

    let root_cert_store = build_root_ca_store(options.root_ca_bytes().as_deref())?;

    let mut config =
        match options.mode() {
            TlsMode::Standard => {
                rustls::ClientConfig::builder()
                    .with_root_certificates(root_cert_store)
                    .with_no_client_auth()
            }

            TlsMode::Mtls => {
                let certs = build_certs(options.certificate_bytes().as_deref().unwrap());
                let private_key_result = build_private_key(options.private_key_bytes().as_deref().unwrap());
                if let Err(rustls_error) = private_key_result {
                    return Err(GneissError::new_tls_error(rustls_error));
                }

                let builder_result =
                    rustls::ClientConfig::builder()
                        .with_root_certificates(root_cert_store)
                        .with_client_auth_cert(certs, private_key_result.unwrap());
                if let Err(builder_error) = builder_result {
                    return Err(GneissError::new_tls_error(builder_error));
                }

                builder_result.unwrap()
            }
        };

    config.alpn_protocols = Vec::new();
    if let Some(alpn) = &options.alpn() {
        config.alpn_protocols.push(alpn.as_bytes().to_vec());
    }

    config.enable_sni = options.verify_peer();
    Ok(config)
}
pub struct RustlsTlsSyncConnectionStepFactory {

}

impl RustlsTlsSyncConnectionStepFactory {
    pub fn new() -> RustlsTlsSyncConnectionStepFactory {
        RustlsTlsSyncConnectionStepFactory {}
    }
}

struct RustlsStream {
    pub(crate) stream: StreamOwned<ClientConnection, StreamHandle>
}

impl RustlsStream {
    pub fn new(stream: StreamOwned<ClientConnection, StreamHandle>) -> RustlsStream {
        RustlsStream { stream }
    }
}

impl CompletableStream for RustlsStream {
    fn complete(&mut self) -> GneissResult<()> {
        CompletableStream::complete(&mut self.stream.sock);
        Ok(())
    }
}

impl Read for RustlsStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buf)
    }
}

impl Write for RustlsStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {

        self.stream.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {

        self.stream.flush()
    }
}

impl TlsSyncConnectionStepFactory for RustlsTlsSyncConnectionStepFactory {
    fn build_connection_step(&self, options: &TlsOptions, host: &str) -> GneissResult<SyncStreamTransform> {

        let domain_name : String =
            if let Some(server_name) = options.server_name_override() {
                server_name.to_string()
            } else {
                host.to_string()
            };

        let domain_result = rustls_pki_types::ServerName::try_from(domain_name);
        if let Err(rustls_error) = domain_result {
            return Err(GneissError::new_tls_error(rustls_error));
        }

        let domain = domain_result.unwrap().to_owned();
        let tls_config = Arc::new(build_rustls_client_config(options)?);

        let transform = move |stream : StreamHandle| {
            let client_connection_result = rustls::client::ClientConnection::new(tls_config.clone(), domain.clone());
            if let Err(rustls_error) = client_connection_result {
                return Err(GneissError::new_tls_error(rustls_error));
            }

            let client_connection = client_connection_result.unwrap();;

            let mut tls_stream = rustls::StreamOwned::new(client_connection, stream);
            debug!("wrap_stream_with_tls_rustls - performing tls handshake");
            let result = tls_stream.flush();
            if result.is_err() {
                return Err(GneissError::new_connection_establishment_failure(result.err().unwrap()));
            }
            debug!("wrap_stream_with_tls_rustls - tls handshake successfully completed");

            let handle = StreamHandle::new(RustlsStream::new(tls_stream));
            Ok(handle)
        };

        let stream_transform = Arc::new(transform);

        Ok(stream_transform)
    }
}

