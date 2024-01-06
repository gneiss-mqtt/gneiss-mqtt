/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Functionality for using [`rustls`](https://crates.io/crates/rustls) as an MQTT client's TLS
implementation.
 */

extern crate rustls;
extern crate rustls_pemfile;
extern crate rustls_pki_types;

use crate::config::{TlsData, TlsMode, TlsOptions};
use crate::config::TlsOptionsBuilder;

use rustls::pki_types::{PrivateKeyDer};


impl TlsOptionsBuilder {

    /// Builds client TLS options using the `rustls` crate
    pub fn build_rustls(self) -> Result<TlsOptions, rustls::Error> {
        if let Ok(root_cert_store) = build_root_ca_store(self.root_ca_bytes.as_deref()) {

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
            if let Some(alpn) = self.alpn {
                config.alpn_protocols.push(alpn);
            }

            return Ok(TlsOptions {
                options: TlsData::Rustls(self.mode, config)
            });
        }

        Err(rustls::Error::General("failed to build TLS context".to_string()))
    }
}

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

fn build_certs(certificate_bytes: &[u8]) -> Vec<rustls_pki_types::CertificateDer<'static>> {
    let mut reader = std::io::BufReader::new(certificate_bytes);

    rustls_pemfile::certs(&mut reader)
        .filter(|cert| { cert.is_ok() })
        .map(|cert| cert.unwrap())
        .collect()
}