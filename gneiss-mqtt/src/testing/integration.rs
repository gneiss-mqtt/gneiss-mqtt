/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::env;

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) enum TlsUsage {
    None,

    #[cfg(feature = "tokio-rustls")]
    Rustls,

    #[cfg(feature = "tokio-native-tls")]
    Nativetls
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) enum WebsocketUsage {
    None,

    #[cfg(feature="tokio-websockets")]
    Tungstenite
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) enum ProxyUsage {
    None,
    Plaintext
}

#[cfg(feature = "tokio-rustls")]
pub(crate) fn get_ca_path() -> String {
    env::var("GNEISS_MQTT_TEST_BROKER_CA_PATH").unwrap()
}

pub(crate) fn get_proxy_endpoint() -> String {
    env::var("GNEISS_MQTT_TEST_HTTP_PROXY_ENDPOINT").unwrap()
}

pub(crate) fn get_proxy_port() -> u16 {
    let port_string = env::var("GNEISS_MQTT_TEST_HTTP_PROXY_PORT").unwrap();

    port_string.parse().unwrap()
}

pub(crate) fn get_broker_endpoint(tls: TlsUsage, ws: WebsocketUsage) -> String {
    if tls == TlsUsage::None {
        if ws == WebsocketUsage::None {
            env::var("GNEISS_MQTT_TEST_DIRECT_PLAINTEXT_ENDPOINT").unwrap()
        } else {
            env::var("GNEISS_MQTT_TEST_WEBSOCKET_PLAINTEXT_ENDPOINT").unwrap()
        }
    } else {
        if ws == WebsocketUsage::None {
            env::var("GNEISS_MQTT_TEST_DIRECT_TLS_ENDPOINT").unwrap()
        } else {
            env::var("GNEISS_MQTT_TEST_WEBSOCKET_TLS_ENDPOINT").unwrap()
        }
    }
}

pub(crate) fn get_broker_port(tls: TlsUsage, ws: WebsocketUsage) -> u16 {
    let port_string =
        if tls == TlsUsage::None {
            if ws == WebsocketUsage::None {
                env::var("GNEISS_MQTT_TEST_DIRECT_PLAINTEXT_PORT").unwrap()
            } else {
                env::var("GNEISS_MQTT_TEST_WEBSOCKET_PLAINTEXT_PORT").unwrap()
            }
        } else {
            if ws == WebsocketUsage::None {
                env::var("GNEISS_MQTT_TEST_DIRECT_TLS_PORT").unwrap()
            } else {
                env::var("GNEISS_MQTT_TEST_WEBSOCKET_TLS_PORT").unwrap()
            }
        };

    port_string.parse().unwrap()
}

