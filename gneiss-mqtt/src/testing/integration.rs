/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::env;
use std::sync::Arc;
use crate::client::ClientEvent;

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) enum TlsUsage {
    None,
    Rustls
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) enum WebsocketUsage {
    None,
    Tungstenite
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) enum ProxyUsage {
    None,
    Plaintext
}

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

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) enum ClientEventType {
    ConnectionAttempt,
    ConnectionSuccess,
    ConnectionFailure,
    Disconnection,
    Stopped,
    PublishReceived,
}

pub(crate) fn client_event_matches(event: &Arc<ClientEvent>, event_type: ClientEventType) -> bool {
    match **event {
        ClientEvent::ConnectionAttempt(_) => { event_type == ClientEventType::ConnectionAttempt }
        ClientEvent::ConnectionSuccess(_) => { event_type == ClientEventType::ConnectionSuccess }
        ClientEvent::ConnectionFailure(_) => { event_type == ClientEventType::ConnectionFailure }
        ClientEvent::Disconnection(_) => { event_type == ClientEventType::Disconnection }
        ClientEvent::Stopped(_) => { event_type == ClientEventType::Stopped }
        ClientEvent::PublishReceived(_) => { event_type == ClientEventType::PublishReceived }
    }
}

pub(crate) type ClientEventPredicate = dyn Fn(&Arc<ClientEvent>) -> bool + Send + Sync;

pub(crate) struct ClientEventWaiterOptions {
    pub(crate) event_type: ClientEventType,

    pub(crate) event_predicate: Option<Box<ClientEventPredicate>>,
}