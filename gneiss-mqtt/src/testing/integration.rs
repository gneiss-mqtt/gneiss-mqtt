/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::env;
use assert_matches::assert_matches;
use crate::client::{AsyncGneissClient, ClientEvent};
use crate::client::waiter::{AsyncClientEventWaiter, ClientEventType};
use crate::error::{MqttError, MqttResult};
use crate::mqtt::{ConnectReasonCode, QualityOfService, SubackReasonCode, SubscribePacket, UnsubackReasonCode, UnsubscribePacket};

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

pub(crate) async fn start_client<T : AsyncClientEventWaiter>(client: &AsyncGneissClient, waiter_factory: fn(AsyncGneissClient, ClientEventType) -> T) -> MqttResult<()> {
    let mut connection_attempt_waiter = waiter_factory(client.clone(), ClientEventType::ConnectionAttempt);
    let mut connection_success_waiter = waiter_factory(client.clone(), ClientEventType::ConnectionSuccess);

    client.start(None)?;

    connection_attempt_waiter.wait().await?;
    let connection_success_events = connection_success_waiter.wait().await?;
    assert_eq!(1, connection_success_events.len());
    let connection_success_event = &connection_success_events[0].event;
    assert_matches!(**connection_success_event, ClientEvent::ConnectionSuccess(_));
    if let ClientEvent::ConnectionSuccess(success_event) = &**connection_success_event {
        assert_eq!(ConnectReasonCode::Success, success_event.connack.reason_code);
    } else {
        panic!("impossible");
    }

    Ok(())
}

pub(crate) async fn stop_client<T : AsyncClientEventWaiter>(client: &AsyncGneissClient, waiter_factory: fn(AsyncGneissClient, ClientEventType) -> T) -> MqttResult<()> {
    let mut disconnection_waiter = waiter_factory(client.clone(), ClientEventType::Disconnection);
    let mut stopped_waiter = waiter_factory(client.clone(), ClientEventType::Stopped);

    client.stop(None)?;

    let disconnect_events = disconnection_waiter.wait().await?;
    assert_eq!(1, disconnect_events.len());
    let disconnect_event = &disconnect_events[0].event;
    assert_matches!(**disconnect_event, ClientEvent::Disconnection(_));
    if let ClientEvent::Disconnection(event) = &**disconnect_event {
        assert_matches!(event.error, MqttError::UserInitiatedDisconnect(_));
    } else {
        panic!("impossible");
    }

    stopped_waiter.wait().await?;

    Ok(())
}

pub(crate) async fn async_subscribe_unsubscribe_test<T : AsyncClientEventWaiter>(client: AsyncGneissClient, waiter_factory: fn(AsyncGneissClient, ClientEventType) -> T) -> MqttResult<()> {
    start_client(&client, waiter_factory).await?;

    let subscribe = SubscribePacket::builder()
        .with_subscription_simple("hello/world".to_string(), QualityOfService::AtLeastOnce)
        .build();

    let subscribe_result = client.subscribe(subscribe, None).await;
    assert!(subscribe_result.is_ok());
    let suback = subscribe_result.unwrap();
    assert_eq!(1, suback.reason_codes.len());
    assert_eq!(SubackReasonCode::GrantedQos1, suback.reason_codes[0]);

    let unsubscribe = UnsubscribePacket::builder()
        .with_topic_filter("hello/world".to_string())
        .with_topic_filter("not/subscribed".to_string())
        .build();

    let unsubscribe_result = client.unsubscribe(unsubscribe, None).await;
    assert!(unsubscribe_result.is_ok());
    let unsuback = unsubscribe_result.unwrap();
    assert_eq!(2, unsuback.reason_codes.len());
    assert_eq!(UnsubackReasonCode::Success, unsuback.reason_codes[0]);
    // broker may or may not give us a not subscribed reason code, so don't verify

    stop_client(&client, waiter_factory).await?;

    Ok(())
}