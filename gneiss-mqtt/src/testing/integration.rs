/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::env;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use assert_matches::assert_matches;
use crate::client::{AsyncGneissClient, ClientEvent, PublishReceivedEvent, PublishResponse, Qos2Response, SyncGneissClient};
use crate::client::waiter::{AsyncClientEventWaiter, ClientEventType, ClientEventWaiterOptions, ClientEventWaitType, SyncClientEventWaiter};
use crate::config::*;
use crate::error::{MqttError, MqttResult};
use crate::mqtt::{ConnectReasonCode, PubackReasonCode, PubcompReasonCode, PublishPacket, QualityOfService, SubackReasonCode, SubscribePacket, UnsubackReasonCode, UnsubscribePacket};

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) enum TlsUsage {
    None,

    #[cfg(any(feature = "tokio-rustls", feature = "threaded-rustls"))]
    Rustls,

    #[cfg(any(feature = "tokio-native-tls", feature = "threaded-native-tls"))]
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

#[cfg(any(feature = "tokio-rustls", feature = "threaded-rustls"))]
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

pub(crate) fn create_client_builder_internal(connect_options: ConnectOptions, _tls_usage: TlsUsage, proxy_config: ProxyUsage, tls_endpoint: TlsUsage, ws_endpoint: WebsocketUsage) -> GenericClientBuilder {
    let client_config = MqttClientOptionsBuilder::new()
        .with_connect_timeout(Duration::from_secs(5))
        .with_offline_queue_policy(OfflineQueuePolicy::PreserveAll)
        .build();

    let endpoint = get_broker_endpoint(tls_endpoint, ws_endpoint);
    let port = get_broker_port(tls_endpoint, ws_endpoint);

    let mut builder = GenericClientBuilder::new(&endpoint, port);
    builder.with_connect_options(connect_options);
    builder.with_client_options(client_config);

    #[cfg(any(feature = "tokio-rustls", feature = "threaded-rustls"))]
    if _tls_usage == TlsUsage::Rustls {
        let mut tls_options_builder = TlsOptionsBuilder::new();
        tls_options_builder.with_verify_peer(false);
        tls_options_builder.with_root_ca_from_path(&get_ca_path()).unwrap();

        builder.with_tls_options(tls_options_builder.build_rustls().unwrap());
    }

    #[cfg(any(feature = "tokio-native-tls", feature = "threaded-native-tls"))]
    if _tls_usage == TlsUsage::Nativetls {
        let mut tls_options_builder = TlsOptionsBuilder::new();
        tls_options_builder.with_verify_peer(false);
        tls_options_builder.with_root_ca_from_path(&get_ca_path()).unwrap();

        builder.with_tls_options(tls_options_builder.build_native_tls().unwrap());
    }

    if proxy_config != ProxyUsage::None {
        let proxy_endpoint = get_proxy_endpoint();
        let proxy_port = get_proxy_port();
        let proxy_options = HttpProxyOptionsBuilder::new(&proxy_endpoint, proxy_port).build();
        builder.with_http_proxy_options(proxy_options);
    }

    builder
}

pub(crate) fn create_good_client_builder(tls: TlsUsage, ws: WebsocketUsage, proxy: ProxyUsage) -> GenericClientBuilder {
    let connect_options = ConnectOptionsBuilder::new()
        .with_rejoin_session_policy(RejoinSessionPolicy::PostSuccess)
        .with_session_expiry_interval_seconds(3600)
        .build();

    create_client_builder_internal(connect_options, tls, proxy, tls, ws)
}

pub(crate) fn create_websocket_options_async(ws: WebsocketUsage) -> Option<AsyncWebsocketOptions> {
    match ws {
        WebsocketUsage::None => { None }
        WebsocketUsage::Tungstenite => { Some(AsyncWebsocketOptionsBuilder::new().build()) }
    }
}

pub(crate) fn create_websocket_options_sync(ws: WebsocketUsage) -> Option<SyncWebsocketOptions> {
    match ws {
        WebsocketUsage::None => { None }
        WebsocketUsage::Tungstenite => { Some(SyncWebsocketOptionsBuilder::new().build()) }
    }
}

pub(crate) type AsyncTestFactoryReturnType = Pin<Box<dyn Future<Output=MqttResult<()>> + Send>>;
pub(crate) type AsyncTestFactory = Box<dyn Fn(GenericClientBuilder) -> AsyncTestFactoryReturnType + Send + Sync>;
pub(crate) type SyncTestFactory = Box<dyn Fn(GenericClientBuilder) -> MqttResult<()>>;

pub(crate) async fn start_async_client<T : AsyncClientEventWaiter>(client: &AsyncGneissClient, waiter_factory: fn(AsyncGneissClient, ClientEventType) -> T) -> MqttResult<()> {
    let connection_attempt_waiter = waiter_factory(client.clone(), ClientEventType::ConnectionAttempt);
    let connection_success_waiter = waiter_factory(client.clone(), ClientEventType::ConnectionSuccess);

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

pub(crate) async fn stop_async_client<T : AsyncClientEventWaiter>(client: &AsyncGneissClient, waiter_factory: fn(AsyncGneissClient, ClientEventType) -> T) -> MqttResult<()> {
    let disconnection_waiter = waiter_factory(client.clone(), ClientEventType::Disconnection);
    let stopped_waiter = waiter_factory(client.clone(), ClientEventType::Stopped);

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

pub(crate) fn start_sync_client<T : SyncClientEventWaiter>(client: &SyncGneissClient, waiter_factory: fn(SyncGneissClient, ClientEventType) -> T) -> MqttResult<()> {
    let connection_attempt_waiter = waiter_factory(client.clone(), ClientEventType::ConnectionAttempt);
    let connection_success_waiter = waiter_factory(client.clone(), ClientEventType::ConnectionSuccess);

    client.start(None)?;

    let connection_attempt_events = connection_attempt_waiter.wait()?;
    assert_eq!(1, connection_attempt_events.len());
    let connection_attempt_event = &connection_attempt_events[0].event;
    assert_matches!(**connection_attempt_event, ClientEvent::ConnectionAttempt(_));

    let connection_success_events = connection_success_waiter.wait()?;
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

pub(crate) fn stop_sync_client<T : SyncClientEventWaiter>(client: &SyncGneissClient, waiter_factory: fn(SyncGneissClient, ClientEventType) -> T) -> MqttResult<()> {
    let disconnection_waiter = waiter_factory(client.clone(), ClientEventType::Disconnection);
    let stopped_waiter = waiter_factory(client.clone(), ClientEventType::Stopped);

    client.stop(None)?;

    let disconnect_events = disconnection_waiter.wait()?;
    assert_eq!(1, disconnect_events.len());
    let disconnect_event = &disconnect_events[0].event;
    assert_matches!(**disconnect_event, ClientEvent::Disconnection(_));
    if let ClientEvent::Disconnection(event) = &**disconnect_event {
        assert_matches!(event.error, MqttError::UserInitiatedDisconnect(_));
    } else {
        panic!("impossible");
    }

    stopped_waiter.wait()?;

    Ok(())
}

pub(crate) async fn async_subscribe_unsubscribe_test<T : AsyncClientEventWaiter>(client: AsyncGneissClient, waiter_factory: fn(AsyncGneissClient, ClientEventType) -> T) -> MqttResult<()> {
    start_async_client(&client, waiter_factory).await?;

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

    stop_async_client(&client, waiter_factory).await?;

    Ok(())
}

pub(crate) fn verify_successful_publish_result(result: &PublishResponse, qos: QualityOfService) {
    match result {
        PublishResponse::Qos0 => {
            assert_eq!(qos, QualityOfService::AtMostOnce);
        }
        PublishResponse::Qos1(puback) => {
            assert_eq!(qos, QualityOfService::AtLeastOnce);
            assert_eq!(PubackReasonCode::Success, puback.reason_code);
        }
        PublishResponse::Qos2(qos2_result) => {
            assert_eq!(qos, QualityOfService::ExactlyOnce);
            assert_matches!(qos2_result, Qos2Response::Pubcomp(_));
            if let Qos2Response::Pubcomp(pubcomp) = qos2_result {
                assert_eq!(PubcompReasonCode::Success, pubcomp.reason_code);
            }
        }
    }
}

pub(crate) fn verify_publish_received(event: &PublishReceivedEvent, expected_topic: &str, expected_qos: QualityOfService, expected_payload: &[u8]) {
    let publish = &event.publish;

    assert_eq!(expected_qos, publish.qos);
    assert_eq!(expected_topic, &publish.topic);
    assert_eq!(expected_payload, publish.payload.as_ref().unwrap().as_slice());
}

pub(crate) async fn async_subscribe_publish_test<T : AsyncClientEventWaiter>(client: AsyncGneissClient, qos: QualityOfService, waiter_factory: fn(AsyncGneissClient, ClientEventType) -> T) -> MqttResult<()> {
    start_async_client(&client, waiter_factory).await?;

    let payload = "derp".as_bytes().to_vec();

    // tests are running in parallel, need a unique topic
    let uuid = uuid::Uuid::new_v4();
    let topic = format!("hello/world/{}", uuid.to_string());
    let subscribe = SubscribePacket::builder()
        .with_subscription_simple(topic.clone(), QualityOfService::ExactlyOnce)
        .build();

    let _ = client.subscribe(subscribe, None).await?;

    let publish_received_waiter = waiter_factory(client.clone(), ClientEventType::PublishReceived);

    let publish = PublishPacket::builder(topic.clone(), qos)
        .with_payload(payload.clone())
        .build();

    let publish_result = client.publish(publish, None).await?;
    verify_successful_publish_result(&publish_result, qos);

    let publish_received_events = publish_received_waiter.wait().await?;
    assert_eq!(1, publish_received_events.len());
    let publish_received_event = &publish_received_events[0].event;
    assert_matches!(**publish_received_event, ClientEvent::PublishReceived(_));
    if let ClientEvent::PublishReceived(event) = &**publish_received_event {
        verify_publish_received(event, &topic, qos, payload.as_slice());
    } else {
        panic!("impossible");
    }

    stop_async_client(&client, waiter_factory).await?;

    Ok(())
}

pub(crate) async fn async_will_test<T : AsyncClientEventWaiter>(base_client_options: GenericClientBuilder, client_factory: fn(GenericClientBuilder, TokioClientOptions) -> AsyncGneissClient, waiter_factory: fn(AsyncGneissClient, ClientEventType) -> T) -> MqttResult<()> {
    let tokio_options = TokioClientOptionsBuilder::new(tokio::runtime::Handle::current().clone()).build();
    let client = client_factory(base_client_options, tokio_options);

    let payload = "Onsecondthought".as_bytes().to_vec();

    // tests are running in parallel, need a unique topic
    let uuid = uuid::Uuid::new_v4();
    let will_topic = format!("goodbye/cruel/world/{}", uuid.to_string());

    let will = PublishPacket::builder(will_topic.clone(), QualityOfService::AtLeastOnce)
        .with_payload(payload.clone())
        .build();

    let connect_options = ConnectOptionsBuilder::new()
        .with_rejoin_session_policy(RejoinSessionPolicy::PostSuccess)
        .with_will(will)
        .build();

    let will_builder = create_client_builder_internal(connect_options, TlsUsage::None, ProxyUsage::None, TlsUsage::None, WebsocketUsage::None);
    let will_tokio_options = TokioClientOptionsBuilder::new(tokio::runtime::Handle::current().clone()).build();
    let will_client = client_factory(will_builder, will_tokio_options);

    start_async_client(&client, waiter_factory).await?;
    start_async_client(&will_client, waiter_factory).await?;

    let subscribe = SubscribePacket::builder()
        .with_subscription_simple(will_topic.clone(), QualityOfService::ExactlyOnce)
        .build();
    let _ = client.subscribe(subscribe, None).await?;

    let publish_received_waiter = waiter_factory(client.clone(), ClientEventType::PublishReceived);

    // no stop options, so we just close the socket locally; the broker should send the will
    stop_async_client(&will_client, waiter_factory).await?;

    let publish_received_events = publish_received_waiter.wait().await?;
    assert_eq!(1, publish_received_events.len());
    let publish_received_event = &publish_received_events[0].event;
    assert_matches!(**publish_received_event, ClientEvent::PublishReceived(_));
    if let ClientEvent::PublishReceived(event) = &**publish_received_event {
        verify_publish_received(event, &will_topic, QualityOfService::AtLeastOnce, payload.as_slice());
    } else {
        panic!("impossible");
    }

    stop_async_client(&client, waiter_factory).await?;

    Ok(())
}

pub(crate) async fn async_connect_disconnect_cycle_session_rejoin_test<T : AsyncClientEventWaiter>(client: AsyncGneissClient,
                                                                                                   single_waiter_factory: fn(AsyncGneissClient, ClientEventType) -> T,
                                                                                                   waiter_factory: fn(AsyncGneissClient, ClientEventWaiterOptions, usize) -> T) -> MqttResult<()> {
    start_async_client(&client, single_waiter_factory).await?;
    stop_async_client(&client, single_waiter_factory).await?;

    for _ in 0..5 {
        let waiter_config = ClientEventWaiterOptions {
            wait_type: ClientEventWaitType::Predicate(Box::new(|ev| {
                if let ClientEvent::ConnectionSuccess(success_event) = &**ev {
                    return success_event.connack.session_present && success_event.settings.rejoined_session;
                }

                false
            })),
        };
        let connection_success_waiter = waiter_factory(client.clone(), waiter_config, 1);

        client.start(None)?;

        let connection_success_events = connection_success_waiter.wait().await?;
        assert_eq!(1, connection_success_events.len());

        stop_async_client(&client, single_waiter_factory).await?;
    }

    Ok(())
}