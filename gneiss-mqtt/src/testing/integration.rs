/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::client::*;
use crate::config::*;

use std::env;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::error::MqttResult;
use crate::testing::client_event_waiter::*;


#[derive(Debug, Eq, PartialEq, Copy, Clone)]
enum TlsUsage {
    None,
    Rustls
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
enum WebsocketUsage {
    None,
    Tungstenite
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
enum ProxyUsage {
    None,
    Plaintext
}

fn get_ca_path() -> String {
    env::var("GNEISS_MQTT_TEST_BROKER_CA_PATH").unwrap()
}

fn get_broker_endpoint(tls: TlsUsage, ws: WebsocketUsage) -> String {
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

fn get_broker_port(tls: TlsUsage, ws: WebsocketUsage) -> u16 {
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

fn build_tokio_client(runtime: &tokio::runtime::Runtime, tls: TlsUsage, ws: WebsocketUsage, proxy: ProxyUsage) -> Mqtt5Client {
    let connect_options = ConnectOptionsBuilder::new()
        .with_rejoin_session_policy(RejoinSessionPolicy::PostSuccess)
        .build();

    let client_config = Mqtt5ClientOptionsBuilder::new()
        .with_connect_timeout(Duration::from_secs(5))
        .with_offline_queue_policy(OfflineQueuePolicy::PreserveAll)
        .build();

    let endpoint = get_broker_endpoint(tls, ws);
    let port = get_broker_port(tls, ws);

    let mut builder = GenericClientBuilder::new(&endpoint, port);
    builder.with_connect_options(connect_options);
    builder.with_client_options(client_config);

    if tls != TlsUsage::None {
        let mut tls_options_builder = TlsOptionsBuilder::new();
        tls_options_builder.with_verify_peer(false);
        tls_options_builder.with_root_ca_from_path(&get_ca_path()).unwrap();

        builder.with_tls_options(tls_options_builder.build_rustls().unwrap());
    }

    if ws != WebsocketUsage::None {
        let websocket_options = WebsocketOptionsBuilder::new().build();
        builder.with_websocket_options(websocket_options);
    }

    builder.build(runtime.handle()).unwrap()
}

type AsyncClientTestFactoryReturnType = Pin<Box<dyn Future<Output = MqttResult<()>> + Send>>;
type AsyncClientTestFactory = Box<dyn Fn(Arc<Mqtt5Client>) -> AsyncClientTestFactoryReturnType + Send + Sync>;

fn do_tokio_client_test(tls: TlsUsage, ws: WebsocketUsage, proxy: ProxyUsage, test_factory: AsyncClientTestFactory) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = Arc::new(build_tokio_client(&runtime, tls, ws, proxy));
    let test_future = (*test_factory)(client);

    runtime.block_on(test_future).unwrap();
}

async fn connect_disconnect_test(client: Arc<Mqtt5Client>) -> MqttResult<()> {
    let mut connection_attempt_waiter = TokioClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionAttempt);
    let mut connection_success_waiter = TokioClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionSuccess);

    client.start(None).ok();

    connection_attempt_waiter.wait().await.ok();
    connection_success_waiter.wait().await.ok();

    let mut disconnection_waiter = TokioClientEventWaiter::new_single(client.clone(), ClientEventType::Disconnection);
    let mut stopped_waiter = TokioClientEventWaiter::new_single(client.clone(), ClientEventType::Stopped);

    client.stop(None).ok();

    disconnection_waiter.wait().await.ok();
    stopped_waiter.wait().await.ok();

    Ok(())
}

#[test]
fn tokio_client_connect_disconnect_direct_plaintext_no_proxy() {
    do_tokio_client_test(TlsUsage::None, WebsocketUsage::None, ProxyUsage::None, Box::new(|client|{
        Box::pin(connect_disconnect_test(client))
    }));
}

#[test]
fn tokio_client_connect_disconnect_direct_rustls_no_proxy() {
    do_tokio_client_test(TlsUsage::Rustls, WebsocketUsage::None, ProxyUsage::None, Box::new(|client|{
        Box::pin(connect_disconnect_test(client))
    }));
}

#[test]
fn tokio_client_connect_disconnect_websocket_plaintext_no_proxy() {
    do_tokio_client_test(TlsUsage::None, WebsocketUsage::Tungstenite, ProxyUsage::None, Box::new(|client|{
        Box::pin(connect_disconnect_test(client))
    }));
}

#[test]
fn tokio_client_connect_disconnect_websocket_rustls_no_proxy() {
    do_tokio_client_test(TlsUsage::Rustls, WebsocketUsage::Tungstenite, ProxyUsage::None, Box::new(|client|{
        Box::pin(connect_disconnect_test(client))
    }));
}