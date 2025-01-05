/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
This crate provides clients for communicating with a message broker using the MQTT protocol.

MQTT is a publish/subscribe protocol commonly chosen in IoT use cases.  This crate supports
both [MQTT5](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html) and
[MQTT311](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html).  We strongly
recommend using MQTT5 over 311 for the significant error handling and communication improvements.
MQTT specification links within crate documentation are made to the MQTT5 spec.

# Overview

Depending on feature selection, the crate can provide either a client based on the [`tokio`](https://crates.io/crates/tokio)
runtime or a client that runs in a background thread.  The interfaces to these two clients
are similar but differ slightly in certain ways (primarily in how operations complete).
Both clients are asynchronous in the sense that requests to perform MQTT operations are carried out
asynchronously, but only the tokio-based client has an interface that uses Rust's async keyword.

### Features

The crate supports a variety of connection methods, including:
* **TLS** - provided by either *[`rustls`](https://crates.io/crates/rustls)* or *[`native-tls`](https://crates.io/crates/native-tls)*
* **Websockets** - provided by *[`tungstenite`](https://crates.io/crates/tungstenite)*
* **HTTP proxies** - bespoke implementation

It is common to see crates provide transport-agnostic clients -- which is clean and minimal -- but at
the cost of forcing the user to construct the transport connection themselves, which can be daunting.
This crate has been designed with the goal of providing a solution, not a piece of a solution.
While this crate does support bring-your-own-transport, it also provides optional features that
greatly simplify the setup required to use common transport level options.

The crate supports the following features:
* **tokio** - enables the tokio-based async client
* **tokio-rustls** - enables TLS (backed by the rustls crate) support within the tokio-based async client
* **tokio-native-tls** - enables TLS (backed by the native-tls crate) support within the tokio-based async client
* **tokio-websockets** - enables websockets support within the tokio-based async client
* **threaded** - enables the thread-based client
* **threaded-rustls** - enables TLS (backed by the rustls crate) support within the thread-based client
* **threaded-native-tls** - enables TLS (backed by the native-tls crate) support within the thread-based client
* **threaded-websockets** - enables websockets support within the thread-based client

# Usage

To use this crate, you'll first need to add it to your project's Cargo.toml:

```toml
[dependencies]
gneiss-mqtt = { version = "<version>", features = [ ... ] }
```

If using the tokio client and your project does not yet include [`tokio`](https://crates.io/crates/tokio),
you will need to add it too:

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
```

This crate contains all the building blocks necessary to connect to most MQTT brokers, but
the configuration to do so can be confusing and delicate.  For complex cases, we recommend
using broker-specific crates that implement all the low-level connector details needed to
successfully establish an MQTT connection to a specific broker.  The documentation for these
specialized crates contains samples and information on how to connect in all of the ways
each broker supports.

Currently, these crates include:
* *[gneiss-mqtt-aws](https://crates.io/crates/gneiss-mqtt-aws)* - A crate with a builder that
  supports all connection methods allowed by the AWS MQTT broker implementation,
  [AWS IoT Core](https://docs.aws.amazon.com/iot/latest/developerguide/iot-gs.html).

# Examples

In addition to the in-docs examples, there are a variety of standalone examples that can be found
in the [project repository](https://github.com/gneiss-mqtt/gneiss-mqtt/tree/main/gneiss-mqtt/examples).
*/

#![cfg_attr(feature = "tokio", doc = r##"
## Example: Connect to a local Mosquitto server with the tokio client

Assuming a default Mosquitto installation, you can connect locally by plaintext on port 1883:

```no_run
use gneiss_mqtt::client::AsyncClient;
use gneiss_mqtt::client::TokioClientBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let client =
        TokioClientBuilder::new("127.0.0.1", 1883)
            .build()?;

    // Once started, the client will recurrently maintain a connection to the endpoint until
    // stop() is invoked
    client.start(None)?;

    // <do stuff with the client>

    Ok(())
}
```"##)]

/*!
## Example: Subscribe to a topic with an async client

In order to receive messages, you must first subscribe to the topics you want to receive messages for.  Subscribing
is straightforward: configure a Subscribe packet and submit it to the client.  The subscribe will
be performed whether or not the result is waited on.

A successful subscribe result resolves into the Suback packet that the broker responded with.  You must check the Suback
reason code vector to verify the success/failure result for each subscription in the original subscribe.

```no_run
use gneiss_mqtt::error::GneissResult;
use gneiss_mqtt::client::{AsyncClient, AsyncClientHandle, SubscribeResult};
use gneiss_mqtt::mqtt::{QualityOfService, SubscribePacket, Subscription};

async fn subscribe_to_topic(client: AsyncClientHandle, topic_filter: String) {
    let subscribe = SubscribePacket::builder()
        .with_subscription_simple(topic_filter, QualityOfService::AtLeastOnce)
        .build();

    let subscribe_result = client.subscribe(subscribe, None).await;
    match subscribe_result {
        Ok(suback) => {
            let rc = suback.reason_codes()[0];
            if rc.is_success() {
                println!("Subscribe success!");
            } else {
                println!("Subscribe failed with reason code: {}", rc.to_string());
            }
        }
        Err(err) => {
            println!("Subscribe failed with error: {}", err);
        }
    }
}
```

## Example: Unsubscribe from a topic with an async client

```no_run
use gneiss_mqtt::error::GneissResult;
use gneiss_mqtt::client::{AsyncClient, AsyncClientHandle, UnsubscribeResult};
use gneiss_mqtt::mqtt::UnsubscribePacket;

async fn unsubscribe_from_topic(client: AsyncClientHandle, topic_filter: String) {
    let unsubscribe = UnsubscribePacket::builder()
        .with_topic_filter(topic_filter)
        .build();

    let unsubscribe_result = client.unsubscribe(unsubscribe, None).await;
    match unsubscribe_result {
        Ok(unsuback) => {
            let rc = unsuback.reason_codes()[0];
            if rc.is_success() {
                println!("Unsubscribe success!");
            } else {
                println!("Unsubscribe failed with reason code: {}", rc.to_string());
            }
        }
        Err(err) => {
            println!("Unsubscribe failed with error: {}", err);
        }
    }
}
```

## Example: Publish to a topic with an async client

```no_run
use gneiss_mqtt::error::GneissResult;
use gneiss_mqtt::client::{AsyncClient, AsyncClientHandle, PublishResponse, PublishResult};
use gneiss_mqtt::mqtt::{PublishPacket, QualityOfService};

async fn publish_to_topic(client: AsyncClientHandle, topic: String, message: String) {
    let publish = PublishPacket::builder(topic, QualityOfService::AtLeastOnce)
        .with_payload(message.into_bytes())
        .build();

    let publish_result = client.publish(publish, None).await;
    match publish_result {
        Ok(publish_response) => {
            match publish_response {
                PublishResponse::Qos1(puback) => {
                    let rc = puback.reason_code();
                    if rc.is_success() {
                        println!("Publish success!");
                    } else {
                        println!("Publish failed with reason code: {}", rc.to_string());
                    }
                }
                _ => { panic!("Illegal publish response to a Qos1 publish!") }
            }
        }
        Err(err) => {
            println!("Publish failed with error: {}", err);
        }
    }
}
```

*/

#![cfg_attr(feature = "tokio", doc = r##"
## Example: React to client events with an async client

In addition to performing MQTT operations with the client, you can also react to events emitted by the
client.  The client emits events when connectivity changes (successful connection, failed connection, disconnection,
etc...) as well as when publishes are received.

To handle client events, pass in a handler when starting the client.  See the [crate::client::ClientEvent] documentation for
more information on what data each event variant may contain.

This example shows how you can capture the client in the event handler closure, letting you perform additional
operations in reaction to client events (the client's public API is immutable).  In this case, we send a "Pong" publish
every time we receive a "Ping" publish:

```no_run
use gneiss_mqtt::client::{AsyncClient, AsyncClientHandle, ClientEvent, TokioClientBuilder};
use gneiss_mqtt::mqtt::{PublishPacket, QualityOfService};
use std::sync::Arc;
use tokio::runtime::Handle;

pub fn client_event_callback(client: AsyncClientHandle, event: Arc<ClientEvent>) {
    if let ClientEvent::PublishReceived(publish_received_event) = event.as_ref() {
        let publish = &publish_received_event.publish;
        if let Some(payload) = publish.payload() {
            if "Ping".as_bytes() == payload {
                // we received a Ping, let's send a Pong in response
                let pong_publish = PublishPacket::builder(publish.topic().to_string(), QualityOfService::AtMostOnce)
                    .with_payload("Pong".as_bytes().to_vec()).build();

                // we're in a synchronous function, but it's being called from an async task within the runtime, so
                // we can await and check the publish result by getting the current runtime and spawning an async
                // task in it
                let runtime_handle = Handle::current();
                runtime_handle.spawn(async move {
                    if let Ok(publish_result) = client.publish(pong_publish, None).await {
                        println!("Successfully published Pong!");
                    } else {
                        println!("Failed to publish Pong!");
                    }
                });
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let client =
        TokioClientBuilder::new("127.0.0.1", 1883)
            .build()?;

    // make a client event handler closure
    let closure_client = client.clone();
    let listener_callback = Arc::new(move |event| { client_event_callback(closure_client.clone(), event) });

    // Pass the event handler callback into start()
    client.start(Some(listener_callback))?;

    // <do stuff with the client>

    Ok(())
}

```"##)]

/*!
# Additional Notes

This crate's public API is expected to be very unstable until v0.4.0.  See the roadmap
in the README for more details.

*/

#![warn(missing_docs)]

#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(any(feature = "tokio", feature = "threaded")), allow(dead_code))]
#![cfg_attr(all(feature = "testing", not(test)), allow(dead_code, unused_imports, unused_macros))]
#![cfg_attr(feature = "strict", deny(warnings))]

pub mod alias;
pub mod client;
mod decode;
mod encode;
pub mod error;
mod logging;
pub mod mqtt;
mod protocol;
#[cfg(feature = "testing")]
pub mod testing;
mod validate;
