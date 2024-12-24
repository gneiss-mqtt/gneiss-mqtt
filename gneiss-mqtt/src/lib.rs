/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
This crate provides clients for communicating with a message broker using the MQTT protocol.

MQTT is a publish/subscribe protocol commonly chose in IoT use cases.  MQTT5 is the latest
version of the protocol and is currently the only protocol version supported.  The previous
version, MQTT311, has also seen widespread adoption
and will eventually be supported in an upcoming release.

# Overview

(Overview)

(Features)

(Concepts)

# Usage

To use this crate, you'll first need to add it to your project's Cargo.toml:

```toml
[dependencies]
gneiss-mqtt = "<version>"
```

If using the tokio client and your project does not yet include [`tokio`](https://crates.io/crates/tokio), you will need to add it too:

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

async fn subscribe_to_topic(client: AsyncClientHandle) {
    let subscribe = SubscribePacket::builder()
        .with_subscription_simple("hello/world/+".to_string(), QualityOfService::AtLeastOnce)
        .build();

    let subscribe_result = client.subscribe(subscribe, None).await;
    match subscribe_result {
        Ok(suback) => {
            let rc = suback.reason_codes()[0];
            if rc.is_success() {
                println!("Subscribe success!");
            } else {
                println!("Subscribe failed with error code: {}", rc.to_string());
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

async fn unsubscribe_from_topic(client: AsyncClientHandle) {
    let unsubscribe = UnsubscribePacket::builder()
        .with_topic_filter("hello/world/+".to_string())
        .build();

    let unsubscribe_result = client.unsubscribe(unsubscribe, None).await;
    if let Ok(unsuback) = unsubscribe_result {
        if unsuback.reason_codes()[0].is_success() {
            println!("Unsubscribe success!");
            return;
        }
    }

    println!("Unsubscribe failed!");
}
```

## Example: Publish to a topic with an async client

```no_run
use gneiss_mqtt::error::GneissResult;
use gneiss_mqtt::client::{AsyncClient, AsyncClientHandle, PublishResponse, PublishResult};
use gneiss_mqtt::mqtt::{PublishPacket, QualityOfService};

async fn publish_to_topic(client: AsyncClientHandle, message: String) {
    let publish = PublishPacket::builder("hello/world/+".to_string(), QualityOfService::AtLeastOnce)
        .with_payload(message.into_bytes())
        .build();

    let publish_result = client.publish(publish, None).await;
    if let Ok(publish_response) = publish_result {
        match publish_response {
            PublishResponse::Qos1(puback) => {
                if puback.reason_code().is_success() {
                    println!("Publish success!");
                    return;
                }
            }
            _ => { panic!("Illegal publish response to a Qos1 publish!") }
        }
    }

    println!("Publish failed!");
}
```

*/

#![cfg_attr(feature = "tokio", doc = r##"
## Example: React to client events with an async client

In addition to performing MQTT operations with the client, you can also react to events emitted by the
client.  The client emits events when connectivity changes (successful connection, failed connection, disconnection,
etc...) as well as when publishes are received.

To handle client events, pass in a handler when starting the client.  See the [ClientEvent] documentation for
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

This crate's public API is expected to be very unstable until v0.5.0.  See the roadmap
in the README for more details.

# Release Summaries

### 0.4.0
* Feature set stabilization
* Threaded client implementation
* Public API largely stabilized

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
