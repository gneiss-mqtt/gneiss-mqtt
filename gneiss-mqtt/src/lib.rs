/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
This crate provides clients for communicating with a message broker using the MQTT5 protocol.

MQTT is a publish/subscribe protocol commonly used in IoT use cases.  MQTT5 is the latest
version of the protocol.  The previous version, MQTT311, has also seen widespread adoption
and will eventually be supported in an upcoming release.

# Usage

To use this crate, you'll first need to add it to your project's Cargo.toml:

```toml
[dependencies]
gneiss-mqtt = "0.2"
```

(Temporary) If your project does not include [`tokio`](https://crates.io/crates/tokio), you will need to add it too:

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
```

Future releases will support other async runtimes as well as a client that runs in a background
thread and does not need an async runtime.  For now, [`tokio`](https://crates.io/crates/tokio) is required.

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

# Example: Connect to a local Mosquitto server

Assuming a default Mosquitto installation, you can connect locally by plaintext on port 1883:

```no_run
use gneiss_mqtt::config::GenericClientBuilder;
use tokio::runtime::Handle;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // In the common case, you will not need a root CA certificate
    let client =
        GenericClientBuilder::new("127.0.0.1", 1883)
            .build(&Handle::current())?;

    // Once started, the client will recurrently maintain a connection to the endpoint until
    // stop() is invoked
    client.start(None)?;

    // <do stuff with the client>

    Ok(())
}
```

# Example: Subscribe to a topic

In order to receive messages, you must first subscribe to the topics you want to receive messages for.  Subscribing
is straightforward: configure a Subscribe packet and submit it to the client.  The subscribe will
be performed whether or not the result is waited on.  (In the async case) Await is simply how the response is
transferred back to you.

A successful subscribe call returns the Suback packet that the broker responded with.  You must check the Suback
reason code vector to verify the success/failure result for each subscription in the original subscribe.

```no_run
use gneiss_mqtt::error::MqttResult;
use gneiss_mqtt::client::{Mqtt5Client, SubscribeResult};
use gneiss_mqtt::mqtt::{QualityOfService, SubscribePacket, Subscription};

async fn subscribe_to_topic(client: Mqtt5Client) {
    let subscribe = SubscribePacket::builder()
        .with_subscription(Subscription::builder("hello/world/+".to_string(), QualityOfService::AtLeastOnce).build())
        .build();

    let subscribe_result = client.subscribe(subscribe, None).await;
    if let Ok(suback) = subscribe_result {
        if suback.reason_codes()[0].is_success() {
            println!("Subscribe success!");
            return;
        }
    }

    println!("Subscribe failed!");
}
```

# Example: Unsubscribe from a topic

TODO

# Example: Publish to a topic

TODO

# Example: React to client events
In addition to performing MQTT operations with the client, you can also react to events emitted by the
client.  The client emits events when connectivity changes (successful connection, failed connection, disconnection,
etc...) as well as when publishes are received.

To handle client events, pass in a handler when starting the client.  See the ClientEvent documentation for
more information on what data each event variant may contain.

This example shows how you can capture the client in the event handler closure, letting you perform additional
operations in reaction to client events (the client's public API is immutable).  In this case, we send a "Pong" publish
every time we receive a "Ping" publish:

```no_run
use gneiss_mqtt::client::{ClientEvent, Mqtt5Client};
use gneiss_mqtt::mqtt::{PublishPacket, QualityOfService};
use std::sync::Arc;

pub fn client_event_callback(client: Arc<Mqtt5Client>, event: Arc<ClientEvent>) {
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

use gneiss_mqtt::config::GenericClientBuilder;
use tokio::runtime::Handle;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // put the client in an Arc so we can capture an Arc clone in the event handler closure
    let client =
        Arc::new(GenericClientBuilder::new("127.0.0.1", 1883)
            .build(&Handle::current())?);

    // make a client event handler closure
    let closure_client = client.clone();
    let listener_callback = Arc::new(move |event| { client_event_callback(closure_client.clone(), event) });

    // Pass the event handler callback into start()
    client.start(Some(listener_callback))?;

    // <do stuff with the client>

    Ok(())
}

```

# Additional Notes

The intention is that this crate will eventually be as agnostic as possible of underlying
implementation details (async runtimes, TLS/transport implementations, etc...)
but at present it has hard dependencies on tokio, rustls, and some
associated helper libraries.  These will get feature-flag-gated before GA, allowing the user
to pare the implementation down to their exact connection needs.  In Rust's current state,
there is a fundamental tension between trying to be transport/runtime agnostic and trying
to provide an easy-to-use interface for getting successful clients set up for the many
different combinations expected by users.

This crate's public API is expected to be very unstable until v0.5.0.  See the roadmap
in the README for more details.
*/

#![warn(missing_docs)]

pub mod alias;
pub mod client;
pub mod config;
mod decode;
mod encode;
pub mod error;
pub mod features;
mod logging;
pub mod mqtt;
mod protocol;
#[cfg(test)]
mod testing;
mod validate;

use std::time::Instant;

fn fold_timepoint(base: &Option<Instant>, new: &Instant) -> Option<Instant> {
    if let Some(base_timepoint) = &base {
        if base_timepoint < new {
            return *base;
        }
    }

    Some(*new)
}

fn fold_optional_timepoint_min(base: &Option<Instant>, new: &Option<Instant>) -> Option<Instant> {
    if let Some(base_timepoint) = base {
        if let Some(new_timepoint) = new {
            if base_timepoint < new_timepoint {
                return *base;
            } else {
                return *new;
            }
        }

        return *base;
    }

    *new
}
