/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use argh::FromArgs;
use gneiss_mqtt::client::{SyncClient, ClientEvent, ThreadedClientBuilder};
use gneiss_mqtt::client::waiter::{ClientEventType, ThreadedClientEventWaiter};
use gneiss_mqtt::error::{GneissError, GneissResult};
use gneiss_mqtt::mqtt::{PublishPacket, QualityOfService, SubscribePacket, Subscription, UnsubscribePacket};
use std::str::{from_utf8, FromStr};
use std::sync::Arc;
use std::time::Duration;

#[derive(FromArgs, Debug, PartialEq)]
/// pubsub-threaded - an example that demonstrates publish-subscribe operations with an MQTT broker over a plaintext connection
struct CommandLineArgs {

    /// endpoint to connect to in the format "host-name:port"
    #[argh(positional)]
    endpoint: String,
}

fn client_event_handler(event: Arc<ClientEvent>) {
    match &*event {
        ClientEvent::ConnectionAttempt(_) => {
            println!("Attempting to connect!");
        }
        ClientEvent::ConnectionSuccess(_) => {
            println!("Connection attempt successful!");
        }
        ClientEvent::ConnectionFailure(failure_event) => {
            println!("Connection attempt failed! noooooooooooooooo");
            println!("  Error: {}", failure_event.error);
        }
        ClientEvent::PublishReceived(publish_event) => {
            let topic = publish_event.publish.topic();
            let payload_option = publish_event.publish.payload();
            if let Some(payload) = payload_option {
                let payload_as_string_result = from_utf8(payload);
                if let Ok(payload_as_string) = payload_as_string_result {
                    println!("Publish received on topic '{}' with payload '{}'", topic, payload_as_string);
                } else {
                    println!("Publish received on topic '{}' with binary payload", topic);
                }
            } else {
                println!("Publish received on topic '{}' with empty payload", topic);
            }
        }
        _ => {}
    }
}

fn parse_endpoint(endpoint: &str) -> GneissResult<(String, u16)> {
    let parts = endpoint.split(':').collect::<Vec<_>>();

    if parts.len() != 2 {
        return Err(GneissError::new_other_error("Invalid endpoint. Endpoint must be in the format 'host:port'"));
    }

    let host = parts[0].to_string();
    let port = u16::from_str(parts[1]).map_err(|_| GneissError::new_other_error("Port must be a number between 0 and 65535"))?;

    Ok((host, port))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("pubsub-threaded - an example that demonstrates publish-subscribe operations with an MQTT broker over a plaintext connection\n");

    let args: CommandLineArgs = argh::from_env();
    let host_and_port = parse_endpoint(&args.endpoint)?;

    // Create the client
    let client = ThreadedClientBuilder::new(&host_and_port.0, host_and_port.1).build()?;

    println!("Connecting to {}:{}...\n", host_and_port.0, host_and_port.1);

    // Before connecting, create a waiter object that completes when it receives a connection
    // success event
    let connection_success_waiter = ThreadedClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionSuccess);

    // Start the client.  Install a simple event handler function that prints out reactions
    // to a few different events.
    client.start(Some(Arc::new(|event| { client_event_handler(event) })))?;

    // We discourage the use of waiters in real applications, but in a minimal example, it keeps
    // things simple.
    connection_success_waiter.wait()?;

    let topic = "gneiss/examples";

    // Demonstrate a Subscribe operation
    println!("\nSubscribing to the topic '{}'...", topic);
    let subscribe_packet = SubscribePacket::builder()
        .with_subscription(Subscription::builder(topic.to_string(), QualityOfService::AtLeastOnce).build())
        .build();
    let subscribe_result = client.subscribe(subscribe_packet, None).recv();
    match subscribe_result {
        Err(error) => {
            println!("Subscribe failed!: {}", error);
            return Err(Box::new(error));
        }
        Ok(_) => {
            println!("Subscribe succeeded!\n");
        }
    }

    // Demonstrate Publish operations
    for i in 1..11 {
        let payload = format!("Gneiss-{}", i);
        println!("Performing Qos1 publish #{} to topic '{}` with payload '{}'...", i, topic, payload);

        let publish_packet = PublishPacket::builder(topic.to_string(), QualityOfService::AtLeastOnce)
            .with_payload(payload.into_bytes())
            .build();

        let publish_result = client.publish(publish_packet, None).recv();
        match publish_result {
            Err(error) => {
                println!("Publish #{} failed!: {}", i, error);
            }
            Ok(_) => {
                println!("Publish #{} succeeded!", i);
            }
        }

        std::thread::sleep(Duration::from_secs(1));
    }

    println!("\nDone publishing!  Taking a siesta...");
    std::thread::sleep(Duration::from_secs(3));

    // Demonstrate an Unsubscribe operation
    println!("\nUnsubscribing from topic '{}'...", topic);
    let unsubscribe_packet = UnsubscribePacket::builder()
        .with_topic_filter(topic.to_string())
        .build();
    let unsubscribe_result = client.unsubscribe(unsubscribe_packet, None).recv();
    match unsubscribe_result {
        Err(error) => {
            println!("Unsubscribe failed!: {}", error);
        }
        Ok(_) => {
            println!("Unsubscribe succeeded!");
        }
    }

    let stopped_waiter = ThreadedClientEventWaiter::new_single(client.clone(), ClientEventType::Stopped);

    println!("\nStopping...");
    client.stop(None)?;

    // wait for async stop operation to complete
    stopped_waiter.wait()?;
    println!("Stopped!");

    Ok(())
}
