/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use argh::FromArgs;
use gneiss_mqtt::client::{AsyncClient, ClientEvent};
use gneiss_mqtt::client::waiter::{ClientEventType, TokioClientEventWaiter};
use gneiss_mqtt_aws::{AwsClientBuilder, WebsocketSigv4OptionsBuilder};
use std::sync::Arc;

#[derive(FromArgs, Debug, PartialEq)]
/// aws-websockets-tokio - an example connecting to AWS IoT Core using a tokio-based client and websockets
struct CommandLineArgs {

    /// signing region for websocket connections; must match the region in endpoint
    #[argh(option)]
    region: String,

    /// endpoint to connect to, specified as a domain name.
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
        _ => {}
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: CommandLineArgs = argh::from_env();

    let sigv4_options = WebsocketSigv4OptionsBuilder::new(&args.region).await.build();

    // Create the client, configuring it to use the websockets configuration specified on the command line
    let client =
        AwsClientBuilder::new_websockets_with_sigv4(&args.endpoint, sigv4_options, None)?
            .build_tokio()?;

    println!("aws-websockets-tokio - an example connecting to AWS IoT Core using a tokio-based client and websockets\n");

    // Before connecting, create a waiter object that completes when it receives a connection
    // success event
    let connection_success_waiter = TokioClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionSuccess);

    // Start the client.  Install a simple event handler function that prints out reactions
    // to a few different events.
    client.start(Some(Arc::new(|event| { client_event_handler(event) })))?;

    // We discourage the use of waiters in real applications, but in a minimal example, it keeps
    // things simple.
    connection_success_waiter.wait().await?;

    client.stop(None)?;

    Ok(())
}
