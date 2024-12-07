/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use argh::FromArgs;
use gneiss_mqtt::client::{AsyncClient, ClientEvent, TokioClientBuilder};
use gneiss_mqtt::client::waiter::{ClientEventType, TokioClientEventWaiter};
use gneiss_mqtt::error::{GneissError, GneissResult};
use std::str::FromStr;
use std::sync::Arc;


#[derive(FromArgs, Debug, PartialEq)]
/// connect-plaintext-tokio - an example connecting to an MQTT broker over TCP using a tokio-based client
struct CommandLineArgs {

    /// endpoint to connect to in the format "host-name:port"
    #[argh(positional)]
    endpoint: String,
}

fn client_event_handler(event: Arc<ClientEvent>) {
    match *event {
        ClientEvent::ConnectionAttempt(_) => {
            println!("Attempting to connect!");
        }
        ClientEvent::ConnectionSuccess(_) => {
            println!("Connection attempt successful!");
        }
        ClientEvent::ConnectionFailure(_) => {
            println!("Connection attempt failed! noooooooooooooooo");
        }
        _ => {}
    }
}

fn parse_endpoint(endpoint: &str) -> GneissResult<(String, u16)> {
    let parts = endpoint.split(':').collect::<Vec<_>>();

    if parts.len() != 2 {
        return Err(GneissError::new_other_error("Invalid endpoint endpoint must be in the format 'host:port'"));
    }

    let host = parts[0].to_string();
    let port = u16::from_str(parts[1]).map_err(|_| GneissError::new_other_error("Port must be a number between 0 and 65535"))?;

    Ok((host, port))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("connect-plaintext-tokio - an example connecting to an MQTT broker over TCP using a tokio-based client\n");

    let args: CommandLineArgs = argh::from_env();
    let host_and_port = parse_endpoint(&args.endpoint)?;
    println!("Connecting to {}:{}...\n", host_and_port.0, host_and_port.1);

    // Create the client
    let client = TokioClientBuilder::new(&host_and_port.0, host_and_port.1).build()?;

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
