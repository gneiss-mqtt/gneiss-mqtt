/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use argh::FromArgs;
use gneiss_mqtt::client::{SyncClient, ClientEvent};
use gneiss_mqtt::client::waiter::{ClientEventType, ThreadedClientEventWaiter};
use gneiss_mqtt_aws::AwsClientBuilder;
use std::sync::Arc;

#[derive(FromArgs, Debug, PartialEq)]
/// aws-mtls-threaded - an example connecting to AWS IoT Core using a tokio-based client and MTLS
struct CommandLineArgs {

    /// path to a client X509 certificate to use while connecting via mTLS.
    #[argh(option)]
    cert: String,

    /// path to the private key associated with the client X509 certificate to use while connecting via mTLS.
    #[argh(option)]
    key: String,

    /// path to the root CA to use when connecting.  If the endpoint URI is a TLS-enabled
    /// scheme and this is not set, then the default system trust store will be used instead.
    #[argh(option)]
    rootca: Option<String>,

    /// endpoint to connect to, specified as a domain name.
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: CommandLineArgs = argh::from_env();

    // Create the client, configuring it to use the MTLS configuration specified on the command line
    let client =
        AwsClientBuilder::new_direct_with_mtls_from_fs(&args.endpoint, &args.cert, &args.key, args.rootca.as_deref())?
            .build_threaded()?;

    println!("aws-mtls-threaded - an example connecting to AWS IoT Core using a thread-based client and MTLS\n");

    // Before connecting, create a waiter object that completes when it receives a connection
    // success event
    let connection_success_waiter = ThreadedClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionSuccess);

    // Start the client.  Install a simple event handler function that prints out reactions
    // to a few different events.
    client.start(Some(Arc::new(|event| { client_event_handler(event) })))?;

    // We discourage the use of waiters in real applications, but in a minimal example, it keeps
    // things simple.
    connection_success_waiter.wait()?;

    client.stop(None)?;

    Ok(())
}
