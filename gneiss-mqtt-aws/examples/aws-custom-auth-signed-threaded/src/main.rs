/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use argh::FromArgs;
use gneiss_mqtt::client::{SyncClient, ClientEvent};
use gneiss_mqtt::client::waiter::{ClientEventType, ThreadedClientEventWaiter};
use gneiss_mqtt_aws::{AwsClientBuilder, AwsCustomAuthOptions};
use std::sync::Arc;

#[derive(FromArgs, Debug, PartialEq)]
/// aws-custom-auth-signed-threaded - an example connecting to AWS IoT Core using a thread-based client and a signed custom authorizer
struct CommandLineArgs {

    /// name of the custom authorizer to invoke.
    #[argh(option)]
    authorizer: String,

    /// custom authorizer signature
    #[argh(option)]
    authorizer_signature: String,

    /// authorizer token key name
    #[argh(option)]
    authorizer_token_key_name: String,

    /// authorizer token key value
    #[argh(option)]
    authorizer_token_key_value: String,

    /// username override for custom auth.  This value will be passed to the authorizer's Lambda
    /// function.
    #[argh(option)]
    username: Option<String>,

    /// password override for custom auth.  This value will be passed to the authorizer's Lambda
    /// function.
    #[argh(option)]
    password: Option<String>,

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

    let mut custom_auth_options = AwsCustomAuthOptions::builder_signed(
        Some(args.authorizer.as_ref()),
        args.authorizer_signature.as_ref(),
        args.authorizer_token_key_name.as_ref(),
        args.authorizer_token_key_value.as_ref(),
    );

    if let Some(username) = &args.username {
        custom_auth_options.with_username(username.as_str());
    }

    if let Some(password) = &args.password {
        custom_auth_options.with_password(password.as_bytes());
    }

    // Create the client, configuring it to use the custom auth configuration specified on the command line
    let client =
        AwsClientBuilder::new_direct_with_custom_auth(&args.endpoint, custom_auth_options.build(), None)?
            .build_threaded()?;

    println!("aws-custom-auth-signed-threaded - an example connecting to AWS IoT Core using a thread-based client and a signed custom authorizer\n");

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
