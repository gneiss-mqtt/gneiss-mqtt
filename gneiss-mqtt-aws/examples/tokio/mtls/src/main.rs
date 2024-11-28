/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use argh::FromArgs;
use gneiss_mqtt::client::asynchronous::AsyncClientHandle;
use gneiss_mqtt_aws::AwsClientBuilder;
use std::sync::{Arc, Condvar, Mutex};
use tokio::runtime::Handle;

#[derive(FromArgs, Debug, PartialEq)]
/// mtls - an example connecting to AWS IoT Core using a tokio-based client and MTLS
struct CommandLineArgs {

    /// Path to a client X509 certificate to use while connecting via mTLS.
    #[argh(option)]
    cert: String,

    /// Path to the private key associated with the client X509 certificate to use while connecting via mTLS.
    #[argh(option)]
    key: String,

    /// Path to the root CA to use when connecting.  If the endpoint URI is a TLS-enabled
    /// scheme and this is not set, then the default system trust store will be used instead.
    #[argh(option)]
    rootca: Option<String>,

    /// Endpoint to connect to, specified as a domain name.
    #[argh(positional)]
    endpoint: String,
}

/*
pub fn client_event_callback(event: Arc<ClientEvent>) {
    match &*event {
        ClientEvent::ConnectionAttempt(_) => {
            println!("Connection Attempt!\n");
        }
        ClientEvent::ConnectionFailure(event) => {
            println!("Connection Failure!");
            println!("{:?}\n", event);
        }
        ClientEvent::ConnectionSuccess(event) => {
            println!("Connection Success!");
            println!("{}", event.connack);
            println!("{}\n", event.settings);
        }
        ClientEvent::Disconnection(event) => {
            println!("Disconnection!");
            println!("{:?}\n", event);
        }
        ClientEvent::Stopped(_) => {
            println!("Stopped!\n");
        }
        ClientEvent::PublishReceived(event) => {
            println!("Publish Received!");
            println!("{}\n", &event.publish);
        }
        _ => {
            println!("Unknon client event!");
        }
    }
}

    let function = |event|{ client_event_callback(event) };
    let listener_callback = Arc::new(function);

    let _ = client.start(Some(listener_callback));
 */

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: CommandLineArgs = argh::from_env();

    let client =
        AwsClientBuilder::new_direct_with_mtls_from_fs(&args.endpoint, &args.cert, &args.key, args.rootca.as_deref())?
            .build_tokio(&Handle::current())?;

    println!("mtls - an example connecting to AWS IoT Core using a tokio-based client and MTLS\n");

    let connected_lock = Arc::new(Mutex::new(false));
    let connected_signal = Arc::new(Condvar::new());

    client.start(Some(Arc::new(|event| {
        match event {
            ClientEvent::ConnectionSuccess(_) => {
                println!("Connected successfully!");
                let mut current_connected = connected_lock.lock().unwrap();
                *current_connected = true;
                connected_signal.notify_one();
            }
            ClientEvent::ConnectionFailure(_) => {
                println!("Connection attempt was unsuccessful! =(");
            }
            _ => {}
        }
    })));

    let mut connected = connected_lock.lock().unwrap();
    while !*connected {
        connected = connected_signal.wait(connected).unwrap();
    }

    client.stop(None);

    Ok(())
}
