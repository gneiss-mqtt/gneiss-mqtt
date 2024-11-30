/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */


use argh::FromArgs;
use gneiss_mqtt::client::*;
use gneiss_mqtt::mqtt::*;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};

use elasti_gneiss_core::*;

fn handle_start(client: &AsyncClientHandle, _: StartArgs) {
    let function = |event|{ client_event_callback(event) };
    let listener_callback = Arc::new(function);

    let _ = client.start(Some(listener_callback));
}

fn handle_stop(client: &AsyncClientHandle, args: StopArgs) {
    let mut stop_options_builder = StopOptions::builder();

    if let Some(reason_code_u8) = args.reason_code {
        if let Ok(reason_code) = DisconnectReasonCode::try_from(reason_code_u8) {
            stop_options_builder = stop_options_builder.with_disconnect_packet(DisconnectPacket::builder().with_reason_code(reason_code).build());
        } else {
            println!("Invalid input!  reason_code must be a valid numeric Disconnect reason code");
            return;
        }
    }

    let _ = client.stop(Some(stop_options_builder.build()));
}

fn handle_close(client: &AsyncClientHandle, _ : CloseArgs) {
    let _ = client.close();
}

async fn handle_publish(client: &AsyncClientHandle, args: PublishArgs) {

    let qos_result = QualityOfService::try_from(args.qos);
    if qos_result.is_err() {
        println!("Invalid input!  Qos must be 0, 1, or 2");
        return;
    }

    let mut publish_builder = PublishPacket::builder(args.topic, qos_result.unwrap());

    if let Some(payload) = &args.payload {
        publish_builder = publish_builder.with_payload(payload.as_bytes().to_vec());
    }

    let correlation_data = vec![0; 1024 * 9];
    publish_builder = publish_builder.with_correlation_data(correlation_data);

    let publish_result = client.publish(publish_builder.build(), None).await;
    match &publish_result {
        Ok(publish_response) => {
            println!("Publish Result: Ok( {} )\n", publish_response);
        }
        Err(err) => {
            println!("Publish Result: Err( {} )\n", err);
        }
    }
}

async fn handle_subscribe(client: &AsyncClientHandle, args: SubscribeArgs) {
    let qos_result = QualityOfService::try_from(args.qos);
    if qos_result.is_err() {
        println!("Invalid input!  Qos must be 0, 1, or 2");
        return;
    }

    let subscribe = SubscribePacket::builder()
        .with_subscription(Subscription::builder(args.topic_filter, qos_result.unwrap()).build())
        .build();

    let subscribe_result = client.subscribe(subscribe, None).await;

    match &subscribe_result {
        Ok(subscribe_response) => {
            println!("Subscribe Result: Ok( {} )\n", subscribe_response);
        }
        Err(err) => {
            println!("Subscribe Result: Err( {} )\n", err);
        }
    }
}

async fn handle_unsubscribe(client: &AsyncClientHandle, args: UnsubscribeArgs) {

    let unsubscribe = UnsubscribePacket::builder().with_topic_filter(args.topic_filter).build();

    let unsubscribe_result = client.unsubscribe(unsubscribe, None).await;

    match &unsubscribe_result {
        Ok(unsubscribe_response) => {
            println!("Unsubscribe Result: Ok( {} )\n", unsubscribe_response);
        }
        Err(err) => {
            println!("Unsubscribe Result: Err( {} )\n", err);
        }
    }
}

async fn handle_input(value: String, client: &AsyncClientHandle) -> bool {
    let args : Vec<&str> = value.split_whitespace().collect();
    if args.is_empty() {
        println!("Invalid input!");
        return false;
    }

    let parsed_result = CommandArgs::from_args(&[], &args[0..]);
    if let Err(err) = parsed_result {
        println!("{}", err.output);

        return false;
    }

    match parsed_result.unwrap().nested {
        SubCommandEnum::Start(args) => { handle_start(client, args) }
        SubCommandEnum::Stop(args) => { handle_stop(client, args) }
        SubCommandEnum::Close(args) => { handle_close(client, args) }
        SubCommandEnum::Quit(_) => { return true; }
        SubCommandEnum::Publish(args) => { handle_publish(client, args).await }
        SubCommandEnum::Subscribe(args) => { handle_subscribe(client, args).await }
        SubCommandEnum::Unsubscribe(args) => { handle_unsubscribe(client, args).await }
    }

    false
}

pub async fn main_loop(client: AsyncClientHandle) {

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        if handle_input(line, &client).await {
            break;
        }
    }

    println!("Done");
}
