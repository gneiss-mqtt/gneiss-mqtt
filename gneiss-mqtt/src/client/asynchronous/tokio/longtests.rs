/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::time::{Duration};
use crate::client::config::*;
use crate::error::MqttResult;
use crate::client::asynchronous::*;
use crate::client::asynchronous::tokio::{TokioClientOptions};
use crate::client::asynchronous::tokio::testing::*;
use crate::mqtt::*;
use crate::testing::mock_server::build_mock_client_server;
use crate::testing::protocol::*;
use crate::testing::waiter::*;
use crate::testing::waiter::asynchronous::*;

async fn simple_reconnect_test(builder : GenericClientBuilder, async_options: AsyncClientOptions, tokio_options: TokioClientOptions, event_count: usize, event_checker: ReconnectEventTestValidatorFn) -> MqttResult<()> {
    let client = builder.build_tokio(async_options, tokio_options).unwrap();

    let wait_options = ClientEventWaiterOptions {
        wait_type: ClientEventWaitType::Predicate(Box::new(|event|{ is_reconnect_related_event(event) }))
    };

    let reconnect_waiter = AsyncClientEventWaiter::new(client.clone(), wait_options, event_count);

    client.start(None)?;

    let reconnect_events = reconnect_waiter.wait().await?;

    client.stop(None)?;
    client.close()?;

    (event_checker)(&reconnect_events)
}

#[test]
fn client_reconnect_with_backoff() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (builder, server) = build_mock_client_server(build_reconnect_test_options());
    let async_options = AsyncClientOptions::builder().build();

    do_builder_test(runtime.handle().clone(), Box::new(|builder, async_options, tokio_options| {
        Box::pin(simple_reconnect_test(builder, async_options, tokio_options, 14, Box::new(|events|{validate_reconnect_failure_sequence(events)})))
    }), builder, async_options);

    server.close();
}

async fn reconnect_backoff_reset_test(builder : GenericClientBuilder, async_options: AsyncClientOptions, tokio_options: TokioClientOptions, first_event_checker: ReconnectEventTestValidatorFn, second_event_checker_fn: ReconnectEventTestValidatorFn, connection_success_wait_millis: u64) -> MqttResult<()> {
    let client = builder.build_tokio(async_options, tokio_options).unwrap();

    let first_wait_options = ClientEventWaiterOptions {
        wait_type: ClientEventWaitType::Predicate(Box::new(|event|{ is_reconnect_related_event(event) }))
    };

    let first_reconnect_waiter = AsyncClientEventWaiter::new(client.clone(), first_wait_options, 12);
    let success_waiter = AsyncClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionSuccess);

    client.start(None)?;

    let first_reconnect_events = first_reconnect_waiter.wait().await?;
    let _ = success_waiter.wait().await?;

    let final_wait_options = ClientEventWaiterOptions {
        wait_type: ClientEventWaitType::Predicate(Box::new(|event|{ is_reconnect_related_event(event) }))
    };

    let final_reconnect_waiter = AsyncClientEventWaiter::new(client.clone(), final_wait_options, 3);

    ::tokio::time::sleep(Duration::from_millis(connection_success_wait_millis)).await;

    let publish = PublishPacket::builder("hello/world".to_string(), QualityOfService::AtLeastOnce).build();
    let _ = client.publish(publish, None);

    let final_reconnect_events = final_reconnect_waiter.wait().await?;

    client.stop(None)?;
    client.close()?;

    (first_event_checker)(&first_reconnect_events)?;
    (second_event_checker_fn)(&final_reconnect_events)
}

#[test]
fn client_reconnect_with_backoff_and_backoff_reset() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let handle = runtime.handle().clone();
    let (builder, server) = build_mock_client_server(build_reconnect_reset_test_options());
    let async_options = AsyncClientOptions::builder().build();

    do_builder_test(handle.clone(), Box::new(move |builder, async_options, tokio_options| {
        Box::pin(reconnect_backoff_reset_test(builder,
                                              async_options,
                                              tokio_options,
                                              Box::new(|events|{validate_reconnect_backoff_failure_sequence(events)}),
                                              Box::new(|events|{validate_reconnect_backoff_reset_sequence(events, Duration::from_millis(500))}),
                                              4000))
    }), builder, async_options);

    server.close();
}

#[test]
fn client_reconnect_with_backoff_and_no_backoff_reset() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let handle = runtime.handle().clone();
    let (builder, server) = build_mock_client_server(build_reconnect_reset_test_options());
    let async_options = AsyncClientOptions::builder().build();

    do_builder_test(handle.clone(), Box::new(move |builder, async_options, tokio_options| {
        Box::pin(reconnect_backoff_reset_test(builder,
                                              async_options,
                                              tokio_options,
                                              Box::new(|events|{validate_reconnect_backoff_failure_sequence(events)}),
                                              Box::new(|events|{validate_reconnect_backoff_reset_sequence(events, Duration::from_millis(6000))}),
                                              500))
    }), builder, async_options);

    server.close();
}