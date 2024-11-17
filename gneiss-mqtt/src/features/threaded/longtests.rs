/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::client::config::*;
use crate::error::MqttResult;
use crate::features::threaded::*;
use crate::features::threaded::testing::*;
use crate::testing::mock_server::build_mock_client_server;
use crate::testing::protocol::*;
use crate::testing::waiter::*;
use crate::testing::waiter::synchronous::*;

fn simple_reconnect_test(builder : GenericClientBuilder, sync_options: SyncClientOptions, threaded_options: ThreadedClientOptions, event_count: usize, event_checker: ReconnectEventTestValidatorFn) -> MqttResult<()> {
    let client = builder.build_threaded(sync_options, threaded_options).unwrap();

    let wait_options = ClientEventWaiterOptions {
        wait_type: ClientEventWaitType::Predicate(Box::new(|event|{ is_reconnect_related_event(event) }))
    };

    let reconnect_waiter = SyncClientEventWaiter::new(client.clone(), wait_options, event_count);

    client.start(None)?;

    let reconnect_events = reconnect_waiter.wait()?;

    client.stop(None)?;
    client.close()?;

    (event_checker)(&reconnect_events)
}

#[test]
fn client_reconnect_with_backoff() {
    let (builder, server) = build_mock_client_server(build_reconnect_test_options());
    let sync_options = SyncClientOptionsBuilder::new().build();

    do_builder_test(Box::new(|builder, sync_options, threaded_options| {
        simple_reconnect_test(builder, sync_options, threaded_options, 14, Box::new(|events|{validate_reconnect_failure_sequence(events)}))
    }), builder, sync_options);

    server.close();
}

fn reconnect_backoff_reset_test(builder : GenericClientBuilder, sync_options: SyncClientOptions, threaded_options: ThreadedClientOptions, first_event_checker: ReconnectEventTestValidatorFn, second_event_checker_fn: ReconnectEventTestValidatorFn, connection_success_wait_millis: u64) -> MqttResult<()> {
    let client = builder.build_threaded(sync_options, threaded_options).unwrap();

    let first_wait_options = ClientEventWaiterOptions {
        wait_type: ClientEventWaitType::Predicate(Box::new(|event|{ is_reconnect_related_event(event) }))
    };

    let first_reconnect_waiter = SyncClientEventWaiter::new(client.clone(), first_wait_options, 12);
    let success_waiter = SyncClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionSuccess);

    client.start(None)?;

    let first_reconnect_events = first_reconnect_waiter.wait()?;
    let _ = success_waiter.wait()?;

    let final_wait_options = ClientEventWaiterOptions {
        wait_type: ClientEventWaitType::Predicate(Box::new(|event|{ is_reconnect_related_event(event) }))
    };

    let final_reconnect_waiter = SyncClientEventWaiter::new(client.clone(), final_wait_options, 3);

    std::thread::sleep(Duration::from_millis(connection_success_wait_millis));

    let publish = PublishPacket::builder("hello/world".to_string(), QualityOfService::AtLeastOnce).build();
    let _ = client.publish(publish, None);

    let final_reconnect_events = final_reconnect_waiter.wait()?;

    client.stop(None)?;
    client.close()?;

    (first_event_checker)(&first_reconnect_events)?;
    (second_event_checker_fn)(&final_reconnect_events)
}

#[test]
fn client_reconnect_with_backoff_and_backoff_reset() {
    let (builder, server) = build_mock_client_server(build_reconnect_reset_test_options());
    let sync_options = SyncClientOptionsBuilder::new().build();

    do_builder_test(Box::new(move |builder, sync_options, threaded_options| {
        reconnect_backoff_reset_test(builder,
                                     sync_options,
                                     threaded_options,
                                     Box::new(|events|{validate_reconnect_backoff_failure_sequence(events)}),
                                     Box::new(|events|{validate_reconnect_backoff_reset_sequence(events, Duration::from_millis(500))}),
                                     4000)
    }), builder, sync_options);

    server.close();
}

#[test]
fn client_reconnect_with_backoff_and_no_backoff_reset() {
    let (builder, server) = build_mock_client_server(build_reconnect_reset_test_options());
    let sync_options = SyncClientOptionsBuilder::new().build();

    do_builder_test(Box::new(move |builder, sync_options, threaded_options| {
        reconnect_backoff_reset_test(builder,
                                     sync_options,
                                     threaded_options,
                                     Box::new(|events|{validate_reconnect_backoff_failure_sequence(events)}),
                                     Box::new(|events|{validate_reconnect_backoff_reset_sequence(events, Duration::from_millis(6000))}),
                                     500)
    }), builder, sync_options);

    server.close();
}
