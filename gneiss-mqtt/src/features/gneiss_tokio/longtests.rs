/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use assert_matches::assert_matches;
use crate::client::ClientEvent;
use crate::client::waiter::{ClientEventType, ClientEventWaiterOptions, ClientEventWaitType};
use crate::config::{ExponentialBackoffJitterType, GenericClientBuilder};
use crate::error::MqttResult;
use crate::features::gneiss_tokio::{ClientEventRecord, ClientEventWaiter};
use crate::mqtt::{ConnackPacket, ConnectReasonCode, DisconnectPacket, DisconnectReasonCode, MqttPacket, PacketType, PublishPacket, QualityOfService};
use crate::testing::mock_server::{build_mock_client_server, ClientTestOptions};
use crate::testing::protocol::{BrokerTestContext, create_default_packet_handlers};


fn is_reconnect_related_event(event: &Arc<ClientEvent>) -> bool {
    match **event {
        ClientEvent::Stopped(_) | ClientEvent::PublishReceived(_) => {
            false
        }
        _ => {
            true
        }
    }
}

type ReconnectEventTestValidatorFn = Box<dyn Fn(&Vec<ClientEventRecord>) -> MqttResult<()> + Send + Sync>;

async fn simple_reconnect_test(builder : GenericClientBuilder, event_count: usize, event_checker: ReconnectEventTestValidatorFn) -> MqttResult<()> {
    let client = builder.build_tokio(&tokio::runtime::Handle::current()).unwrap();

    let wait_options = ClientEventWaiterOptions {
        wait_type: ClientEventWaitType::Predicate(Box::new(|event|{ is_reconnect_related_event(event) }))
    };

    let mut reconnect_waiter = ClientEventWaiter::new(client.clone(), wait_options, event_count);

    client.start(None)?;

    let reconnect_events = reconnect_waiter.wait().await?;

    client.stop(None)?;
    client.close()?;

    (event_checker)(&reconnect_events)
}

fn build_reconnect_test_options() -> ClientTestOptions {
    let mut test_options = ClientTestOptions::default();

    test_options.client_options_mutator_fn = Some(Box::new(|builder| {
        builder.with_base_reconnect_period(Duration::from_millis(250));
        builder.with_max_reconnect_period(Duration::from_millis(6000));
        builder.with_reconnect_period_jitter(ExponentialBackoffJitterType::None);
        builder.with_reconnect_stability_reset_period(Duration::from_millis(5000));
    }));

    test_options.packet_handler_set_factory_fn = Some(Box::new(|| {
        let mut handlers = create_default_packet_handlers();

        handlers.insert(PacketType::Connect, Box::new(crate::testing::protocol::handle_connect_with_failure_connack));

        handlers
    }));

    test_options
}

fn validate_reconnect_failure_sequence(events: &Vec<ClientEventRecord>) -> MqttResult<()> {
    let mut connection_failures: usize = 0;
    let mut previous_failure_time : Option<Instant> = None;
    let mut actual_delays : Vec<Duration> = Vec::new();

    for (i, event_record) in events.iter().enumerate() {
        if i % 2 == 0 {
            assert_matches!(*event_record.event, ClientEvent::ConnectionAttempt(_));
            if let Some(previous_timestamp) = &previous_failure_time {
                assert!(*previous_timestamp < event_record.timestamp);
                actual_delays.push(event_record.timestamp - *previous_timestamp);
            }
        } else {
            assert_matches!(*event_record.event, ClientEvent::ConnectionFailure(_));
            connection_failures += 1;
            if let Some(old_failure_time) = previous_failure_time {
                assert!(old_failure_time < event_record.timestamp);
            }
            previous_failure_time = Some(event_record.timestamp);
        }
    }

    assert_eq!(7, connection_failures);

    let expected_delays : Vec<Duration> = vec!(250, 500, 1000, 2000, 4000, 6000).into_iter().map(|val| {Duration::from_millis(val)}).collect();
    assert_eq!(expected_delays.len(), actual_delays.len());

    let zipped_iter = expected_delays.iter().zip(actual_delays.iter());

    for (_, (expected_delay, actual_delay)) in zipped_iter.enumerate() {
        assert!(*actual_delay >= *expected_delay);
    }

    Ok(())
}

#[test]
fn client_reconnect_with_backoff() {
    let (builder, server) = build_mock_client_server(build_reconnect_test_options());

    crate::features::gneiss_tokio::testing::do_builder_test(Box::new(|builder| {
        Box::pin(simple_reconnect_test(builder, 14, Box::new(|events|{validate_reconnect_failure_sequence(events)})))
    }), builder);

    server.close();
}

pub(crate) fn handle_connect_with_conditional_connack(packet: &Box<MqttPacket>, response_packets: &mut VecDeque<Box<MqttPacket>>, context: &mut BrokerTestContext) -> MqttResult<()> {
    if let MqttPacket::Connect(_) = &**packet {
        context.connect_count += 1;

        if context.connect_count < 6 {
            let response = Box::new(MqttPacket::Connack(ConnackPacket {
                reason_code: ConnectReasonCode::Banned,
                ..Default::default()
            }));
            response_packets.push_back(response);
        } else {
            let response = Box::new(MqttPacket::Connack(ConnackPacket {
                reason_code: ConnectReasonCode::Success,
                assigned_client_identifier: Some("client-id".to_string()),
                ..Default::default()
            }));
            response_packets.push_back(response);
        }

        return Ok(());
    }

    panic!("Invalid packet handler state")
}

pub(crate) fn handle_publish_with_disconnect(packet: &Box<MqttPacket>, response_packets: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> MqttResult<()> {
    if let MqttPacket::Publish(_) = &**packet {
        let response = Box::new(MqttPacket::Disconnect(DisconnectPacket {
            reason_code: DisconnectReasonCode::NotAuthorized,
            ..Default::default()
        }));
        response_packets.push_back(response);

        return Ok(());
    }

    panic!("Invalid packet handler state")
}

fn build_reconnect_reset_test_options() -> ClientTestOptions {
    let mut test_options = ClientTestOptions::default();

    test_options.client_options_mutator_fn = Some(Box::new(|builder| {
        builder.with_base_reconnect_period(Duration::from_millis(500));
        builder.with_max_reconnect_period(Duration::from_millis(6000));
        builder.with_reconnect_period_jitter(ExponentialBackoffJitterType::None);
        builder.with_reconnect_stability_reset_period(Duration::from_millis(3000));
    }));

    test_options.packet_handler_set_factory_fn = Some(Box::new(|| {
        let mut handlers = create_default_packet_handlers();

        handlers.insert(PacketType::Connect, Box::new(handle_connect_with_conditional_connack));
        handlers.insert(PacketType::Publish, Box::new(handle_publish_with_disconnect));

        handlers
    }));

    test_options
}

async fn reconnect_backoff_reset_test(builder : GenericClientBuilder, first_event_checker: ReconnectEventTestValidatorFn, second_event_checker_fn: ReconnectEventTestValidatorFn, connection_success_wait_millis: u64) -> MqttResult<()> {
    let client = builder.build_tokio(&tokio::runtime::Handle::current()).unwrap();

    let first_wait_options = ClientEventWaiterOptions {
        wait_type: ClientEventWaitType::Predicate(Box::new(|event|{ is_reconnect_related_event(event) }))
    };

    let mut first_reconnect_waiter = ClientEventWaiter::new(client.clone(), first_wait_options, 12);
    let mut success_waiter = ClientEventWaiter::new_single(client.clone(), ClientEventType::ConnectionSuccess);

    client.start(None)?;

    let first_reconnect_events = first_reconnect_waiter.wait().await?;
    let _ = success_waiter.wait().await?;

    let final_wait_options = ClientEventWaiterOptions {
        wait_type: ClientEventWaitType::Predicate(Box::new(|event|{ is_reconnect_related_event(event) }))
    };

    let mut final_reconnect_waiter = ClientEventWaiter::new(client.clone(), final_wait_options, 3);

    tokio::time::sleep(Duration::from_millis(connection_success_wait_millis)).await;

    let publish = PublishPacket::builder("hello/world".to_string(), QualityOfService::AtLeastOnce).build();
    let _ = client.publish(publish, None);

    let final_reconnect_events = final_reconnect_waiter.wait().await?;

    client.stop(None)?;
    client.close()?;

    (first_event_checker)(&first_reconnect_events)?;
    (second_event_checker_fn)(&final_reconnect_events)
}

fn validate_reconnect_backoff_failure_sequence(events: &Vec<ClientEventRecord>) -> MqttResult<()> {
    let mut connection_failures: usize = 0;

    for (i, event_record) in events.iter().enumerate() {
        if i % 2 == 0 {
            assert_matches!(*event_record.event, ClientEvent::ConnectionAttempt(_));
        } else if i < 11 {
            assert_matches!(*event_record.event, ClientEvent::ConnectionFailure(_));
            connection_failures += 1;
        } else {
            assert_matches!(*event_record.event, ClientEvent::ConnectionSuccess(_));
        }
    }

    assert_eq!(5, connection_failures);
    Ok(())
}

fn validate_reconnect_backoff_reset_sequence(events: &Vec<ClientEventRecord>, expected_reconnect_delay: Duration) -> MqttResult<()> {

    let record1 = &events[0];
    let record2 = &events[1];
    let record3 = &events[2];

    assert_matches!(*record1.event, ClientEvent::Disconnection(_));
    assert_matches!(*record2.event, ClientEvent::ConnectionAttempt(_));
    assert_matches!(*record3.event, ClientEvent::ConnectionSuccess(_));

    let reconnect_delay = record2.timestamp - record1.timestamp;

    assert!(reconnect_delay >= expected_reconnect_delay && reconnect_delay < 2 * expected_reconnect_delay);

    Ok(())
}

#[test]
fn client_reconnect_with_backoff_and_backoff_reset() {
    let (builder, server) = build_mock_client_server(build_reconnect_reset_test_options());

    crate::features::gneiss_tokio::testing::do_builder_test(Box::new(|builder| {
        Box::pin(reconnect_backoff_reset_test(builder,
                                              Box::new(|events|{validate_reconnect_backoff_failure_sequence(events)}),
                                              Box::new(|events|{validate_reconnect_backoff_reset_sequence(events, Duration::from_millis(500))}),
                                              4000))
    }), builder);

    server.close();
}

#[test]
fn client_reconnect_with_backoff_and_no_backoff_reset() {
    let (builder, server) = build_mock_client_server(build_reconnect_reset_test_options());

    crate::features::gneiss_tokio::testing::do_builder_test(Box::new(|builder| {
        Box::pin(reconnect_backoff_reset_test(builder,
                                              Box::new(|events|{validate_reconnect_backoff_failure_sequence(events)}),
                                              Box::new(|events|{validate_reconnect_backoff_reset_sequence(events, Duration::from_millis(6000))}),
                                              500))
    }), builder);

    server.close();
}