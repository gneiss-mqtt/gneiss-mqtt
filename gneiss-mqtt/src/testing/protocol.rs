/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use crate::protocol::*;
use assert_matches::assert_matches;
use crate::alias::{OutboundAliasResolution, OutboundAliasResolverFactory};
use crate::client::*;
use crate::client::waiter::ClientEventRecord;
use crate::client::config::*;
use crate::decode::{Decoder, DecodingContext};
use crate::encode::{Encoder, EncodeResult, EncodingContext};
use crate::encode::MAXIMUM_VARIABLE_LENGTH_INTEGER;
use crate::error::{GneissError, GneissResult};
use crate::mqtt::*;
use crate::mqtt::utils::mqtt_packet_to_packet_type;
use crate::testing::mock_server::ClientTestOptions;
use crate::validate::testing::verify_validation_failure;

const CONNACK_TIMEOUT_MILLIS: u64 = 10000;

fn build_standard_test_config() -> ProtocolStateConfig {
    ProtocolStateConfig {
        connect_options : ConnectOptions::builder().with_client_id("DefaultTesting").with_keep_alive_interval_seconds(None).build(),
        base_timestamp: Instant::now(),
        offline_queue_policy: OfflineQueuePolicy::PreserveAll,
        ping_timeout: Duration::from_millis(30000),
        outbound_alias_resolver: None,
    }
}

#[derive(Default)]
pub(crate) struct BrokerTestContext {
    pub(crate) connect_count: usize,
}

pub(crate) type PacketHandler = Box<dyn Fn(&MqttPacket, &mut VecDeque<Box<MqttPacket>>, &mut BrokerTestContext) -> GneissResult<()> + Send + Sync + 'static>;
pub(crate) type PacketHandlerSet = HashMap<PacketType, PacketHandler>;
pub(crate) type PacketHandlerSetFactory = Box<dyn Fn() -> PacketHandlerSet + Send + Sync>;

fn handle_connect_with_successful_connack(packet: &MqttPacket, response_packets: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> GneissResult<()> {
    if let MqttPacket::Connect(connect) = packet {
        let mut assigned_client_identifier = None;
        if connect.client_id.is_none() {
            assigned_client_identifier = Some("auto-assigned-client-id".to_string());
        }

        let response = Box::new(MqttPacket::Connack(ConnackPacket {
            assigned_client_identifier,
            ..Default::default()
        }));
        response_packets.push_back(response);

        return Ok(());
    }

    panic!("Invalid packet handler state")
}

fn handle_connect_with_session_resumption(packet: &MqttPacket, response_packets: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> GneissResult<()> {
    if let MqttPacket::Connect(connect) = packet {
        let mut assigned_client_identifier = None;
        if connect.client_id.is_none() {
            assigned_client_identifier = Some("auto-assigned-client-id".to_string());
        }

        let response = Box::new(MqttPacket::Connack(ConnackPacket {
            assigned_client_identifier,
            session_present : !connect.clean_start,
            ..Default::default()
        }));
        response_packets.push_back(response);

        return Ok(());
    }

    panic!("Invalid packet handler state")
}

fn handle_connect_with_low_receive_maximum(packet: &MqttPacket, response_packets: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> GneissResult<()> {
    if let MqttPacket::Connect(connect) = packet {
        let mut assigned_client_identifier = None;
        if connect.client_id.is_none() {
            assigned_client_identifier = Some("auto-assigned-client-id".to_string());
        }

        let response = Box::new(MqttPacket::Connack(ConnackPacket {
            assigned_client_identifier,
            receive_maximum: Some(1),
            ..Default::default()
        }));
        response_packets.push_back(response);

        return Ok(());
    }

    panic!("Invalid packet handler state")
}

fn handle_connect_with_topic_aliasing(packet: &MqttPacket, response_packets: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> GneissResult<()> {
    if let MqttPacket::Connect(connect) = packet {
        let mut assigned_client_identifier = None;
        if connect.client_id.is_none() {
            assigned_client_identifier = Some("auto-assigned-client-id".to_string());
        }

        let response = Box::new(MqttPacket::Connack(ConnackPacket {
            assigned_client_identifier,
            topic_alias_maximum: Some(2),
            ..Default::default()
        }));
        response_packets.push_back(response);

        return Ok(());
    }

    panic!("Invalid packet handler state")
}

fn create_connack_rejection() -> ConnackPacket {
    ConnackPacket {
        reason_code : ConnectReasonCode::Banned,
        ..Default::default()
    }
}

pub(crate) fn handle_connect_with_failure_connack(packet: &MqttPacket, response_packets: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> GneissResult<()> {
    if let MqttPacket::Connect(_) = packet {
        let response = Box::new(MqttPacket::Connack(create_connack_rejection()));
        response_packets.push_back(response);

        return Ok(());
    }

    panic!("Invalid packet handler state")
}

fn handle_connect_with_tiny_maximum_packet_size(packet: &MqttPacket, response_packets: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> GneissResult<()> {
    if let MqttPacket::Connect(_) = packet {
        let response = Box::new(MqttPacket::Connack(ConnackPacket {
            maximum_packet_size: Some(10),
            ..Default::default()
        }));
        response_packets.push_back(response);

        return Ok(());
    }

    panic!("Invalid packet handler state")
}

fn handle_pingreq_with_pingresp(_: &MqttPacket, response_packets: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> GneissResult<()> {
    let response = Box::new(MqttPacket::Pingresp(PingrespPacket{}));
    response_packets.push_back(response);

    Ok(())
}

fn handle_publish_with_success_no_relay(packet: &MqttPacket, response_packets: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> GneissResult<()> {
    if let MqttPacket::Publish(publish) = packet {
        match publish.qos {
            QualityOfService::AtMostOnce => {}
            QualityOfService::AtLeastOnce => {
                let response = Box::new(MqttPacket::Puback(PubackPacket{
                    packet_id : publish.packet_id,
                    ..Default::default()
                }));
                response_packets.push_back(response);
            }
            QualityOfService::ExactlyOnce => {
                let response = Box::new(MqttPacket::Pubrec(PubrecPacket{
                    packet_id : publish.packet_id,
                    ..Default::default()
                }));
                response_packets.push_back(response);
            }
        }

        return Ok(());
    }

    panic!("Invalid packet handler state")
}

fn handle_publish_with_failure(packet: &MqttPacket, response_packets: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> GneissResult<()> {
    if let MqttPacket::Publish(publish) = packet {
        match publish.qos {
            QualityOfService::AtMostOnce => {}
            QualityOfService::AtLeastOnce => {
                let response = Box::new(MqttPacket::Puback(PubackPacket{
                    packet_id : publish.packet_id,
                    reason_code : PubackReasonCode::QuotaExceeded,
                    ..Default::default()
                }));
                response_packets.push_back(response);
            }
            QualityOfService::ExactlyOnce => {
                let response = Box::new(MqttPacket::Pubrec(PubrecPacket{
                    packet_id : publish.packet_id,
                    reason_code : PubrecReasonCode::QuotaExceeded,
                    ..Default::default()
                }));
                response_packets.push_back(response);
            }
        }

        return Ok(());
    }

    panic!("Invalid packet handler state")
}

fn handle_pubrec_with_success(packet: &MqttPacket, response_packets: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> GneissResult<()> {
    if let MqttPacket::Pubrec(pubrec) = packet {
        let response = Box::new(MqttPacket::Pubrel(PubrelPacket{
            packet_id : pubrec.packet_id,
            ..Default::default()
        }));
        response_packets.push_back(response);

        return Ok(());
    }

    panic!("Invalid packet handler state")
}

fn handle_pubrel_with_success(packet: &MqttPacket, response_packets: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> GneissResult<()> {
    if let MqttPacket::Pubrel(pubrel) = packet {
        let response = Box::new(MqttPacket::Pubcomp(PubcompPacket{
            packet_id : pubrel.packet_id,
            ..Default::default()
        }));
        response_packets.push_back(response);

        return Ok(());
    }

    panic!("Invalid packet handler state")
}

fn handle_pubrel_with_failure(packet: &MqttPacket, response_packets: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> GneissResult<()> {
    if let MqttPacket::Pubrel(pubrel) = packet {
        let response = Box::new(MqttPacket::Pubcomp(PubcompPacket{
            packet_id : pubrel.packet_id,
            reason_code : PubcompReasonCode::PacketIdentifierNotFound,
            ..Default::default()
        }));
        response_packets.push_back(response);

        return Ok(());
    }

    panic!("Invalid packet handler state")
}

fn handle_subscribe_with_success(packet: &MqttPacket, response_packets: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> GneissResult<()> {
    if let MqttPacket::Subscribe(subscribe) = packet {
        let mut reason_codes = Vec::new();
        for subscription in &subscribe.subscriptions {
            match subscription.qos {
                QualityOfService::AtMostOnce => { reason_codes.push(SubackReasonCode::GrantedQos0); }
                QualityOfService::AtLeastOnce => { reason_codes.push(SubackReasonCode::GrantedQos1); }
                QualityOfService::ExactlyOnce => { reason_codes.push(SubackReasonCode::GrantedQos2); }
            }
        }

        let response = Box::new(MqttPacket::Suback(SubackPacket{
            packet_id : subscribe.packet_id,
            reason_codes,
            ..Default::default()
        }));
        response_packets.push_back(response);

        return Ok(());
    }

    panic!("Invalid packet handler state")
}

fn handle_subscribe_with_failure(packet: &MqttPacket, response_packets: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> GneissResult<()> {
    if let MqttPacket::Subscribe(subscribe) = packet {
        let mut reason_codes = Vec::new();
        for _ in &subscribe.subscriptions {
            reason_codes.push(SubackReasonCode::NotAuthorized);
        }

        let response = Box::new(MqttPacket::Suback(SubackPacket{
            packet_id : subscribe.packet_id,
            reason_codes,
            ..Default::default()
        }));
        response_packets.push_back(response);

        return Ok(());
    }

    panic!("Invalid packet handler state")
}

fn handle_unsubscribe_with_failure(packet: &MqttPacket, response_packets: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> GneissResult<()> {
    if let MqttPacket::Unsubscribe(unsubscribe) = packet {
        let mut reason_codes = Vec::new();
        for _ in &unsubscribe.topic_filters {
            reason_codes.push(UnsubackReasonCode::ImplementationSpecificError);
        }

        let response = Box::new(MqttPacket::Unsuback(UnsubackPacket{
            packet_id : unsubscribe.packet_id,
            reason_codes,
            ..Default::default()
        }));
        response_packets.push_back(response);

        return Ok(());
    }

    panic!("Invalid packet handler state")
}

fn handle_unsubscribe_with_success(packet: &MqttPacket, response_packets: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> GneissResult<()> {
    if let MqttPacket::Unsubscribe(unsubscribe) = packet {
        let mut reason_codes = Vec::new();
        for _ in &unsubscribe.topic_filters {
            reason_codes.push(UnsubackReasonCode::Success);
        }

        let response = Box::new(MqttPacket::Unsuback(UnsubackPacket{
            packet_id : unsubscribe.packet_id,
            reason_codes,
            ..Default::default()
        }));
        response_packets.push_back(response);

        return Ok(());
    }

    panic!("Invalid packet handler state")
}

fn handle_with_panic(_: &MqttPacket, _: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> GneissResult<()> {
    panic!("Invalid packet handler state")
}

fn handle_with_nothing(_: &MqttPacket, _: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> GneissResult<()> {
    Ok(())
}

pub(crate) fn create_default_packet_handlers() -> PacketHandlerSet {
    let mut handlers : HashMap<PacketType, PacketHandler> = HashMap::new();

    handlers.insert(PacketType::Connect, Box::new(handle_connect_with_successful_connack));
    handlers.insert(PacketType::Pingreq, Box::new(handle_pingreq_with_pingresp));
    handlers.insert(PacketType::Publish, Box::new(handle_publish_with_success_no_relay));
    handlers.insert(PacketType::Pubrec, Box::new(handle_pubrec_with_success));
    handlers.insert(PacketType::Pubrel, Box::new(handle_pubrel_with_success));
    handlers.insert(PacketType::Subscribe, Box::new(handle_subscribe_with_success));
    handlers.insert(PacketType::Unsubscribe, Box::new(handle_unsubscribe_with_success));

    handlers.insert(PacketType::Disconnect, Box::new(handle_with_nothing));
    handlers.insert(PacketType::Auth, Box::new(handle_with_nothing));
    handlers.insert(PacketType::Puback, Box::new(handle_with_nothing));
    handlers.insert(PacketType::Pubcomp, Box::new(handle_with_nothing));

    handlers.insert(PacketType::Connack, Box::new(handle_with_panic));
    handlers.insert(PacketType::Suback, Box::new(handle_with_panic));
    handlers.insert(PacketType::Unsuback, Box::new(handle_with_panic));
    handlers.insert(PacketType::Pingresp, Box::new(handle_with_panic));

    handlers
}

struct ProtocolStateTestFixture {
    base_timestamp: Instant,

    broker_decoder: Decoder,
    broker_encoder: Encoder,

    pub client_state: ProtocolState,

    pub client_packet_events: VecDeque<PacketEvent>,

    pub to_broker_packet_stream: VecDeque<Box<MqttPacket>>,
    pub to_client_packet_stream: VecDeque<Box<MqttPacket>>,

    pub broker_packet_handlers: HashMap<PacketType, PacketHandler>,

    test_context: BrokerTestContext
}

impl ProtocolStateTestFixture {


    pub(crate) fn new(config : ProtocolStateConfig) -> Self {
        Self {
            base_timestamp : config.base_timestamp,
            broker_decoder: Decoder::new(),
            broker_encoder: Encoder::new(),
            client_state: ProtocolState::new(config),
            client_packet_events : VecDeque::new(),
            to_broker_packet_stream : VecDeque::new(),
            to_client_packet_stream : VecDeque::new(),
            broker_packet_handlers : create_default_packet_handlers(),
            test_context: BrokerTestContext::default()
        }
    }

    fn handle_to_broker_packet(&mut self, packet: &MqttPacket, response_bytes: &mut Vec<u8>) -> GneissResult<()> {
        let mut response_packets = VecDeque::new();
        let packet_type = mqtt_packet_to_packet_type(packet);

        if let Some(handler) = self.broker_packet_handlers.get(&packet_type) {
            (*handler)(packet, &mut response_packets, &mut self.test_context)?;

            let mut encode_buffer = Vec::with_capacity(4096);

            for response_packet in &response_packets {
                let encoding_context = EncodingContext {
                    outbound_alias_resolution: OutboundAliasResolution {
                        skip_topic: false,
                        alias: None,
                    }
                };

                self.broker_encoder.reset(response_packet, &encoding_context)?;

                let mut encode_result = EncodeResult::Full;
                while encode_result == EncodeResult::Full {
                    encode_result = self.broker_encoder.encode(response_packet, &mut encode_buffer)?;
                    response_bytes.append(&mut encode_buffer);
                    encode_buffer.clear(); // redundant probably
                }
            }

            self.to_client_packet_stream.append(&mut response_packets);
        }

        Ok(())
    }

    pub(crate) fn service_once(&mut self, elapsed_millis: u64, socket_buffer_size: usize) -> GneissResult<Vec<u8>> {
        let current_time = self.base_timestamp + Duration::from_millis(elapsed_millis);

        let mut to_socket = Vec::with_capacity(socket_buffer_size);

        let mut service_context = ServiceContext {
            to_socket: &mut to_socket,
            current_time,
        };

        self.client_state.service(&mut service_context)?;

        Ok(to_socket)
    }

    pub(crate) fn write_to_socket(&mut self, bytes: &[u8]) -> GneissResult<Vec<u8>> {
        let mut response_bytes = Vec::new();
        let mut broker_packets = VecDeque::new();

        let mut maximum_packet_size_to_server : u32 = MAXIMUM_VARIABLE_LENGTH_INTEGER as u32;
        if let Some(settings) = self.client_state.get_negotiated_settings() {
            maximum_packet_size_to_server = settings.maximum_packet_size_to_server;
        }

        let mut decode_context = DecodingContext {
            maximum_packet_size : maximum_packet_size_to_server,
            decoded_packets: &mut broker_packets,
        };

        self.broker_decoder.decode_bytes(bytes, &mut decode_context)?;

        for packet in &broker_packets {
            self.handle_to_broker_packet(packet, &mut response_bytes)?;
        }

        self.to_broker_packet_stream.append(&mut broker_packets);

        Ok(response_bytes)
    }

    pub(crate) fn reset(&mut self, elapsed_millis: u64) {
        let current_time = self.base_timestamp + Duration::from_millis(elapsed_millis);
        self.client_state.reset(&current_time);
    }

    pub(crate) fn service_with_drain(&mut self, elapsed_millis: u64, socket_buffer_size: usize) -> GneissResult<Vec<u8>> {
        let current_time = self.base_timestamp + Duration::from_millis(elapsed_millis);
        let mut done = false;
        let mut response_bytes = Vec::new();

        while !done {
            let mut to_socket = Vec::with_capacity(socket_buffer_size);
            let mut broker_packets = VecDeque::new();

            let mut service_context = ServiceContext {
                to_socket: &mut to_socket,
                current_time,
            };

            self.client_state.service(&mut service_context)?;
            if !to_socket.is_empty() {
                let mut network_event = NetworkEventContext {
                    event: NetworkEvent::WriteCompletion,
                    current_time,
                    packet_events: &mut self.client_packet_events,
                };

                let completion_result = self.client_state.handle_network_event(&mut network_event);
                let mut maximum_packet_size_to_server : u32 = MAXIMUM_VARIABLE_LENGTH_INTEGER as u32;
                if let Some(settings) = self.client_state.get_negotiated_settings() {
                    maximum_packet_size_to_server = settings.maximum_packet_size_to_server;
                }

                let mut decode_context = DecodingContext {
                    maximum_packet_size : maximum_packet_size_to_server,
                    decoded_packets: &mut broker_packets,
                };

                if self.broker_decoder.decode_bytes(to_socket.as_slice(), &mut decode_context).is_err() {
                    panic!("Test triggered broker decode failure");
                }

                for packet in &broker_packets {
                    if self.handle_to_broker_packet(packet, &mut response_bytes).is_err() {
                        panic!("Test triggered broker packet handling failure");
                    }
                }

                self.to_broker_packet_stream.append(&mut broker_packets);

                completion_result?
            } else {
                done = true;
            }
        }

        Ok(response_bytes)
    }

    pub(crate) fn service_round_trip(&mut self, service_time: u64, response_time: u64, socket_buffer_size: usize) -> GneissResult<()> {
        let server_bytes = self.service_with_drain(service_time, socket_buffer_size)?;

        self.on_incoming_bytes(response_time, server_bytes.as_slice())?;

        Ok(())
    }

    pub(crate) fn on_connection_opened(&mut self, elapsed_millis: u64) -> GneissResult<()> {
        let now = self.base_timestamp + Duration::from_millis(elapsed_millis);
        let establishment_timeout = now + Duration::from_millis(CONNACK_TIMEOUT_MILLIS);

        let mut context = NetworkEventContext {
            current_time : self.base_timestamp + Duration::from_millis(elapsed_millis),
            event: NetworkEvent::ConnectionOpened(ConnectionOpenedContext{
                establishment_timeout
            }),
            packet_events: &mut self.client_packet_events,
        };

        self.client_state.handle_network_event(&mut context)
    }

    pub(crate) fn on_write_completion(&mut self, elapsed_millis: u64) -> GneissResult<()> {
        let mut context = NetworkEventContext {
            current_time : self.base_timestamp + Duration::from_millis(elapsed_millis),
            event: NetworkEvent::WriteCompletion,
            packet_events: &mut self.client_packet_events,
        };

        self.client_state.handle_network_event(&mut context)
    }

    pub(crate) fn on_connection_closed(&mut self, elapsed_millis: u64) -> GneissResult<()> {
        self.broker_decoder.reset_for_new_connection();

        let mut context = NetworkEventContext {
            current_time : self.base_timestamp + Duration::from_millis(elapsed_millis),
            event: NetworkEvent::ConnectionClosed,
            packet_events: &mut self.client_packet_events,
        };

        self.client_state.handle_network_event(&mut context)
    }

    pub(crate) fn on_incoming_bytes(&mut self, elapsed_millis: u64, bytes: &[u8]) -> GneissResult<()> {
        let mut context = NetworkEventContext {
            current_time : self.base_timestamp + Duration::from_millis(elapsed_millis),
            event: NetworkEvent::IncomingData(bytes),
            packet_events: &mut self.client_packet_events,
        };

        self.client_state.handle_network_event(&mut context)
    }

    pub(crate) fn get_next_service_time(&mut self, elapsed_millis: u64) -> Option<u64> {
        let current_time = self.base_timestamp + Duration::from_millis(elapsed_millis);
        let next_service_timepoint = self.client_state.get_next_service_timepoint(&current_time);

        if let Some(service_timepoint) = &next_service_timepoint {
            let next_service_millis = (*service_timepoint - self.base_timestamp).as_millis();
            return Some(next_service_millis as u64);
        }

        None
    }

    pub(crate) fn subscribe(&mut self, elapsed_millis: u64, subscribe: SubscribePacket, options: SubscribeOptions) -> GneissResult<std::sync::mpsc::Receiver<SubscribeResult>> {
        let (sender, receiver) = std::sync::mpsc::channel();
        let packet = Box::new(MqttPacket::Subscribe(subscribe));
        let handler: ResponseHandler<SubscribeResult> = Box::new(move |res| {
            sender.send(res)?;
            Ok(())
        });
        let subscribe_options = SubscribeOptionsInternal {
            options,
            response_handler : Some(handler)
        };

        let subscribe_event = UserEvent::Subscribe(packet, subscribe_options);

        self.client_state.handle_user_event(UserEventContext {
            event: subscribe_event,
            current_time: self.base_timestamp + Duration::from_millis(elapsed_millis)
        });

        Ok(receiver)
    }

    pub(crate) fn unsubscribe(&mut self, elapsed_millis: u64, unsubscribe: UnsubscribePacket, options: UnsubscribeOptions) -> GneissResult<std::sync::mpsc::Receiver<UnsubscribeResult>> {
        let (sender, receiver) = std::sync::mpsc::channel();
        let packet = Box::new(MqttPacket::Unsubscribe(unsubscribe));
        let handler : ResponseHandler<UnsubscribeResult> = Box::new(move |res| {
            sender.send(res)?;
            Ok(())
        });
        let unsubscribe_options = UnsubscribeOptionsInternal {
            options,
            response_handler : Some(handler)
        };

        let unsubscribe_event = UserEvent::Unsubscribe(packet, unsubscribe_options);

        self.client_state.handle_user_event(UserEventContext {
            event: unsubscribe_event,
            current_time: self.base_timestamp + Duration::from_millis(elapsed_millis)
        });

        Ok(receiver)
    }

    pub(crate) fn publish(&mut self, elapsed_millis: u64, publish: PublishPacket, options: PublishOptions) -> GneissResult<std::sync::mpsc::Receiver<PublishResult>> {
        let (sender, receiver) = std::sync::mpsc::channel();
        let packet = Box::new(MqttPacket::Publish(publish));
        let handler : ResponseHandler<PublishResult> = Box::new(move |res| {
            sender.send(res)?;
            Ok(())
        });
        let publish_options = PublishOptionsInternal {
            options,
            response_handler : Some(handler)
        };

        let publish_event = UserEvent::Publish(packet, publish_options);

        self.client_state.handle_user_event(UserEventContext {
            event: publish_event,
            current_time: self.base_timestamp + Duration::from_millis(elapsed_millis)
        });

        Ok(receiver)
    }

    pub(crate) fn disconnect(&mut self, elapsed_millis: u64, disconnect: DisconnectPacket) -> GneissResult<()> {
        let packet = Box::new(MqttPacket::Disconnect(disconnect));
        let disconnect_event = UserEvent::Disconnect(packet);

        self.client_state.handle_user_event(UserEventContext {
            event: disconnect_event,
            current_time: self.base_timestamp + Duration::from_millis(elapsed_millis)
        });

        Ok(())
    }

    pub(crate) fn advance_disconnected_to_state(&mut self, state: ProtocolStateType, elapsed_millis: u64) -> GneissResult<()> {
        assert_eq!(ProtocolStateType::Disconnected, self.client_state.state);

        let result = match state {
            ProtocolStateType::PendingConnack => {
                self.on_connection_opened(elapsed_millis)
            }
            ProtocolStateType::Connected => {
                self.on_connection_opened(elapsed_millis)?;
                let server_bytes = self.service_with_drain(elapsed_millis, 4096)?;
                self.on_incoming_bytes(elapsed_millis, server_bytes.as_slice())
            }
            ProtocolStateType::PendingDisconnect => {
                panic!("Not supported");
            }
            ProtocolStateType::Halted => {
                self.on_connection_opened(elapsed_millis)?;
                self.on_connection_opened(elapsed_millis).unwrap_or(());
                Ok(())
            }
            ProtocolStateType::Disconnected => { Ok(()) }
        };

        assert_eq!(state, self.client_state.state);

        result
    }
}

fn find_nth_packet_of_type<'a, T>(packet_sequence : T, packet_type : PacketType, count: usize, start_position : Option<usize>, end_position : Option<usize>) -> Option<(usize, &'a MqttPacket)> where T : Iterator<Item = &'a Box<MqttPacket>> {
    let start = start_position.unwrap_or(0);
    let mut index = start;
    let mut seen = 0;

    for packet in packet_sequence.skip(start) {
        if mqtt_packet_to_packet_type(packet) == packet_type {
            seen += 1;
            if seen == count {
                return Some((index, packet));
            }
        }

        index += 1;
        if let Some(end) = end_position {
            if index >= end {
                return None;
            }
        }
    }

    None
}

fn verify_packet_type_sequence<'a, T, U>(packet_sequence : T, expected_sequence : U, start_position : Option<usize>) where T : Iterator<Item = &'a Box<MqttPacket>>, U : Iterator<Item = PacketType> {
    let start = start_position.unwrap_or(0);
    let type_sequence = packet_sequence.skip(start).map(|packet|{ mqtt_packet_to_packet_type(packet) });

    assert!(expected_sequence.eq(type_sequence));
}

fn verify_protocol_state_empty(fixture: &ProtocolStateTestFixture) {
    assert_eq!(0, fixture.client_state.operations.len());
    assert_eq!(0, fixture.client_state.user_operation_queue.len());
    assert_eq!(0, fixture.client_state.resubmit_operation_queue.len());
    assert_eq!(0, fixture.client_state.high_priority_operation_queue.len());
    assert_eq!(0, fixture.client_state.operation_ack_timeouts.len());
    assert_eq!(0, fixture.client_state.allocated_packet_ids.len());
    assert_eq!(0, fixture.client_state.pending_publish_operations.len());
    assert_eq!(0, fixture.client_state.pending_non_publish_operations.len());
    assert_eq!(0, fixture.client_state.pending_write_completion_operations.len());
}

pub(crate) fn is_reconnect_related_event(event: &Arc<ClientEvent>) -> bool {
    !matches!(**event, ClientEvent::Stopped(_) | ClientEvent::PublishReceived(_))
}

#[allow(clippy::field_reassign_with_default)]
pub(crate) fn build_reconnect_test_options() -> ClientTestOptions {
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

pub(crate) fn validate_reconnect_failure_sequence(events: &[ClientEventRecord]) -> GneissResult<()> {
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

    for (expected_delay, actual_delay) in zipped_iter {
        assert!(*actual_delay >= *expected_delay);
    }

    Ok(())
}

pub(crate) type ReconnectEventTestValidatorFn = Box<dyn Fn(&Vec<ClientEventRecord>) -> GneissResult<()> + Send + Sync>;

pub(crate) fn handle_connect_with_conditional_connack(packet: &MqttPacket, response_packets: &mut VecDeque<Box<MqttPacket>>, context: &mut BrokerTestContext) -> GneissResult<()> {
    if let MqttPacket::Connect(_) = packet {
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

pub(crate) fn handle_publish_with_disconnect(packet: &MqttPacket, response_packets: &mut VecDeque<Box<MqttPacket>>, _: &mut BrokerTestContext) -> GneissResult<()> {
    if let MqttPacket::Publish(_) = packet {
        let response = Box::new(MqttPacket::Disconnect(DisconnectPacket {
            reason_code: DisconnectReasonCode::NotAuthorized,
            ..Default::default()
        }));
        response_packets.push_back(response);

        return Ok(());
    }

    panic!("Invalid packet handler state")
}

#[allow(clippy::field_reassign_with_default)]
pub(crate) fn build_reconnect_reset_test_options() -> ClientTestOptions {
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

pub(crate) fn validate_reconnect_backoff_failure_sequence(events: &[ClientEventRecord]) -> GneissResult<()> {
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

pub(crate) fn validate_reconnect_backoff_reset_sequence(events: &[ClientEventRecord], expected_reconnect_delay: Duration) -> GneissResult<()> {

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
fn disconnected_state_network_event_handler_fails() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert_eq!(ProtocolStateType::Disconnected, fixture.client_state.state);

    assert_matches!(fixture.on_connection_closed(0).err().unwrap(), GneissError::InternalStateError(_));
    assert!(fixture.client_packet_events.is_empty());

    assert_matches!(fixture.on_write_completion(0).err().unwrap(), GneissError::InternalStateError(_));
    assert!(fixture.client_packet_events.is_empty());

    let bytes : Vec<u8> = vec!(0, 1, 2, 3, 4, 5);
    assert_matches!(fixture.on_incoming_bytes(0, bytes.as_slice()).err().unwrap(), GneissError::InternalStateError(_));
    assert!(fixture.client_packet_events.is_empty());
}

#[test]
fn disconnected_state_next_service_time_never() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert_eq!(ProtocolStateType::Disconnected, fixture.client_state.state);

    assert_eq!(None, fixture.get_next_service_time(0));
}

fn verify_service_does_nothing(fixture : &mut ProtocolStateTestFixture) {
    let client_packet_events_length = fixture.client_packet_events.len();
    let to_broker_packet_stream_length = fixture.to_broker_packet_stream.len();
    let to_client_packet_stream_length = fixture.to_client_packet_stream.len();

    let publish_receiver = fixture.publish(0,
                                           PublishPacket {
                                               topic: "derp".to_string(),
                                               qos: QualityOfService::AtLeastOnce,
                                               ..Default::default()
                                           },
                                           PublishOptions::builder().build());
    assert!(publish_receiver.is_ok());
    assert!(!fixture.client_state.operations.is_empty());
    assert!(!fixture.client_state.user_operation_queue.is_empty());

    if let Ok(bytes) = fixture.service_with_drain(0, 4096) {
        assert_eq!(0, bytes.len());
    }

    assert_eq!(client_packet_events_length, fixture.client_packet_events.len());
    assert_eq!(to_broker_packet_stream_length, fixture.to_broker_packet_stream.len());
    assert_eq!(to_client_packet_stream_length, fixture.to_client_packet_stream.len());
}

#[test]
fn disconnected_state_service_does_nothing() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert_eq!(ProtocolStateType::Disconnected, fixture.client_state.state);

    verify_service_does_nothing(&mut fixture);
}

#[test]
fn halted_state_network_event_handler_fails() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Halted, 0).is_ok());

    assert_matches!(fixture.on_connection_opened(0).err().unwrap(), GneissError::InternalStateError(_));
    assert_eq!(ProtocolStateType::Halted, fixture.client_state.state);
    assert!(fixture.client_packet_events.is_empty());

    assert_matches!(fixture.on_write_completion(0).err().unwrap(), GneissError::InternalStateError(_));
    assert_eq!(ProtocolStateType::Halted, fixture.client_state.state);
    assert!(fixture.client_packet_events.is_empty());

    let bytes : Vec<u8> = vec!(0, 1, 2, 3, 4, 5);
    assert_matches!(fixture.on_incoming_bytes(0, bytes.as_slice()).err().unwrap(), GneissError::InternalStateError(_));
    assert_eq!(ProtocolStateType::Halted, fixture.client_state.state);
    assert!(fixture.client_packet_events.is_empty());
}

#[test]
fn halted_state_next_service_time_never() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Halted, 0).is_ok());

    assert_eq!(None, fixture.get_next_service_time(0));
}

#[test]
fn halted_state_service_does_nothing() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Halted, 0).is_ok());

    verify_service_does_nothing(&mut fixture);
}

#[test]
fn halted_state_transition_out_on_connection_closed() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Halted, 0).is_ok());

    assert!(fixture.on_connection_closed(0).is_ok());
    assert_eq!(ProtocolStateType::Disconnected, fixture.client_state.state);
}

#[test]
fn pending_connack_state_network_event_connection_opened_fails() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());

    assert!(fixture.on_connection_opened(0).is_ok());
    assert_eq!(ProtocolStateType::PendingConnack, fixture.client_state.state);

    assert_matches!(fixture.on_connection_opened(0).err().unwrap(), GneissError::InternalStateError(_));
    assert_eq!(ProtocolStateType::Halted, fixture.client_state.state);
    assert!(fixture.client_packet_events.is_empty());
}

#[test]
fn pending_connack_state_network_event_write_completion_fails() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());

    assert!(fixture.on_connection_opened(0).is_ok());
    assert_eq!(ProtocolStateType::PendingConnack, fixture.client_state.state);

    assert_matches!(fixture.on_write_completion(0).err().unwrap(), GneissError::InternalStateError(_));
    assert_eq!(ProtocolStateType::Halted, fixture.client_state.state);
    assert!(fixture.client_packet_events.is_empty());
}

#[test]
fn pending_connack_state_connack_timeout() {
    let config = build_standard_test_config();
    let connack_timeout_millis = CONNACK_TIMEOUT_MILLIS;

    let mut fixture = ProtocolStateTestFixture::new(config);
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::PendingConnack, 1).is_ok());

    assert_eq!(Some(1), fixture.get_next_service_time(1));

    let service_result = fixture.service_with_drain(1, 4096);
    assert!(service_result.is_ok());

    assert_eq!(Some(1 + connack_timeout_millis as u64), fixture.get_next_service_time(1));

    // service post-timeout
    assert_matches!(fixture.service_with_drain(1 + connack_timeout_millis as u64, 4096), Err(GneissError::ConnectionEstablishmentFailure(_)));
    assert_eq!(ProtocolStateType::Halted, fixture.client_state.state);
    assert!(fixture.client_packet_events.is_empty());
    verify_protocol_state_empty(&fixture);
}

#[test]
fn pending_connack_state_failure_connack() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());

    fixture.broker_packet_handlers.insert(PacketType::Connect, Box::new(handle_connect_with_failure_connack));

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::PendingConnack, 0).is_ok());

    let server_bytes = fixture.service_with_drain(0, 4096).unwrap();

    assert_matches!(fixture.on_incoming_bytes(0, server_bytes.as_slice()), Err(GneissError::ConnectionEstablishmentFailure(_)));
    assert_eq!(ProtocolStateType::Halted, fixture.client_state.state);

    let expected_events = VecDeque::from(vec!(PacketEvent::Connack(create_connack_rejection())));
    assert_eq!(expected_events, fixture.client_packet_events);

    verify_protocol_state_empty(&fixture);
}

#[test]
fn pending_connack_state_connection_closed() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::PendingConnack, 0).is_ok());

    let _ = fixture.service_with_drain(0, 4096).unwrap();

    assert!(fixture.on_connection_closed(0).is_ok());
    assert_eq!(ProtocolStateType::Disconnected, fixture.client_state.state);
    assert!(fixture.client_packet_events.is_empty());
    verify_protocol_state_empty(&fixture);
}

#[test]
fn pending_connack_state_incoming_garbage_data() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::PendingConnack, 0).is_ok());

    let mut server_bytes = fixture.service_with_drain(0, 4096).unwrap();
    server_bytes.clear();
    let mut garbage = vec!(1, 2, 3, 4, 5, 6, 7, 8);
    server_bytes.append(&mut garbage);

    assert_matches!(fixture.on_incoming_bytes(0, server_bytes.as_slice()), Err(GneissError::DecodingFailure(_)));
    assert_eq!(ProtocolStateType::Halted, fixture.client_state.state);
    assert!(fixture.client_packet_events.is_empty());
    verify_protocol_state_empty(&fixture);
}

fn encode_packet_to_buffer(packet: MqttPacket, buffer: &mut Vec<u8>) -> GneissResult<()> {
    encode_packet_to_buffer_with_alias_resolution(packet, buffer, OutboundAliasResolution{
        skip_topic: false,
        alias: None,
    })
}

fn encode_packet_to_buffer_with_alias_resolution(packet: MqttPacket, buffer: &mut Vec<u8>, alias_resolution: OutboundAliasResolution) -> GneissResult<()> {
    let mut encode_buffer = Vec::with_capacity(4096);
    let encoding_context = EncodingContext {
        outbound_alias_resolution: alias_resolution
    };

    let mut encoder = Encoder::new();
    encoder.reset(&packet, &encoding_context)?;

    let mut encode_result = EncodeResult::Full;
    while encode_result == EncodeResult::Full {
        encode_result = encoder.encode(&packet, &mut encode_buffer)?;
        buffer.append(&mut encode_buffer);
    }

    Ok(())
}

fn do_pending_connack_state_non_connack_packet_test(packet: MqttPacket) {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::PendingConnack, 0).is_ok());

    let mut server_bytes = fixture.service_with_drain(0, 4096).unwrap();
    server_bytes.clear();

    assert!(encode_packet_to_buffer(packet, &mut server_bytes).is_ok());

    assert_matches!(fixture.on_incoming_bytes(0, server_bytes.as_slice()), Err(GneissError::ProtocolError(_)));
    assert_eq!(ProtocolStateType::Halted, fixture.client_state.state);
    assert!(fixture.client_packet_events.is_empty());
    verify_protocol_state_empty(&fixture);
}

#[test]
fn pending_connack_state_unexpected_packets() {
    // Not a protocol error: Connack, Auth
    let packets = vec!(
        MqttPacket::Connect(ConnectPacket {
            ..Default::default()
        }),
        MqttPacket::Pingreq(PingreqPacket {}),
        MqttPacket::Pingresp(PingrespPacket {}),
        MqttPacket::Publish(PublishPacket {
            topic: "hello/there".to_string(),
            ..Default::default()
        }),
        MqttPacket::Puback(PubackPacket {
            packet_id : 1,
            ..Default::default()
        }),
        MqttPacket::Pubrec(PubrecPacket {
            packet_id : 1,
            ..Default::default()
        }),
        MqttPacket::Pubrel(PubrelPacket {
            packet_id : 1,
            ..Default::default()
        }),
        MqttPacket::Pubcomp(PubcompPacket {
            packet_id : 1,
            ..Default::default()
        }),
        MqttPacket::Subscribe(SubscribePacket {
            packet_id : 1,
            ..Default::default()
        }),
        MqttPacket::Suback(SubackPacket {
            packet_id : 1,
            ..Default::default()
        }),
        MqttPacket::Unsubscribe(UnsubscribePacket {
            packet_id : 1,
            ..Default::default()
        }),
        MqttPacket::Unsuback(UnsubackPacket {
            packet_id : 1,
            ..Default::default()
        }),
        MqttPacket::Disconnect(DisconnectPacket {
            ..Default::default()
        }),
    );

    for packet in packets {
        do_pending_connack_state_non_connack_packet_test(packet);
    }
}

#[test]
fn pending_connack_state_connack_received_too_soon() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::PendingConnack, 0).is_ok());

    let mut server_bytes = Vec::new();

    assert!(encode_packet_to_buffer(MqttPacket::Connack(ConnackPacket{
        ..Default::default()
    }), &mut server_bytes).is_ok());

    assert_matches!(fixture.on_incoming_bytes(0, server_bytes.as_slice()), Err(GneissError::ProtocolError(_)));
    assert_eq!(ProtocolStateType::Halted, fixture.client_state.state);
    assert!(fixture.client_packet_events.is_empty());
}

#[test]
fn pending_connack_state_transition_to_disconnected() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::PendingConnack, 0).is_ok());

    assert!(fixture.on_connection_closed(0).is_ok());
    assert_eq!(ProtocolStateType::Disconnected, fixture.client_state.state);
    verify_protocol_state_empty(&fixture);
}

#[test]
fn connected_state_transition_to_success() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    let connack = ConnackPacket {
        ..Default::default()
    };

    let expected_events = VecDeque::from(vec!(PacketEvent::Connack(connack)));
    assert_eq!(expected_events, fixture.client_packet_events);

    verify_protocol_state_empty(&fixture);
}

#[test]
fn connected_state_network_event_connection_opened_fails() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    let client_event_count = fixture.client_packet_events.len();

    assert_matches!(fixture.on_connection_opened(0).err().unwrap(), GneissError::InternalStateError(_));
    assert_eq!(ProtocolStateType::Halted, fixture.client_state.state);
    assert_eq!(client_event_count, fixture.client_packet_events.len());
    verify_protocol_state_empty(&fixture);
}

#[test]
fn connected_state_network_event_write_completion_fails() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    assert_matches!(fixture.on_write_completion(0).err().unwrap(), GneissError::InternalStateError(_));
    assert_eq!(ProtocolStateType::Halted, fixture.client_state.state);
    verify_protocol_state_empty(&fixture);
}

#[test]
fn connected_state_transition_to_disconnected() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    assert!(fixture.on_connection_closed(0).is_ok());
    assert_eq!(ProtocolStateType::Disconnected, fixture.client_state.state);
    verify_protocol_state_empty(&fixture);
}

#[test]
fn connected_state_incoming_garbage_data() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    let garbage = vec!(1, 2, 3, 4, 5, 6, 7, 8);

    assert_matches!(fixture.on_incoming_bytes(0, garbage.as_slice()), Err(GneissError::DecodingFailure(_)));
    assert_eq!(ProtocolStateType::Halted, fixture.client_state.state);
    verify_protocol_state_empty(&fixture);
}

fn do_connected_state_unexpected_packet_test(packet : MqttPacket) {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    let mut buffer = Vec::new();
    assert!(encode_packet_to_buffer(packet, &mut buffer).is_ok());

    assert_matches!(fixture.on_incoming_bytes(0, buffer.as_slice()), Err(GneissError::ProtocolError(_)));
    assert_eq!(ProtocolStateType::Halted, fixture.client_state.state);
    verify_protocol_state_empty(&fixture);
}

#[test]
fn connected_state_unexpected_packets() {
    let packets = vec!(
        MqttPacket::Connect(ConnectPacket{
            ..Default::default()
        }),
        MqttPacket::Connack(ConnackPacket{
            ..Default::default()
        }),
        MqttPacket::Subscribe(SubscribePacket{
            ..Default::default()
        }),
        MqttPacket::Unsubscribe(UnsubscribePacket{
            ..Default::default()
        }),
        MqttPacket::Pingreq(PingreqPacket{})
    );

    for packet in packets {
        do_connected_state_unexpected_packet_test(packet);
    }
}

fn do_connected_state_invalid_ack_packet_id_test(packet : MqttPacket) -> GneissResult<()> {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    let mut buffer = Vec::new();
    assert!(encode_packet_to_buffer(packet, &mut buffer).is_ok());

    let incoming_bytes_result = fixture.on_incoming_bytes(0, buffer.as_slice());
    assert!(incoming_bytes_result.is_err());

    assert_eq!(ProtocolStateType::Halted, fixture.client_state.state);
    verify_protocol_state_empty(&fixture);

    incoming_bytes_result
}

#[test]
fn connected_state_unknown_ack_packet_id() {
    let packets = vec!(
        MqttPacket::Puback(PubackPacket{
            packet_id : 5,
            ..Default::default()
        }),
        MqttPacket::Suback(SubackPacket{
            packet_id : 42,
            ..Default::default()
        }),
        MqttPacket::Unsuback(UnsubackPacket{
            packet_id : 47,
            ..Default::default()
        }),
        MqttPacket::Pubrec(PubrecPacket{
            packet_id : 666,
            ..Default::default()
        }),
        MqttPacket::Pubcomp(PubcompPacket{
            packet_id : 1023,
            ..Default::default()
        }),
    );

    for packet in packets {
        assert_matches!(do_connected_state_invalid_ack_packet_id_test(packet), Err(GneissError::ProtocolError(_)));
    }
}

#[test]
fn connected_state_invalid_ack_packet_id() {
    let packets = vec!(
        (MqttPacket::Publish(PublishPacket{
            qos: QualityOfService::AtLeastOnce,
            ..Default::default()
        }), PacketType::Publish),
        (MqttPacket::Puback(PubackPacket{
            ..Default::default()
        }), PacketType::Puback),
        (MqttPacket::Suback(SubackPacket{
            ..Default::default()
        }), PacketType::Suback),
        (MqttPacket::Unsuback(UnsubackPacket{
            ..Default::default()
        }), PacketType::Unsuback),
        (MqttPacket::Pubrec(PubrecPacket{
            ..Default::default()
        }), PacketType::Pubrec),
        (MqttPacket::Pubcomp(PubcompPacket{
            ..Default::default()
        }), PacketType::Pubcomp),
    );

    for (packet, validation_error_packet_type) in packets {
        let result = do_connected_state_invalid_ack_packet_id_test(packet);
        assert!(result.is_err());
        assert_matches!(result, Err(GneissError::PacketValidationFailure(_)));
        if let Err(GneissError::PacketValidationFailure(packet_validation_context)) = result {
            assert_eq!(validation_error_packet_type, packet_validation_context.packet_type);
        }
    }
}

fn do_ping_sequence_test(connack_delay: u64, response_delay_millis: u64, request_delay_millis: u64) {
    const PING_TIMEOUT_MILLIS: u32 = 10000;
    const KEEP_ALIVE_SECONDS: u16 = 20;
    const KEEP_ALIVE_MILLIS: u64 = (KEEP_ALIVE_SECONDS as u64) * 1000;

    assert!(response_delay_millis < PING_TIMEOUT_MILLIS as u64);

    let mut config = build_standard_test_config();
    config.ping_timeout = Duration::from_millis(PING_TIMEOUT_MILLIS as u64);
    config.connect_options.keep_alive_interval_seconds = Some(KEEP_ALIVE_SECONDS);

    let mut fixture = ProtocolStateTestFixture::new(config);

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, connack_delay).is_ok());

    let mut current_time = connack_delay;
    let mut rolling_ping_time = current_time + KEEP_ALIVE_MILLIS;
    for i in 0..5 {
        // verify next service time the outbound ping time and nothing happens until then
        assert!(fixture.client_state.ping_timeout_timepoint.is_none());
        assert!(fixture.client_state.next_ping_timepoint.is_some());
        assert_eq!(rolling_ping_time, fixture.get_next_service_time(current_time).unwrap());
        assert_eq!(1 + i, fixture.to_broker_packet_stream.len());

        // trigger a ping, verify it goes out and a pingresp comes back
        current_time = rolling_ping_time + request_delay_millis;
        let server_bytes = fixture.service_with_drain(current_time, 4096).unwrap();
        assert_eq!(2 + i, fixture.to_broker_packet_stream.len());
        let (index, _) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Pingreq, i + 1, None, None).unwrap();
        assert_eq!(1 + i, index);

        // verify next service time is the ping timeout
        let ping_timeout = current_time + PING_TIMEOUT_MILLIS as u64;
        assert!(fixture.client_state.ping_timeout_timepoint.is_some());
        assert_eq!(ping_timeout, fixture.get_next_service_time(current_time).unwrap());

        // receive pingresp, verify timeout reset
        assert_eq!(2 + i, fixture.to_client_packet_stream.len());
        let (index, _) = find_nth_packet_of_type(fixture.to_client_packet_stream.iter(), PacketType::Pingresp, i + 1, None, None).unwrap();
        assert_eq!(1 + i, index);

        assert!(fixture.on_incoming_bytes(current_time + response_delay_millis, server_bytes.as_slice()).is_ok());
        assert_eq!(None, fixture.client_state.ping_timeout_timepoint);

        rolling_ping_time = current_time + KEEP_ALIVE_MILLIS;
    }

    verify_protocol_state_empty(&fixture);
}

#[test]
fn connected_state_ping_sequence_instant() {
    do_ping_sequence_test(0, 0, 0);
}

#[test]
fn connected_state_ping_sequence_response_delayed() {
    do_ping_sequence_test(1,2500, 0);
}

#[test]
fn connected_state_ping_sequence_request_delayed() {
    do_ping_sequence_test(3, 0, 1000);
}

#[test]
fn connected_state_ping_sequence_both_delayed() {
    do_ping_sequence_test(7, 999, 131);
}

#[allow(clippy::type_complexity)]
fn do_connected_state_ping_push_out_test(operation_function: Box<dyn Fn(&mut ProtocolStateTestFixture, u64, u64)>, transmission_time: u64, response_time: u64, expected_push_out: u64) {
    const PING_TIMEOUT_MILLIS: u64 = 10000;
    const KEEP_ALIVE_SECONDS: u16 = 20;
    const KEEP_ALIVE_MILLIS: u64 = (KEEP_ALIVE_SECONDS as u64) * 1000;

    let mut config = build_standard_test_config();
    config.ping_timeout = Duration::from_millis(PING_TIMEOUT_MILLIS);
    config.connect_options.keep_alive_interval_seconds = Some(KEEP_ALIVE_SECONDS);

    let mut fixture = ProtocolStateTestFixture::new(config);

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    operation_function(&mut fixture, transmission_time, response_time);

    assert_eq!(Some(expected_push_out + KEEP_ALIVE_MILLIS), fixture.get_next_service_time(response_time));
    verify_protocol_state_empty(&fixture);
}

fn do_subscribe_success(fixture : &mut ProtocolStateTestFixture, transmission_time: u64, response_time: u64, expected_reason_code: SubackReasonCode) {
    let subscribe = SubscribePacket {
        subscriptions: vec!(
            Subscription{
                topic_filter : "hello/world".to_string(),
                qos : QualityOfService::AtLeastOnce,
                ..Default::default()
            }
        ),
        ..Default::default()
    };

    let subscribe_result_receiver = fixture.subscribe(0, subscribe.clone(), SubscribeOptions{ ..Default::default() }).unwrap();
    assert!(fixture.service_round_trip(transmission_time, response_time, 4096).is_ok());

    let (index, to_broker_packet) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Subscribe, 1, None, None).unwrap();
    assert_eq!(1, index);
    if let MqttPacket::Subscribe(to_broker_subscribe) = to_broker_packet {
        assert_eq!(subscribe.subscriptions, to_broker_subscribe.subscriptions);
    } else {
        panic!("Expected subscribe");
    }

    let result = subscribe_result_receiver.recv();
    assert!(result.is_ok());

    let subscribe_result = result.unwrap();
    assert!(subscribe_result.is_ok());

    let suback_result = subscribe_result.unwrap();
    assert_eq!(1, suback_result.reason_codes.len());
    assert_eq!(expected_reason_code, suback_result.reason_codes[0]);

    let (index, to_client_packet) = find_nth_packet_of_type(fixture.to_client_packet_stream.iter(), PacketType::Suback, 1, None, None).unwrap();
    assert_eq!(1, index);
    if let MqttPacket::Suback(to_client_suback) = to_client_packet {
        assert_eq!(suback_result, *to_client_suback);
    } else {
        panic!("Expected suback");
    }

    verify_protocol_state_empty(fixture);
}

fn do_publish_success(fixture : &mut ProtocolStateTestFixture, qos: QualityOfService, transmission_time: u64, response_time: u64, expected_response: PublishResponse) {
    let publish = PublishPacket {
        topic: "hello/world".to_string(),
        qos,
        ..Default::default()
    };

    let publish_result_receiver = fixture.publish(0, publish.clone(), PublishOptions{ ..Default::default() }).unwrap();
    assert!(fixture.service_round_trip(transmission_time, response_time, 4096).is_ok());

    let (index, to_broker_packet) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Publish, 1, None, None).unwrap();
    assert_eq!(1, index);
    if let MqttPacket::Publish(to_broker_publish) = to_broker_packet {
        assert_eq!(publish.qos, to_broker_publish.qos);
        assert_eq!(publish.topic, to_broker_publish.topic);
    } else {
        panic!("Expected publish");
    }

    // pubrel/pubcomp needs another full service cycle
    if qos == QualityOfService::ExactlyOnce {
        assert!(fixture.service_round_trip(response_time, response_time, 4096).is_ok());
    }

    let result = publish_result_receiver.recv();
    assert!(result.is_ok());

    let op_result = result.unwrap();
    assert!(op_result.is_ok());

    let publish_response = op_result.unwrap();

    match &publish_response {
        PublishResponse::Qos0 => {
            assert_eq!(expected_response, publish_response);
        }

        PublishResponse::Qos1(puback) => {
            if let PublishResponse::Qos1(expected_puback) = &expected_response {
                assert_eq!(expected_puback.reason_code, puback.reason_code);
            } else {
                panic!("expected puback");
            }
            let (index, _) = find_nth_packet_of_type(fixture.to_client_packet_stream.iter(), PacketType::Puback, 1, None, None).unwrap();
            assert_eq!(1, index);
        }

        PublishResponse::Qos2(qos2_response) => {
            let (index, _) = find_nth_packet_of_type(fixture.to_client_packet_stream.iter(), PacketType::Pubrec, 1, None, None).unwrap();
            assert_eq!(1, index);

            match &qos2_response {
                Qos2Response::Pubcomp(pubcomp) => {
                    if let PublishResponse::Qos2(Qos2Response::Pubcomp(expected_pubcomp)) = &expected_response {
                        assert_eq!(expected_pubcomp.reason_code, pubcomp.reason_code);
                    } else {
                        panic!("expected pubcomp");
                    }

                    let (index, _) = find_nth_packet_of_type(fixture.to_client_packet_stream.iter(), PacketType::Pubcomp, 1, None, None).unwrap();
                    assert_eq!(2, index);

                    let (index, _) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Pubrel, 1, None, None).unwrap();
                    assert_eq!(2, index);
                }

                Qos2Response::Pubrec(pubrec) => {
                    if let PublishResponse::Qos2(Qos2Response::Pubrec(expected_pubrec)) = expected_response {
                        assert_eq!(expected_pubrec.reason_code, pubrec.reason_code);
                    } else {
                        panic!("expected pubcomp");
                    }
                }
            }
        }
    }

    verify_protocol_state_empty(fixture);
}

fn do_unsubscribe_success(fixture : &mut ProtocolStateTestFixture, transmission_time: u64, response_time: u64, expected_reason_code: UnsubackReasonCode) {
    let unsubscribe = UnsubscribePacket {
        topic_filters: vec!("hello/world".to_string()),
        ..Default::default()
    };

    let unsubscribe_result_receiver = fixture.unsubscribe(0, unsubscribe.clone(), UnsubscribeOptions{ ..Default::default() }).unwrap();
    assert!(fixture.service_round_trip(transmission_time, response_time, 4096).is_ok());

    let (index, to_broker_packet) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Unsubscribe, 1, None, None).unwrap();
    assert_eq!(1, index);
    if let MqttPacket::Unsubscribe(to_broker_unsubscribe) = to_broker_packet {
        assert_eq!(unsubscribe.topic_filters, to_broker_unsubscribe.topic_filters);
    } else {
        panic!("Expected unsubscribe");
    }

    let result = unsubscribe_result_receiver.recv();
    assert!(result.is_ok());

    let unsubscribe_result = result.unwrap();
    assert!(unsubscribe_result.is_ok());

    let unsuback_result = unsubscribe_result.unwrap();
    assert_eq!(1, unsuback_result.reason_codes.len());
    assert_eq!(expected_reason_code, unsuback_result.reason_codes[0]);

    let (index, to_client_packet) = find_nth_packet_of_type(fixture.to_client_packet_stream.iter(), PacketType::Unsuback, 1, None, None).unwrap();
    assert_eq!(1, index);
    if let MqttPacket::Unsuback(to_client_unsuback) = to_client_packet {
        assert_eq!(unsuback_result, *to_client_unsuback);
    } else {
        panic!("Expected unsuback");
    }

    verify_protocol_state_empty(fixture);
}

#[test]
fn connected_state_ping_push_out_by_subscribe_completion() {
    do_connected_state_ping_push_out_test(Box::new(
        |transmission_time, response_time, expected_push_out| {
            do_subscribe_success(transmission_time, response_time, expected_push_out, SubackReasonCode::GrantedQos1)
        }
    ), 666, 1337, 666);
}

#[test]
fn connected_state_ping_push_out_by_unsubscribe_completion() {
    do_connected_state_ping_push_out_test(Box::new(
        |transmission_time, response_time, expected_push_out| {
            do_unsubscribe_success(transmission_time, response_time, expected_push_out, UnsubackReasonCode::Success)
        }
    ), 666, 1337, 666);
}

#[test]
fn connected_state_ping_no_push_out_by_qos0_publish_completion() {
    do_connected_state_ping_push_out_test(Box::new(
        |fixture, transmission_time, response_time|{
            do_publish_success(fixture, QualityOfService::AtMostOnce, transmission_time, response_time, PublishResponse::Qos0);
        }
    ), 666, 1336, 0);
}

#[test]
fn connected_state_ping_push_out_by_qos1_publish_completion() {
    do_connected_state_ping_push_out_test(Box::new(
        |fixture, transmission_time, response_time|{
            do_publish_success(fixture, QualityOfService::AtLeastOnce, transmission_time, response_time, PublishResponse::Qos1(PubackPacket{
                reason_code: PubackReasonCode::Success,
                ..Default::default()
            }));
        }
    ), 333, 777, 333);
}

#[test]
fn connected_state_ping_push_out_by_qos2_publish_completion() {
    do_connected_state_ping_push_out_test(Box::new(
        |fixture, transmission_time, response_time|{
            do_publish_success(fixture, QualityOfService::ExactlyOnce, transmission_time, response_time, PublishResponse::Qos2(Qos2Response::Pubcomp(PubcompPacket{
                reason_code: PubcompReasonCode::Success,
                ..Default::default()
            })));
        }
    ), 444, 888, 888);
}

fn do_connected_state_ping_pingresp_timeout(ping_timeout_millis: u64, keep_alive_seconds: u16) {
    const CONNACK_TIME: u64 = 11;
    let keep_alive_millis: u64 = (keep_alive_seconds as u64) * 1000;

    let mut config = build_standard_test_config();
    config.ping_timeout = Duration::from_millis(ping_timeout_millis);
    config.connect_options.keep_alive_interval_seconds = Some(keep_alive_seconds);

    let mut fixture = ProtocolStateTestFixture::new(config);

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, CONNACK_TIME).is_ok());
    let expected_ping_time = CONNACK_TIME + keep_alive_millis;
    assert_eq!(Some(expected_ping_time), fixture.get_next_service_time(CONNACK_TIME));

    // trigger a ping
    assert!(fixture.service_with_drain(expected_ping_time, 4096).is_ok());
    let ping_timeout_timepoint = CONNACK_TIME + keep_alive_millis + u64::min(ping_timeout_millis, keep_alive_millis / 2);
    assert_eq!(ping_timeout_timepoint, fixture.get_next_service_time(expected_ping_time).unwrap());
    let (index, _) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Pingreq, 1, None, None).unwrap();
    assert_eq!(1, index); // [connect, pingreq]

    // right before timeout, nothing should happen
    let outbound_packet_count = fixture.to_broker_packet_stream.len();
    assert_eq!(2, outbound_packet_count);

    assert!(fixture.service_once(ping_timeout_timepoint - 1, 4096).is_ok());
    assert_eq!(outbound_packet_count, fixture.to_broker_packet_stream.len());

    // invoke service after timeout, verify failure and halt
    assert_matches!(fixture.service_once(ping_timeout_timepoint, 4096), Err(GneissError::ConnectionClosed(_)));
    assert_eq!(ProtocolStateType::Halted, fixture.client_state.state);
    assert_eq!(outbound_packet_count, fixture.to_broker_packet_stream.len());
    verify_protocol_state_empty(&fixture);
}

#[test]
fn connected_state_ping_pingresp_timeout_normal() {
    do_connected_state_ping_pingresp_timeout(10000, 20);
}

#[test]
fn connected_state_ping_pingresp_timeout_too_long() {
    do_connected_state_ping_pingresp_timeout(30000, 20);
}

#[test]
fn connected_state_ping_no_pings_on_zero_keep_alive() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());
    assert_eq!(None, fixture.get_next_service_time(0));

    for i in 0..3600 {
        let elapsed_millis : u64 = i * 1000;
        assert!(fixture.service_round_trip(elapsed_millis, elapsed_millis, 4096).is_ok());
        assert_eq!(None, fixture.get_next_service_time(0));
        assert_eq!(1, fixture.to_broker_packet_stream.len());
        assert_eq!(1, fixture.to_client_packet_stream.len());
    }

    verify_protocol_state_empty(&fixture);
}

#[test]
fn connected_state_subscribe_immediate_success() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    do_subscribe_success(&mut fixture, 1, 2, SubackReasonCode::GrantedQos1);
}

#[test]
fn connected_state_unsubscribe_immediate_success() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    do_unsubscribe_success(&mut fixture, 3, 7, UnsubackReasonCode::Success);
}

#[test]
fn connected_state_publish_qos0_immediate_success() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    do_publish_success(&mut fixture, QualityOfService::AtMostOnce, 11, 13, PublishResponse::Qos0);
}

#[test]
fn connected_state_publish_qos1_immediate_success() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    do_publish_success(&mut fixture, QualityOfService::AtLeastOnce, 17, 23, PublishResponse::Qos1(PubackPacket{
        reason_code: PubackReasonCode::Success,
        ..Default::default()
    }));
}

#[test]
fn connected_state_publish_qos2_immediate_success() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    do_publish_success(&mut fixture, QualityOfService::ExactlyOnce, 29, 31, PublishResponse::Qos2(Qos2Response::Pubcomp(PubcompPacket{
        reason_code: PubcompReasonCode::Success,
        ..Default::default()
    })));
}

macro_rules! define_operation_success_reconnect_while_in_user_queue_test {
        ($test_helper_name: ident, $build_operation_function_name: ident, $operation_api: ident, $operation_options_type: ident, $verify_function_name: ident, $queue_policy: expr) => {
            fn $test_helper_name() {
                let mut config = build_standard_test_config();
                config.offline_queue_policy = $queue_policy;

                let mut fixture = ProtocolStateTestFixture::new(config);
                assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

                let operation = $build_operation_function_name();

                let operation_result_receiver = fixture.$operation_api(0, operation.clone(), $operation_options_type{ ..Default::default()}).unwrap();
                assert!(operation_result_receiver.try_recv().is_err());
                assert_eq!(1, fixture.client_state.user_operation_queue.len());

                assert!(fixture.on_connection_closed(0).is_ok());
                assert!(operation_result_receiver.try_recv().is_err());
                assert_eq!(1, fixture.client_state.user_operation_queue.len());

                assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());
                assert!(operation_result_receiver.try_recv().is_err());
                assert_eq!(1, fixture.client_state.user_operation_queue.len());

                assert!(fixture.service_round_trip(10, 20, 4096).is_ok());
                assert!(fixture.service_round_trip(30, 40, 4096).is_ok()); // qos 2

                if let Ok(Ok(ack)) = operation_result_receiver.recv() {
                    $verify_function_name(&ack);
                } else {
                    panic!("Expected ack result");
                }

                verify_protocol_state_empty(&fixture);
            }
        };
    }

fn verify_successful_test_suback(suback: &SubackPacket) {
    assert_eq!(1, suback.reason_codes.len());
    assert_eq!(SubackReasonCode::GrantedQos2, suback.reason_codes[0]);
}

fn build_subscribe_success_packet() -> SubscribePacket {
    SubscribePacket {
        subscriptions: vec!(
            Subscription{
                topic_filter : "hello/world".to_string(),
                qos : QualityOfService::ExactlyOnce,
                ..Default::default()
            }
        ),
        ..Default::default()
    }
}

define_operation_success_reconnect_while_in_user_queue_test!(
        connected_state_subscribe_success_reconnect_while_in_user_queue_helper,
        build_subscribe_success_packet,
        subscribe,
        SubscribeOptions,
        verify_successful_test_suback,
        OfflineQueuePolicy::PreserveAcknowledged
    );

#[test]
fn connected_state_subscribe_success_reconnect_while_in_user_queue() {
    connected_state_subscribe_success_reconnect_while_in_user_queue_helper();
}

fn verify_successful_test_unsuback(unsuback: &UnsubackPacket) {
    assert_eq!(1, unsuback.reason_codes.len());
    assert_eq!(UnsubackReasonCode::Success, unsuback.reason_codes[0]);
}

fn build_unsubscribe_success_packet() -> UnsubscribePacket {
    UnsubscribePacket {
        topic_filters: vec!(
            "hello/world".to_string()
        ),
        ..Default::default()
    }
}

define_operation_success_reconnect_while_in_user_queue_test!(
        connected_state_unsubscribe_success_reconnect_while_in_user_queue_helper,
        build_unsubscribe_success_packet,
        unsubscribe,
        UnsubscribeOptions,
        verify_successful_test_unsuback,
        OfflineQueuePolicy::PreserveAcknowledged
    );

#[test]
fn connected_state_unsubscribe_success_reconnect_while_in_user_queue() {
    connected_state_unsubscribe_success_reconnect_while_in_user_queue_helper();
}

fn verify_successful_test_qos0_publish(response: &PublishResponse) {
    assert_eq!(PublishResponse::Qos0, *response);
}

fn build_qos0_publish_success_packet() -> PublishPacket {
    PublishPacket {
        topic: "hello/world".to_string(),
        qos: QualityOfService::AtMostOnce,
        ..Default::default()
    }
}

define_operation_success_reconnect_while_in_user_queue_test!(
        connected_state_qos0_publish_success_reconnect_while_in_user_queue_helper,
        build_qos0_publish_success_packet,
        publish,
        PublishOptions,
        verify_successful_test_qos0_publish,
        OfflineQueuePolicy::PreserveAll
    );

#[test]
fn connected_state_qos0_publish_success_reconnect_while_in_user_queue() {
    connected_state_qos0_publish_success_reconnect_while_in_user_queue_helper();
}

fn verify_successful_test_qos1_publish(response: &PublishResponse) {
    if let PublishResponse::Qos1(puback) = &response {
        assert_eq!(PubackReasonCode::Success, puback.reason_code);
        return;
    }

    panic!("Expected puback");
}

fn build_qos1_publish_success_packet() -> PublishPacket {
    PublishPacket {
        topic: "hello/world".to_string(),
        qos: QualityOfService::AtLeastOnce,
        ..Default::default()
    }
}

define_operation_success_reconnect_while_in_user_queue_test!(
        connected_state_qos1_publish_success_reconnect_while_in_user_queue_helper,
        build_qos1_publish_success_packet,
        publish,
        PublishOptions,
        verify_successful_test_qos1_publish,
        OfflineQueuePolicy::PreserveAcknowledged
    );

#[test]
fn connected_state_qos1_publish_success_reconnect_while_in_user_queue() {
    connected_state_qos1_publish_success_reconnect_while_in_user_queue_helper();
}

fn verify_successful_test_qos2_publish(response: &PublishResponse) {
    if let PublishResponse::Qos2(Qos2Response::Pubcomp(pubcomp)) = &response {
        assert_eq!(PubcompReasonCode::Success, pubcomp.reason_code);
        return;
    }

    panic!("Expected pubcomp");
}

fn build_qos2_publish_success_packet() -> PublishPacket {
    PublishPacket {
        topic: "hello/world".to_string(),
        qos: QualityOfService::ExactlyOnce,
        ..Default::default()
    }
}

define_operation_success_reconnect_while_in_user_queue_test!(
        connected_state_qos2_publish_success_reconnect_while_in_user_queue_helper,
        build_qos2_publish_success_packet,
        publish,
        PublishOptions,
        verify_successful_test_qos2_publish,
        OfflineQueuePolicy::PreserveAcknowledged
    );

#[test]
fn connected_state_qos2_publish_success_reconnect_while_in_user_queue() {
    connected_state_qos2_publish_success_reconnect_while_in_user_queue_helper();
}

macro_rules! define_operation_success_reconnect_while_current_operation_test {
        ($test_helper_name: ident, $build_operation_function_name: ident, $operation_api: ident, $operation_options_type: ident, $verify_function_name: ident, $queue_policy: expr) => {
            fn $test_helper_name() {
                let mut config = build_standard_test_config();
                config.offline_queue_policy = $queue_policy;

                let mut fixture = ProtocolStateTestFixture::new(config);
                assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

                let operation = $build_operation_function_name();

                let operation_result_receiver = fixture.$operation_api(0, operation.clone(), $operation_options_type{ ..Default::default()}).unwrap();
                assert!(operation_result_receiver.try_recv().is_err());
                assert_eq!(1, fixture.client_state.user_operation_queue.len());

                let service_result = fixture.service_once(10, 10);
                assert!(service_result.is_ok());
                assert!(operation_result_receiver.try_recv().is_err());
                assert_eq!(0, fixture.client_state.user_operation_queue.len());
                assert!(fixture.client_state.current_operation.is_some());

                assert!(fixture.on_connection_closed(0).is_ok());
                assert!(operation_result_receiver.try_recv().is_err());
                assert_eq!(1, fixture.client_state.user_operation_queue.len());

                assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());
                assert!(operation_result_receiver.try_recv().is_err());
                assert_eq!(1, fixture.client_state.user_operation_queue.len());

                assert!(fixture.service_round_trip(10, 20, 4096).is_ok());
                assert!(fixture.service_round_trip(30, 40, 4096).is_ok()); // qos 2

                if let Ok(Ok(ack)) = operation_result_receiver.recv() {
                    $verify_function_name(&ack);
                } else {
                    panic!("Expected ack result");
                }

                verify_protocol_state_empty(&fixture);
            }
        };
    }

define_operation_success_reconnect_while_current_operation_test!(
        connected_state_subscribe_success_reconnect_while_current_operation_helper,
        build_subscribe_success_packet,
        subscribe,
        SubscribeOptions,
        verify_successful_test_suback,
        OfflineQueuePolicy::PreserveAcknowledged
    );

#[test]
fn connected_state_subscribe_success_reconnect_while_current_operation() {
    connected_state_subscribe_success_reconnect_while_current_operation_helper();
}

define_operation_success_reconnect_while_current_operation_test!(
        connected_state_unsubscribe_success_reconnect_while_current_operation_helper,
        build_unsubscribe_success_packet,
        unsubscribe,
        UnsubscribeOptions,
        verify_successful_test_unsuback,
        OfflineQueuePolicy::PreserveAcknowledged
    );

#[test]
fn connected_state_unsubscribe_success_reconnect_while_current_operation() {
    connected_state_unsubscribe_success_reconnect_while_current_operation_helper();
}

define_operation_success_reconnect_while_current_operation_test!(
        connected_state_qos0_publish_success_reconnect_while_current_operation_helper,
        build_qos0_publish_success_packet,
        publish,
        PublishOptions,
        verify_successful_test_qos0_publish,
        OfflineQueuePolicy::PreserveAll
    );

#[test]
fn connected_state_qos0_publish_success_reconnect_while_current_operation() {
    connected_state_qos0_publish_success_reconnect_while_current_operation_helper();
}

define_operation_success_reconnect_while_current_operation_test!(
        connected_state_qos1_publish_success_reconnect_while_current_operation_helper,
        build_qos1_publish_success_packet,
        publish,
        PublishOptions,
        verify_successful_test_qos1_publish,
        OfflineQueuePolicy::PreserveQos1PlusPublishes
    );

#[test]
fn connected_state_qos1_publish_success_reconnect_while_current_operation() {
    connected_state_qos1_publish_success_reconnect_while_current_operation_helper();
}

define_operation_success_reconnect_while_current_operation_test!(
        connected_state_qos2_publish_success_reconnect_while_current_operation_helper,
        build_qos2_publish_success_packet,
        publish,
        PublishOptions,
        verify_successful_test_qos2_publish,
        OfflineQueuePolicy::PreserveQos1PlusPublishes
    );

#[test]
fn connected_state_qos2_publish_success_reconnect_while_current_operation() {
    connected_state_qos2_publish_success_reconnect_while_current_operation_helper();
}

macro_rules! define_operation_success_reconnect_no_session_while_pending_test {
        ($test_helper_name: ident, $build_operation_function_name: ident, $operation_api: ident, $operation_options_type: ident, $verify_function_name: ident, $queue_policy: expr) => {
            fn $test_helper_name() {
                let mut config = build_standard_test_config();
                config.offline_queue_policy = $queue_policy;

                let mut fixture = ProtocolStateTestFixture::new(config);
                assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

                let operation = $build_operation_function_name();

                let operation_result_receiver = fixture.$operation_api(0, operation.clone(), $operation_options_type{ ..Default::default()}).unwrap();
                assert!(operation_result_receiver.try_recv().is_err());
                assert_eq!(1, fixture.client_state.user_operation_queue.len());

                let service_result = fixture.service_once(0, 4096);
                assert!(service_result.is_ok());
                assert!(operation_result_receiver.try_recv().is_err());
                assert_eq!(0, fixture.client_state.user_operation_queue.len());
                assert!(fixture.client_state.current_operation.is_none());

                let is_pending_ack = (1 == fixture.client_state.allocated_packet_ids.len()) && ((1 == fixture.client_state.pending_publish_operations.len()) != (1 == fixture.client_state.pending_non_publish_operations.len()));
                let is_pending_write_complete = (1 == fixture.client_state.pending_write_completion_operations.len());
                assert!(is_pending_ack != is_pending_write_complete); // xor - one or the other must be true, not both

                assert!(fixture.on_connection_closed(0).is_ok());
                assert!(operation_result_receiver.try_recv().is_err());

                assert_eq!(1, fixture.client_state.resubmit_operation_queue.len() + fixture.client_state.user_operation_queue.len());

                assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());
                assert!(operation_result_receiver.try_recv().is_err());
                assert_eq!(1, fixture.client_state.user_operation_queue.len());

                assert!(fixture.service_round_trip(10, 20, 4096).is_ok());
                assert!(fixture.service_round_trip(30, 40, 4096).is_ok()); // qos 2

                if let Ok(Ok(ack)) = operation_result_receiver.recv() {
                    $verify_function_name(&ack);
                } else {
                    panic!("Expected ack result");
                }

                verify_protocol_state_empty(&fixture);
            }
        };
    }

define_operation_success_reconnect_no_session_while_pending_test!(
        connected_state_subscribe_success_reconnect_no_session_while_pending_helper,
        build_subscribe_success_packet,
        subscribe,
        SubscribeOptions,
        verify_successful_test_suback,
        OfflineQueuePolicy::PreserveAcknowledged
    );

#[test]
fn connected_state_subscribe_success_reconnect_no_session_while_pending() {
    connected_state_subscribe_success_reconnect_no_session_while_pending_helper();
}

define_operation_success_reconnect_no_session_while_pending_test!(
        connected_state_unsubscribe_success_reconnect_no_session_while_pending_helper,
        build_unsubscribe_success_packet,
        unsubscribe,
        UnsubscribeOptions,
        verify_successful_test_unsuback,
        OfflineQueuePolicy::PreserveAcknowledged
    );

#[test]
fn connected_state_unsubscribe_success_reconnect_no_session_while_pending() {
    connected_state_unsubscribe_success_reconnect_no_session_while_pending_helper();
}

define_operation_success_reconnect_no_session_while_pending_test!(
        connected_state_qos0_publish_success_reconnect_no_session_while_pending_helper,
        build_qos0_publish_success_packet,
        publish,
        PublishOptions,
        verify_successful_test_qos0_publish,
        OfflineQueuePolicy::PreserveAll
    );

#[test]
fn connected_state_qos0_publish_success_reconnect_no_session_while_pending() {
    connected_state_qos0_publish_success_reconnect_no_session_while_pending_helper();
}

define_operation_success_reconnect_no_session_while_pending_test!(
        connected_state_qos1_publish_success_reconnect_no_session_while_pending_helper,
        build_qos1_publish_success_packet,
        publish,
        PublishOptions,
        verify_successful_test_qos1_publish,
        OfflineQueuePolicy::PreserveQos1PlusPublishes
    );

#[test]
fn connected_state_qos1_publish_success_reconnect_no_session_while_pending() {
    connected_state_qos1_publish_success_reconnect_no_session_while_pending_helper();
}

define_operation_success_reconnect_no_session_while_pending_test!(
        connected_state_qos2_publish_success_reconnect_no_session_while_pending_helper,
        build_qos2_publish_success_packet,
        publish,
        PublishOptions,
        verify_successful_test_qos2_publish,
        OfflineQueuePolicy::PreserveQos1PlusPublishes
    );

#[test]
fn connected_state_qos2_publish_success_reconnect_no_session_while_pending() {
    connected_state_qos2_publish_success_reconnect_no_session_while_pending_helper();
}

#[test]
fn connected_state_subscribe_success_failing_reason_code() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    fixture.broker_packet_handlers.insert(PacketType::Subscribe, Box::new(handle_subscribe_with_failure));

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    do_subscribe_success(&mut fixture, 0, 0, SubackReasonCode::NotAuthorized);
}

#[test]
fn connected_state_unsubscribe_success_failing_reason_code() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    fixture.broker_packet_handlers.insert(PacketType::Unsubscribe, Box::new(handle_unsubscribe_with_failure));

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    do_unsubscribe_success(&mut fixture, 0, 0, UnsubackReasonCode::ImplementationSpecificError);
}

#[test]
fn connected_state_qos1_publish_success_failing_reason_code() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    fixture.broker_packet_handlers.insert(PacketType::Publish, Box::new(handle_publish_with_failure));

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    do_publish_success(&mut fixture, QualityOfService::AtLeastOnce, 0, 0, PublishResponse::Qos1(PubackPacket{
        reason_code: PubackReasonCode::QuotaExceeded,
        ..Default::default()
    }));
}

#[test]
fn connected_state_qos2_publish_success_failing_reason_code_pubrec() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    fixture.broker_packet_handlers.insert(PacketType::Publish, Box::new(handle_publish_with_failure));

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    do_publish_success(&mut fixture, QualityOfService::ExactlyOnce, 0, 0, PublishResponse::Qos2(Qos2Response::Pubrec(PubrecPacket{
        reason_code: PubrecReasonCode::QuotaExceeded,
        ..Default::default()
    })));
}

#[test]
fn connected_state_qos2_publish_success_failing_reason_code_pubcomp() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    fixture.broker_packet_handlers.insert(PacketType::Pubrel, Box::new(handle_pubrel_with_failure));

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    do_publish_success(&mut fixture, QualityOfService::ExactlyOnce, 0, 0, PublishResponse::Qos2(Qos2Response::Pubcomp(PubcompPacket{
        reason_code: PubcompReasonCode::PacketIdentifierNotFound,
        ..Default::default()
    })));
}

macro_rules! define_operation_failure_validation_helper {
        ($test_helper_name: ident, $build_operation_function_name: ident, $operation_api: ident, $operation_options_type: ident, $expected_packet_validation_error_packet_type: expr) => {
            fn $test_helper_name() {
                let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
                fixture.broker_packet_handlers.insert(PacketType::Connect, Box::new(handle_connect_with_tiny_maximum_packet_size));

                assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

                let packet = $build_operation_function_name();

                let operation_result_receiver = fixture.$operation_api(0, packet, $operation_options_type{ ..Default::default() }).unwrap();
                assert!(fixture.service_round_trip(0, 0, 4096).is_ok());

                let result = operation_result_receiver.recv();
                assert!(!result.is_err());

                let operation_result = result.unwrap();
                assert!(operation_result.is_err());

                verify_validation_failure!(operation_result, $expected_packet_validation_error_packet_type);
                verify_protocol_state_empty(&fixture);
            }
        };
    }

fn build_subscribe_failure_validation_packet() -> SubscribePacket {
    SubscribePacket {
        subscriptions: vec!(
            Subscription {
                topic_filter : "hello/world".to_string(),
                qos : QualityOfService::AtLeastOnce,
                ..Default::default()
            }
        ),
        ..Default::default()
    }
}

define_operation_failure_validation_helper!(
        connected_state_subscribe_failure_validation_helper,
        build_subscribe_failure_validation_packet,
        subscribe,
        SubscribeOptions,
        PacketType::Subscribe
    );

#[test]
fn connected_state_subscribe_failure_validation() {
    connected_state_subscribe_failure_validation_helper();
}

fn build_unsubscribe_failure_validation_packet() -> UnsubscribePacket {
    UnsubscribePacket {
        topic_filters: vec!(
            "Hello/World/Derp".to_string()
        ),
        ..Default::default()
    }
}

define_operation_failure_validation_helper!(
        connected_state_unsubscribe_failure_validation_helper,
        build_unsubscribe_failure_validation_packet,
        unsubscribe,
        UnsubscribeOptions,
        PacketType::Unsubscribe
    );

#[test]
fn connected_state_unsubscribe_failure_validation() {
    connected_state_unsubscribe_failure_validation_helper();
}

fn build_qos0_publish_failure_validation_packet() -> PublishPacket {
    PublishPacket {
        topic: "derrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrp".to_string(),
        qos: QualityOfService::AtMostOnce,
        payload: Some("Some Kind of Payload".as_bytes().to_vec()),
        ..Default::default()
    }
}

define_operation_failure_validation_helper!(
        connected_state_qos0_publish_failure_validation_helper,
        build_qos0_publish_failure_validation_packet,
        publish,
        PublishOptions,
        PacketType::Publish
    );

#[test]
fn connected_state_qos0_publish_failure_validation() {
    connected_state_qos0_publish_failure_validation_helper();
}

fn build_qos1_publish_failure_validation_packet() -> PublishPacket {
    PublishPacket {
        topic: "derrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrp".to_string(),
        qos: QualityOfService::AtLeastOnce,
        payload: Some("Some Kind of Payload".as_bytes().to_vec()),
        ..Default::default()
    }
}

define_operation_failure_validation_helper!(
        connected_state_qos1_publish_failure_validation_helper,
        build_qos1_publish_failure_validation_packet,
        publish,
        PublishOptions,
        PacketType::Publish
    );

#[test]
fn connected_state_qos1_publish_failure_validation() {
    connected_state_qos1_publish_failure_validation_helper();
}

fn build_qos2_publish_failure_validation_packet() -> PublishPacket {
    PublishPacket {
        topic: "derrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrp".to_string(),
        qos: QualityOfService::ExactlyOnce,
        payload: Some("Some Kind of Payload".as_bytes().to_vec()),
        ..Default::default()
    }
}

define_operation_failure_validation_helper!(
        connected_state_qos2_publish_failure_validation_helper,
        build_qos2_publish_failure_validation_packet,
        publish,
        PublishOptions,
        PacketType::Publish
    );

#[test]
fn connected_state_qos2_publish_failure_validation() {
    connected_state_qos2_publish_failure_validation_helper();
}

macro_rules! define_operation_failure_timeout_helper {
        ($test_helper_name: ident, $build_operation_function_name: ident, $operation_api: ident, $operation_options_type_builder: ident, $packet_type: ident) => {
            fn $test_helper_name() {
                let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());

                fixture.broker_packet_handlers.insert(PacketType::$packet_type, Box::new(handle_with_nothing));
                assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

                let packet = $build_operation_function_name();

                let operation_result_receiver = fixture.$operation_api(0, packet, $operation_options_type_builder::new().with_ack_timeout(Duration::from_secs(30)).build()).unwrap();
                assert!(fixture.service_round_trip(0, 0, 4096).is_ok());

                let (index, _) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::$packet_type, 1, None, None).unwrap();
                assert_eq!(1, index);

                assert_eq!(Some(30000), fixture.get_next_service_time(0));

                for i in 0..30 {
                    let elapsed_millis = i * 1000;
                    assert_eq!(Some(30000), fixture.get_next_service_time(elapsed_millis));
                    assert!(fixture.service_round_trip(elapsed_millis, elapsed_millis, 4096).is_ok());
                    let recv_result = operation_result_receiver.try_recv();
                    assert!(recv_result.is_err());
                    let mqtt_error = GneissError::from(recv_result.unwrap_err());
                    assert_matches!(mqtt_error, GneissError::OperationChannelFailure(_));
                }

                assert!(fixture.service_round_trip(30000, 30000, 4096).is_ok());
                let result = operation_result_receiver.recv();
                assert_matches!(result.unwrap().unwrap_err(), GneissError::AckTimeout(_));
                verify_protocol_state_empty(&fixture);
            }
        };
    }

define_operation_failure_timeout_helper!(
        connected_state_subscribe_failure_timeout_helper,
        build_subscribe_success_packet,
        subscribe,
        SubscribeOptionsBuilder,
        Subscribe
    );

#[test]
fn connected_state_subscribe_failure_timeout() {
    connected_state_subscribe_failure_timeout_helper();
}

define_operation_failure_timeout_helper!(
        connected_state_unsubscribe_failure_timeout_helper,
        build_unsubscribe_success_packet,
        unsubscribe,
        UnsubscribeOptionsBuilder,
        Unsubscribe
    );

#[test]
fn connected_state_unsubscribe_failure_timeout() {
    connected_state_unsubscribe_failure_timeout_helper();
}

define_operation_failure_timeout_helper!(
        connected_state_qos1_publish_failure_timeout_helper,
        build_qos1_publish_success_packet,
        publish,
        PublishOptionsBuilder,
        Publish
    );

#[test]
fn connected_state_qos1_publish_failure_timeout() {
    connected_state_qos1_publish_failure_timeout_helper();
}

define_operation_failure_timeout_helper!(
        connected_state_qos2_publish_failure_timeout_helper,
        build_qos2_publish_success_packet,
        publish,
        PublishOptionsBuilder,
        Publish
    );

#[test]
fn connected_state_qos2_publish_failure_timeout() {
    connected_state_qos2_publish_failure_timeout_helper();
}

#[test]
fn connected_state_qos2_publish_failure_pubrel_timeout() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());

    fixture.broker_packet_handlers.insert(PacketType::Pubrel, Box::new(handle_with_nothing));
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    let packet = build_qos2_publish_success_packet();

    let operation_result_receiver = fixture.publish(0, packet, PublishOptions::builder().with_ack_timeout(Duration::from_secs(30)).build()).unwrap();
    assert!(fixture.service_round_trip(0, 0, 4096).is_ok());
    assert!(fixture.service_round_trip(10, 10, 4096).is_ok());

    let (index, _) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Publish, 1, None, None).unwrap();
    assert_eq!(1, index);

    let (index, _) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Pubrel, 1, None, None).unwrap();
    assert_eq!(2, index);

    assert_eq!(Some(30000), fixture.get_next_service_time(0));

    for i in 0..30 {
        let elapsed_millis = i * 1000;
        assert_eq!(Some(30000), fixture.get_next_service_time(elapsed_millis));
        assert!(fixture.service_round_trip(elapsed_millis, elapsed_millis, 4096).is_ok());
        let error = GneissError::from(operation_result_receiver.try_recv().unwrap_err());
        assert_matches!(error, GneissError::OperationChannelFailure(_));
    }

    assert!(fixture.service_round_trip(30000, 30000, 4096).is_ok());
    let result = operation_result_receiver.recv();
    assert_matches!(result.unwrap().unwrap_err(), GneissError::AckTimeout(_));
    assert!(fixture.service_round_trip(30010, 30010, 4096).is_ok());
    verify_protocol_state_empty(&fixture);
}

macro_rules! define_operation_failure_offline_submit_and_policy_fail_helper {
        ($test_helper_name: ident, $build_operation_function_name: ident, $operation_api: ident, $operation_options_type: ident, $offline_policy: ident) => {
            fn $test_helper_name() {
                let mut config = build_standard_test_config();
                config.offline_queue_policy = OfflineQueuePolicy::$offline_policy;

                let mut fixture = ProtocolStateTestFixture::new(config);

                let packet = $build_operation_function_name();

                let operation_result_receiver = fixture.$operation_api(0, packet, $operation_options_type{ ..Default::default() }).unwrap();

                let result = operation_result_receiver.recv();
                assert!(!result.is_err());

                let operation_result = result.unwrap();
                assert!(operation_result.is_err());

                assert_matches!(operation_result.unwrap_err(), GneissError::OfflineQueuePolicyFailed(_));
                verify_protocol_state_empty(&fixture);
            }
        };
    }

define_operation_failure_offline_submit_and_policy_fail_helper!(
        connected_state_subscribe_failure_offline_submit_and_policy_fail_helper,
        build_subscribe_success_packet,
        subscribe,
        SubscribeOptions,
        PreserveQos1PlusPublishes
    );

#[test]
fn connected_state_subscribe_failure_offline_submit_and_policy_fail() {
    connected_state_subscribe_failure_offline_submit_and_policy_fail_helper();
}

define_operation_failure_offline_submit_and_policy_fail_helper!(
        connected_state_unsubscribe_failure_offline_submit_and_policy_fail_helper,
        build_unsubscribe_success_packet,
        unsubscribe,
        UnsubscribeOptions,
        PreserveQos1PlusPublishes
    );

#[test]
fn connected_state_unsubscribe_failure_offline_submit_and_policy_fail() {
    connected_state_unsubscribe_failure_offline_submit_and_policy_fail_helper();
}

define_operation_failure_offline_submit_and_policy_fail_helper!(
        connected_state_qos0_publish_failure_offline_submit_and_policy_fail_helper,
        build_qos0_publish_success_packet,
        publish,
        PublishOptions,
        PreserveAcknowledged
    );

#[test]
fn connected_state_qos0_publish_failure_offline_submit_and_policy_fail() {
    connected_state_qos0_publish_failure_offline_submit_and_policy_fail_helper();
}

define_operation_failure_offline_submit_and_policy_fail_helper!(
        connected_state_qos1_publish_failure_offline_submit_and_policy_fail_helper,
        build_qos1_publish_success_packet,
        publish,
        PublishOptions,
        PreserveNothing
    );

#[test]
fn connected_state_qos1_publish_failure_offline_submit_and_policy_fail() {
    connected_state_qos1_publish_failure_offline_submit_and_policy_fail_helper();
}

define_operation_failure_offline_submit_and_policy_fail_helper!(
        connected_state_qos2_publish_failure_offline_submit_and_policy_fail_helper,
        build_qos2_publish_success_packet,
        publish,
        PublishOptions,
        PreserveNothing
    );

#[test]
fn connected_state_qos2_publish_failure_offline_submit_and_policy_fail() {
    connected_state_qos2_publish_failure_offline_submit_and_policy_fail_helper();
}

macro_rules! define_operation_failure_disconnect_user_queue_with_failing_offline_policy_helper {
        ($test_helper_name: ident, $build_operation_function_name: ident, $operation_api: ident, $operation_options_type: ident, $offline_policy: ident) => {
            fn $test_helper_name() {
                let mut config = build_standard_test_config();
                config.offline_queue_policy = OfflineQueuePolicy::$offline_policy;

                let mut fixture = ProtocolStateTestFixture::new(config);
                assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

                let packet = $build_operation_function_name();

                let operation_result_receiver = fixture.$operation_api(0, packet, $operation_options_type{ ..Default::default() }).unwrap();

                assert!(operation_result_receiver.try_recv().is_err());
                assert_eq!(1, fixture.client_state.user_operation_queue.len());

                assert!(fixture.on_connection_closed(0).is_ok());
                verify_protocol_state_empty(&fixture);

                let result = operation_result_receiver.recv();
                assert!(!result.is_err());

                let operation_result = result.unwrap();
                assert!(operation_result.is_err());

                assert_matches!(operation_result.unwrap_err(), GneissError::OfflineQueuePolicyFailed(_));
                verify_protocol_state_empty(&fixture);
            }
        };
    }

define_operation_failure_disconnect_user_queue_with_failing_offline_policy_helper!(
        connected_state_subscribe_failure_disconnect_user_queue_with_failing_offline_policy_helper,
        build_subscribe_success_packet,
        subscribe,
        SubscribeOptions,
        PreserveQos1PlusPublishes
    );

#[test]
fn connected_state_subscribe_failure_disconnect_user_queue_with_failing_offline_policy() {
    connected_state_subscribe_failure_disconnect_user_queue_with_failing_offline_policy_helper();
}

define_operation_failure_disconnect_user_queue_with_failing_offline_policy_helper!(
        connected_state_unsubscribe_failure_disconnect_user_queue_with_failing_offline_policy_helper,
        build_unsubscribe_success_packet,
        unsubscribe,
        UnsubscribeOptions,
        PreserveQos1PlusPublishes
    );

#[test]
fn connected_state_unsubscribe_failure_disconnect_user_queue_with_failing_offline_policy() {
    connected_state_unsubscribe_failure_disconnect_user_queue_with_failing_offline_policy_helper();
}

define_operation_failure_disconnect_user_queue_with_failing_offline_policy_helper!(
        connected_state_qos0_publish_failure_disconnect_user_queue_with_failing_offline_policy_helper,
        build_qos0_publish_success_packet,
        publish,
        PublishOptions,
        PreserveAcknowledged
    );

#[test]
fn connected_state_qos0_publish_failure_disconnect_user_queue_with_failing_offline_policy() {
    connected_state_qos0_publish_failure_disconnect_user_queue_with_failing_offline_policy_helper();
}

define_operation_failure_disconnect_user_queue_with_failing_offline_policy_helper!(
        connected_state_qos1_publish_failure_disconnect_user_queue_with_failing_offline_policy_helper,
        build_qos1_publish_success_packet,
        publish,
        PublishOptions,
        PreserveNothing
    );

#[test]
fn connected_state_qos1_publish_failure_disconnect_user_queue_with_failing_offline_policy() {
    connected_state_qos1_publish_failure_disconnect_user_queue_with_failing_offline_policy_helper();
}

define_operation_failure_disconnect_user_queue_with_failing_offline_policy_helper!(
        connected_state_qos2_publish_failure_disconnect_user_queue_with_failing_offline_policy_helper,
        build_qos2_publish_success_packet,
        publish,
        PublishOptions,
        PreserveNothing
    );

#[test]
fn connected_state_qos2_publish_failure_disconnect_user_queue_with_failing_offline_policy() {
    connected_state_qos2_publish_failure_disconnect_user_queue_with_failing_offline_policy_helper();
}

macro_rules! define_operation_failure_disconnect_current_operation_with_failing_offline_policy_helper {
        ($test_helper_name: ident, $build_operation_function_name: ident, $operation_api: ident, $operation_options_type: ident, $offline_policy: ident) => {
            fn $test_helper_name() {
                let mut config = build_standard_test_config();
                config.offline_queue_policy = OfflineQueuePolicy::$offline_policy;

                let mut fixture = ProtocolStateTestFixture::new(config);
                assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

                let packet = $build_operation_function_name();

                let operation_result_receiver = fixture.$operation_api(0, packet, $operation_options_type{ ..Default::default() }).unwrap();

                assert!(operation_result_receiver.try_recv().is_err());
                assert_eq!(1, fixture.client_state.user_operation_queue.len());

                let service_result = fixture.service_once(10, 10);
                assert!(service_result.is_ok());
                assert!(operation_result_receiver.try_recv().is_err());
                assert_eq!(0, fixture.client_state.user_operation_queue.len());
                assert!(fixture.client_state.current_operation.is_some());

                assert!(fixture.on_connection_closed(0).is_ok());
                verify_protocol_state_empty(&fixture);

                let result = operation_result_receiver.recv();
                assert!(!result.is_err());

                let operation_result = result.unwrap();
                assert!(operation_result.is_err());

                assert_matches!(operation_result.unwrap_err(), GneissError::OfflineQueuePolicyFailed(_));
                verify_protocol_state_empty(&fixture);
            }
        };
    }

define_operation_failure_disconnect_current_operation_with_failing_offline_policy_helper!(
        connected_state_subscribe_failure_disconnect_current_operation_with_failing_offline_policy_helper,
        build_subscribe_success_packet,
        subscribe,
        SubscribeOptions,
        PreserveQos1PlusPublishes
    );

#[test]
fn connected_state_subscribe_failure_disconnect_current_operation_with_failing_offline_policy() {
    connected_state_subscribe_failure_disconnect_current_operation_with_failing_offline_policy_helper();
}

define_operation_failure_disconnect_current_operation_with_failing_offline_policy_helper!(
        connected_state_unsubscribe_failure_disconnect_current_operation_with_failing_offline_policy_helper,
        build_unsubscribe_success_packet,
        unsubscribe,
        UnsubscribeOptions,
        PreserveQos1PlusPublishes
    );

#[test]
fn connected_state_unsubscribe_failure_disconnect_current_operation_with_failing_offline_policy() {
    connected_state_unsubscribe_failure_disconnect_current_operation_with_failing_offline_policy_helper();
}

define_operation_failure_disconnect_current_operation_with_failing_offline_policy_helper!(
        connected_state_qos0_publish_failure_disconnect_current_operation_with_failing_offline_policy_helper,
        build_qos0_publish_success_packet,
        publish,
        PublishOptions,
        PreserveAcknowledged
    );

#[test]
fn connected_state_qos0_publish_failure_disconnect_current_operation_with_failing_offline_policy() {
    connected_state_qos0_publish_failure_disconnect_current_operation_with_failing_offline_policy_helper();
}

define_operation_failure_disconnect_current_operation_with_failing_offline_policy_helper!(
        connected_state_qos1_publish_failure_disconnect_current_operation_with_failing_offline_policy_helper,
        build_qos1_publish_success_packet,
        publish,
        PublishOptions,
        PreserveNothing
    );

#[test]
fn connected_state_qos1_publish_failure_disconnect_current_operation_with_failing_offline_policy() {
    connected_state_qos1_publish_failure_disconnect_current_operation_with_failing_offline_policy_helper();
}

define_operation_failure_disconnect_current_operation_with_failing_offline_policy_helper!(
        connected_state_qos2_publish_failure_disconnect_current_operation_with_failing_offline_policy_helper,
        build_qos2_publish_success_packet,
        publish,
        PublishOptions,
        PreserveNothing
    );

#[test]
fn connected_state_qos2_publish_failure_disconnect_current_operation_with_failing_offline_policy() {
    connected_state_qos2_publish_failure_disconnect_current_operation_with_failing_offline_policy_helper();
}

macro_rules! define_operation_failure_disconnect_pending_with_failing_offline_policy_helper {
        ($test_helper_name: ident, $build_operation_function_name: ident, $operation_api: ident, $operation_options_type: ident, $offline_policy: ident) => {
            fn $test_helper_name() {
                let mut config = build_standard_test_config();
                config.offline_queue_policy = OfflineQueuePolicy::$offline_policy;

                let mut fixture = ProtocolStateTestFixture::new(config);
                assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

                let packet = $build_operation_function_name();

                let operation_result_receiver = fixture.$operation_api(0, packet, $operation_options_type{ ..Default::default() }).unwrap();

                assert!(operation_result_receiver.try_recv().is_err());
                assert_eq!(1, fixture.client_state.user_operation_queue.len());

                let service_result = fixture.service_once(10, 4096);
                assert!(service_result.is_ok());
                assert!(operation_result_receiver.try_recv().is_err());
                assert_eq!(0, fixture.client_state.user_operation_queue.len());
                assert!(fixture.client_state.current_operation.is_none());
                if OfflineQueuePolicy::$offline_policy != OfflineQueuePolicy::PreserveAcknowledged {
                    assert_eq!(1, fixture.client_state.allocated_packet_ids.len());
                    assert_eq!(1, fixture.client_state.pending_non_publish_operations.len());
                }

                assert!(fixture.on_connection_closed(0).is_ok());
                verify_protocol_state_empty(&fixture);

                let result = operation_result_receiver.recv();
                assert!(!result.is_err());

                let operation_result = result.unwrap();
                assert!(operation_result.is_err());

                assert_matches!(operation_result.unwrap_err(), GneissError::OfflineQueuePolicyFailed(_));
                verify_protocol_state_empty(&fixture);
            }
        };
    }

define_operation_failure_disconnect_pending_with_failing_offline_policy_helper!(
        connected_state_subscribe_failure_disconnect_pending_with_failing_offline_policy_helper,
        build_subscribe_success_packet,
        subscribe,
        SubscribeOptions,
        PreserveQos1PlusPublishes
    );

#[test]
fn connected_state_subscribe_failure_disconnect_pending_with_failing_offline_policy() {
    connected_state_subscribe_failure_disconnect_pending_with_failing_offline_policy_helper();
}

define_operation_failure_disconnect_pending_with_failing_offline_policy_helper!(
        connected_state_unsubscribe_failure_disconnect_pending_with_failing_offline_policy_helper,
        build_unsubscribe_success_packet,
        unsubscribe,
        UnsubscribeOptions,
        PreserveQos1PlusPublishes
    );

#[test]
fn connected_state_unsubscribe_failure_disconnect_pending_with_failing_offline_policy() {
    connected_state_unsubscribe_failure_disconnect_pending_with_failing_offline_policy_helper();
}

define_operation_failure_disconnect_pending_with_failing_offline_policy_helper!(
        connected_state_qos0_publish_failure_disconnect_pending_with_failing_offline_policy_helper,
        build_qos0_publish_success_packet,
        publish,
        PublishOptions,
        PreserveAcknowledged
    );

#[test]
fn connected_state_qos0_publish_failure_disconnect_pending_with_failing_offline_policy() {
    connected_state_qos0_publish_failure_disconnect_pending_with_failing_offline_policy_helper();
}

macro_rules! define_acked_publish_failure_disconnect_pending_no_session_resumption_with_failing_offline_policy_helper {
        ($test_helper_name: ident, $build_operation_function_name: ident, $qos2_pubrel_timeout: expr) => {
            fn $test_helper_name() {
                let mut config = build_standard_test_config();
                config.offline_queue_policy = OfflineQueuePolicy::PreserveNothing;

                let mut fixture = ProtocolStateTestFixture::new(config);
                assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

                let packet = $build_operation_function_name();

                let operation_result_receiver = fixture.publish(0, packet, PublishOptions{ ..Default::default() }).unwrap();

                assert!(operation_result_receiver.try_recv().is_err());
                assert_eq!(1, fixture.client_state.user_operation_queue.len());

                let service_result = fixture.service_once(10, 4096);
                assert!(service_result.is_ok());
                if $qos2_pubrel_timeout {
                    assert!(fixture.on_incoming_bytes(20, service_result.unwrap().as_slice()).is_ok());
                    assert!(fixture.service_with_drain(30, 4096).is_ok());
                }

                assert!(operation_result_receiver.try_recv().is_err());
                assert_eq!(0, fixture.client_state.user_operation_queue.len());
                assert!(fixture.client_state.current_operation.is_none());
                assert_eq!(1, fixture.client_state.allocated_packet_ids.len());
                assert_eq!(1, fixture.client_state.pending_publish_operations.len());
                assert_eq!(0, fixture.client_state.pending_non_publish_operations.len());

                assert!(fixture.on_connection_closed(0).is_ok());
                assert_eq!(1, fixture.client_state.resubmit_operation_queue.len());
                let operation_id = fixture.client_state.resubmit_operation_queue.get(0).unwrap();
                let operation = fixture.client_state.operations.get(operation_id).unwrap();
                if let MqttPacket::Publish(publish) = &*operation.packet {
                    assert_eq!(true, publish.duplicate);
                    assert!(publish.packet_id != 0);
                } else {
                    panic!("Expected publish!");
                }

                assert!(fixture.on_connection_opened(10).is_ok());
                assert!(fixture.service_round_trip(20, 30, 4096).is_ok());
                assert!(fixture.service_round_trip(40, 50, 4096).is_ok());

                let result = operation_result_receiver.recv();
                assert!(!result.is_err());

                let operation_result = result.unwrap();
                assert_matches!(operation_result.unwrap_err(), GneissError::OfflineQueuePolicyFailed(_));
            }
        };
    }

define_acked_publish_failure_disconnect_pending_no_session_resumption_with_failing_offline_policy_helper!(
        connected_state_qos1_publish_failure_disconnect_pending_no_session_resumption_with_failing_offline_policy_helper,
        build_qos1_publish_success_packet,
        false
    );

#[test]
fn connected_state_qos1_publish_failure_disconnect_pending_no_session_resumption_with_failing_offline_policy() {
    connected_state_qos1_publish_failure_disconnect_pending_no_session_resumption_with_failing_offline_policy_helper();
}

define_acked_publish_failure_disconnect_pending_no_session_resumption_with_failing_offline_policy_helper!(
        connected_state_qos2_publish_failure_disconnect_pending_no_session_resumption_with_failing_offline_policy_helper,
        build_qos2_publish_success_packet,
        false
    );

#[test]
fn connected_state_qos2_publish_failure_disconnect_pending_no_session_resumption_failing_offline_policy() {
    connected_state_qos2_publish_failure_disconnect_pending_no_session_resumption_with_failing_offline_policy_helper();
}

define_acked_publish_failure_disconnect_pending_no_session_resumption_with_failing_offline_policy_helper!(
        connected_state_qos2_publish_failure_disconnect_pubrel_pending_no_session_resumption_with_failing_offline_policy_helper,
        build_qos2_publish_success_packet,
        true
    );

#[test]
fn connected_state_qos2_publish_failure_disconnect_pubrel_pending_no_session_resumption_failing_offline_policy() {
    connected_state_qos2_publish_failure_disconnect_pubrel_pending_no_session_resumption_with_failing_offline_policy_helper();
}

// verify duplicate set and resubmit queue

macro_rules! define_acked_publish_success_disconnect_pending_with_session_resumption_helper {
        ($test_helper_name: ident, $build_operation_function_name: ident, $verify_result_function_name: ident, $qos2_pubrel_timeout: expr) => {
            fn $test_helper_name() {
                let mut config = build_standard_test_config();
                config.offline_queue_policy = OfflineQueuePolicy::PreserveNothing;
                config.connect_options.rejoin_session_policy = RejoinSessionPolicy::PostSuccess;

                let mut fixture = ProtocolStateTestFixture::new(config);
                fixture.broker_packet_handlers.insert(PacketType::Connect, Box::new(handle_connect_with_session_resumption));

                assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

                let packet = $build_operation_function_name();

                let operation_result_receiver = fixture.publish(0, packet, PublishOptions{ ..Default::default() }).unwrap();

                assert!(operation_result_receiver.try_recv().is_err());
                assert_eq!(1, fixture.client_state.user_operation_queue.len());

                let service_result = fixture.service_with_drain(10, 4096);
                assert!(service_result.is_ok());
                if $qos2_pubrel_timeout { // we want to interrupt pubrel
                    assert!(fixture.on_incoming_bytes(20, service_result.unwrap().as_slice()).is_ok());
                    assert!(fixture.service_with_drain(30, 4096).is_ok());
                }

                assert!(operation_result_receiver.try_recv().is_err());
                assert_eq!(0, fixture.client_state.user_operation_queue.len());
                assert!(fixture.client_state.current_operation.is_none());
                assert_eq!(1, fixture.client_state.allocated_packet_ids.len());
                assert_eq!(1, fixture.client_state.pending_publish_operations.len());
                assert_eq!(0, fixture.client_state.pending_non_publish_operations.len());

                assert!(fixture.on_connection_closed(0).is_ok());
                assert_eq!(1, fixture.client_state.resubmit_operation_queue.len());
                let operation_id = fixture.client_state.resubmit_operation_queue.get(0).unwrap();
                let operation = fixture.client_state.operations.get(operation_id).unwrap();
                if let MqttPacket::Publish(publish) = &*operation.packet {
                    assert_eq!(true, publish.duplicate);
                    assert!(publish.packet_id != 0);
                } else {
                    panic!("Expected publish!");
                }

                if $qos2_pubrel_timeout {
                    assert!(operation.qos2_pubrel.is_some());
                }

                assert!(fixture.on_connection_opened(10).is_ok());
                assert!(fixture.service_round_trip(20, 30, 4096).is_ok()); // connect -> connack
                assert!(fixture.service_round_trip(40, 50, 4096).is_ok()); // publish -> puback/pubrec
                assert!(fixture.service_round_trip(60, 70, 4096).is_ok()); // Optional: pubrel -> pubcomp

                let result = operation_result_receiver.recv();
                assert!(!result.is_err());

                let operation_result = result.unwrap().unwrap();

                $verify_result_function_name(operation_result);

                let (_, first_publish_packet) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Publish, 1, None, None).unwrap();
                let packet_id =
                    match first_publish_packet {
                        MqttPacket::Publish(first_publish) => {
                            assert_eq!(false, first_publish.duplicate);
                            first_publish.packet_id
                        }
                        _ => {
                            panic!("Expected publish");
                        }
                    };

                if $qos2_pubrel_timeout {
                    assert!(find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Publish, 2, None, None).is_none());
                    let (_, first_pubrel_packet) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Pubrel, 1, None, None).unwrap();
                    if let MqttPacket::Pubrel(first_pubrel) = first_pubrel_packet {
                        assert_eq!(packet_id, first_pubrel.packet_id);
                    } else {
                        panic!("Expected pubrel");
                    }

                    let (_, second_pubrel_packet) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Pubrel, 2, None, None).unwrap();
                    if let MqttPacket::Pubrel(second_pubrel) = second_pubrel_packet {
                        assert_eq!(packet_id, second_pubrel.packet_id);
                    } else {
                        panic!("Expected pubrel");
                    }
                } else {
                    let (_, second_publish_packet) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Publish, 2, None, None).unwrap();
                    if let MqttPacket::Publish(second_publish) = second_publish_packet {
                        assert!(second_publish.duplicate);
                        assert_eq!(second_publish.packet_id, packet_id);
                    } else {
                        panic!("Expected publish");
                    }
                }
            }
        };
    }

fn verify_qos1_publish_session_resumption_result(result: PublishResponse) {
    if let PublishResponse::Qos1(puback) = result {
        assert_eq!(PubackReasonCode::Success, puback.reason_code);
    } else {
        panic!("Expected puback");
    }
}

define_acked_publish_success_disconnect_pending_with_session_resumption_helper!(
        connected_state_qos1_publish_success_disconnect_pending_with_session_resumption_helper,
        build_qos1_publish_success_packet,
        verify_qos1_publish_session_resumption_result,
        false
    );

#[test]
fn connected_state_qos1_publish_success_disconnect_pending_with_session_resumption() {
    connected_state_qos1_publish_success_disconnect_pending_with_session_resumption_helper();
}

fn verify_qos2_publish_session_resumption_result(result: PublishResponse) {
    if let PublishResponse::Qos2(Qos2Response::Pubcomp(pubcomp)) = result {
        assert_eq!(PubcompReasonCode::Success, pubcomp.reason_code);
    } else {
        panic!("Expected pubcomp");
    }
}

define_acked_publish_success_disconnect_pending_with_session_resumption_helper!(
        connected_state_qos2_publish_success_disconnect_pending_with_session_resumption_helper,
        build_qos2_publish_success_packet,
        verify_qos2_publish_session_resumption_result,
        false
    );

#[test]
fn connected_state_qos2_publish_success_disconnect_pending_with_session_resumption() {
    connected_state_qos2_publish_success_disconnect_pending_with_session_resumption_helper();
}


define_acked_publish_success_disconnect_pending_with_session_resumption_helper!(
        connected_state_qos2_publish_success_disconnect_pubrel_pending_with_session_resumption_helper,
        build_qos2_publish_success_packet,
        verify_qos2_publish_session_resumption_result,
        true
    );

#[test]
fn connected_state_qos2_publish_success_disconnect_pubrel_pending_with_session_resumption() {
    connected_state_qos2_publish_success_disconnect_pubrel_pending_with_session_resumption_helper();
}

#[test]
fn connected_state_user_disconnect_success_empty_queues() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    // submit a disconnect
    let disconnect = DisconnectPacket {
        reason_code: DisconnectReasonCode::DisconnectWithWillMessage,
        ..Default::default()
    };

    assert!(fixture.disconnect(0, disconnect).is_ok());
    assert_eq!(1, fixture.client_state.high_priority_operation_queue.len());

    // process it but don't write complete yet, verify state expectations
    assert!(fixture.service_once(10, 4096).is_ok());
    assert_eq!(ProtocolStateType::PendingDisconnect, fixture.client_state.state);
    assert_eq!(1, fixture.client_state.pending_write_completion_operations.len());

    // write complete and verify final state
    assert_matches!(fixture.on_write_completion(20), Err(GneissError::UserInitiatedDisconnect(_)));
    assert_eq!(ProtocolStateType::Halted, fixture.client_state.state);
    verify_protocol_state_empty(&fixture);
}


#[test]
fn connected_state_user_disconnect_success_non_empty_queues() {
    let mut config = build_standard_test_config();
    config.offline_queue_policy = OfflineQueuePolicy::PreserveQos1PlusPublishes;
    config.connect_options.rejoin_session_policy = RejoinSessionPolicy::PostSuccess;

    let mut fixture = ProtocolStateTestFixture::new(config);
    fixture.broker_packet_handlers.insert(PacketType::Connect, Box::new(handle_connect_with_session_resumption));

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    // (1)-(6) set up so that there's a user operation, resubmit operation and high priority operation

    // (1) submit three publishes
    //     qos2 in the lead; the pubrel will become the high priority operation
    let qos2_publish = PublishPacket {
        qos: QualityOfService::ExactlyOnce,
        topic: "hello/world".to_string(),
        payload: Some("qos2".as_bytes().to_vec()),
        ..Default::default()
    };

    assert!(fixture.publish(10, qos2_publish, PublishOptions{ ..Default::default() }).is_ok());

    // the second publish will become the current operation (no easy way to prevent that)
    let first_qos1_publish = PublishPacket {
        qos: QualityOfService::AtLeastOnce,
        topic: "hello/world".to_string(),
        payload: Some("qos1-1".as_bytes().to_vec()),
        ..Default::default()
    };

    assert!(fixture.publish(10, first_qos1_publish, PublishOptions{ ..Default::default() }).is_ok());

    // the third publish will become a resubmit queue entry
    let second_qos1_publish = PublishPacket {
        qos: QualityOfService::AtLeastOnce,
        topic: "hello/world".to_string(),
        payload: Some("qos1-2".as_bytes().to_vec()),
        ..Default::default()
    };

    assert!(fixture.publish(10, second_qos1_publish, PublishOptions{ ..Default::default() }).is_ok());
    assert_eq!(3, fixture.client_state.user_operation_queue.len());

    // (2) service so that all three are processed and moved to the unacked_publish list
    assert!(fixture.service_once(20, 4096).is_ok());
    assert_eq!(3, fixture.client_state.pending_publish_operations.len());
    assert_eq!(0, fixture.client_state.user_operation_queue.len());

    // (3) connection closed, which should move all three to the resubmit queue
    assert!(fixture.on_connection_closed(30).is_ok());
    assert_eq!(0, fixture.client_state.pending_publish_operations.len());
    assert_eq!(3, fixture.client_state.resubmit_operation_queue.len());

    // (4) reconnect and rejoin the session
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 40).is_ok());
    assert_eq!(3, fixture.client_state.resubmit_operation_queue.len());

    // (5) service carefully so that only the qos2 goes out and we process the response from
    //     the fake broker.
    let to_broker_bytes = fixture.service_once(50,25).unwrap();
    let to_client_bytes = fixture.write_to_socket(to_broker_bytes.as_slice()).unwrap();
    assert!(fixture.on_write_completion(55).is_ok());
    assert!(fixture.on_incoming_bytes(55, to_client_bytes.as_slice()).is_ok());

    // we should be partway through the second operation and we should have received a pubrec
    // leading to the pubrel going into the high priority operation queue
    assert!(fixture.client_state.current_operation.is_some());
    assert_eq!(1, fixture.client_state.resubmit_operation_queue.len());
    assert_eq!(1, fixture.client_state.high_priority_operation_queue.len());
    assert_eq!(0, fixture.client_state.user_operation_queue.len());

    // (6) add a publish so that something is in the user queue
    let third_qos1_publish = PublishPacket {
        qos: QualityOfService::AtLeastOnce,
        topic: "hello/world".to_string(),
        payload: Some("qos1-3".as_bytes().to_vec()),
        ..Default::default()
    };

    assert!(fixture.publish(60, third_qos1_publish, PublishOptions{ ..Default::default() }).is_ok());
    assert_eq!(1, fixture.client_state.user_operation_queue.len());

    // (7) submit a disconnect
    let disconnect = DisconnectPacket {
        reason_code: DisconnectReasonCode::DisconnectWithWillMessage,
        ..Default::default()
    };

    assert!(fixture.disconnect(0, disconnect).is_ok());
    assert_eq!(2, fixture.client_state.high_priority_operation_queue.len());

    // (8) do a complete service
    let disconnect_service_result = fixture.service_round_trip(70, 80, 4096);
    assert_matches!(disconnect_service_result, Err(GneissError::UserInitiatedDisconnect(_)));

    // (9) verify the current operation got sent, the disconnect was sent and nothing else happened
    assert_eq!(ProtocolStateType::Halted, fixture.client_state.state);
    assert_eq!(1, fixture.client_state.resubmit_operation_queue.len());
    assert_eq!(1, fixture.client_state.high_priority_operation_queue.len());
    assert_eq!(1, fixture.client_state.user_operation_queue.len());
    assert!(fixture.client_state.current_operation.is_none());


    // After the second connect, the outbound packet stream should be:
    //   connect, qos 2 publish, qos 1 publish 1, disconnect
    let (second_connect_index, _) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Connect, 2, None, None).unwrap();

    let packet_types = vec!(PacketType::Connect, PacketType::Publish, PacketType::Publish, PacketType::Disconnect);
    verify_packet_type_sequence(fixture.to_broker_packet_stream.iter(), packet_types.into_iter(), Some(second_connect_index));

    let (_, expected_qos2_publish) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Publish, 1, Some(second_connect_index), None).unwrap();
    if let MqttPacket::Publish(expected_qos2) = expected_qos2_publish {
        assert_eq!(QualityOfService::ExactlyOnce, expected_qos2.qos);
        assert_eq!(Some("qos2".as_bytes().to_vec()), expected_qos2.payload);
    } else {
        panic!("Expected qos 2 publish after reconnect");
    }

    let (_, expected_qos1_publish) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Publish, 2, Some(second_connect_index), None).unwrap();
    if let MqttPacket::Publish(expected_qos1) = expected_qos1_publish {
        assert_eq!(QualityOfService::AtLeastOnce, expected_qos1.qos);
        assert_eq!(Some("qos1-1".as_bytes().to_vec()), expected_qos1.payload);
    } else {
        panic!("Expected qos 1 publish after reconnect");
    }
}


#[test]
fn connected_state_user_disconnect_failure_invalid_packet() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    fixture.broker_packet_handlers.insert(PacketType::Connect, Box::new(handle_connect_with_tiny_maximum_packet_size));

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    let disconnect = DisconnectPacket {
        reason_code: DisconnectReasonCode::DisconnectWithWillMessage,
        reason_string: Some("This exceeds the maximum packet length".to_string()),
        ..Default::default()
    };

    assert!(fixture.disconnect(10, disconnect).is_ok());

    assert_matches!(fixture.service_round_trip(0, 0, 4096), Err(GneissError::UserInitiatedDisconnect(_)));
    assert_eq!(ProtocolStateType::Halted, fixture.client_state.state);

    verify_protocol_state_empty(&fixture);

    // we didn't send it, it was invalid
    assert!(find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Disconnect, 1, None, None).is_none());
}

#[test]
fn connected_state_server_disconnect() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    let server_disconnect = DisconnectPacket {
        reason_code: DisconnectReasonCode::UseAnotherServer,
        reason_string: Some("You're weird".to_string()),
        ..Default::default()
    };

    let encoding_context = EncodingContext {
        outbound_alias_resolution: OutboundAliasResolution {
            skip_topic: false,
            alias: None,
        }
    };

    let packet = MqttPacket::Disconnect(server_disconnect.clone());
    assert!(fixture.broker_encoder.reset(&packet, &encoding_context).is_ok());

    let mut encoded_buffer = Vec::with_capacity(4096);
    let encode_result = fixture.broker_encoder.encode(&packet, &mut encoded_buffer).unwrap();
    assert_eq!(EncodeResult::Complete, encode_result);

    assert_matches!(fixture.on_incoming_bytes(10, encoded_buffer.as_slice()), Err(GneissError::ConnectionClosed(_)));
    verify_protocol_state_empty(&fixture);

    assert_eq!(2, fixture.client_packet_events.len());
    let client_event = fixture.client_packet_events.get(1).unwrap();
    if let PacketEvent::Disconnect(incoming_disconnect) = client_event {
        assert_eq!(server_disconnect, *incoming_disconnect);
    } else {
        panic!("Expected disconnect client event")
    }
}

fn rejoin_session_test_build_clean_start_sequence(rejoin_policy : RejoinSessionPolicy) -> Vec<bool> {
    let mut config = build_standard_test_config();
    config.connect_options.rejoin_session_policy = rejoin_policy;

    let mut fixture = ProtocolStateTestFixture::new(config);
    fixture.broker_packet_handlers.insert(PacketType::Connect, Box::new(handle_connect_with_session_resumption));

    for i in 0..4 {
        assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, i * 10).is_ok());
        assert!(fixture.on_connection_closed(i * 10).is_ok());
    }

    assert_eq!(4, fixture.to_broker_packet_stream.len());

    fixture.to_broker_packet_stream.iter().map(|packet| {
        if let MqttPacket::Connect(connect) = &**packet {
            connect.clean_start
        } else {
            panic!("Expected connect packet");
        }
    }).collect()
}

#[test]
fn connected_state_rejoin_session_policy_always() {
    let clean_starts = rejoin_session_test_build_clean_start_sequence(RejoinSessionPolicy::Always);
    assert_eq!(vec!(false, false, false, false), clean_starts);
}

#[test]
fn connected_state_rejoin_session_policy_never() {
    let clean_starts = rejoin_session_test_build_clean_start_sequence(RejoinSessionPolicy::Never);
    assert_eq!(vec!(true, true, true, true), clean_starts);
}

#[test]
fn connected_state_rejoin_session_policy_post_success() {
    let clean_starts = rejoin_session_test_build_clean_start_sequence(RejoinSessionPolicy::PostSuccess);
    assert_eq!(vec!(true, false, false, false), clean_starts);
}

#[test]
fn connected_state_maximum_inflight_publish_limit_respected() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    fixture.broker_packet_handlers.insert(PacketType::Connect, Box::new(handle_connect_with_low_receive_maximum));

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());
    if let Some(settings) = &fixture.client_state.current_settings {
        assert_eq!(1, settings.receive_maximum_from_server);
    } else {
        panic!("Expected settings");
    }

    let publish1 = PublishPacket {
        qos: QualityOfService::ExactlyOnce,
        topic: "hello/world".to_string(),
        payload: Some("qos2".as_bytes().to_vec()),
        ..Default::default()
    };

    let publish2 = PublishPacket {
        qos: QualityOfService::AtLeastOnce,
        topic: "hello/world".to_string(),
        payload: Some("qos1-1".as_bytes().to_vec()),
        ..Default::default()
    };

    let publish3 = PublishPacket {
        qos: QualityOfService::AtLeastOnce,
        topic: "hello/world".to_string(),
        payload: Some("qos1-2".as_bytes().to_vec()),
        ..Default::default()
    };

    let result1 = fixture.publish(10, publish1, PublishOptions{ ..Default::default() }).unwrap();
    let result2 = fixture.publish(10, publish2, PublishOptions{ ..Default::default() }).unwrap();
    let result3 = fixture.publish(10, publish3, PublishOptions{ ..Default::default() }).unwrap();

    let response1a_bytes = fixture.service_with_drain(20, 4096).unwrap();
    assert!(fixture.get_next_service_time(20).is_none());
    assert_eq!(0, fixture.service_with_drain(30, 4096).unwrap().len());
    assert_eq!(0, fixture.service_with_drain(300, 4096).unwrap().len());
    assert_eq!(0, fixture.service_with_drain(3000, 4096).unwrap().len());
    assert_eq!(1, fixture.client_state.allocated_packet_ids.len());
    assert_eq!(1, fixture.client_state.pending_publish_operations.len());
    assert_eq!(2, fixture.client_state.user_operation_queue.len());
    assert!(fixture.client_state.current_operation.is_none());
    assert!(result1.try_recv().is_err());
    assert!(result2.try_recv().is_err());
    assert!(result3.try_recv().is_err());

    assert!(fixture.on_incoming_bytes(3000, response1a_bytes.as_slice()).is_ok());
    assert_eq!(Some(3000), fixture.get_next_service_time(3000));

    let response1b_bytes = fixture.service_with_drain(4000, 4096).unwrap();
    assert!(fixture.get_next_service_time(4000).is_none());
    assert_eq!(0, fixture.service_with_drain(5000, 4096).unwrap().len());
    assert_eq!(0, fixture.service_with_drain(6000, 4096).unwrap().len());
    assert_eq!(0, fixture.service_with_drain(7000, 4096).unwrap().len());
    assert_eq!(1, fixture.client_state.allocated_packet_ids.len());
    assert_eq!(1, fixture.client_state.pending_publish_operations.len());
    assert_eq!(2, fixture.client_state.user_operation_queue.len());
    assert!(fixture.client_state.current_operation.is_none());
    assert!(result1.try_recv().is_err());
    assert!(result2.try_recv().is_err());
    assert!(result3.try_recv().is_err());

    assert!(fixture.on_incoming_bytes(10000, response1b_bytes.as_slice()).is_ok());
    assert_eq!(Some(10000), fixture.get_next_service_time(10000));
    assert_eq!(0, fixture.client_state.allocated_packet_ids.len());
    assert_eq!(0, fixture.client_state.pending_publish_operations.len());
    assert!(result1.try_recv().is_ok());
    assert!(result2.try_recv().is_err());
    assert!(result3.try_recv().is_err());

    let response2_bytes = fixture.service_with_drain(15000, 4096).unwrap();
    assert!(fixture.get_next_service_time(15000).is_none());
    assert_eq!(0, fixture.service_with_drain(16000, 4096).unwrap().len());
    assert_eq!(0, fixture.service_with_drain(17000, 4096).unwrap().len());
    assert_eq!(1, fixture.client_state.allocated_packet_ids.len());
    assert_eq!(1, fixture.client_state.pending_publish_operations.len());
    assert_eq!(1, fixture.client_state.user_operation_queue.len());
    assert!(fixture.client_state.current_operation.is_none());
    assert!(result2.try_recv().is_err());
    assert!(result3.try_recv().is_err());

    assert!(fixture.on_incoming_bytes(20000, response2_bytes.as_slice()).is_ok());
    assert_eq!(Some(20000), fixture.get_next_service_time(20000));
    assert_eq!(0, fixture.client_state.allocated_packet_ids.len());
    assert_eq!(0, fixture.client_state.pending_publish_operations.len());
    assert!(result2.try_recv().is_ok());
    assert!(result3.try_recv().is_err());

    let response3_bytes = fixture.service_with_drain(25000, 4096).unwrap();
    assert!(fixture.get_next_service_time(25000).is_none());
    assert_eq!(0, fixture.service_with_drain(26000, 4096).unwrap().len());
    assert_eq!(0, fixture.service_with_drain(27000, 4096).unwrap().len());
    assert_eq!(1, fixture.client_state.allocated_packet_ids.len());
    assert_eq!(1, fixture.client_state.pending_publish_operations.len());
    assert_eq!(0, fixture.client_state.user_operation_queue.len());
    assert!(fixture.client_state.current_operation.is_none());
    assert!(result3.try_recv().is_err());

    assert!(fixture.on_incoming_bytes(30000, response3_bytes.as_slice()).is_ok());
    assert_eq!(0, fixture.client_state.allocated_packet_ids.len());
    assert_eq!(0, fixture.client_state.pending_publish_operations.len());
    assert!(result3.try_recv().is_ok());

    verify_protocol_state_empty(&fixture);
}

#[test]
fn connected_state_ack_order() {
    let mut config = build_standard_test_config();
    config.connect_options = ConnectOptions::builder().with_topic_alias_maximum(2).build();

    let mut fixture = ProtocolStateTestFixture::new(config);
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    let mut incoming_packet_buffer = Vec::with_capacity(4096);

    let incoming_packets: Vec<MqttPacket> = vec!(
        MqttPacket::Publish(PublishPacket{
            qos: QualityOfService::ExactlyOnce,
            packet_id: 1,
            topic: "topic1".to_string(),
            payload: Some("1".as_bytes().to_vec()),
            ..Default::default()
        }),
        MqttPacket::Publish(PublishPacket {
            qos: QualityOfService::AtLeastOnce,
            packet_id: 2,
            topic: "topic1".to_string(),
            payload: Some("2".as_bytes().to_vec()),
            ..Default::default()
        }),
        MqttPacket::Publish(PublishPacket {
            qos: QualityOfService::ExactlyOnce,
            packet_id: 3,
            topic: "topic2".to_string(),
            payload: Some("3".as_bytes().to_vec()),
            ..Default::default()
        }),
        MqttPacket::Publish(PublishPacket {
            qos: QualityOfService::AtMostOnce,
            packet_id: 0,
            topic: "topic3".to_string(),
            payload: Some("4".as_bytes().to_vec()),
            ..Default::default()
        }),
        MqttPacket::Publish(PublishPacket {
            qos: QualityOfService::AtLeastOnce,
            packet_id: 4,
            topic: "topic3".to_string(),
            payload: Some("5".as_bytes().to_vec()),
            ..Default::default()
        }),
        MqttPacket::Pubrel(PubrelPacket {
            packet_id: 1,
            ..Default::default()
        }),
        MqttPacket::Publish(PublishPacket {
            qos: QualityOfService::AtLeastOnce,
            packet_id: 5,
            topic: "topic3".to_string(),
            payload: Some("5".as_bytes().to_vec()),
            ..Default::default()
        }));

    let qos2_publish1_topic = "topic1".to_string();

    let mut lru_resolver  = OutboundAliasResolverFactory::new_lru_factory(2)();
    lru_resolver.reset_for_new_connection(2);

    let qos2_publish1 = incoming_packets[0].clone();

    assert!(encode_packet_to_buffer_with_alias_resolution(qos2_publish1, &mut incoming_packet_buffer, lru_resolver.resolve_and_apply_topic_alias(&None, &qos2_publish1_topic)).is_ok());
    assert!(fixture.on_incoming_bytes(10, incoming_packet_buffer.as_slice()).is_ok());
    assert_eq!(1, fixture.client_state.high_priority_operation_queue.len());
    assert!(fixture.service_with_drain(15, 4096).is_ok());
    assert_eq!(0, fixture.client_state.high_priority_operation_queue.len());
    assert_eq!(2, fixture.to_broker_packet_stream.len());

    incoming_packet_buffer.clear();

    incoming_packets.iter().skip(1).for_each(|packet| {
        let mut topic = "Panic".to_string();
        if let MqttPacket::Publish(publish) = packet {
            topic = publish.topic.clone();
        }
        assert!(encode_packet_to_buffer_with_alias_resolution(packet.clone(), &mut incoming_packet_buffer, lru_resolver.resolve_and_apply_topic_alias(&None, &topic)).is_ok());
    });

    assert!(fixture.on_incoming_bytes(20, incoming_packet_buffer.as_slice()).is_ok());
    assert!(fixture.service_round_trip(30, 40, 4096).is_ok());

    // check client events
    let packet_event_count = fixture.client_packet_events.len();
    assert_eq!(7, packet_event_count); // connack + 6 publishes
    let mut event_index = 1; // skip the connack
    for packet in &incoming_packets {
        if let MqttPacket::Publish(publish) = packet {
            let client_event = &fixture.client_packet_events[event_index];
            if let PacketEvent::Publish(publish_event) = client_event {
                assert_eq!(publish_event.packet_id, publish.packet_id);
                assert_eq!(publish_event.qos, publish.qos);
                assert_eq!(publish_event.topic, publish.topic);
                assert_eq!(publish_event.payload, publish.payload);
            } else {
                panic!("Expected publish client event");
            }

            event_index += 1;
        }
    }

    // check outbound ack sequence
    let expected_ack_sequence = vec!(
        MqttPacket::Pubrec(PubrecPacket{
            packet_id: 1,
            ..Default::default()
        }),
        MqttPacket::Puback(PubackPacket{
            packet_id: 2,
            ..Default::default()
        }),
        MqttPacket::Pubrec(PubrecPacket{
            packet_id: 3,
            ..Default::default()
        }),
        MqttPacket::Puback(PubackPacket{
            packet_id: 4,
            ..Default::default()
        }),
        MqttPacket::Pubcomp(PubcompPacket{
            packet_id: 1,
            ..Default::default()
        }),
        MqttPacket::Puback(PubackPacket{
            packet_id: 5,
            ..Default::default()
        }),
    );

    let sent_acks: Vec<MqttPacket> = fixture.to_broker_packet_stream.iter().skip(1).map(|boxed_packet|{ *boxed_packet.clone() }).collect();
    assert_eq!(expected_ack_sequence, sent_acks);
}

fn packet_to_sequence_number(packet: &MqttPacket) -> GneissResult<Option<u64>> {
    match packet {
        MqttPacket::Publish(publish) => {
            if let Some(payload) = &publish.payload {
                let payload_string = String::from_utf8(payload.clone()).unwrap();
                return Ok(Some(payload_string.parse::<u64>().unwrap()));
            }
        }
        MqttPacket::Subscribe(subscribe) => {
            if let Some(subscription) = subscribe.subscriptions.first() {
                return Ok(Some(subscription.topic_filter.parse::<u64>().unwrap()));
            }
        }
        MqttPacket::Unsubscribe(unsubscribe) => {
            if let Some(filter) = unsubscribe.topic_filters.first() {
                return Ok(Some(filter.parse::<u64>().unwrap()));
            }
        }
        MqttPacket::Pubrel(_) => {
            return Ok(None);
        }
        _ => {}
    }

    Err(GneissError::new_internal_state_error("unexpected packet type"))
}

struct MultiOperationContext {
    outbound_packets: Vec<MqttPacket>,
    to_client_bytes: Vec<u8>,

    publish_receivers: HashMap<u64, std::sync::mpsc::Receiver<PublishResult>>,
    subscribe_receivers: HashMap<u64, std::sync::mpsc::Receiver<SubscribeResult>>,
    unsubscribe_receivers: HashMap<u64, std::sync::mpsc::Receiver<UnsubscribeResult>>,
}

impl MultiOperationContext {
    fn new() -> Self {
        MultiOperationContext {
            outbound_packets: Vec::new(),
            to_client_bytes: Vec::new(),
            publish_receivers: HashMap::new(),
            subscribe_receivers: HashMap::new(),
            unsubscribe_receivers: HashMap::new(),
        }
    }

    fn find_packet_with_sequence_number(&self, sequence_number: u64) -> Option<&MqttPacket> {
        for packet in self.outbound_packets.iter() {
            if let Ok(Some(number)) = packet_to_sequence_number(packet) {
                if number == sequence_number {
                    return Some(packet);
                }
            }
        }

        None
    }
}

fn submit_sequenced_packet(fixture: &mut ProtocolStateTestFixture, context: &mut MultiOperationContext, packet_index: usize, elapsed: u64) {
    let packet = &context.outbound_packets[packet_index];
    match packet {
        MqttPacket::Publish(publish) => {
            let result = fixture.publish(elapsed, publish.clone(), PublishOptions{ ..Default::default() }).unwrap();
            context.publish_receivers.insert(packet_to_sequence_number(packet).unwrap().unwrap(), result);
        }
        MqttPacket::Subscribe(subscribe) => {
            let result = fixture.subscribe(elapsed, subscribe.clone(), SubscribeOptions{ ..Default::default() }).unwrap();
            context.subscribe_receivers.insert(packet_to_sequence_number(packet).unwrap().unwrap(), result);
        }
        MqttPacket::Unsubscribe(unsubscribe) => {
            let result = fixture.unsubscribe(elapsed, unsubscribe.clone(), UnsubscribeOptions{ ..Default::default() }).unwrap();
            context.unsubscribe_receivers.insert(packet_to_sequence_number(packet).unwrap().unwrap(), result);
        }
        _ => {}
    }
}

fn initialize_multi_operation_sequence_test(fixture: &mut ProtocolStateTestFixture) -> MultiOperationContext {
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    let mut context = MultiOperationContext::new();

    context.outbound_packets = vec!(
        // in-progress
        MqttPacket::Publish(PublishPacket{
            qos: QualityOfService::ExactlyOnce,
            topic: "topic1".to_string(),
            payload: Some("1".as_bytes().to_vec()),
            ..Default::default()
        }),
        MqttPacket::Publish(PublishPacket{
            qos: QualityOfService::AtLeastOnce,
            topic: "topic1".to_string(),
            payload: Some("2".as_bytes().to_vec()),
            ..Default::default()
        }),
        MqttPacket::Unsubscribe(UnsubscribePacket{
            topic_filters: vec!("3".to_string()),
            ..Default::default()
        }),
        MqttPacket::Publish(PublishPacket{
            qos: QualityOfService::ExactlyOnce,
            topic: "topic1".to_string(),
            payload: Some("4".as_bytes().to_vec()),
            ..Default::default()
        }),
        MqttPacket::Subscribe(SubscribePacket{
            subscriptions: vec!(Subscription{
                topic_filter: "5".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        MqttPacket::Publish(PublishPacket{
            qos: QualityOfService::AtMostOnce,
            topic: "topic1".to_string(),
            payload: Some("6".as_bytes().to_vec()),
            ..Default::default()
        }),

        // current
        MqttPacket::Publish(PublishPacket{
            qos: QualityOfService::AtLeastOnce,
            topic: "topic1".to_string().repeat(1024),
            payload: Some("7".as_bytes().to_vec()),
            ..Default::default()
        }),

        // user queue
        MqttPacket::Publish(PublishPacket{
            qos: QualityOfService::ExactlyOnce,
            topic: "topic1".to_string(),
            payload: Some("8".as_bytes().to_vec()),
            ..Default::default()
        }),
        MqttPacket::Unsubscribe(UnsubscribePacket{
            topic_filters: vec!("9".to_string()),
            ..Default::default()
        }),
        MqttPacket::Publish(PublishPacket{
            qos: QualityOfService::AtLeastOnce,
            topic: "topic1".to_string(),
            payload: Some("10".as_bytes().to_vec()),
            ..Default::default()
        }),
        MqttPacket::Subscribe(SubscribePacket{
            subscriptions: vec!(Subscription{
                topic_filter: "11".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        MqttPacket::Publish(PublishPacket{
            qos: QualityOfService::AtMostOnce,
            topic: "topic1".to_string(),
            payload: Some("12".as_bytes().to_vec()),
            ..Default::default()
        }),
    );

    submit_sequenced_packet(fixture, &mut context, 0, 10);
    assert!(fixture.service_round_trip(20, 30, 4096).is_ok());
    assert_eq!(1, fixture.client_state.high_priority_operation_queue.len());
    assert_eq!(1, fixture.client_state.allocated_packet_ids.len());

    for i in 1..7 {
        submit_sequenced_packet(fixture, &mut context, i, 50);
    }

    let to_server = fixture.service_once(50, 4096).unwrap();
    context.to_client_bytes = fixture.write_to_socket(to_server.as_slice()).unwrap();
    assert_eq!(0, fixture.client_state.high_priority_operation_queue.len());
    assert_eq!(1, fixture.client_state.pending_write_completion_operations.len());
    assert_eq!(3, fixture.client_state.pending_publish_operations.len());
    assert_eq!(2, fixture.client_state.pending_non_publish_operations.len());
    assert!(fixture.client_state.current_operation.is_some());
    assert_eq!(6, fixture.client_state.allocated_packet_ids.len()); // the current operation is also bound

    let packet_len = context.outbound_packets.len();
    for i in 7..packet_len {
        submit_sequenced_packet(fixture, &mut context, i, 150);
    }

    assert_eq!(5, fixture.client_state.user_operation_queue.len());

    context
}

#[test]
fn connected_state_multi_operation_sequence_simple_success() {
    let config = build_standard_test_config();
    let mut fixture = ProtocolStateTestFixture::new(config);

    let mut context = initialize_multi_operation_sequence_test(&mut fixture);
    assert!(fixture.on_write_completion(150).is_ok());
    assert!(fixture.on_incoming_bytes(300, context.to_client_bytes.as_slice()).is_ok());

    let to_client2 = fixture.service_with_drain(200, 65536).unwrap();
    assert!(fixture.on_incoming_bytes(300, to_client2.as_slice()).is_ok());
    assert!(fixture.service_round_trip(500, 500, 65536).is_ok());

    verify_protocol_state_empty(&fixture);

    for (_, result) in context.publish_receivers.iter_mut() {
        let result_value = result.try_recv().unwrap();
        assert!(result_value.is_ok());
    }

    for (_, result) in context.subscribe_receivers.iter_mut() {
        let result_value = result.try_recv().unwrap();
        assert!(result_value.is_ok());
    }
    for (_, result) in context.unsubscribe_receivers.iter_mut() {
        let result_value = result.try_recv().unwrap();
        assert!(result_value.is_ok());
    }

    let mut expected_next_sequence_id = 1;
    for packet in fixture.to_broker_packet_stream.iter().skip(1) {
        let sequence_id_option = packet_to_sequence_number(&**packet).unwrap();
        if let Some(sequence_id) = sequence_id_option {
            assert_eq!(expected_next_sequence_id, sequence_id);
            expected_next_sequence_id += 1;
        }
    }

    assert_eq!(13, expected_next_sequence_id);
}

fn is_qos1plus_publish(packet: &MqttPacket) -> bool {
    if let MqttPacket::Publish(publish) = packet {
        return publish.qos != QualityOfService::AtMostOnce;
    }

    false
}

fn verify_offline_failures_and_build_sorted_successful_sequence_id_list(context: &mut MultiOperationContext, session_present: bool, offline_queue_policy: OfflineQueuePolicy) -> Vec<u64> {
    // start off by dividing into successful and unsuccessful groups
    let mut failing_sequence_ids = Vec::new();
    let mut successful_sequence_ids = Vec::new();

    for (sequence_id, result) in context.publish_receivers.iter_mut() {
        let result_value = result.try_recv().unwrap();
        if result_value.is_ok() {
            successful_sequence_ids.push(*sequence_id);
        } else {
            assert_matches!(result_value, Err(GneissError::OfflineQueuePolicyFailed(_)));
            failing_sequence_ids.push(*sequence_id);
        }
    }

    for (sequence_id, result) in context.subscribe_receivers.iter_mut() {
        let result_value = result.try_recv().unwrap();
        if result_value.is_ok() {
            successful_sequence_ids.push(*sequence_id);
        } else {
            assert_matches!(result_value, Err(GneissError::OfflineQueuePolicyFailed(_)));
            failing_sequence_ids.push(*sequence_id);
        }
    }

    for (sequence_id, result) in context.unsubscribe_receivers.iter_mut() {
        let result_value = result.try_recv().unwrap();
        if result_value.is_ok() {
            successful_sequence_ids.push(*sequence_id);
        } else {
            assert_matches!(result_value, Err(GneissError::OfflineQueuePolicyFailed(_)));
            failing_sequence_ids.push(*sequence_id);
        }
    }

    // for each unsuccessful operation, verify that the associated packet fails the offline
    // queue policy
    for failing_sequence_id in &failing_sequence_ids {
        if let Some(packet) = context.find_packet_with_sequence_number(*failing_sequence_id) {
            assert!(!crate::protocol::does_packet_pass_offline_queue_policy(packet, &offline_queue_policy));
        } else {
            panic!("Expected packet with given sequence number");
        }
    }

    // for each successful operation, verify that the associated packet passes the offline
    // queue policy or we protocol compliance forced us to keep and resubmit it
    for successful_sequence_id in &successful_sequence_ids {
        if let Some(packet) = context.find_packet_with_sequence_number(*successful_sequence_id) {
            let passes_offline_policy = crate::protocol::does_packet_pass_offline_queue_policy(packet, &offline_queue_policy);
            let was_required_to_resubmit = *successful_sequence_id < 7 && session_present && is_qos1plus_publish(packet);

            assert!(passes_offline_policy || was_required_to_resubmit);
        } else {
            panic!("Expected packet with given sequence number");
        }
    }

    if session_present {
        // when we rejoin a session, the in-progress qos1+ publishes jump ahead of other
        // operations as part of session resumption spec compliance
        //
        // the net effect is sequence element 3 (unsubscribe) and 4 (qos 2 publish)
        // swap (1, 2, 4, 3, etc...) but let's compute it
        // abstractly rather than just hard code the change
        successful_sequence_ids.sort_by(|a, b|{
            let is_a_republish = *a < 7 && is_qos1plus_publish(&context.outbound_packets[(*a - 1) as usize]);
            let is_b_republish = *b < 7 && is_qos1plus_publish(&context.outbound_packets[(*b - 1) as usize]);
            if is_a_republish && !is_b_republish {
                Ordering::Less
            } else if !is_a_republish && is_b_republish {
                Ordering::Greater
            } else {
                a.cmp(b)
            }
        });
    } else {
        successful_sequence_ids.sort();
    }

    successful_sequence_ids
}

fn verify_successful_reconnect_sequencing(fixture: &ProtocolStateTestFixture, successful_sequence_ids: &[u64], second_connect_index: usize) {
    let mut expected_next_sequence_id_index = 0;
    for packet in fixture.to_broker_packet_stream.iter().skip(second_connect_index + 1) {
        let sequence_id_option = packet_to_sequence_number(packet).unwrap();
        if let Some(sequence_id) = sequence_id_option {
            assert_eq!(successful_sequence_ids[expected_next_sequence_id_index], sequence_id);
            expected_next_sequence_id_index += 1;
        }
    }

    assert_eq!(successful_sequence_ids.len(), expected_next_sequence_id_index);
}

fn do_connected_state_multi_operation_reconnect_test(offline_queue_policy: OfflineQueuePolicy, rejoin_session: bool) {
    let mut config = build_standard_test_config();
    config.connect_options.rejoin_session_policy = RejoinSessionPolicy::PostSuccess;
    config.offline_queue_policy = offline_queue_policy;

    let mut fixture = ProtocolStateTestFixture::new(config);
    if rejoin_session {
        fixture.broker_packet_handlers.insert(PacketType::Connect, Box::new(handle_connect_with_session_resumption));
    }

    let mut context = initialize_multi_operation_sequence_test(&mut fixture);
    assert!(fixture.on_connection_closed(150).is_ok());

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 200).is_ok());
    assert!(fixture.service_round_trip(500, 500, 65536).is_ok());
    assert!(fixture.service_round_trip(600, 500, 65536).is_ok());

    verify_protocol_state_empty(&fixture);
    if let Some(settings) = &fixture.client_state.current_settings {
        assert_eq!(rejoin_session, settings.rejoined_session);
    } else {
        panic!("Supposed to have settings");
    }

    let successful_sequence_ids = verify_offline_failures_and_build_sorted_successful_sequence_id_list(&mut context, rejoin_session, offline_queue_policy);

    let (second_connect_index, _) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Connect, 2, None, None).unwrap();

    // If we rejoin a session then we resume the qos 2 publish at the pubrel stage which
    // means the publish won't be present in the packet stream after the reconnect and so
    // we should skip it when verifying post-reconnect sequencing.  In the case though,
    // verify that the first post-connect packet we send is that pubrel.
    if rejoin_session {
        assert_eq!(1, successful_sequence_ids[0]);
        if let MqttPacket::Pubrel(_) = &*fixture.to_broker_packet_stream[second_connect_index + 1] {
        } else {
            panic!("Expected a pubrel after the reconnect");
        }
    }

    let check_sequence =
        if rejoin_session {
            &successful_sequence_ids.as_slice()[1..]
        } else {
            &successful_sequence_ids.as_slice()[0..]
        };

    verify_successful_reconnect_sequencing(&fixture, check_sequence, second_connect_index);
}

#[test]
fn connected_state_multi_operation_sequence_disconnect_no_session_present_offline_preserve_nothing () {
    do_connected_state_multi_operation_reconnect_test(OfflineQueuePolicy::PreserveNothing, false);
}

#[test]
fn connected_state_multi_operation_sequence_disconnect_session_present_offline_preserve_nothing () {
    do_connected_state_multi_operation_reconnect_test(OfflineQueuePolicy::PreserveNothing, true);
}

#[test]
fn connected_state_multi_operation_sequence_disconnect_no_session_present_offline_preserve_all () {
    do_connected_state_multi_operation_reconnect_test(OfflineQueuePolicy::PreserveAll, false);
}

#[test]
fn connected_state_multi_operation_sequence_disconnect_session_present_offline_preserve_all () {
    do_connected_state_multi_operation_reconnect_test(OfflineQueuePolicy::PreserveAll, true);
}

#[test]
fn connected_state_multi_operation_sequence_disconnect_no_session_present_offline_preserve_ack () {
    do_connected_state_multi_operation_reconnect_test(OfflineQueuePolicy::PreserveAcknowledged, false);
}

#[test]
fn connected_state_multi_operation_sequence_disconnect_session_present_offline_preserve_ack () {
    do_connected_state_multi_operation_reconnect_test(OfflineQueuePolicy::PreserveAcknowledged, true);
}

#[test]
fn connected_state_multi_operation_sequence_disconnect_no_session_present_offline_preserve_qos1plus () {
    do_connected_state_multi_operation_reconnect_test(OfflineQueuePolicy::PreserveQos1PlusPublishes, false);
}

#[test]
fn connected_state_multi_operation_sequence_disconnect_session_present_offline_preserve_qos1plus () {
    do_connected_state_multi_operation_reconnect_test(OfflineQueuePolicy::PreserveQos1PlusPublishes, true);
}

fn scoped_qos0_publish(fixture: &mut ProtocolStateTestFixture) {
    let publish = PublishPacket {
        qos: QualityOfService::AtMostOnce,
        topic: "hello/world".to_string(),
        ..Default::default()
    };

    let _ = fixture.publish(10, publish, PublishOptions{ ..Default::default() });
    assert_eq!(1, fixture.client_state.user_operation_queue.len());
}

#[test]
fn connected_state_qos0_publish_no_failure_on_dropped_receiver() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    scoped_qos0_publish(&mut fixture);

    assert!(fixture.service_round_trip(100, 150, 4096).is_ok());
    assert!(fixture.service_round_trip(200, 250, 4096).is_ok());

    verify_protocol_state_empty(&fixture);
}

fn scoped_qos1_publish(fixture: &mut ProtocolStateTestFixture) {
    let publish = PublishPacket {
        qos: QualityOfService::AtLeastOnce,
        topic: "hello/world".to_string(),
        ..Default::default()
    };

    let _ = fixture.publish(10, publish, PublishOptions{ ..Default::default() });
    assert_eq!(1, fixture.client_state.user_operation_queue.len());
}

#[test]
fn connected_state_qos1_publish_no_failure_on_dropped_receiver() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    scoped_qos1_publish(&mut fixture);

    assert!(fixture.service_round_trip(100, 150, 4096).is_ok());
    assert!(fixture.service_round_trip(200, 250, 4096).is_ok());

    verify_protocol_state_empty(&fixture);
}

fn scoped_qos2_publish(fixture: &mut ProtocolStateTestFixture) {
    let publish = PublishPacket {
        qos: QualityOfService::AtLeastOnce,
        topic: "hello/world".to_string(),
        ..Default::default()
    };

    let _ = fixture.publish(10, publish, PublishOptions{ ..Default::default() });
    assert_eq!(1, fixture.client_state.user_operation_queue.len());
}

#[test]
fn connected_state_qos2_publish_no_failure_on_dropped_receiver() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    scoped_qos2_publish(&mut fixture);

    assert!(fixture.service_round_trip(100, 150, 4096).is_ok());
    assert!(fixture.service_round_trip(200, 250, 4096).is_ok());

    verify_protocol_state_empty(&fixture);
}

fn scoped_subscribe(fixture: &mut ProtocolStateTestFixture) {
    let subscribe = SubscribePacket {
        subscriptions: vec!(
            Subscription {
                topic_filter: "hello/+".to_string(),
                qos: QualityOfService::ExactlyOnce,
                ..Default::default()
            }
        ),
        ..Default::default()
    };

    let _ = fixture.subscribe(10, subscribe, SubscribeOptions{ ..Default::default() });
    assert_eq!(1, fixture.client_state.user_operation_queue.len());
}

#[test]
fn connected_state_subscribe_no_failure_on_dropped_receiver() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    scoped_subscribe(&mut fixture);

    assert!(fixture.service_round_trip(100, 150, 4096).is_ok());
    assert!(fixture.service_round_trip(200, 250, 4096).is_ok());

    verify_protocol_state_empty(&fixture);
}

fn scoped_unsubscribe(fixture: &mut ProtocolStateTestFixture) {
    let unsubscribe = UnsubscribePacket {
        topic_filters: vec!("hello/+".to_string()),
        ..Default::default()
    };

    let _ = fixture.unsubscribe(10, unsubscribe, UnsubscribeOptions{ ..Default::default() });
    assert_eq!(1, fixture.client_state.user_operation_queue.len());
}

#[test]
fn connected_state_unsubscribe_no_failure_on_dropped_receiver() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    scoped_unsubscribe(&mut fixture);

    assert!(fixture.service_round_trip(100, 150, 4096).is_ok());
    assert!(fixture.service_round_trip(200, 250, 4096).is_ok());

    verify_protocol_state_empty(&fixture);
}

#[test]
fn connected_state_outbound_topic_aliasing_used() {
    let mut config = build_standard_test_config();

    let mut lru_resolver  = OutboundAliasResolverFactory::new_lru_factory(2)();
    lru_resolver.reset_for_new_connection(2);
    config.outbound_alias_resolver = Some(lru_resolver);

    let mut fixture = ProtocolStateTestFixture::new(config);
    fixture.broker_packet_handlers.insert(PacketType::Connect, Box::new(handle_connect_with_topic_aliasing));

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    let publish = PublishPacket {
        qos: QualityOfService::AtLeastOnce,
        topic: "hello/world".to_string(),
        ..Default::default()
    };

    let _ = fixture.publish(10, publish.clone(), PublishOptions{ ..Default::default() });
    let _ = fixture.publish(10, publish.clone(), PublishOptions{ ..Default::default() });
    let _ = fixture.publish(10, publish.clone(), PublishOptions{ ..Default::default() });

    assert!(fixture.service_round_trip(100, 150, 4096).is_ok());
    assert!(fixture.service_round_trip(200, 250, 4096).is_ok());

    verify_protocol_state_empty(&fixture);

    let (_, first_packet) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Publish, 1, None, None).unwrap();
    if let MqttPacket::Publish(publish) = first_packet {
        assert_eq!(Some(1), publish.topic_alias);
        assert_eq!("hello/world".to_string(), publish.topic);
    } else {
        panic!("Expected publish");
    }

    let (_, second_packet) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Publish, 2, None, None).unwrap();
    if let MqttPacket::Publish(publish) = second_packet {
        assert_eq!(Some(1), publish.topic_alias);
        assert_eq!(0, publish.topic.len());
    } else {
        panic!("Expected publish");
    }

    let (_, third_packet) = find_nth_packet_of_type(fixture.to_broker_packet_stream.iter(), PacketType::Publish, 3, None, None).unwrap();
    if let MqttPacket::Publish(publish) = third_packet {
        assert_eq!(Some(1), publish.topic_alias);
        assert_eq!(0, publish.topic.len());
    } else {
        panic!("Expected publish");
    }

}

#[test]
fn connected_state_reset_test() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    let _ = initialize_multi_operation_sequence_test(&mut fixture);

    fixture.reset(500);

    verify_protocol_state_empty(&fixture);
}

#[test]
fn connected_state_disconnect_with_high_priority_pubrel() {
    let mut fixture = ProtocolStateTestFixture::new(build_standard_test_config());
    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 0).is_ok());

    let publish = PublishPacket {
        qos: QualityOfService::ExactlyOnce,
        topic: "hello/world".to_string(),
        ..Default::default()
    };

    let receiver = fixture.publish(100, publish, PublishOptions{ ..Default::default() }).unwrap();

    assert!(fixture.service_round_trip(150, 200, 4096).is_ok());
    assert_eq!(1, fixture.client_state.high_priority_operation_queue.len());

    assert!(fixture.on_connection_closed(250).is_ok());
    assert_eq!(0, fixture.client_state.high_priority_operation_queue.len());
    assert_eq!(1, fixture.client_state.resubmit_operation_queue.len());

    assert!(fixture.advance_disconnected_to_state(ProtocolStateType::Connected, 300).is_ok());
    assert!(fixture.service_round_trip(350, 400, 4096).is_ok());
    assert!(fixture.service_round_trip(450, 500, 4096).is_ok());

    verify_protocol_state_empty(&fixture);

    let result = receiver.try_recv().unwrap();
    if let PublishResponse::Qos2(Qos2Response::Pubcomp(pubcomp)) = &result.unwrap() {
        assert_eq!(PubcompReasonCode::Success, pubcomp.reason_code);
    } else {
        panic!("Expected a pubcomp");
    }
}