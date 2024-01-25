/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::*;
use crate::decode::utils::*;
use crate::encode::*;
use crate::encode::utils::*;
use crate::error::{MqttError, MqttResult};
use crate::logging::*;
use crate::spec::*;
use crate::spec::utils::*;
use crate::validate::*;
use crate::validate::utils::*;

use log::*;
use std::collections::VecDeque;
use std::fmt;

/// Data model of an [MQTT5 SUBSCRIBE](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901161) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SubscribePacket {

    /// Packet Id of the subscribe.  Setting this value on an outbound subscribe has no effect on the
    /// actual packet id used by the client.
    pub packet_id: u16,

    /// List of topic filter subscriptions that the client wishes to listen to
    ///
    /// See [MQTT5 Subscribe Payload](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901168)
    pub subscriptions: Vec<Subscription>,

    /// A positive integer to associate with all subscriptions in this request.  Publish packets that match
    /// a subscription in this request should include this identifier in the resulting message.
    ///
    /// See [MQTT5 Subscription Identifier](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901166)
    pub subscription_identifier: Option<u32>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901167)
    pub user_properties: Option<Vec<UserProperty>>,
}


#[rustfmt::skip]
fn compute_subscribe_packet_length_properties(packet: &SubscribePacket) -> MqttResult<(u32, u32)> {
    let mut subscribe_property_section_length = compute_user_properties_length(&packet.user_properties);
    add_optional_u32_property_length!(subscribe_property_section_length, packet.subscription_identifier);

    let mut total_remaining_length : usize = 2 + compute_variable_length_integer_encode_size(subscribe_property_section_length)?;
    total_remaining_length += subscribe_property_section_length;

    total_remaining_length += packet.subscriptions.len() * 3;
    for subscription in &packet.subscriptions {
        total_remaining_length += subscription.topic_filter.len();
    }

    Ok((total_remaining_length as u32, subscribe_property_section_length as u32))
}

fn get_subscribe_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Subscribe(subscribe) = packet {
        if let Some(properties) = &subscribe.user_properties {
            return &properties[index];
        }
    }

    panic!("Internal encoding error: invalid user property state");
}

fn get_subscribe_packet_topic_filter(packet: &MqttPacket, index: usize) -> &str {
    if let MqttPacket::Subscribe(subscribe) = packet {
        return &subscribe.subscriptions[index].topic_filter;
    }

    panic!("Internal encoding error: invalid subscribe topic filter state");
}

fn compute_subscription_options_byte(subscription: &Subscription) -> u8 {
    let mut options_byte = subscription.qos as u8;

    if subscription.no_local {
        options_byte |= SUBSCRIPTION_OPTIONS_NO_LOCAL_MASK;
    }

    if subscription.retain_as_published {
        options_byte |= SUBSCRIPTION_OPTIONS_RETAIN_AS_PUBLISHED_MASK;
    }

    options_byte |= (subscription.retain_handling_type as u8) << SUBSCRIPTION_OPTIONS_RETAIN_HANDLING_SHIFT;

    options_byte
}

#[rustfmt::skip]
pub(crate) fn write_subscribe_encoding_steps(packet: &SubscribePacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> MqttResult<()> {
    let (total_remaining_length, subscribe_property_length) = compute_subscribe_packet_length_properties(packet)?;

    encode_integral_expression!(steps, Uint8, SUBSCRIBE_FIRST_BYTE);
    encode_integral_expression!(steps, Vli, total_remaining_length);

    encode_integral_expression!(steps, Uint16, packet.packet_id);
    encode_integral_expression!(steps, Vli, subscribe_property_length);
    encode_optional_property!(steps, Uint32, PROPERTY_KEY_SUBSCRIPTION_IDENTIFIER, packet.subscription_identifier);
    encode_user_properties!(steps, get_subscribe_packet_user_property, packet.user_properties);

    let subscriptions = &packet.subscriptions;
    for (i, subscription) in subscriptions.iter().enumerate() {
        encode_indexed_string!(steps, get_subscribe_packet_topic_filter, subscription.topic_filter, i);
        encode_integral_expression!(steps, Uint8, compute_subscription_options_byte(subscription));
    }

    Ok(())
}

fn decode_subscribe_properties(property_bytes: &[u8], packet : &mut SubscribePacket) -> MqttResult<()> {
    let mut mutable_property_bytes = property_bytes;

    while !mutable_property_bytes.is_empty() {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_SUBSCRIPTION_IDENTIFIER => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut packet.subscription_identifier)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            _ => {
                error!("SubscribePacket Decode - Invalid property type ({})", property_key);
                return Err(MqttError::new_decoding_failure("invalid property type for subscribe packet"));
            }
        }
    }

    Ok(())
}

const SUBSCRIPTION_OPTIONS_RESERVED_BITS_MASK : u8 = 192;

pub(crate) fn decode_subscribe_packet(first_byte: u8, packet_body: &[u8]) -> MqttResult<Box<MqttPacket>> {

    if first_byte != SUBSCRIBE_FIRST_BYTE {
        error!("SubscribePacket Decode - invalid first byte");
        return Err(MqttError::new_decoding_failure("invalid first byte for subscribe packet"));
    }

    let mut box_packet = Box::new(MqttPacket::Subscribe(SubscribePacket { ..Default::default() }));
    if let MqttPacket::Subscribe(packet) = box_packet.as_mut() {
        let mut mutable_body = packet_body;
        mutable_body = decode_u16(mutable_body, &mut packet.packet_id)?;

        let mut properties_length: usize = 0;
        mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
        if properties_length > mutable_body.len() {
            error!("SubscribePacket Decode - property length exceeds overall packet length");
            return Err(MqttError::new_decoding_failure("property length exceeds overall packet length for subscribe packet"));
        }

        let properties_bytes = &mutable_body[..properties_length];
        let mut payload_bytes = &mutable_body[properties_length..];

        decode_subscribe_properties(properties_bytes, packet)?;

        while !payload_bytes.is_empty() {
            let mut subscription = Subscription {
                ..Default::default()
            };

            payload_bytes = decode_length_prefixed_string(payload_bytes, &mut subscription.topic_filter)?;

            let mut subscription_options: u8 = 0;
            payload_bytes = decode_u8(payload_bytes, &mut subscription_options)?;

            if (subscription_options & SUBSCRIPTION_OPTIONS_RESERVED_BITS_MASK) != 0 {
                return Err(MqttError::new_decoding_failure("invalid subscription reserved bit flags for subscribe packet"));
            }

            subscription.qos = convert_u8_to_quality_of_service(subscription_options & 0x03)?;

            if (subscription_options & SUBSCRIPTION_OPTIONS_NO_LOCAL_MASK) != 0 {
                subscription.no_local = true;
            }

            if (subscription_options & SUBSCRIPTION_OPTIONS_RETAIN_AS_PUBLISHED_MASK) != 0 {
                subscription.retain_as_published = true;
            }

            subscription.retain_handling_type = convert_u8_to_retain_handling_type((subscription_options >> SUBSCRIPTION_OPTIONS_RETAIN_HANDLING_SHIFT) & 0x03)?;

            packet.subscriptions.push(subscription);
        }

        return Ok(box_packet);
    }

    panic!("SubscribePacket Decode - Internal error");
}

pub(crate) fn validate_subscribe_packet_outbound(packet: &SubscribePacket) -> MqttResult<()> {

    if packet.packet_id != 0 {
        error!("SubscribePacket Outbound Validation - packet id may not be set");
        return Err(MqttError::new_packet_validation(PacketType::Subscribe, "packet id is non-zero"));
    }

    if packet.subscriptions.is_empty() {
        error!("SubscribePacket Outbound Validation - empty subscription set");
        return Err(MqttError::new_packet_validation(PacketType::Subscribe, "empty subscription set"));
    }

    validate_user_properties(&packet.user_properties, PacketType::Subscribe, "Subscribe")?;

    Ok(())
}

pub(crate) fn validate_subscribe_packet_outbound_internal(packet: &SubscribePacket, context: &OutboundValidationContext) -> MqttResult<()> {

    let (total_remaining_length, _) = compute_subscribe_packet_length_properties(packet)?;
    let total_packet_length = 1 + total_remaining_length + compute_variable_length_integer_encode_size(total_remaining_length as usize)? as u32;
    if total_packet_length > context.negotiated_settings.unwrap().maximum_packet_size_to_server {
        error!("SubscribePacket Outbound Validation - packet length exceeds maximum packet size allowed to server");
        return Err(MqttError::new_packet_validation(PacketType::Subscribe, "packet length exceeds maximum packet size allowed"));
    }

    if packet.packet_id == 0 {
        error!("SubscribePacket Outbound Validation - packet id is zero");
        return Err(MqttError::new_packet_validation(PacketType::Subscribe, "packet id is zero"));
    }

    for subscription in &packet.subscriptions {
        if !is_valid_topic_filter_internal(&subscription.topic_filter, context, Some(subscription.no_local)) {
            error!("SubscribePacket Outbound Validation - invalid topic filter");
            return Err(MqttError::new_packet_validation(PacketType::Subscribe, "invalid topic filter"));
        }
    }

    Ok(())
}

impl fmt::Display for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{")?;
        log_string!(self.topic_filter, f, "topic_filter");
        log_enum!(self.qos, f, "qos", quality_of_service_to_str);
        log_primitive_value!(self.no_local, f, "no_local");
        log_primitive_value!(self.retain_as_published, f, "retain_as_published");
        log_enum!(self.retain_handling_type, f, "retain_handling_type", retain_handling_type_to_str);
        write!(f, " }}")
    }
}

impl fmt::Display for SubscribePacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SubscribePacket {{")?;
        log_primitive_value!(self.packet_id, f, "packet_id");
        log_optional_primitive_value!(self.subscription_identifier, f, "subscription_identifier", value);
        log_user_properties!(self.user_properties, f, "user_properties", value);
        write!(f, " subscriptions: [")?;
        for (i, subscription) in self.subscriptions.iter().enumerate() {
            write!(f, " {}:{}", i, subscription)?;
        }
        write!(f, " ] }}")
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;
    use crate::validate::utils::testing::verify_validation_failure;

    #[test]
    fn subscribe_round_trip_encode_decode_default() {
        let packet = SubscribePacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Subscribe(packet)));
    }

    #[test]
    fn subscribe_round_trip_encode_decode_basic() {
        let packet = SubscribePacket {
            packet_id : 123,
            subscriptions : vec![ Subscription { topic_filter: "hello/world".to_string(), qos: QualityOfService::AtLeastOnce, ..Default::default() } ],
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Subscribe(packet)));
    }

    fn create_subscribe_all_properties() -> SubscribePacket {
        SubscribePacket {
            packet_id : 123,
            subscriptions : vec![
                Subscription {
                    topic_filter: "a/b/c/d/e".to_string(),
                    qos: QualityOfService::ExactlyOnce,
                    retain_as_published: true,
                    no_local: false,
                    retain_handling_type: RetainHandlingType::DontSend
                },
                Subscription {
                    topic_filter: "the/best/+/filter/*".to_string(),
                    qos: QualityOfService::AtMostOnce,
                    retain_as_published: false,
                    no_local: true,
                    retain_handling_type: RetainHandlingType::SendOnSubscribeIfNew
                }
            ],
            subscription_identifier : Some(41),
            user_properties: Some(vec!(
                UserProperty{name: "Worms".to_string(), value: "inmyhead".to_string()},
            )),
        }
    }

    #[test]
    fn subscribe_round_trip_encode_decode_all_properties() {
        let packet = create_subscribe_all_properties();
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Subscribe(packet)));
    }

    #[test]
    fn subscribe_decode_failure_bad_fixed_header() {
        let packet = SubscribePacket {
            packet_id : 123,
            subscriptions : vec![ Subscription { topic_filter: "hello/world".to_string(), qos: QualityOfService::AtLeastOnce, ..Default::default() } ],
            ..Default::default()
        };

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Subscribe(packet), 7);
    }

    const SUBSCRIBE_PACKET_TEST_SUBSCRIPTION_OPTIONS_INDEX : usize = 18;

    #[test]
    fn subscribe_decode_failure_subscription_qos3() {
        let packet = SubscribePacket {
            packet_id : 123,
            subscriptions : vec![ Subscription { topic_filter: "hello/world".to_string(), qos: QualityOfService::AtLeastOnce, ..Default::default() } ],
            ..Default::default()
        };

        let invalidate_subscription_qos = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[SUBSCRIBE_PACKET_TEST_SUBSCRIPTION_OPTIONS_INDEX] |= 0x03;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Subscribe(packet), invalidate_subscription_qos);
    }

    #[test]
    fn subscribe_decode_failure_subscription_retain_handling3() {
        let packet = SubscribePacket {
            packet_id : 123,
            subscriptions : vec![ Subscription { topic_filter: "hello/world".to_string(), qos: QualityOfService::AtLeastOnce, ..Default::default() } ],
            ..Default::default()
        };

        let invalidate_subscription_qos = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[SUBSCRIBE_PACKET_TEST_SUBSCRIPTION_OPTIONS_INDEX] |= 0x03 << 4;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Subscribe(packet), invalidate_subscription_qos);
    }

    #[test]
    fn subscribe_decode_failure_subscription_reserved_bits() {
        let packet = SubscribePacket {
            packet_id : 123,
            subscriptions : vec![ Subscription { topic_filter: "hello/world".to_string(), qos: QualityOfService::AtLeastOnce, ..Default::default() } ],
            ..Default::default()
        };

        let invalidate_subscription_qos = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[SUBSCRIBE_PACKET_TEST_SUBSCRIPTION_OPTIONS_INDEX] |= 192;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Subscribe(packet), invalidate_subscription_qos);
    }

    #[test]
    fn subscribe_decode_failure_inbound_packet_size() {
        let packet = create_subscribe_all_properties();

        do_inbound_size_decode_failure_test(&MqttPacket::Subscribe(packet));
    }

    use crate::validate::testing::*;

    #[test]
    fn unsubscribe_validate_success() {
        let mut packet = create_subscribe_all_properties();
        packet.packet_id = 0;

        let outbound_packet = MqttPacket::Subscribe(packet);

        assert!(validate_packet_outbound(&outbound_packet).is_ok());

        let mut packet2 = create_subscribe_all_properties();
        packet2.packet_id = 1;

        let outbound_internal_packet = MqttPacket::Subscribe(packet2);

        let test_validation_context = create_pinned_validation_context();

        let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);
        assert!(validate_packet_outbound_internal(&outbound_internal_packet, &outbound_validation_context).is_ok());
    }

    #[test]
    fn subscribe_validate_failure_outbound_packet_id_non_zero() {
        let mut packet = create_subscribe_all_properties();
        packet.packet_id = 1;

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Subscribe(packet)), PacketType::Subscribe);
    }

    #[test]
    fn subscribe_validate_failure_outbound_topic_filters_empty() {
        let mut packet = create_subscribe_all_properties();
        packet.subscriptions = vec![];

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Subscribe(packet)), PacketType::Subscribe);
    }

    #[test]
    fn subscribe_validate_failure_outbound_user_properties_invalid() {
        let mut packet = create_subscribe_all_properties();
        packet.user_properties = Some(create_invalid_user_properties());

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Subscribe(packet)), PacketType::Subscribe);
    }

    #[test]
    fn subscribe_validate_failure_outbound_size() {
        let packet = create_subscribe_all_properties();

        do_outbound_size_validate_failure_test(&MqttPacket::Subscribe(packet), PacketType::Subscribe);
    }

    #[test]
    fn subscribe_validate_failure_outbound_internal_packet_id_zero() {
        let mut packet = create_subscribe_all_properties();
        packet.packet_id = 0;

        let packet = MqttPacket::Subscribe(packet);

        let test_validation_context = create_pinned_validation_context();
        let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);

        verify_validation_failure!(validate_packet_outbound_internal(&packet, &outbound_validation_context), PacketType::Subscribe);
    }

    #[test]
    fn subscribe_validate_failure_outbound_internal_topic_filter_invalid() {
        let mut packet = create_subscribe_all_properties();
        packet.subscriptions[0].topic_filter = "a/#/c".to_string();

        let packet = MqttPacket::Subscribe(packet);

        let test_validation_context = create_pinned_validation_context();
        let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);

        verify_validation_failure!(validate_packet_outbound_internal(&packet, &outbound_validation_context), PacketType::Subscribe);
    }

    #[test]
    fn subscribe_validate_failure_outbound_internal_shared_topic_filter_not_allowed() {
        let mut packet = create_subscribe_all_properties();
        packet.subscriptions[0].topic_filter = "$share/sharename/hello/world".to_string();
        packet.subscriptions[0].no_local = false;

        let packet = MqttPacket::Subscribe(packet);

        let mut test_validation_context = create_pinned_validation_context();
        test_validation_context.settings.shared_subscriptions_available = false;

        let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);

        verify_validation_failure!(validate_packet_outbound_internal(&packet, &outbound_validation_context), PacketType::Subscribe);
    }

    #[test]
    fn subscribe_validate_failure_outbound_internal_shared_topic_filter_no_local() {
        let mut packet = create_subscribe_all_properties();
        packet.subscriptions[0].topic_filter = "$share/sharename/hello/world".to_string();
        packet.subscriptions[0].no_local = true;

        let packet = MqttPacket::Subscribe(packet);

        let test_validation_context = create_pinned_validation_context();
        let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);

        verify_validation_failure!(validate_packet_outbound_internal(&packet, &outbound_validation_context), PacketType::Subscribe);
    }

    #[test]
    fn subscribe_validate_failure_outbound_internal_wildcard_topic_filter_not_allowed() {
        let mut packet = create_subscribe_all_properties();
        packet.subscriptions[0].topic_filter = "a/+/+".to_string();

        let packet = MqttPacket::Subscribe(packet);

        let mut test_validation_context = create_pinned_validation_context();
        test_validation_context.settings.wildcard_subscriptions_available = false;

        let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);

        verify_validation_failure!(validate_packet_outbound_internal(&packet, &outbound_validation_context), PacketType::Subscribe);
    }
}