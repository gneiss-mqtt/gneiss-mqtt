/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#[cfg(test)]
use crate::decode::*;
use crate::encode::*;
use crate::error::{GneissError, GneissResult};
use crate::logging::*;
use crate::mqtt::*;
use crate::mqtt::utils::*;
use crate::validate::*;

use std::collections::VecDeque;
use std::fmt;

#[rustfmt::skip]
fn compute_unsubscribe_packet_length_properties5(packet: &UnsubscribePacket) -> GneissResult<(u32, u32)> {
    let unsubscribe_property_section_length = compute_user_properties_length(&packet.user_properties);

    let mut total_remaining_length : usize = 2 + compute_variable_length_integer_encode_size(unsubscribe_property_section_length)?;
    total_remaining_length += unsubscribe_property_section_length;

    total_remaining_length += packet.topic_filters.len() * 2;
    for filter in &packet.topic_filters {
        total_remaining_length += filter.len();
    }

    Ok((total_remaining_length as u32, unsubscribe_property_section_length as u32))
}

fn get_unsubscribe_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Unsubscribe(unsubscribe) = packet {
        if let Some(properties) = &unsubscribe.user_properties {
            return &properties[index];
        }
    }

    panic!("get_unsubscribe_packet_user_property - invalid user property state");
}

fn get_unsubscribe_packet_topic_filter(packet: &MqttPacket, index: usize) -> &str {
    if let MqttPacket::Unsubscribe(unsubscribe) = packet {
        return &unsubscribe.topic_filters[index];
    }

    panic!("get_unsubscribe_packet_topic_filter - invalid unsubscribe topic filter state");
}

#[rustfmt::skip]
pub(crate) fn write_unsubscribe_encoding_steps5(packet: &UnsubscribePacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> GneissResult<()> {
    let (total_remaining_length, unsubscribe_property_length) = compute_unsubscribe_packet_length_properties5(packet)?;

    encode_integral_expression!(steps, Uint8, UNSUBSCRIBE_FIRST_BYTE);
    encode_integral_expression!(steps, Vli, total_remaining_length);

    encode_integral_expression!(steps, Uint16, packet.packet_id);
    encode_integral_expression!(steps, Vli, unsubscribe_property_length);
    encode_user_properties!(steps, get_unsubscribe_packet_user_property, packet.user_properties);

    let topic_filters = &packet.topic_filters;
    for (i, topic_filter) in topic_filters.iter().enumerate() {
        encode_indexed_string!(steps, get_unsubscribe_packet_topic_filter, topic_filter, i);
    }

    Ok(())
}

fn compute_unsubscribe_packet_length_properties311(packet: &UnsubscribePacket) -> GneissResult<u32> {
    let mut total_remaining_length : usize = 2;
    total_remaining_length += packet.topic_filters.len() * 2;

    for filter in &packet.topic_filters {
        total_remaining_length += filter.len();
    }

    Ok(total_remaining_length as u32)
}

pub(crate) fn write_unsubscribe_encoding_steps311(packet: &UnsubscribePacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> GneissResult<()> {
    let total_remaining_length = compute_unsubscribe_packet_length_properties311(packet)?;

    encode_integral_expression!(steps, Uint8, UNSUBSCRIBE_FIRST_BYTE);
    encode_integral_expression!(steps, Vli, total_remaining_length);

    encode_integral_expression!(steps, Uint16, packet.packet_id);

    let topic_filters = &packet.topic_filters;
    for (i, topic_filter) in topic_filters.iter().enumerate() {
        encode_indexed_string!(steps, get_unsubscribe_packet_topic_filter, topic_filter, i);
    }

    Ok(())
}

#[cfg(test)]
fn decode_unsubscribe_properties(property_bytes: &[u8], packet : &mut UnsubscribePacket) -> GneissResult<()> {
    let mut mutable_property_bytes = property_bytes;

    while !mutable_property_bytes.is_empty() {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            _ => {
                let message = format!("decode_unsubscribe_properties - invalid property type ({})", property_key);
                error!("{}", message);
                return Err(GneissError::new_decoding_failure(message));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
pub(crate) fn decode_unsubscribe_packet5(first_byte: u8, packet_body: &[u8]) -> GneissResult<Box<MqttPacket>> {

    if first_byte != UNSUBSCRIBE_FIRST_BYTE {
        let message = "decode_unsubscribe_packet5 - invalid first byte";
        error!("{}", message);
        return Err(GneissError::new_decoding_failure(message));
    }

    let mut box_packet = Box::new(MqttPacket::Unsubscribe(UnsubscribePacket { ..Default::default() }));

    if let MqttPacket::Unsubscribe(packet) = box_packet.as_mut() {
        let mut mutable_body = packet_body;
        mutable_body = decode_u16(mutable_body, &mut packet.packet_id)?;

        let mut properties_length: usize = 0;
        mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
        if properties_length > mutable_body.len() {
            let message = "decode_unsubscribe_packet5 - property length exceeds overall packet length";
            error!("{}", message);
            return Err(GneissError::new_decoding_failure(message));
        }

        let properties_bytes = &mutable_body[..properties_length];
        let mut payload_bytes = &mutable_body[properties_length..];

        decode_unsubscribe_properties(properties_bytes, packet)?;

        while !payload_bytes.is_empty() {
            let mut topic_filter = String::new();
            payload_bytes = decode_length_prefixed_string(payload_bytes, &mut topic_filter)?;

            packet.topic_filters.push(topic_filter);
        }

        return Ok(box_packet);
    }

    panic!("decode_unsubscribe_packet5 - internal error");
}

#[cfg(not(test))]
pub(crate) fn decode_unsubscribe_packet5(_: u8, _: &[u8]) -> GneissResult<Box<MqttPacket>> {
    Err(GneissError::new_unimplemented("decode_unsubscribe_packet5 - test-only functionality"))
}

#[cfg(test)]
pub(crate) fn decode_unsubscribe_packet311(first_byte: u8, packet_body: &[u8]) -> GneissResult<Box<MqttPacket>> {

    if first_byte != UNSUBSCRIBE_FIRST_BYTE {
        let message = "decode_unsubscribe_packet311 - invalid first byte";
        error!("{}", message);
        return Err(GneissError::new_decoding_failure(message));
    }

    let mut box_packet = Box::new(MqttPacket::Unsubscribe(UnsubscribePacket { ..Default::default() }));

    if let MqttPacket::Unsubscribe(packet) = box_packet.as_mut() {
        let mut mutable_body = packet_body;
        mutable_body = decode_u16(mutable_body, &mut packet.packet_id)?;

        while !mutable_body.is_empty() {
            let mut topic_filter = String::new();
            mutable_body = decode_length_prefixed_string(mutable_body, &mut topic_filter)?;

            packet.topic_filters.push(topic_filter);
        }

        return Ok(box_packet);
    }

    panic!("decode_unsubscribe_packet311 - internal error");
}

#[cfg(not(test))]
pub(crate) fn decode_unsubscribe_packet311(_: u8, _: &[u8]) -> GneissResult<Box<MqttPacket>> {
    Err(GneissError::new_unimplemented("decode_unsubscribe_packet311 - test-only functionality"))
}

pub(crate) fn validate_unsubscribe_packet_outbound(packet: &UnsubscribePacket) -> GneissResult<()> {
    if packet.packet_id != 0 {
        let message = "validate_unsubscribe_packet_outbound - packet id may not be set";
        error!("{}", message);
        return Err(GneissError::new_packet_validation(PacketType::Unsubscribe, message));
    }

    if packet.topic_filters.is_empty() {
        let message = "validate_unsubscribe_packet_outbound - empty topic filters list";
        error!("{}", message);
        return Err(GneissError::new_packet_validation(PacketType::Unsubscribe, message));
    }

    // topic filters are checked in detail in the internal validator

    validate_user_properties(&packet.user_properties, PacketType::Unsubscribe, "validate_unsubscribe_packet_outbound")?;

    Ok(())
}

pub(crate) fn validate_unsubscribe_packet_outbound_internal(packet: &UnsubscribePacket, context: &OutboundValidationContext) -> GneissResult<()> {

    let (total_remaining_length, _) = compute_unsubscribe_packet_length_properties5(packet)?;
    let total_packet_length = 1 + total_remaining_length + compute_variable_length_integer_encode_size(total_remaining_length as usize)? as u32;
    if total_packet_length > context.negotiated_settings.unwrap().maximum_packet_size_to_server {
        let message = "validate_unsubscribe_packet_outbound_internal - packet length exceeds maximum packet size allowed to server";
        error!("{}", message);
        return Err(GneissError::new_packet_validation(PacketType::Unsubscribe, message));
    }

    if packet.packet_id == 0 {
        let message = "validate_unsubscribe_packet_outbound_internal - packet id is zero";
        error!("{}", message);
        return Err(GneissError::new_packet_validation(PacketType::Unsubscribe, message));
    }

    for filter in &packet.topic_filters {
        if !is_valid_topic_filter_internal(filter, context, None) {
            let message = "validate_unsubscribe_packet_outbound_internal - invalid topic filter";
            error!("{}", message);
            return Err(GneissError::new_packet_validation(PacketType::Unsubscribe, message));
        }
    }

    Ok(())
}

impl fmt::Display for UnsubscribePacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UnsubscribePacket {{")?;
        log_primitive_value!(self.packet_id, f, "packet_id");
        log_user_properties!(self.user_properties, f, "user_properties", value);
        write!(f, " topic_filters: [")?;
        for (i, topic_filter) in self.topic_filters.iter().enumerate() {
            write!(f, " {}:{}", i, topic_filter)?;
        }
        write!(f, " ] }}")
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;
    use crate::validate::testing::*;

    fn do_unsubscribe_round_trip_encode_decode_default_test(protocol_version: ProtocolVersion) {
        let packet = UnsubscribePacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsubscribe(packet), protocol_version));
    }

    #[test]
    fn unsubscribe_round_trip_encode_decode_default5() {
        do_unsubscribe_round_trip_encode_decode_default_test(ProtocolVersion::Mqtt5);
    }

    #[test]
    fn unsubscribe_round_trip_encode_decode_default311() {
        do_unsubscribe_round_trip_encode_decode_default_test(ProtocolVersion::Mqtt311);
    }

    fn do_unsubscribe_round_trip_encode_decode_basic_test(protocol_version: ProtocolVersion) {
        let packet = UnsubscribePacket {
            packet_id : 123,
            topic_filters : vec![ "hello/world".to_string() ],
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsubscribe(packet), protocol_version));
    }

    #[test]
    fn unsubscribe_round_trip_encode_decode_basic5() {
        do_unsubscribe_round_trip_encode_decode_basic_test(ProtocolVersion::Mqtt5);
    }

    #[test]
    fn unsubscribe_round_trip_encode_decode_basic311() {
        do_unsubscribe_round_trip_encode_decode_basic_test(ProtocolVersion::Mqtt311);
    }

    fn create_unsubscribe_all_properties() -> UnsubscribePacket {
        UnsubscribePacket {
            packet_id : 123,
            topic_filters : vec![
                "hello/world".to_string(),
                "calvin/is/a/goof".to_string(),
                "wild/+/card".to_string()
            ],
            user_properties: Some(vec!(
                UserProperty{name: "Clickergames".to_string(), value: "arelame".to_string()},
            )),
        }
    }

    #[test]
    fn unsubscribe_round_trip_encode_decode_all_properties5() {
        let packet = create_unsubscribe_all_properties();
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsubscribe(packet), ProtocolVersion::Mqtt5));
    }

    #[test]
    fn unsubscribe_round_trip_encode_decode_all_properties311() {
        let packet = create_unsubscribe_all_properties();
        let mut expected_packet = create_unsubscribe_all_properties();
        expected_packet.user_properties = None;

        assert!(do_311_filter_encode_decode_test(&MqttPacket::Unsubscribe(packet), &MqttPacket::Unsubscribe(expected_packet)));
    }

    fn do_unsubscribe_decode_failure_bad_fixed_header_test(protocol_version: ProtocolVersion) {
        let packet = UnsubscribePacket {
            packet_id : 123,
            topic_filters : vec![ "hello/world".to_string() ],
            ..Default::default()
        };

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Unsubscribe(packet), protocol_version, 14);
    }

    #[test]
    fn unsubscribe_decode_failure_bad_fixed_header5() {
        do_unsubscribe_decode_failure_bad_fixed_header_test(ProtocolVersion::Mqtt5);
    }

    #[test]
    fn unsubscribe_decode_failure_bad_fixed_header311() {
        do_unsubscribe_decode_failure_bad_fixed_header_test(ProtocolVersion::Mqtt311);
    }

    #[test]
    fn unsubscribe_decode_failure_inbound_packet_size5() {
        let packet = create_unsubscribe_all_properties();

        do_inbound_size_decode_failure_test(&MqttPacket::Unsubscribe(packet), ProtocolVersion::Mqtt5);
    }

    #[test]
    fn unsubscribe_decode_failure_inbound_packet_size311() {
        let mut packet = create_unsubscribe_all_properties();
        packet.user_properties = None;

        do_inbound_size_decode_failure_test(&MqttPacket::Unsubscribe(packet), ProtocolVersion::Mqtt311);
    }

    #[test]
    fn unsubscribe_validate_success() {
        let mut packet = create_unsubscribe_all_properties();
        packet.packet_id = 0;

        let outbound_packet = MqttPacket::Unsubscribe(packet);

        assert!(validate_packet_outbound(&outbound_packet).is_ok());

        let mut packet2 = create_unsubscribe_all_properties();
        packet2.packet_id = 1;

        let outbound_internal_packet = MqttPacket::Unsubscribe(packet2);

        let test_validation_context = create_pinned_validation_context();

        let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);
        assert!(validate_packet_outbound_internal(&outbound_internal_packet, &outbound_validation_context).is_ok());
    }

    #[test]
    fn unsubscribe_validate_failure_outbound_packet_id_non_zero() {
        let mut packet = create_unsubscribe_all_properties();
        packet.packet_id = 1;

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Unsubscribe(packet)), PacketType::Unsubscribe);
    }

    #[test]
    fn unsubscribe_validate_failure_outbound_topic_filters_empty() {
        let mut packet = create_unsubscribe_all_properties();
        packet.topic_filters = vec![];

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Unsubscribe(packet)), PacketType::Unsubscribe);
    }

    #[test]
    fn unsubscribe_validate_failure_outbound_user_properties_invalid() {
        let mut packet = create_unsubscribe_all_properties();
        packet.user_properties = Some(create_invalid_user_properties());

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Unsubscribe(packet)), PacketType::Unsubscribe);
    }

    #[test]
    fn unsubscribe_validate_failure_outbound_size5() {
        let packet = create_unsubscribe_all_properties();

        do_outbound_size_validate_failure_test(&MqttPacket::Unsubscribe(packet), ProtocolVersion::Mqtt5, PacketType::Unsubscribe);
    }

    #[test]
    fn unsubscribe_validate_failure_outbound_size311() {
        let packet = create_unsubscribe_all_properties();

        do_outbound_size_validate_failure_test(&MqttPacket::Unsubscribe(packet), ProtocolVersion::Mqtt311, PacketType::Unsubscribe);
    }

    #[test]
    fn unsubscribe_validate_failure_outbound_internal_packet_id_zero() {
        let mut packet = create_unsubscribe_all_properties();
        packet.packet_id = 0;

        let packet = MqttPacket::Unsubscribe(packet);

        let test_validation_context = create_pinned_validation_context();
        let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);

        verify_validation_failure!(validate_packet_outbound_internal(&packet, &outbound_validation_context), PacketType::Unsubscribe);
    }

    #[test]
    fn unsubscribe_validate_failure_outbound_internal_topic_filter_invalid() {
        let mut packet = create_unsubscribe_all_properties();
        packet.topic_filters = vec![ "a/#/c".to_string() ];

        let packet = MqttPacket::Unsubscribe(packet);

        let test_validation_context = create_pinned_validation_context();
        let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);

        verify_validation_failure!(validate_packet_outbound_internal(&packet, &outbound_validation_context), PacketType::Unsubscribe);
    }

    #[test]
    fn unsubscribe_validate_failure_outbound_internal_shared_topic_filter_not_allowed() {
        let mut packet = create_unsubscribe_all_properties();
        packet.topic_filters = vec![ "$share/sharename/hello/world".to_string() ];

        let packet = MqttPacket::Unsubscribe(packet);

        let mut test_validation_context = create_pinned_validation_context();
        test_validation_context.settings.shared_subscriptions_available = false;

        let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);

        verify_validation_failure!(validate_packet_outbound_internal(&packet, &outbound_validation_context), PacketType::Unsubscribe);
    }

    #[test]
    fn unsubscribe_validate_failure_outbound_internal_wildcard_topic_filter_not_allowed() {
        let mut packet = create_unsubscribe_all_properties();
        packet.topic_filters = vec![ "a/+/c".to_string() ];

        let packet = MqttPacket::Unsubscribe(packet);

        let mut test_validation_context = create_pinned_validation_context();
        test_validation_context.settings.wildcard_subscriptions_available = false;

        let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);

        verify_validation_failure!(validate_packet_outbound_internal(&packet, &outbound_validation_context), PacketType::Unsubscribe);
    }
}
