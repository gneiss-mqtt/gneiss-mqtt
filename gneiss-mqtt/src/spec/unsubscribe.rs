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

/// Data model of an [MQTT5 UNSUBSCRIBE](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901179) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UnsubscribePacket {

    /// Packet Id of the unsubscribe.  Setting this value on an outbound unsubscribe has no effect on the
    /// actual packet id used by the client.
    pub packet_id: u16,

    /// List of topic filters that the client wishes to unsubscribe from.
    ///
    /// See [MQTT5 Unsubscribe Payload](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901185)
    pub topic_filters: Vec<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901184)
    pub user_properties: Option<Vec<UserProperty>>,
}

#[rustfmt::skip]
fn compute_unsubscribe_packet_length_properties(packet: &UnsubscribePacket) -> MqttResult<(u32, u32)> {
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

    panic!("Internal encoding error: invalid user property state");
}

fn get_unsubscribe_packet_topic_filter(packet: &MqttPacket, index: usize) -> &str {
    if let MqttPacket::Unsubscribe(unsubscribe) = packet {
        return &unsubscribe.topic_filters[index];
    }

    panic!("Internal encoding error: invalid unsubscribe topic filter state");
}

#[rustfmt::skip]
pub(crate) fn write_unsubscribe_encoding_steps(packet: &UnsubscribePacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> MqttResult<()> {
    let (total_remaining_length, unsubscribe_property_length) = compute_unsubscribe_packet_length_properties(packet)?;

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

fn decode_unsubscribe_properties(property_bytes: &[u8], packet : &mut UnsubscribePacket) -> MqttResult<()> {
    let mut mutable_property_bytes = property_bytes;

    while !mutable_property_bytes.is_empty() {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            _ => {
                error!("UnsubscribePacket Decode - Invalid property type ({})", property_key);
                return Err(MqttError::new_decoding_failure("invalid property type for unsubscribe packet"));
            }
        }
    }

    Ok(())
}

pub(crate) fn decode_unsubscribe_packet(first_byte: u8, packet_body: &[u8]) -> MqttResult<Box<MqttPacket>> {

    if first_byte != UNSUBSCRIBE_FIRST_BYTE {
        error!("UnsubscribePacket Decode - invalid first byte");
        return Err(MqttError::new_decoding_failure("invalid first byte for unsubscribe packet"));
    }

    let mut box_packet = Box::new(MqttPacket::Unsubscribe(UnsubscribePacket { ..Default::default() }));

    if let MqttPacket::Unsubscribe(packet) = box_packet.as_mut() {
        let mut mutable_body = packet_body;
        mutable_body = decode_u16(mutable_body, &mut packet.packet_id)?;

        let mut properties_length: usize = 0;
        mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
        if properties_length > mutable_body.len() {
            error!("UnsubscribePacket Decode - property length exceeds overall packet length");
            return Err(MqttError::new_decoding_failure("property length exceeds overall packet length for unsubscribe packet"));
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

    panic!("UnsubscribePacket Decode - Internal error");
}

pub(crate) fn validate_unsubscribe_packet_outbound(packet: &UnsubscribePacket) -> MqttResult<()> {
    if packet.packet_id != 0 {
        error!("UnsubscribePacket Outbound Validation - packet id may not be set");
        return Err(MqttError::new_packet_validation(PacketType::Unsubscribe, "packet id set"));
    }

    if packet.topic_filters.is_empty() {
        error!("UnsubscribePacket Outbound Validation - empty topic filters list");
        return Err(MqttError::new_packet_validation(PacketType::Unsubscribe, "topic filters empty"));
    }

    // topic filters are checked in detail in the internal validator

    validate_user_properties(&packet.user_properties, PacketType::Unsubscribe, "Unsubscribe")?;

    Ok(())
}

pub(crate) fn validate_unsubscribe_packet_outbound_internal(packet: &UnsubscribePacket, context: &OutboundValidationContext) -> MqttResult<()> {

    let (total_remaining_length, _) = compute_unsubscribe_packet_length_properties(packet)?;
    let total_packet_length = 1 + total_remaining_length + compute_variable_length_integer_encode_size(total_remaining_length as usize)? as u32;
    if total_packet_length > context.negotiated_settings.unwrap().maximum_packet_size_to_server {
        error!("UnsubscribePacket Outbound Validation - packet length exceeds maximum packet size allowed to server");
        return Err(MqttError::new_packet_validation(PacketType::Unsubscribe, "packet length exceeds maximum packet size allowed"));
    }

    if packet.packet_id == 0 {
        error!("UnsubscribePacket Outbound Validation - packet id is zero");
        return Err(MqttError::new_packet_validation(PacketType::Unsubscribe, "packet id is zero"));
    }

    for filter in &packet.topic_filters {
        if !is_valid_topic_filter_internal(filter, context, None) {
            error!("UnsubscribePacket Outbound Validation - invalid topic filter");
            return Err(MqttError::new_packet_validation(PacketType::Unsubscribe, "invalid topic filter"));
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

    #[test]
    fn unsubscribe_round_trip_encode_decode_default() {
        let packet = UnsubscribePacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsubscribe(packet)));
    }

    #[test]
    fn unsubscribe_round_trip_encode_decode_basic() {
        let packet = UnsubscribePacket {
            packet_id : 123,
            topic_filters : vec![ "hello/world".to_string() ],
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsubscribe(packet)));
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
    fn unsubscribe_round_trip_encode_decode_all_properties() {
        let packet = create_unsubscribe_all_properties();
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsubscribe(packet)));
    }

    #[test]
    fn unsubscribe_decode_failure_bad_fixed_header() {
        let packet = UnsubscribePacket {
            packet_id : 123,
            topic_filters : vec![ "hello/world".to_string() ],
            ..Default::default()
        };

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Unsubscribe(packet), 14);
    }

    #[test]
    fn unsubscribe_decode_failure_inbound_packet_size() {
        let packet = create_unsubscribe_all_properties();

        do_inbound_size_decode_failure_test(&MqttPacket::Unsubscribe(packet));
    }

    use crate::validate::testing::*;

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

    use crate::validate::utils::testing::verify_validation_failure;

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
    fn unsubscribe_validate_failure_outbound_size() {
        let packet = create_unsubscribe_all_properties();

        do_outbound_size_validate_failure_test(&MqttPacket::Unsubscribe(packet), PacketType::Unsubscribe);
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
