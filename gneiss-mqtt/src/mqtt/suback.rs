/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::decode::*;
use crate::encode::*;
use crate::error::{GneissError, GneissResult};
use crate::logging::*;
use crate::mqtt::*;
use crate::mqtt::utils::*;
use crate::validate::*;

use std::collections::VecDeque;
use std::fmt;

#[cfg(test)]
#[rustfmt::skip]
fn compute_suback_packet_length_properties5(packet: &SubackPacket) -> GneissResult<(u32, u32)> {
    let mut suback_property_section_length = compute_user_properties_length(&packet.user_properties);
    add_optional_string_property_length!(suback_property_section_length, packet.reason_string);

    let mut total_remaining_length : usize = 2 + compute_variable_length_integer_encode_size(suback_property_section_length)?;
    total_remaining_length += suback_property_section_length;

    total_remaining_length += packet.reason_codes.len();

    Ok((total_remaining_length as u32, suback_property_section_length as u32))
}

#[cfg(test)]
fn get_suback_packet_reason_string(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Suback, reason_string)
}

#[cfg(test)]
fn get_suback_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Suback(suback) = packet {
        if let Some(properties) = &suback.user_properties {
            return &properties[index];
        }
    }

    panic!("get_suback_packet_user_property - invalid user property state");
}

#[rustfmt::skip]
#[cfg(test)]
pub(crate) fn write_suback_encoding_steps5(packet: &SubackPacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> GneissResult<()> {
    let (total_remaining_length, suback_property_length) = compute_suback_packet_length_properties5(packet)?;

    encode_integral_expression!(steps, Uint8, SUBACK_FIRST_BYTE);
    encode_integral_expression!(steps, Vli, total_remaining_length);

    encode_integral_expression!(steps, Uint16, packet.packet_id);
    encode_integral_expression!(steps, Vli, suback_property_length);

    encode_optional_string_property!(steps, get_suback_packet_reason_string, PROPERTY_KEY_REASON_STRING, packet.reason_string);
    encode_user_properties!(steps, get_suback_packet_user_property, packet.user_properties);

    let reason_codes = &packet.reason_codes;
    for reason_code in reason_codes {
        encode_enum!(steps, Uint8, u8, *reason_code);
    }

    Ok(())
}

#[cfg(not(test))]
pub(crate) fn write_suback_encoding_steps5(_: &SubackPacket, _: &EncodingContext, _: &mut VecDeque<EncodingStep>) -> GneissResult<()> {
    Err(GneissError::new_unimplemented("write_suback_encoding_steps5 - test-only functionality"))
}

#[cfg(test)]
#[rustfmt::skip]
fn compute_suback_packet_length_properties311(packet: &SubackPacket) -> GneissResult<u32> {
    let mut total_remaining_length : usize = 2;
    total_remaining_length += packet.reason_codes.len();

    Ok(total_remaining_length as u32)
}

#[cfg(test)]
pub(crate) fn write_suback_encoding_steps311(packet: &SubackPacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> GneissResult<()> {
    let total_remaining_length = compute_suback_packet_length_properties311(packet)?;

    encode_integral_expression!(steps, Uint8, SUBACK_FIRST_BYTE);
    encode_integral_expression!(steps, Vli, total_remaining_length);

    encode_integral_expression!(steps, Uint16, packet.packet_id);

    let reason_codes = &packet.reason_codes;
    for reason_code in reason_codes {
        encode_enum_with_function!(steps, Uint8, u8, *reason_code, convert_suback_reason_code_to_311_encoding);
    }

    Ok(())
}

#[cfg(not(test))]
pub(crate) fn write_suback_encoding_steps311(_: &SubackPacket, _: &EncodingContext, _: &mut VecDeque<EncodingStep>) -> GneissResult<()> {
    Err(GneissError::new_unimplemented("write_suback_encoding_steps311 - test-only functionality"))
}

fn decode_suback_properties(property_bytes: &[u8], packet : &mut SubackPacket) -> GneissResult<()> {
    let mut mutable_property_bytes = property_bytes;

    while !mutable_property_bytes.is_empty() {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_REASON_STRING => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.reason_string)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            _ => {
                let message = format!("decode_suback_properties - Invalid property type ({})", property_key);
                error!("{}", message);
                return Err(GneissError::new_decoding_failure(message));
            }
        }
    }

    Ok(())
}

pub(crate) fn decode_suback_packet5(first_byte: u8, packet_body: &[u8]) -> GneissResult<Box<MqttPacket>> {
    if first_byte != SUBACK_FIRST_BYTE {
        let message = "decode_suback_packet5 - invalid first byte";
        error!("{}", message);
        return Err(GneissError::new_decoding_failure(message));
    }

    let mut box_packet = Box::new(MqttPacket::Suback(SubackPacket { ..Default::default() }));

    if let MqttPacket::Suback(packet) = box_packet.as_mut() {
        let mut mutable_body = packet_body;
        mutable_body = decode_u16(mutable_body, &mut packet.packet_id)?;

        let mut properties_length: usize = 0;
        mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
        if properties_length > mutable_body.len() {
            let message = "decode_suback_packet5 - property length exceeds overall packet length";
            error!("{}", message);
            return Err(GneissError::new_decoding_failure(message));
        }

        let properties_bytes = &mutable_body[..properties_length];
        let payload_bytes = &mutable_body[properties_length..];

        decode_suback_properties(properties_bytes, packet)?;

        let reason_code_count = payload_bytes.len();
        packet.reason_codes.reserve(reason_code_count);

        for payload_byte in payload_bytes.iter().take(reason_code_count) {
            packet.reason_codes.push(SubackReasonCode::try_from(*payload_byte)?);
        }

        return Ok(box_packet);
    }

    panic!("decode_suback_packet5 - internal error");
}

pub(crate) fn decode_suback_packet311(first_byte: u8, packet_body: &[u8]) -> GneissResult<Box<MqttPacket>> {
    if first_byte != SUBACK_FIRST_BYTE {
        let message = "decode_suback_packet311 - invalid first byte";
        error!("{}", message);
        return Err(GneissError::new_decoding_failure(message));
    }

    let mut box_packet = Box::new(MqttPacket::Suback(SubackPacket { ..Default::default() }));

    if let MqttPacket::Suback(packet) = box_packet.as_mut() {
        let mut mutable_body = packet_body;
        mutable_body = decode_u16(mutable_body, &mut packet.packet_id)?;

        let reason_code_count = mutable_body.len();
        packet.reason_codes.reserve(reason_code_count);

        for payload_byte in mutable_body.iter().take(reason_code_count) {
            packet.reason_codes.push(convert_311_encoding_to_suback_reason_code(*payload_byte)?);
        }

        return Ok(box_packet);
    }

    panic!("decode_suback_packet311 - internal error");
}

validate_ack_inbound_internal!(validate_suback_packet_inbound_internal, SubackPacket, PacketType::Suback, "validate_suback_packet_inbound_internal");

impl fmt::Display for SubackPacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SubackPacket {{")?;
        log_primitive_value!(self.packet_id, f, "packet_id");
        log_optional_string!(self.reason_string, f, "reason_string", value);
        log_user_properties!(self.user_properties, f, "user_properties", value);
        write!(f, " reason_codes: [")?;
        for (i, rc) in self.reason_codes.iter().enumerate() {
            write!(f, " {}: {}", i, rc)?;
        }
        write!(f, " ]")?;
        write!(f, " }}")
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    fn do_suback_round_trip_encode_decode_default(protocol_version: ProtocolVersion) {
        let packet = SubackPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Suback(packet), protocol_version));
    }

    #[test]
    fn suback_round_trip_encode_decode_default5() {
        do_suback_round_trip_encode_decode_default(ProtocolVersion::Mqtt5);
    }

    #[test]
    fn suback_round_trip_encode_decode_default311() {
        do_suback_round_trip_encode_decode_default(ProtocolVersion::Mqtt311);
    }

    fn do_suback_round_trip_encode_decode_required_test(protocol_version: ProtocolVersion) {
        let packet = SubackPacket {
            packet_id : 1023,
            reason_codes : vec![
                SubackReasonCode::GrantedQos1,
                SubackReasonCode::GrantedQos2,
                SubackReasonCode::UnspecifiedError,
            ],
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Suback(packet), protocol_version));
    }

    #[test]
    fn suback_round_trip_encode_decode_required5() {
        do_suback_round_trip_encode_decode_required_test(ProtocolVersion::Mqtt5);
    }

    #[test]
    fn suback_round_trip_encode_decode_required311() {
        do_suback_round_trip_encode_decode_required_test(ProtocolVersion::Mqtt311);
    }

    fn create_suback_all_properties() -> SubackPacket {
        SubackPacket {
            packet_id : 1023,
            reason_codes : vec![
                SubackReasonCode::GrantedQos2,
                SubackReasonCode::UnspecifiedError,
                SubackReasonCode::SharedSubscriptionsNotSupported
            ],
            reason_string : Some("Maybe tomorrow".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "This".to_string(), value: "wasfast".to_string()},
                UserProperty{name: "Onepacket".to_string(), value: "left".to_string()},
            ))
        }
    }

    #[test]
    fn suback_round_trip_encode_decode_all5() {
        let packet = create_suback_all_properties();
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Suback(packet), ProtocolVersion::Mqtt5));
    }

    #[test]
    fn suback_round_trip_encode_decode_all311() {
        let packet = create_suback_all_properties();
        let expected_packet = SubackPacket {
            packet_id : packet.packet_id,
            reason_codes : vec![
                SubackReasonCode::GrantedQos2,
                SubackReasonCode::UnspecifiedError,
                SubackReasonCode::UnspecifiedError
            ],
            ..Default::default()
        };
        assert!(do_311_filter_encode_decode_test(&MqttPacket::Suback(packet), &MqttPacket::Suback(expected_packet)));
    }

    fn do_suback_decode_failure_bad_fixed_header_test(protocol_version: ProtocolVersion) {
        let packet = SubackPacket {
            packet_id : 1023,
            reason_codes : vec![
                SubackReasonCode::GrantedQos1,
                SubackReasonCode::QuotaExceeded,
                SubackReasonCode::SubscriptionIdentifiersNotSupported,
            ],
            ..Default::default()
        };

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Suback(packet), protocol_version, 15);
    }

    #[test]
    fn suback_decode_failure_bad_fixed_header5() {
        do_suback_decode_failure_bad_fixed_header_test(ProtocolVersion::Mqtt5);
    }

    #[test]
    fn suback_decode_failure_bad_fixed_header311() {
        do_suback_decode_failure_bad_fixed_header_test(ProtocolVersion::Mqtt311);
    }

    fn do_suback_decode_failure_reason_code_invalid_test(protocol_version: ProtocolVersion, index: usize) {
        let packet = SubackPacket {
            packet_id : 1023,
            reason_codes : vec![
                SubackReasonCode::GrantedQos1
            ],
            ..Default::default()
        };

        let corrupt_reason_code = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[index] = 196;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Suback(packet), protocol_version, corrupt_reason_code);
    }

    #[test]
    fn suback_decode_failure_reason_code_invalid5() {
        do_suback_decode_failure_reason_code_invalid_test(ProtocolVersion::Mqtt5, 5);
    }

    #[test]
    fn suback_decode_failure_reason_code_invalid311() {
        do_suback_decode_failure_reason_code_invalid_test(ProtocolVersion::Mqtt311, 4);
    }

    const SUBACK_PACKET_TEST_PROPERTY_LENGTH_INDEX : usize = 4;
    const SUBACK_PACKET_TEST_PAYLOAD_INDEX : usize = 12;

    #[test]
    fn suback_decode_failure_duplicate_reason_string5() {

        let packet = SubackPacket {
            packet_id : 1023,
            reason_string: Some("derp".to_string()),
            reason_codes : vec![
                SubackReasonCode::GrantedQos1
            ],
            ..Default::default()
        };

        let duplicate_reason_string = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[1] += 4;
            clone[SUBACK_PACKET_TEST_PROPERTY_LENGTH_INDEX] += 4;

            clone.insert(SUBACK_PACKET_TEST_PAYLOAD_INDEX, 65);
            clone.insert(SUBACK_PACKET_TEST_PAYLOAD_INDEX, 1);
            clone.insert(SUBACK_PACKET_TEST_PAYLOAD_INDEX, 0);
            clone.insert(SUBACK_PACKET_TEST_PAYLOAD_INDEX, PROPERTY_KEY_REASON_STRING);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Suback(packet), ProtocolVersion::Mqtt5, duplicate_reason_string);
    }

    #[test]
    fn suback_decode_failure_inbound_packet_size5() {
        let packet = create_suback_all_properties();

        do_inbound_size_decode_failure_test(&MqttPacket::Suback(packet), ProtocolVersion::Mqtt5);
    }

    #[test]
    fn suback_decode_failure_inbound_packet_size311() {
        let packet = create_suback_all_properties();

        do_inbound_size_decode_failure_test(&MqttPacket::Suback(packet), ProtocolVersion::Mqtt311);
    }

    use crate::validate::testing::*;

    test_ack_validate_failure_inbound_packet_id_zero!(suback_validate_failure_internal_packet_id_zero, Suback, create_suback_all_properties, PacketType::Suback);
}
