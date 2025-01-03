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

#[rustfmt::skip]
#[cfg(test)]
fn compute_unsuback_packet_length_properties5(packet: &UnsubackPacket) -> GneissResult<(u32, u32)> {
    let mut unsuback_property_section_length = compute_user_properties_length(&packet.user_properties);
    add_optional_string_property_length!(unsuback_property_section_length, packet.reason_string);

    let mut total_remaining_length : usize = 2 + compute_variable_length_integer_encode_size(unsuback_property_section_length)?;
    total_remaining_length += unsuback_property_section_length;

    total_remaining_length += packet.reason_codes.len();

    Ok((total_remaining_length as u32, unsuback_property_section_length as u32))
}

#[cfg(test)]
fn get_unsuback_packet_reason_string(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Unsuback, reason_string)
}

#[cfg(test)]
fn get_unsuback_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Unsuback(unsuback) = packet {
        if let Some(properties) = &unsuback.user_properties {
            return &properties[index];
        }
    }

    panic!("get_unsuback_packet_user_property - invalid user property state");
}

#[rustfmt::skip]
#[cfg(test)]
pub(crate) fn write_unsuback_encoding_steps5(packet: &UnsubackPacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> GneissResult<()> {
    let (total_remaining_length, unsuback_property_length) = compute_unsuback_packet_length_properties5(packet)?;

    encode_integral_expression!(steps, Uint8, UNSUBACK_FIRST_BYTE);
    encode_integral_expression!(steps, Vli, total_remaining_length);

    encode_integral_expression!(steps, Uint16, packet.packet_id);
    encode_integral_expression!(steps, Vli, unsuback_property_length);

    encode_optional_string_property!(steps, get_unsuback_packet_reason_string, PROPERTY_KEY_REASON_STRING, packet.reason_string);
    encode_user_properties!(steps, get_unsuback_packet_user_property, packet.user_properties);

    let reason_codes = &packet.reason_codes;
    for reason_code in reason_codes {
        encode_enum!(steps, Uint8, u8, *reason_code);
    }

    Ok(())
}

#[cfg(not(test))]
pub(crate) fn write_unsuback_encoding_steps5(_: &UnsubackPacket, _: &EncodingContext, _: &mut VecDeque<EncodingStep>) -> GneissResult<()> {
    Err(GneissError::new_unimplemented("write_unsuback_encoding_steps5 - test-only functionality"))
}

#[cfg(test)]
pub(crate) fn write_unsuback_encoding_steps311(packet: &UnsubackPacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> GneissResult<()> {
    encode_integral_expression!(steps, Uint8, UNSUBACK_FIRST_BYTE);
    encode_integral_expression!(steps, Vli, 2);
    encode_integral_expression!(steps, Uint16, packet.packet_id);

    Ok(())
}

#[cfg(not(test))]
pub(crate) fn write_unsuback_encoding_steps311(_: &UnsubackPacket, _: &EncodingContext, _: &mut VecDeque<EncodingStep>) -> GneissResult<()> {
    Err(GneissError::new_unimplemented("write_unsuback_encoding_steps311 - test-only functionality"))
}

fn decode_unsuback_properties(property_bytes: &[u8], packet : &mut UnsubackPacket) -> GneissResult<()> {
    let mut mutable_property_bytes = property_bytes;

    while !mutable_property_bytes.is_empty() {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_REASON_STRING => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.reason_string)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            _ => {
                let message = format!("decode_unsuback_properties - invalid property type ({})", property_key);
                error!("{}", message);
                return Err(GneissError::new_decoding_failure(message));
            }
        }
    }

    Ok(())
}

pub(crate) fn decode_unsuback_packet5(first_byte: u8, packet_body: &[u8]) -> GneissResult<Box<MqttPacket>> {

    if first_byte != UNSUBACK_FIRST_BYTE {
        let message = "decode_unsuback_packet5 - invalid first byte";
        error!("{}", message);
        return Err(GneissError::new_decoding_failure(message));
    }

    let mut box_packet = Box::new(MqttPacket::Unsuback(UnsubackPacket { ..Default::default() }));

    if let MqttPacket::Unsuback(packet) = box_packet.as_mut() {
        let mut mutable_body = packet_body;
        mutable_body = decode_u16(mutable_body, &mut packet.packet_id)?;

        let mut properties_length: usize = 0;
        mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
        if properties_length > mutable_body.len() {
            let message = "decode_unsuback_packet5 - property length exceeds overall packet length";
            error!("{}", message);
            return Err(GneissError::new_decoding_failure(message));
        }

        let properties_bytes = &mutable_body[..properties_length];
        let payload_bytes = &mutable_body[properties_length..];

        decode_unsuback_properties(properties_bytes, packet)?;

        let reason_code_count = payload_bytes.len();
        packet.reason_codes.reserve(reason_code_count);

        for payload_byte in payload_bytes.iter().take(reason_code_count) {
            packet.reason_codes.push(UnsubackReasonCode::try_from(*payload_byte)?);
        }

        return Ok(box_packet);
    }

    panic!("decode_unsuback_packet5 - internal error");
}

pub(crate) fn decode_unsuback_packet311(first_byte: u8, packet_body: &[u8]) -> GneissResult<Box<MqttPacket>> {
    if first_byte != UNSUBACK_FIRST_BYTE {
        let message = "decode_unsuback_packet311 - invalid first byte";
        error!("{}", message);
        return Err(GneissError::new_decoding_failure(message));
    }

    if packet_body.len () != 2 {
        let message = "decode_unsuback_packet311 - invalid remaining length";
        error!("{}", message);
        return Err(GneissError::new_decoding_failure(message));
    }

    let mut box_packet = Box::new(MqttPacket::Unsuback(UnsubackPacket { ..Default::default() }));
    if let MqttPacket::Unsuback(packet) = box_packet.as_mut() {
        decode_u16(packet_body, &mut packet.packet_id)?;

        return Ok(box_packet);
    }

    panic!("decode_unsuback_packet311 - internal error");
}

validate_ack_inbound_internal!(validate_unsuback_packet_inbound_internal, UnsubackPacket, PacketType::Unsuback, "validate_unsuback_packet_inbound_internal");

impl fmt::Display for UnsubackPacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UnsubackPacket {{")?;
        log_primitive_value!(self.packet_id, f, "packet_id");
        log_optional_string!(self.reason_string, f, "reason_string", value);
        log_user_properties!(self.user_properties, f, "user_properties", value);
        write!(f, " reason_codes: [")?;
        for (i, rc) in self.reason_codes.iter().enumerate() {
            write!(f, " {}: {}", i, rc)?;
        }
        write!(f, " ] }}")
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    fn do_unsuback_round_trip_encode_decode_default_test(protocol_version: ProtocolVersion) {
        let packet = UnsubackPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsuback(packet), protocol_version));
    }

    #[test]
    fn unsuback_round_trip_encode_decode_default5() {
        do_unsuback_round_trip_encode_decode_default_test(ProtocolVersion::Mqtt5);
    }

    #[test]
    fn unsuback_round_trip_encode_decode_default311() {
        do_unsuback_round_trip_encode_decode_default_test(ProtocolVersion::Mqtt311);
    }

    #[test]
    fn unsuback_round_trip_encode_decode_required5() {
        let packet = UnsubackPacket {
            packet_id : 1023,
            reason_codes : vec![
                UnsubackReasonCode::ImplementationSpecificError,
                UnsubackReasonCode::Success,
                UnsubackReasonCode::TopicNameInvalid
            ],
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsuback(packet), ProtocolVersion::Mqtt5));
    }

    #[test]
    fn unsuback_round_trip_encode_decode_required311() {
        let packet = UnsubackPacket {
            packet_id : 1023,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsuback(packet), ProtocolVersion::Mqtt311));
    }

    fn create_unsuback_all_properties() -> UnsubackPacket {
        UnsubackPacket {
            packet_id : 1023,
            reason_codes : vec![
                UnsubackReasonCode::NotAuthorized,
                UnsubackReasonCode::PacketIdentifierInUse,
                UnsubackReasonCode::Success
            ],
            reason_string : Some("Didn't feel like it".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "Time".to_string(), value: "togohome".to_string()},
                UserProperty{name: "Ouch".to_string(), value: "backhurts".to_string()},
            ))
        }
    }

    #[test]
    fn unsuback_round_trip_encode_decode_all5() {
        let packet = create_unsuback_all_properties();
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Unsuback(packet), ProtocolVersion::Mqtt5));
    }

    #[test]
    fn unsuback_round_trip_encode_decode_all311() {
        let packet = create_unsuback_all_properties();
        let expected_packet = UnsubackPacket {
            packet_id: packet.packet_id,
            ..Default::default()
        };

        assert!(do_311_filter_encode_decode_test(&MqttPacket::Unsuback(packet), &MqttPacket::Unsuback(expected_packet)));
    }

    fn do_unsuback_decode_failure_bad_fixed_header_test(protocol_version: ProtocolVersion) {
        let packet = UnsubackPacket {
            packet_id : 1023,
            reason_codes : vec![
                UnsubackReasonCode::ImplementationSpecificError,
                UnsubackReasonCode::Success,
                UnsubackReasonCode::TopicNameInvalid
            ],
            ..Default::default()
        };

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Unsuback(packet), protocol_version, 9);
    }

    #[test]
    fn unsuback_decode_failure_bad_fixed_header5() {
        do_unsuback_decode_failure_bad_fixed_header_test(ProtocolVersion::Mqtt5);
    }

    #[test]
    fn unsuback_decode_failure_bad_fixed_header311() {
        do_unsuback_decode_failure_bad_fixed_header_test(ProtocolVersion::Mqtt311);
    }

    #[test]
    fn unsuback_decode_failure_reason_code_invalid5() {
        let packet = SubackPacket {
            packet_id : 1023,
            reason_codes : vec![
                SubackReasonCode::GrantedQos1
            ],
            ..Default::default()
        };

        let corrupt_reason_code = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // for acks, the reason code is in byte 4
            clone[5] = 196;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Suback(packet), ProtocolVersion::Mqtt5, corrupt_reason_code);
    }

    const UNSUBACK_PACKET_TEST_PROPERTY_LENGTH_INDEX : usize = 4;
    const UNSUBACK_PACKET_TEST_PAYLOAD_INDEX : usize = 12;

    #[test]
    fn unsuback_decode_failure_duplicate_reason_string5() {

        let packet = UnsubackPacket {
            packet_id : 1023,
            reason_string: Some("derp".to_string()),
            reason_codes : vec![
                UnsubackReasonCode::UnspecifiedError
            ],
            ..Default::default()
        };

        let duplicate_reason_string = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[1] += 6;
            clone[UNSUBACK_PACKET_TEST_PROPERTY_LENGTH_INDEX] += 6;

            clone.insert(UNSUBACK_PACKET_TEST_PAYLOAD_INDEX, 67);
            clone.insert(UNSUBACK_PACKET_TEST_PAYLOAD_INDEX, 67);
            clone.insert(UNSUBACK_PACKET_TEST_PAYLOAD_INDEX, 67);
            clone.insert(UNSUBACK_PACKET_TEST_PAYLOAD_INDEX, 3);
            clone.insert(UNSUBACK_PACKET_TEST_PAYLOAD_INDEX, 0);
            clone.insert(UNSUBACK_PACKET_TEST_PAYLOAD_INDEX, PROPERTY_KEY_REASON_STRING);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Unsuback(packet), ProtocolVersion::Mqtt5, duplicate_reason_string);
    }

    #[test]
    fn unsuback_decode_failure_inbound_packet_size5() {
        let packet = create_unsuback_all_properties();

        do_inbound_size_decode_failure_test(&MqttPacket::Unsuback(packet), ProtocolVersion::Mqtt5);
    }

    #[test]
    fn unsuback_decode_failure_inbound_packet_size311() {
        let packet = create_unsuback_all_properties();

        do_inbound_size_decode_failure_test(&MqttPacket::Unsuback(packet), ProtocolVersion::Mqtt311);
    }

    use crate::validate::testing::*;

    test_ack_validate_failure_inbound_packet_id_zero!(unsuback_validate_failure_internal_packet_id_zero, Unsuback, create_unsuback_all_properties, PacketType::Unsuback);
}