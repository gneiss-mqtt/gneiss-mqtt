/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */


use crate::decode::utils::*;
use crate::encode::*;
use crate::encode::utils::*;
use crate::error::{MqttError, MqttResult};
use crate::logging::*;
use crate::mqtt::*;
use crate::mqtt::utils::*;
use crate::validate::*;
use crate::validate::utils::*;

use std::collections::VecDeque;
use std::fmt;


#[rustfmt::skip]
fn compute_auth_packet_length_properties(packet: &AuthPacket) -> MqttResult<(u32, u32)> {
    let mut auth_property_section_length = compute_user_properties_length(&packet.user_properties);

    add_optional_string_property_length!(auth_property_section_length, packet.authentication_method);
    add_optional_bytes_property_length!(auth_property_section_length, packet.authentication_data);
    add_optional_string_property_length!(auth_property_section_length, packet.reason_string);

    /* 2-byte auth packets are allowed by the spec when there are no properties and the reason code is success */
    if auth_property_section_length == 0 && packet.reason_code == AuthenticateReasonCode::Success {
        return Ok((0, 0));
    }

    let mut total_remaining_length : usize = 1 + compute_variable_length_integer_encode_size(auth_property_section_length)?;
    total_remaining_length += auth_property_section_length;

    Ok((total_remaining_length as u32, auth_property_section_length as u32))
}

fn get_auth_packet_authentication_method(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Auth, authentication_method)
}

fn get_auth_packet_authentication_data(packet: &MqttPacket) -> &[u8] {
    get_optional_packet_field!(packet, MqttPacket::Auth, authentication_data)
}

fn get_auth_packet_reason_string(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Auth, reason_string)
}

fn get_auth_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Auth(auth) = packet {
        if let Some(properties) = &auth.user_properties {
            return &properties[index];
        }
    }

    panic!("Internal encoding error: invalid user property state");
}

#[rustfmt::skip]
pub(crate) fn write_auth_encoding_steps(packet: &AuthPacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> MqttResult<()> {
    let (total_remaining_length, auth_property_length) = compute_auth_packet_length_properties(packet)?;

    encode_integral_expression!(steps, Uint8, PACKET_TYPE_AUTH << 4);
    encode_integral_expression!(steps, Vli, total_remaining_length);

    if total_remaining_length == 0 {
        return Ok(());
    }

    encode_enum!(steps, Uint8, u8, packet.reason_code);
    encode_integral_expression!(steps, Vli, auth_property_length);

    encode_optional_string_property!(steps, get_auth_packet_authentication_method, PROPERTY_KEY_AUTHENTICATION_METHOD, packet.authentication_method);
    encode_optional_bytes_property!(steps, get_auth_packet_authentication_data, PROPERTY_KEY_AUTHENTICATION_DATA, packet.authentication_data);
    encode_optional_string_property!(steps, get_auth_packet_reason_string, PROPERTY_KEY_REASON_STRING, packet.reason_string);
    encode_user_properties!(steps, get_auth_packet_user_property, packet.user_properties);

    Ok(())
}


fn decode_auth_properties(property_bytes: &[u8], packet : &mut AuthPacket) -> MqttResult<()> {
    let mut mutable_property_bytes = property_bytes;

    while !mutable_property_bytes.is_empty() {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_AUTHENTICATION_METHOD => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.authentication_method)?; }
            PROPERTY_KEY_AUTHENTICATION_DATA => { mutable_property_bytes = decode_optional_length_prefixed_bytes(mutable_property_bytes, &mut packet.authentication_data)?; }
            PROPERTY_KEY_REASON_STRING => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.reason_string)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            _ => {
                error!("AuthPacket Decode - Invalid property type ({})", property_key);
                return Err(MqttError::new_decoding_failure("invalid property type for auth packet"));
            }
        }
    }

    Ok(())
}

pub(crate) fn decode_auth_packet(first_byte: u8, packet_body: &[u8]) -> MqttResult<Box<MqttPacket>> {
    if first_byte != (PACKET_TYPE_AUTH << 4) {
        error!("AuthPacket Decode - invalid first byte");
        return Err(MqttError::new_decoding_failure("invalid first byte for auth packet"));
    }

    let mut box_packet = Box::new(MqttPacket::Auth(AuthPacket { ..Default::default() }));
    if let MqttPacket::Auth(packet) = box_packet.as_mut() {
        let mut mutable_body = packet_body;
        if mutable_body.is_empty() {
            return Ok(box_packet);
        }

        mutable_body = decode_u8_as_enum(mutable_body, &mut packet.reason_code, convert_u8_to_authenticate_reason_code)?;

        let mut properties_length : usize = 0;
        mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
        if properties_length != mutable_body.len() {
            error!("AuthPacket Decode - property length does not match expected overall packet length");
            return Err(MqttError::new_decoding_failure("property length mismatches overall packet length for auth packet"));
        }

        decode_auth_properties(mutable_body, packet)?;

        return Ok(box_packet);
    }

    panic!("AuthPacket Decode - Internal error");
}

pub(crate) fn validate_auth_packet_outbound(packet: &AuthPacket) -> MqttResult<()> {

    if packet.authentication_method.is_none() {
        error!("AuthPacket Outbound Validation - authentication method must be set");
        // while optional from an encode/decode perspective, method is required from a protocol
        // perspective
        return Err(MqttError::new_packet_validation(PacketType::Auth, "missing authentication_method field"));
    }

    validate_optional_string_length(&packet.authentication_method, PacketType::Auth, "Auth", "authentication_method")?;
    validate_optional_binary_length(&packet.authentication_data, PacketType::Auth, "Auth", "authentication_data")?;
    validate_optional_string_length(&packet.reason_string, PacketType::Auth, "Auth", "reason_string")?;
    validate_user_properties(&packet.user_properties, PacketType::Auth, "Auth")?;

    Ok(())
}

pub(crate) fn validate_auth_packet_outbound_internal(packet: &AuthPacket, context: &OutboundValidationContext) -> MqttResult<()> {

    let (total_remaining_length, _) = compute_auth_packet_length_properties(packet)?;
    let total_packet_length = 1 + total_remaining_length + compute_variable_length_integer_encode_size(total_remaining_length as usize)? as u32;
    if total_packet_length > context.negotiated_settings.unwrap().maximum_packet_size_to_server {
        error!("AuthPacket Outbound Validation - packet length exceeds maximum packet size allowed to server");
        return Err(MqttError::new_packet_validation(PacketType::Auth, "packet length exceeds maximum allowed packet size"));
    }

    Ok(())
}

pub(crate) fn validate_auth_packet_inbound_internal(packet: &AuthPacket, _: &InboundValidationContext) -> MqttResult<()> {

    if packet.authentication_method.is_none() {
        // while optional from an encode/decode perspective, method is required from a protocol
        // perspective
        error!("AuthPacket Inbound Validation - authentication method must be set");
        return Err(MqttError::new_packet_validation(PacketType::Auth, "missing authentication_method field"));
    }

    /* TODO: validation based on in-progress auth exchange */

    Ok(())
}

impl fmt::Display for AuthPacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AuthPacket {{")?;
        log_enum!(self.reason_code, f, "reason_code", authenticate_reason_code_to_str);
        log_optional_string!(self.authentication_method, f, "authentication_method", value);
        log_optional_binary_data_sensitive!(self.authentication_data, f, "authentication_data");
        log_optional_string!(self.reason_string, f, "reason_string", value);
        log_user_properties!(self.user_properties, f, "user_properties", value);
        write!(f, " }}")
    }
}

#[cfg(test)]
mod tests {
    use crate::decode::testing::*;
    use super::*;

    #[test]
    fn auth_round_trip_encode_decode_default() {
        let packet = AuthPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Auth(packet)));
    }

    #[test]
    fn auth_round_trip_encode_decode_required() {
        let packet = AuthPacket {
            reason_code : AuthenticateReasonCode::ContinueAuthentication,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Auth(packet)));
    }

    fn create_all_properties_auth_packet() -> AuthPacket {
        AuthPacket {
            reason_code : AuthenticateReasonCode::ContinueAuthentication,
            authentication_method : Some("UnbreakableAuthExchange".to_string()),
            authentication_data : Some("Noonewillguessthis".as_bytes().to_vec()),
            reason_string : Some("Myfavoritebroker".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "Roblox".to_string(), value: "Wheredidmymoneygo".to_string()},
                UserProperty{name: "Beeswarmsimulator".to_string(), value: "Lootbox".to_string()},
            )),
        }
    }

    #[test]
    fn auth_round_trip_encode_decode_all_properties() {
        let packet = create_all_properties_auth_packet();

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Auth(packet)));
    }

    #[test]
    fn auth_decode_failure_bad_fixed_header() {
        let packet = AuthPacket {
            reason_code : AuthenticateReasonCode::ContinueAuthentication,
            ..Default::default()
        };

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Auth(packet), 1);
    }

    #[test]
    fn auth_decode_failure_bad_reason_code() {
        let packet = AuthPacket {
            reason_code : AuthenticateReasonCode::ContinueAuthentication,
            ..Default::default()
        };

        let corrupt_reason_code = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // the reason code is at position 2
            clone[2] = 1;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Auth(packet), corrupt_reason_code);
    }

    #[test]
    fn auth_decode_failure_duplicate_authentication_method() {
        let packet = AuthPacket {
            reason_code : AuthenticateReasonCode::ContinueAuthentication,
            authentication_method : Some("A".to_string()),
            ..Default::default()
        };

        let duplicate_authentication_method = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length by 4
            clone[1] += 4;

            // increase property section length by 4
            clone[3] += 4;

            // add the duplicate property
            clone.push(PROPERTY_KEY_AUTHENTICATION_METHOD);
            clone.push(1);
            clone.push(0);
            clone.push(66); // 'B'

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Auth(packet), duplicate_authentication_method);
    }

    #[test]
    fn auth_decode_failure_duplicate_authentication_data() {
        let packet = AuthPacket {
            reason_code : AuthenticateReasonCode::ContinueAuthentication,
            authentication_data : Some("A".as_bytes().to_vec()),
            ..Default::default()
        };

        let duplicate_authentication_data = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length by 4
            clone[1] += 4;

            // increase property section length by 4
            clone[3] += 4;

            // add the duplicate property
            clone.push(PROPERTY_KEY_AUTHENTICATION_DATA);
            clone.push(1);
            clone.push(0);
            clone.push(66); // 'B'

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Auth(packet), duplicate_authentication_data);
    }

    #[test]
    fn auth_decode_failure_duplicate_reason_string() {
        let packet = AuthPacket {
            reason_code : AuthenticateReasonCode::ContinueAuthentication,
            reason_string : Some("Derp".to_string()),
            ..Default::default()
        };

        let duplicate_reason_string = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length by 4
            clone[1] += 5;

            // increase property section length by 4
            clone[3] += 5;

            // add the duplicate property
            clone.push(PROPERTY_KEY_REASON_STRING);
            clone.push(2);
            clone.push(0);
            clone.push(72); // 'H'
            clone.push(105); // 'i'

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Auth(packet), duplicate_reason_string);
    }

    #[test]
    fn auth_decode_failure_packet_size() {
        let packet = AuthPacket {
            reason_code : AuthenticateReasonCode::ContinueAuthentication,
            reason_string : Some("Derp".to_string()),
            ..Default::default()
        };

        do_inbound_size_decode_failure_test(&MqttPacket::Auth(packet));
    }

    use crate::validate::testing::*;
    use crate::validate::utils::testing::verify_validation_failure;

    #[test]
    fn auth_validate_success_all_properties() {
        let packet = MqttPacket::Auth(create_all_properties_auth_packet());

        assert!(validate_packet_outbound(&packet).is_ok());

        let test_validation_context = create_pinned_validation_context();

        let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);
        assert!(validate_packet_outbound_internal(&packet, &outbound_validation_context).is_ok());

        let inbound_validation_context = create_inbound_validation_context_from_pinned(&test_validation_context);
        assert!(validate_packet_inbound_internal(&packet, &inbound_validation_context).is_ok());
    }

    #[test]
    fn auth_validate_outbound_failure_authentication_method_length() {
        let mut packet = create_all_properties_auth_packet();
        packet.authentication_method = Some("a".repeat(65537));

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Auth(packet)), PacketType::Auth);
    }

    #[test]
    fn auth_validate_outbound_failure_authentication_method_missing() {
        let mut packet = create_all_properties_auth_packet();
        packet.authentication_method = None;

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Auth(packet)), PacketType::Auth);
    }

    #[test]
    fn auth_validate_inbound_failure_authentication_method_missing() {
        let mut packet = create_all_properties_auth_packet();
        packet.authentication_method = None;

        let test_validation_context = create_pinned_validation_context();
        let inbound_validation_context = create_inbound_validation_context_from_pinned(&test_validation_context);
        verify_validation_failure!(validate_packet_inbound_internal(&MqttPacket::Auth(packet), &inbound_validation_context), PacketType::Auth);
    }

    #[test]
    fn auth_validate_outbound_failure_authentication_data_length() {
        let mut packet = create_all_properties_auth_packet();
        packet.authentication_data = Some(vec![0; 128 * 1024]);

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Auth(packet)), PacketType::Auth);
    }

    #[test]
    fn auth_validate_outbound_failure_reason_string_length() {
        let mut packet = create_all_properties_auth_packet();
        packet.reason_string = Some("a".repeat(199000));

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Auth(packet)), PacketType::Auth);
    }

    #[test]
    fn auth_validate_outbound_failure_invalid_user_properties() {
        let mut packet = create_all_properties_auth_packet();
        packet.user_properties = Some(create_invalid_user_properties());

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Auth(packet)), PacketType::Auth);
    }

    #[test]
    fn auth_validate_failure_context_specific_outbound_size() {
        let packet = create_all_properties_auth_packet();

        do_outbound_size_validate_failure_test(&MqttPacket::Auth(packet), PacketType::Auth);
    }
}