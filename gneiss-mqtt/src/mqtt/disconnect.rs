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
fn compute_disconnect_packet_length_properties(packet: &DisconnectPacket) -> GneissResult<(u32, u32)> {
    let mut disconnect_property_section_length = compute_user_properties_length(&packet.user_properties);

    add_optional_u32_property_length!(disconnect_property_section_length, packet.session_expiry_interval_seconds);
    add_optional_string_property_length!(disconnect_property_section_length, packet.reason_string);
    add_optional_string_property_length!(disconnect_property_section_length, packet.server_reference);

    if disconnect_property_section_length == 0 {
        if packet.reason_code == DisconnectReasonCode::NormalDisconnection {
            return Ok((0, 0));
        } else {
            return Ok((1, 0));
        }
    }

    let mut total_remaining_length : usize = 1 + compute_variable_length_integer_encode_size(disconnect_property_section_length)?;
    total_remaining_length += disconnect_property_section_length;

    Ok((total_remaining_length as u32, disconnect_property_section_length as u32))
}

fn get_disconnect_packet_reason_string(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Disconnect, reason_string)
}

fn get_disconnect_packet_server_reference(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Disconnect, server_reference)
}

fn get_disconnect_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Disconnect(disconnect) = packet {
        if let Some(properties) = &disconnect.user_properties {
            return &properties[index];
        }
    }

    panic!("get_disconnect_packet_user_property - invalid user property state");
}

#[rustfmt::skip]
pub(crate) fn write_disconnect_encoding_steps5(packet: &DisconnectPacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> GneissResult<()> {
    let (total_remaining_length, disconnect_property_length) = compute_disconnect_packet_length_properties(packet)?;

    encode_integral_expression!(steps, Uint8, PACKET_TYPE_DISCONNECT << 4);
    encode_integral_expression!(steps, Vli, total_remaining_length);

    if disconnect_property_length == 0 && packet.reason_code == DisconnectReasonCode::NormalDisconnection {
        assert_eq!(0, total_remaining_length);
        return Ok(());
    }

    encode_enum!(steps, Uint8, u8, packet.reason_code);

    if disconnect_property_length == 0 {
        assert_eq!(1, total_remaining_length);
        return Ok(());
    }

    encode_integral_expression!(steps, Vli, disconnect_property_length);

    encode_optional_property!(steps, Uint32, PROPERTY_KEY_SESSION_EXPIRY_INTERVAL, packet.session_expiry_interval_seconds);
    encode_optional_string_property!(steps, get_disconnect_packet_reason_string, PROPERTY_KEY_REASON_STRING, packet.reason_string);
    encode_optional_string_property!(steps, get_disconnect_packet_server_reference, PROPERTY_KEY_SERVER_REFERENCE, packet.server_reference);
    encode_user_properties!(steps, get_disconnect_packet_user_property, packet.user_properties);

    Ok(())
}

pub(crate) fn write_disconnect_encoding_steps311(_: &DisconnectPacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> GneissResult<()> {
    encode_integral_expression!(steps, Uint8, PACKET_TYPE_DISCONNECT << 4);
    encode_integral_expression!(steps, Uint8, 0);

    Ok(())
}

fn decode_disconnect_properties(property_bytes: &[u8], packet : &mut DisconnectPacket) -> GneissResult<()> {
    let mut mutable_property_bytes = property_bytes;

    while !mutable_property_bytes.is_empty() {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_SESSION_EXPIRY_INTERVAL => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut packet.session_expiry_interval_seconds)?; }
            PROPERTY_KEY_REASON_STRING => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.reason_string)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            PROPERTY_KEY_SERVER_REFERENCE => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.server_reference)?; }
            _ => {
                let message = format!("decode_disconnect_properties - invalid property type ({})", property_key);
                error!("{}", message);
                return Err(GneissError::new_decoding_failure(message));
            }
        }
    }

    Ok(())
}

pub(crate) fn decode_disconnect_packet5(first_byte: u8, packet_body: &[u8]) -> GneissResult<Box<MqttPacket>> {
    if first_byte != (PACKET_TYPE_DISCONNECT << 4) {
        let message = "decode_disconnect_packet5 - invalid first byte";
        error!("{}", message);
        return Err(GneissError::new_decoding_failure(message));
    }

    let mut box_packet = Box::new(MqttPacket::Disconnect(DisconnectPacket { ..Default::default() }));

    if let MqttPacket::Disconnect(packet) = box_packet.as_mut() {
        let mut mutable_body = packet_body;
        if mutable_body.is_empty() {
            return Ok(box_packet);
        }

        mutable_body = decode_u8_as_enum(mutable_body, &mut packet.reason_code, DisconnectReasonCode::try_from)?;
        if mutable_body.is_empty() {
            return Ok(box_packet);
        }

        let mut properties_length : usize = 0;
        mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
        if properties_length != mutable_body.len() {
            let message = "decode_disconnect_packet5 - property length exceeds overall packet length";
            error!("{}", message);
            return Err(GneissError::new_decoding_failure(message));
        }

        decode_disconnect_properties(mutable_body, packet)?;

        return Ok(box_packet);
    }

    panic!("decode_disconnect_packet - internal error");
}

pub(crate) fn decode_disconnect_packet311(first_byte: u8, packet_body: &[u8]) -> GneissResult<Box<MqttPacket>> {
    if !packet_body.is_empty() {
        let message = "decode_disconnect_packet311 - non-zero remaining length";
        error!("{}", message);
        return Err(GneissError::new_decoding_failure(message));
    }

    if first_byte != (PACKET_TYPE_DISCONNECT << 4) {
        let message = "decode_disconnect_packet311 - invalid first byte";
        error!("{}", message);
        return Err(GneissError::new_decoding_failure(message));
    }

    Ok(Box::new(MqttPacket::Disconnect(DisconnectPacket {
        reason_code : DisconnectReasonCode::NormalDisconnection,
        ..Default::default()
    })))
}

pub(crate) fn validate_disconnect_packet_outbound(packet: &DisconnectPacket) -> GneissResult<()> {

    validate_optional_string_length(&packet.reason_string, PacketType::Disconnect, "validate_disconnect_packet_outbound", "reason_string")?;
    validate_user_properties(&packet.user_properties, PacketType::Disconnect, "validate_disconnect_packet_outbound")?;
    validate_optional_string_length(&packet.server_reference, PacketType::Disconnect, "validate_disconnect_packet_outbound", "server_reference")?;

    Ok(())
}

pub(crate) fn validate_disconnect_packet_outbound_internal(packet: &DisconnectPacket, context: &OutboundValidationContext) -> GneissResult<()> {

    let (total_remaining_length, _) = compute_disconnect_packet_length_properties(packet)?;
    let total_packet_length = 1 + total_remaining_length + compute_variable_length_integer_encode_size(total_remaining_length as usize)? as u32;
    if total_packet_length > context.negotiated_settings.unwrap().maximum_packet_size_to_server {
        let message = "validate_disconnect_packet_outbound_internal - packet length exceeds maximum packet size allowed to server";
        error!("{}", message);
        return Err(GneissError::new_packet_validation(PacketType::Disconnect, message));
    }

    /*
     * the client is not allowed to set a non zero session expiry if a zero session expiry
     * was sent in the CONNECT.
     */
    let mut connect_session_expiry_interval = 0;
    if let Some(connect) = &context.connect_options {
        connect_session_expiry_interval = connect.session_expiry_interval_seconds.unwrap_or(0);
    }
    let disconnect_session_expiry_interval = packet.session_expiry_interval_seconds.unwrap_or(connect_session_expiry_interval);

    if connect_session_expiry_interval == 0 && disconnect_session_expiry_interval > 0 {
        let message = "validate_disconnect_packet_outbound_internal - session expiry interval cannot be non-zero when connect session expiry interval was zero";
        error!("{}", message);
        return Err(GneissError::new_packet_validation(PacketType::Disconnect, message));
    }

    Ok(())
}

pub(crate) fn validate_disconnect_packet_inbound_internal(packet: &DisconnectPacket, _: &InboundValidationContext) -> GneissResult<()> {

    /* protocol error for the server to send us a session expiry interval property */
    if packet.session_expiry_interval_seconds.is_some() {
        let message = "validate_disconnect_packet_inbound_internal - session expiry interval is non zero";
        error!("{}", message);
        return Err(GneissError::new_packet_validation(PacketType::Disconnect, message));
    }

    Ok(())
}

impl fmt::Display for DisconnectPacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DisconnectPacket {{")?;
        log_enum!(self.reason_code, f, "reason_code", DisconnectReasonCode);
        log_optional_primitive_value!(self.session_expiry_interval_seconds, f, "session_expiry_interval_seconds", value);
        log_user_properties!(self.user_properties, f, "user_properties", value);
        log_optional_string!(self.server_reference, f, "server_reference", value);
        write!(f, " }}")
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::client::config::*;
    use crate::decode::testing::*;
    use crate::validate::testing::*;

    fn do_disconnect_round_trip_encode_decode_default_test(protocol_version: ProtocolVersion) {
        let packet = DisconnectPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Disconnect(packet), protocol_version));
    }

    #[test]
    fn disconnect_round_trip_encode_decode_default5() {
        do_disconnect_round_trip_encode_decode_default_test(ProtocolVersion::Mqtt5);
    }

    #[test]
    fn disconnect_round_trip_encode_decode_default311() {
        do_disconnect_round_trip_encode_decode_default_test(ProtocolVersion::Mqtt311);
    }

    #[test]
    fn disconnect_round_trip_encode_decode_normal_reason_code5() {
        let packet = DisconnectPacket {
            reason_code : DisconnectReasonCode::NormalDisconnection,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Disconnect(packet), ProtocolVersion::Mqtt5));
    }

    #[test]
    fn disconnect_round_trip_encode_decode_abnormal_reason_code5() {
        let packet = DisconnectPacket {
            reason_code : DisconnectReasonCode::ConnectionRateExceeded,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Disconnect(packet), ProtocolVersion::Mqtt5));
    }

    fn create_disconnect_packet_all_properties() -> DisconnectPacket {
         DisconnectPacket {
            reason_code : DisconnectReasonCode::ConnectionRateExceeded,
            reason_string : Some("I don't like you".to_string()),
            server_reference : Some("far.far.away.com".to_string()),
            session_expiry_interval_seconds : Some(14400),
            user_properties: Some(vec!(
                UserProperty{name: "Super".to_string(), value: "Meatboy".to_string()},
                UserProperty{name: "Minsc".to_string(), value: "Boo".to_string()},
            )),
        }
    }

    #[test]
    fn disconnect_round_trip_encode_decode_all_properties5() {
        let packet = create_disconnect_packet_all_properties();

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Disconnect(packet), ProtocolVersion::Mqtt5));
    }

    #[test]
    fn disconnect_round_trip_encode_decode_all_properties311() {
        let packet = create_disconnect_packet_all_properties();
        let expected_packet = DisconnectPacket {
            ..Default::default()
        };

        assert!(do_311_filter_encode_decode_test(&MqttPacket::Disconnect(packet), &MqttPacket::Disconnect(expected_packet)));
    }

    fn do_disconnect_decode_failure_bad_fixed_header_test(protocol_version: ProtocolVersion) {
        let packet = DisconnectPacket {
            ..Default::default()
        };

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Disconnect(packet), protocol_version, 12);
    }

    #[test]
    fn disconnect_decode_failure_bad_fixed_header5() {
        do_disconnect_decode_failure_bad_fixed_header_test(ProtocolVersion::Mqtt5);
    }

    #[test]
    fn disconnect_decode_failure_bad_fixed_header311() {
        do_disconnect_decode_failure_bad_fixed_header_test(ProtocolVersion::Mqtt311);
    }

    #[test]
    fn disconnect_decode_failure_bad_reason_code5() {
        let packet = DisconnectPacket {
            reason_code : DisconnectReasonCode::DisconnectWithWillMessage,
            ..Default::default()
        };

        let corrupt_reason_code = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // for this packet, the reason code is in byte 2
            clone[2] = 240;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Disconnect(packet), ProtocolVersion::Mqtt5, corrupt_reason_code);
    }

    #[test]
    fn disconnect_decode_failure_duplicate_reason_string5() {
        let packet = create_disconnect_packet_all_properties();

        let duplicate_reason_string = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length
            clone[1] += 5;

            // increase property section length
            clone[3] += 5;

            // add the duplicate property
            clone.push(PROPERTY_KEY_REASON_STRING);
            clone.push(0);
            clone.push(2);
            clone.push(67);
            clone.push(67);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Disconnect(packet), ProtocolVersion::Mqtt5, duplicate_reason_string);
    }

    #[test]
    fn disconnect_decode_failure_duplicate_server_reference5() {
        let packet = create_disconnect_packet_all_properties();

        let duplicate_server_reference = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length
            clone[1] += 7;

            // increase property section length
            clone[3] += 7;

            // add the duplicate property
            clone.push(PROPERTY_KEY_SERVER_REFERENCE);
            clone.push(0);
            clone.push(4);
            clone.push(68);
            clone.push(69);
            clone.push(82);
            clone.push(80);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Disconnect(packet), ProtocolVersion::Mqtt5, duplicate_server_reference);
    }

    #[test]
    fn disconnect_decode_failure_duplicate_session_expiry_interval5() {
        let packet = create_disconnect_packet_all_properties();

        let duplicate_session_expiry_interval = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length
            clone[1] += 5;

            // increase property section length
            clone[3] += 5;

            // add the duplicate property
            clone.push(PROPERTY_KEY_SESSION_EXPIRY_INTERVAL);
            clone.push(1);
            clone.push(2);
            clone.push(3);
            clone.push(4);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Disconnect(packet), ProtocolVersion::Mqtt5, duplicate_session_expiry_interval);
    }

    #[test]
    fn disconnect_decode_failure_packet_size5() {
        let packet = create_disconnect_packet_all_properties();

        do_inbound_size_decode_failure_test(&MqttPacket::Disconnect(packet), ProtocolVersion::Mqtt5);
    }

    #[test]
    fn disconnect_validate_success() {
        let mut packet = create_disconnect_packet_all_properties();
        packet.session_expiry_interval_seconds = None;
        let mqtt_packet = MqttPacket::Disconnect(packet);

        assert!(validate_packet_outbound(&mqtt_packet).is_ok());

        let test_validation_context = create_pinned_validation_context();

        let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);
        assert!(validate_packet_outbound_internal(&mqtt_packet, &outbound_validation_context).is_ok());

        let inbound_validation_context = create_inbound_validation_context_from_pinned(&test_validation_context);
        assert!(validate_packet_inbound_internal(&mqtt_packet, &inbound_validation_context).is_ok());
    }

    #[test]
    fn disconnect_validate_failure_reason_string_length() {
        let mut packet = create_disconnect_packet_all_properties();
        packet.reason_string = Some("A".repeat(128 * 1024).to_string());

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Disconnect(packet)), PacketType::Disconnect);
    }

    #[test]
    fn disconnect_validate_failure_user_properties_invalid() {
        let mut packet = create_disconnect_packet_all_properties();
        packet.user_properties = Some(create_invalid_user_properties());

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Disconnect(packet)), PacketType::Disconnect);
    }

    #[test]
    fn disconnect_validate_failure_server_reference_length() {
        let mut packet = create_disconnect_packet_all_properties();
        packet.server_reference = Some("Z".repeat(65 * 1024).to_string());

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Disconnect(packet)), PacketType::Disconnect);
    }

    #[test]
    fn disconnect_validate_failure_inbound_session_expiry_interval_set_by_server() {
        let packet = MqttPacket::Disconnect( DisconnectPacket {
            reason_code: DisconnectReasonCode::ConnectionRateExceeded,
            session_expiry_interval_seconds: Some(3600),
            ..Default::default()
        });

        let test_validation_context = create_pinned_validation_context();
        let validation_context = create_inbound_validation_context_from_pinned(&test_validation_context);

        verify_validation_failure!(validate_packet_inbound_internal(&packet, &validation_context), PacketType::Disconnect);
    }

    #[test]
    fn disconnect_validate_failure_outbound_session_expiry_interval_set_after_implicit_zero() {
        let packet = create_disconnect_packet_all_properties();

        let mut test_validation_context = create_pinned_validation_context();
        test_validation_context.connect_options = ConnectOptions::builder().build();

        let validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);

        verify_validation_failure!(validate_packet_outbound_internal(&MqttPacket::Disconnect(packet), &validation_context), PacketType::Disconnect);
    }

    #[test]
    fn disconnect_validate_failure_context_session_expiry_interval_set_after_explicit_zero() {
        let packet = create_disconnect_packet_all_properties();

        let mut test_validation_context = create_pinned_validation_context();
        test_validation_context.connect_options = ConnectOptions::builder().with_session_expiry_interval_seconds(0).build();

        let validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);

        verify_validation_failure!(validate_packet_outbound_internal(&MqttPacket::Disconnect(packet), &validation_context), PacketType::Disconnect);
    }

    #[test]
    fn disconnect_validate_failure_context_specific_outbound_size() {
        let packet = DisconnectPacket {
            reason_code: DisconnectReasonCode::MalformedPacket,
            reason_string: Some("The rats in the walls".to_string()),
            ..Default::default()
        };

        do_outbound_size_validate_failure_test(&MqttPacket::Disconnect(packet), ProtocolVersion::Mqtt5, PacketType::Disconnect);
    }
}