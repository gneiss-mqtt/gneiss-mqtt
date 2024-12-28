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

fn get_connect_packet_client_id(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Connect, client_id)
}

fn get_connect_packet_authentication_method(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Connect, authentication_method)
}

fn get_connect_packet_authentication_data(packet: &MqttPacket) -> &[u8] {
    get_optional_packet_field!(packet, MqttPacket::Connect, authentication_data)
}

fn get_connect_packet_username(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Connect, username)
}

fn get_connect_packet_password(packet: &MqttPacket) -> &[u8] {
    get_optional_packet_field!(packet, MqttPacket::Connect, password)
}

fn get_connect_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(properties) = &connect.user_properties {
            return &properties[index];
        }
    }

    panic!("get_connect_packet_user_property - invalid user property state");
}

fn get_connect_packet_will_content_type(packet: &MqttPacket) -> &str {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(will) = &connect.will {
            if let Some(content_type) = &will.content_type {
                return content_type;
            }
        }
    }

    panic!("get_connect_packet_will_content_type - will content type accessor invoked in an invalid state");
}

fn get_connect_packet_will_response_topic(packet: &MqttPacket) -> &str {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(will) = &connect.will {
            if let Some(response_topic) = &will.response_topic {
                return response_topic;
            }
        }
    }

    panic!("get_connect_packet_will_response_topic - will response topic accessor invoked in an invalid state");
}

fn get_connect_packet_will_correlation_data(packet: &MqttPacket) -> &[u8] {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(will) = &connect.will {
            if let Some(correlation_data) = &will.correlation_data {
                return correlation_data;
            }
        }
    }

    panic!("get_connect_packet_will_correlation_data - will correlation data accessor invoked in an invalid state");
}

fn get_connect_packet_will_topic(packet: &MqttPacket) -> &str {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(will) = &connect.will {
            return &will.topic;
        }
    }

    panic!("get_connect_packet_will_topic - will topic accessor invoked in an invalid state");
}

fn get_connect_packet_will_payload(packet: &MqttPacket) -> &[u8] {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(will) = &connect.will {
            if let Some(payload) = &will.payload {
                return payload;
            }
        }
    }

    panic!("get_connect_packet_will_payload - will payload accessor invoked in an invalid state");
}

fn get_connect_packet_will_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Connect(connect) = packet {
        if let Some(publish) = &connect.will {
            if let Some(properties) = &publish.user_properties {
                return &properties[index];
            }
        }
    }

    panic!("get_connect_packet_will_user_property - invalid user property state");
}

static MQTT5_CONNECT_PROTOCOL_BYTES: [u8; 7] = [0, 4, 77, 81, 84, 84, 5];
fn get_connect_protocol_bytes5(_: &MqttPacket) -> &'static [u8] {
    &MQTT5_CONNECT_PROTOCOL_BYTES
}

static MQTT311_CONNECT_PROTOCOL_BYTES: [u8; 7] = [0, 4, 77, 81, 84, 84, 4];
fn get_connect_protocol_bytes311(_: &MqttPacket) -> &'static [u8] {
    &MQTT311_CONNECT_PROTOCOL_BYTES
}

fn compute_connect_flags(packet: &ConnectPacket) -> u8 {
    let mut flags: u8 = 0;
    if packet.clean_start {
        flags |= 1u8 << 1;
    }

    if let Some(will) = &packet.will {
        flags |= 1u8 << 2;
        flags |= (will.qos as u8) << 3;
        if will.retain {
            flags |= 1u8 << 5;
        }
    }

    if packet.password.is_some() {
        flags |= 1u8 << 6;
    }

    if packet.username.is_some() {
        flags |= 1u8 << 7;
    }

    flags
}

#[rustfmt::skip]
fn compute_connect_packet_length_properties5(packet: &ConnectPacket) -> GneissResult<(u32, u32, u32)> {
    let mut connect_property_section_length = compute_user_properties_length(&packet.user_properties);

    add_optional_u32_property_length!(connect_property_section_length, packet.session_expiry_interval_seconds);
    add_optional_u16_property_length!(connect_property_section_length, packet.receive_maximum);
    add_optional_u32_property_length!(connect_property_section_length, packet.maximum_packet_size_bytes);
    add_optional_u16_property_length!(connect_property_section_length, packet.topic_alias_maximum);
    add_optional_u8_property_length!(connect_property_section_length, packet.request_response_information);
    add_optional_u8_property_length!(connect_property_section_length, packet.request_problem_information);
    add_optional_string_property_length!(connect_property_section_length, packet.authentication_method);
    add_optional_bytes_property_length!(connect_property_section_length, packet.authentication_data);

    /* variable header length =
     *    10 bytes (6 for mqtt string, 1 for protocol version, 1 for flags, 2 for keep alive)
     *  + # bytes(variable_length_encoding(connect_property_section_length))
     *  + connect_property_section_length
     */
    let mut variable_header_length = compute_variable_length_integer_encode_size(connect_property_section_length)?;
    variable_header_length += 10 + connect_property_section_length;

    let mut payload_length : usize = 0;
    add_optional_string_length!(payload_length, packet.client_id);

    let mut will_property_length : usize = 0;
    if let Some(will) = &packet.will {
        will_property_length = compute_user_properties_length(&will.user_properties);

        add_optional_u32_property_length!(will_property_length, packet.will_delay_interval_seconds);
        add_optional_u8_property_length!(will_property_length, will.payload_format);
        add_optional_u32_property_length!(will_property_length, will.message_expiry_interval_seconds);
        add_optional_string_property_length!(will_property_length, will.content_type);
        add_optional_string_property_length!(will_property_length, will.response_topic);
        add_optional_bytes_property_length!(will_property_length, will.correlation_data);

        let will_properties_length_encode_size = compute_variable_length_integer_encode_size(will_property_length)?;

        payload_length += will_property_length;
        payload_length += will_properties_length_encode_size;
        payload_length += 2 + will.topic.len();
        add_optional_bytes_length!(payload_length, will.payload);
    }

    if let Some(username) = &packet.username {
        payload_length += 2 + username.len();
    }

    if let Some(password) = &packet.password {
        payload_length += 2 + password.len();
    }

    let total_remaining_length : usize = payload_length + variable_header_length;

    if total_remaining_length > MAXIMUM_VARIABLE_LENGTH_INTEGER {
        return Err(GneissError::new_encoding_failure("compute_connect_packet_length_properties5 - vli value exceeds the protocol maximum (2 ^ 28 - 1)"));
    }

    Ok((total_remaining_length as u32, connect_property_section_length as u32, will_property_length as u32))
}

#[rustfmt::skip]
pub(crate) fn write_connect_encoding_steps5(packet: &ConnectPacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> GneissResult<()> {
    let (total_remaining_length, connect_property_length, will_property_length) = compute_connect_packet_length_properties5(packet)?;

    encode_integral_expression!(steps, Uint8, 1u8 << 4);
    encode_integral_expression!(steps, Vli, total_remaining_length);
    encode_raw_bytes!(steps, get_connect_protocol_bytes5);
    encode_integral_expression!(steps, Uint8, compute_connect_flags(packet));
    encode_integral_expression!(steps, Uint16, packet.keep_alive_interval_seconds);

    encode_integral_expression!(steps, Vli, connect_property_length);
    encode_optional_property!(steps, Uint32, PROPERTY_KEY_SESSION_EXPIRY_INTERVAL, packet.session_expiry_interval_seconds);
    encode_optional_property!(steps, Uint16, PROPERTY_KEY_RECEIVE_MAXIMUM, packet.receive_maximum);
    encode_optional_property!(steps, Uint32, PROPERTY_KEY_MAXIMUM_PACKET_SIZE, packet.maximum_packet_size_bytes);
    encode_optional_property!(steps, Uint16, PROPERTY_KEY_TOPIC_ALIAS_MAXIMUM, packet.topic_alias_maximum);
    encode_optional_boolean_property!(steps, PROPERTY_KEY_REQUEST_RESPONSE_INFORMATION, packet.request_response_information);
    encode_optional_boolean_property!(steps, PROPERTY_KEY_REQUEST_PROBLEM_INFORMATION, packet.request_problem_information);
    encode_optional_string_property!(steps, get_connect_packet_authentication_method, PROPERTY_KEY_AUTHENTICATION_METHOD, packet.authentication_method);
    encode_optional_bytes_property!(steps, get_connect_packet_authentication_data, PROPERTY_KEY_AUTHENTICATION_DATA, packet.authentication_data);
    encode_user_properties!(steps, get_connect_packet_user_property, packet.user_properties);

    encode_length_prefixed_optional_string!(steps, get_connect_packet_client_id, packet.client_id);

    if let Some(will) = &packet.will {
        encode_integral_expression!(steps, Vli, will_property_length);
        encode_optional_property!(steps, Uint32, PROPERTY_KEY_WILL_DELAY_INTERVAL, packet.will_delay_interval_seconds);
        encode_optional_enum_property!(steps, Uint8, PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR, u8, will.payload_format);
        encode_optional_property!(steps, Uint32, PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL, will.message_expiry_interval_seconds);
        encode_optional_string_property!(steps, get_connect_packet_will_content_type, PROPERTY_KEY_CONTENT_TYPE, &will.content_type);
        encode_optional_string_property!(steps, get_connect_packet_will_response_topic, PROPERTY_KEY_RESPONSE_TOPIC, &will.response_topic);
        encode_optional_bytes_property!(steps, get_connect_packet_will_correlation_data, PROPERTY_KEY_CORRELATION_DATA, will.correlation_data);
        encode_user_properties!(steps, get_connect_packet_will_user_property, will.user_properties);

        encode_length_prefixed_string!(steps, get_connect_packet_will_topic, will.topic);
        encode_length_prefixed_optional_bytes!(steps, get_connect_packet_will_payload, will.payload);
    }

    if packet.username.is_some() {
        encode_length_prefixed_optional_string!(steps, get_connect_packet_username, packet.username);
    }

    if packet.password.is_some() {
        encode_length_prefixed_optional_bytes!(steps, get_connect_packet_password, packet.password);
    }

    Ok(())
}

fn compute_connect_packet_length_properties311(packet: &ConnectPacket) -> GneissResult<u32> {

    /* variable header length =
     *    10 bytes (6 for mqtt string, 1 for protocol version, 1 for flags, 2 for keep alive)
     *  + # bytes(variable_length_encoding(connect_property_section_length))
     */
    let variable_header_length = 10;

    let mut payload_length : usize = 0;
    add_optional_string_length!(payload_length, packet.client_id);

    if let Some(will) = &packet.will {
        payload_length += 2 + will.topic.len();
        add_optional_bytes_length!(payload_length, will.payload);
    }

    if let Some(username) = &packet.username {
        payload_length += 2 + username.len();
    }

    if let Some(password) = &packet.password {
        payload_length += 2 + password.len();
    }

    let total_remaining_length : usize = payload_length + variable_header_length;

    if total_remaining_length > MAXIMUM_VARIABLE_LENGTH_INTEGER {
        return Err(GneissError::new_encoding_failure("compute_connect_packet_length_properties311 - vli value exceeds the protocol maximum (2 ^ 28 - 1)"));
    }

    Ok(total_remaining_length as u32)
}

pub(crate) fn write_connect_encoding_steps311(packet: &ConnectPacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> GneissResult<()> {
    let total_remaining_length = compute_connect_packet_length_properties311(packet)?;

    encode_integral_expression!(steps, Uint8, 1u8 << 4);
    encode_integral_expression!(steps, Vli, total_remaining_length);
    encode_raw_bytes!(steps, get_connect_protocol_bytes311);
    encode_integral_expression!(steps, Uint8, compute_connect_flags(packet));
    encode_integral_expression!(steps, Uint16, packet.keep_alive_interval_seconds);

    encode_length_prefixed_optional_string!(steps, get_connect_packet_client_id, packet.client_id);

    if let Some(will) = &packet.will {
        encode_length_prefixed_string!(steps, get_connect_packet_will_topic, will.topic);
        encode_length_prefixed_optional_bytes!(steps, get_connect_packet_will_payload, will.payload);
    }

    if packet.username.is_some() {
        encode_length_prefixed_optional_string!(steps, get_connect_packet_username, packet.username);
    }

    if packet.password.is_some() {
        encode_length_prefixed_optional_bytes!(steps, get_connect_packet_password, packet.password);
    }

    Ok(())
}

#[cfg(test)]
fn decode_connect_properties(property_bytes: &[u8], packet : &mut ConnectPacket) -> GneissResult<()> {
    let mut mutable_property_bytes = property_bytes;

    while !mutable_property_bytes.is_empty() {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_SESSION_EXPIRY_INTERVAL => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut packet.session_expiry_interval_seconds)?; }
            PROPERTY_KEY_RECEIVE_MAXIMUM => { mutable_property_bytes = decode_optional_u16(mutable_property_bytes, &mut packet.receive_maximum)?; }
            PROPERTY_KEY_MAXIMUM_PACKET_SIZE => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut packet.maximum_packet_size_bytes)?; }
            PROPERTY_KEY_TOPIC_ALIAS_MAXIMUM => { mutable_property_bytes = decode_optional_u16(mutable_property_bytes, &mut packet.topic_alias_maximum)?; }
            PROPERTY_KEY_REQUEST_RESPONSE_INFORMATION => { mutable_property_bytes = decode_optional_u8_as_bool(mutable_property_bytes, &mut packet.request_response_information)?; }
            PROPERTY_KEY_REQUEST_PROBLEM_INFORMATION => { mutable_property_bytes = decode_optional_u8_as_bool(mutable_property_bytes, &mut packet.request_problem_information)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            PROPERTY_KEY_AUTHENTICATION_METHOD => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.authentication_method)?; }
            PROPERTY_KEY_AUTHENTICATION_DATA => { mutable_property_bytes = decode_optional_length_prefixed_bytes(mutable_property_bytes, &mut packet.authentication_data)?; }
            _ => {
                let message = format!("decode_connect_properties - Invalid property type ({})", property_key);
                error!(message);
                return Err(GneissError::new_decoding_failure(message));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
fn decode_will_properties(property_bytes: &[u8], will: &mut PublishPacket, connect : &mut ConnectPacket) -> GneissResult<()> {
    let mut mutable_property_bytes = property_bytes;

    while !mutable_property_bytes.is_empty() {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_WILL_DELAY_INTERVAL => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut connect.will_delay_interval_seconds)?; }
            PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR => { mutable_property_bytes = decode_optional_u8_as_enum(mutable_property_bytes, &mut will.payload_format, PayloadFormatIndicator::try_from)?; }
            PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut will.message_expiry_interval_seconds)?; }
            PROPERTY_KEY_CONTENT_TYPE => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut will.content_type)?; }
            PROPERTY_KEY_RESPONSE_TOPIC => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut will.response_topic)?; }
            PROPERTY_KEY_CORRELATION_DATA => { mutable_property_bytes = decode_optional_length_prefixed_bytes(mutable_property_bytes, &mut will.correlation_data)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut will.user_properties)?; }
            _ => {
                let message = format!("decode_will_properties - invalid will property type ({})", property_key);
                error!(message);
                return Err(GneissError::new_decoding_failure(message));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
const CONNECT_HEADER_PROTOCOL_LENGTH : usize = 7;

#[cfg(test)]
pub(crate) fn decode_connect_packet5(first_byte: u8, packet_body: &[u8]) -> GneissResult<Box<MqttPacket>> {
    if first_byte != (PACKET_TYPE_CONNECT << 4)  {
        let message = "decode_connect_packet5 - invalid first byte";
        error!(message);
        return Err(GneissError::new_decoding_failure(message));
    }

    let mut box_packet = Box::new(MqttPacket::Connect(ConnectPacket { ..Default::default() }));

    if let MqttPacket::Connect(packet) = box_packet.as_mut() {
        let mut mutable_body = packet_body;
        if mutable_body.len() < CONNECT_HEADER_PROTOCOL_LENGTH {
            let message = "decode_connect_packet5 - packet too short";
            error!(message);
            return Err(GneissError::new_decoding_failure(message));
        }

        let protocol_bytes = &mutable_body[..CONNECT_HEADER_PROTOCOL_LENGTH];
        mutable_body = &mutable_body[CONNECT_HEADER_PROTOCOL_LENGTH..];

        match protocol_bytes {
            MQTT5_CONNECT_PROTOCOL_BYTES => { }
            _ => {
                let message = "decode_connect_packet5 - invalid protocol";
                error!(message);
                return Err(GneissError::new_decoding_failure(message));
            }
        }

        let mut connect_flags : u8 = 0;
        mutable_body = decode_u8(mutable_body, &mut connect_flags)?;

        // if the reserved bit is set, that's fatal
        if (connect_flags & 0x01) != 0 {
            let message = "decode_connect_packet5 - connect flags reserved bit set";
            error!(message);
            return Err(GneissError::new_decoding_failure(message));
        }

        packet.clean_start = (connect_flags & CONNECT_PACKET_CLEAN_START_FLAG_MASK) != 0;
        let has_will = (connect_flags & CONNECT_PACKET_HAS_WILL_FLAG_MASK) != 0;
        let will_retain = (connect_flags & CONNECT_PACKET_WILL_RETAIN_FLAG_MASK) != 0;
        let will_qos = QualityOfService::try_from((connect_flags >> CONNECT_PACKET_WILL_QOS_FLAG_SHIFT) & QOS_MASK)?;

        if !has_will {
            /* indirectly check bits of connect flags vs. spec */
            if will_retain || will_qos != QualityOfService::AtMostOnce {
                let message = "decode_connect_packet5 - no will but has will flags set";
                error!(message);
                return Err(GneissError::new_decoding_failure(message));
            }
        }

        let has_username = (connect_flags & CONNECT_PACKET_HAS_USERNAME_FLAG_MASK) != 0;
        let has_password = (connect_flags & CONNECT_PACKET_HAS_PASSWORD_FLAG_MASK) != 0;

        mutable_body = decode_u16(mutable_body, &mut packet.keep_alive_interval_seconds)?;

        let mut connect_property_length : usize = 0;
        mutable_body = decode_vli_into_mutable(mutable_body, &mut connect_property_length)?;

        if mutable_body.len() < connect_property_length {
            let message = "decode_connect_packet5 - property length exceeds overall packet length";
            error!(message);
            return Err(GneissError::new_decoding_failure(message));
        }

        let property_body = &mutable_body[..connect_property_length];
        mutable_body = &mutable_body[connect_property_length..];

        decode_connect_properties(property_body, packet)?;

        mutable_body = decode_length_prefixed_optional_string(mutable_body, &mut packet.client_id)?;

        if has_will {
            let mut will_property_length : usize = 0;
            mutable_body = decode_vli_into_mutable(mutable_body, &mut will_property_length)?;

            if mutable_body.len() < will_property_length {
                let message = "decode_connect_packet5 - will property length exceeds overall packet length";
                error!(message);
                return Err(GneissError::new_decoding_failure(message));
            }

            let will_property_body = &mutable_body[..will_property_length];
            mutable_body = &mutable_body[will_property_length..];

            let mut will : PublishPacket = PublishPacket {
                qos : will_qos,
                retain : will_retain,
                ..Default::default()
            };

            decode_will_properties(will_property_body, &mut will, packet)?;

            mutable_body = decode_length_prefixed_string(mutable_body, &mut will.topic)?;
            mutable_body = decode_length_prefixed_optional_bytes(mutable_body, &mut will.payload)?;

            packet.will = Some(will);
        }

        if has_username {
            mutable_body = decode_optional_length_prefixed_string(mutable_body, &mut packet.username)?;
        }

        if has_password {
            mutable_body = decode_optional_length_prefixed_bytes(mutable_body, &mut packet.password)?;
        }

        if !mutable_body.is_empty() {
            let message = "decode_connect_packet5 - body length does not match expected overall packet length";
            error!(message);
            return Err(GneissError::new_decoding_failure(message));
        }

        return Ok(box_packet);
    }

    panic!("decode_connect_packet5 - Internal error");
}

#[cfg(not(test))]
pub(crate) fn decode_connect_packet5(_: u8, _: &[u8]) -> GneissResult<Box<MqttPacket>> {
    Err(GneissError::new_unimplemented("decode_connect_packet5 - test-only functionality"))
}

#[cfg(test)]
pub(crate) fn decode_connect_packet311(first_byte: u8, packet_body: &[u8]) -> GneissResult<Box<MqttPacket>> {
    if first_byte != (PACKET_TYPE_CONNECT << 4)  {
        let message = "decode_connect_packet311 - invalid first byte";
        error!(message);
        return Err(GneissError::new_decoding_failure(message));
    }

    let mut box_packet = Box::new(MqttPacket::Connect(ConnectPacket { ..Default::default() }));

    if let MqttPacket::Connect(packet) = box_packet.as_mut() {
        let mut mutable_body = packet_body;
        if mutable_body.len() < CONNECT_HEADER_PROTOCOL_LENGTH {
            let message = "decode_connect_packet311 - packet too short";
            error!(message);
            return Err(GneissError::new_decoding_failure(message));
        }

        let protocol_bytes = &mutable_body[..CONNECT_HEADER_PROTOCOL_LENGTH];
        mutable_body = &mutable_body[CONNECT_HEADER_PROTOCOL_LENGTH..];

        match protocol_bytes {
            MQTT311_CONNECT_PROTOCOL_BYTES => { }
            _ => {
                let message = "decode_connect_packet311 - invalid protocol";
                error!(message);
                return Err(GneissError::new_decoding_failure(message));
            }
        }

        let mut connect_flags : u8 = 0;
        mutable_body = decode_u8(mutable_body, &mut connect_flags)?;

        // if the reserved bit is set, that's fatal
        if (connect_flags & 0x01) != 0 {
            let message = "decode_connect_packet311 - connect flags reserved bit set";
            error!(message);
            return Err(GneissError::new_decoding_failure(message));
        }

        packet.clean_start = (connect_flags & CONNECT_PACKET_CLEAN_START_FLAG_MASK) != 0;
        let has_will = (connect_flags & CONNECT_PACKET_HAS_WILL_FLAG_MASK) != 0;
        let will_retain = (connect_flags & CONNECT_PACKET_WILL_RETAIN_FLAG_MASK) != 0;
        let will_qos = QualityOfService::try_from((connect_flags >> CONNECT_PACKET_WILL_QOS_FLAG_SHIFT) & QOS_MASK)?;

        if !has_will {
            /* indirectly check bits of connect flags vs. spec */
            if will_retain || will_qos != QualityOfService::AtMostOnce {
                let message = "decode_connect_packet311 - no will but has will flags set";
                error!(message);
                return Err(GneissError::new_decoding_failure(message));
            }
        }

        let has_username = (connect_flags & CONNECT_PACKET_HAS_USERNAME_FLAG_MASK) != 0;
        let has_password = (connect_flags & CONNECT_PACKET_HAS_PASSWORD_FLAG_MASK) != 0;

        mutable_body = decode_u16(mutable_body, &mut packet.keep_alive_interval_seconds)?;
        mutable_body = decode_length_prefixed_optional_string(mutable_body, &mut packet.client_id)?;

        if has_will {
            mutable_body = decode_length_prefixed_string(mutable_body, &mut will.topic)?;
            mutable_body = decode_length_prefixed_optional_bytes(mutable_body, &mut will.payload)?;

            packet.will = Some(will);
        }

        if has_username {
            mutable_body = decode_optional_length_prefixed_string(mutable_body, &mut packet.username)?;
        }

        if has_password {
            mutable_body = decode_optional_length_prefixed_bytes(mutable_body, &mut packet.password)?;
        }

        if !mutable_body.is_empty() {
            let message = "decode_connect_packet311 - body length does not match expected overall packet length";
            error!(message);
            return Err(GneissError::new_decoding_failure(message));
        }

        return Ok(box_packet);
    }

    panic!("decode_connect_packet311 - Internal error");
}

#[cfg(not(test))]
pub(crate) fn decode_connect_packet311(_: u8, _: &[u8]) -> GneissResult<Box<MqttPacket>> {
    Err(GneissError::new_unimplemented("decode_connect_packet311 - test-only functionality"))
}

pub(crate) fn validate_connect_packet_outbound(packet: &ConnectPacket) -> GneissResult<()> {

    validate_optional_string_length(&packet.client_id, PacketType::Connect, "validate_connect_packet_outbound", "client_id")?;
    validate_optional_integer_non_zero!(receive_maximum, packet.receive_maximum, PacketType::Connect, "validate_connect_packet_outbound", "receive_maximum");
    validate_optional_integer_non_zero!(maximum_packet_size, packet.maximum_packet_size_bytes, PacketType::Connect, "validate_connect_packet_outbound", "maximum_packet_size");

    if packet.authentication_data.is_some() && packet.authentication_method.is_none() {
        let message = "validate_connect_packet_outbound - authentication data without authentication method";
        error!(message);
        return Err(GneissError::new_packet_validation(PacketType::Connect, message));
    }

    validate_optional_string_length(&packet.authentication_method, PacketType::Connect, "validate_connect_packet_outbound", "authentication_method")?;
    validate_optional_binary_length(&packet.authentication_data, PacketType::Connect, "validate_connect_packet_outbound", "authentication_data")?;
    validate_optional_string_length(&packet.username, PacketType::Connect, "validate_connect_packet_outbound", "username")?;
    validate_optional_binary_length(&packet.password, PacketType::Connect, "validate_connect_packet_outbound", "password")?;
    validate_user_properties(&packet.user_properties, PacketType::Connect, "validate_connect_packet_outbound")?;

    if let Some(will) = &packet.will {
        validate_optional_string_length(&will.content_type, PacketType::Connect, "(will)validate_connect_packet_outbound", "content_type")?;
        validate_optional_string_length(&will.response_topic, PacketType::Connect, "(will)validate_connect_packet_outbound", "response_topic")?;
        validate_optional_binary_length(&will.correlation_data, PacketType::Connect, "(will)validate_connect_packet_outbound", "correlation_data")?;
        validate_user_properties(&will.user_properties, PacketType::Connect, "(will)validate_connect_packet_outbound")?;
        validate_string_length(will.topic.as_str(), PacketType::Connect, "(will)validate_connect_packet_outbound", "topic")?;
        validate_optional_binary_length(&will.payload, PacketType::Connect, "(will)validate_connect_packet_outbound", "payload")?;
    }

    Ok(())
}

impl fmt::Display for ConnectPacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ConnectPacket {{")?;
        log_primitive_value!(self.keep_alive_interval_seconds, f, "keep_alive_interval_seconds");
        log_primitive_value!(self.clean_start, f, "clean_start");
        log_optional_string_sensitive!(self.username, f, "username");
        log_optional_binary_data_sensitive!(self.password, f, "password");
        log_optional_primitive_value!(self.session_expiry_interval_seconds, f, "session_expiry_interval_seconds", value);
        log_optional_primitive_value!(self.request_response_information, f, "request_response_information", value);
        log_optional_primitive_value!(self.request_problem_information, f, "request_problem_information", value);
        log_optional_primitive_value!(self.receive_maximum, f, "receive_maximum", value);
        log_optional_primitive_value!(self.topic_alias_maximum, f, "topic_alias_maximum", value);
        log_optional_primitive_value!(self.maximum_packet_size_bytes, f, "maximum_packet_size_bytes", value);
        log_optional_string!(self.authentication_method, f, "authentication_method", value);
        log_optional_binary_data_sensitive!(self.authentication_data, f, "authentication_data");
        log_user_properties!(self.user_properties, f, "user_properties", value);

        log_optional_primitive_value!(self.will_delay_interval_seconds, f, "will_delay_interval_seconds", value);
        if let Some(will) = &self.will {
            write!(f, "will: {{")?;
            log_string!(will.topic, f, "topic");
            log_enum!(will.qos, f, "qos", QualityOfService);
            log_primitive_value!(will.retain, f, "retain");
            log_optional_binary_data!(will.payload, f, "payload", value);
            log_optional_enum!(will.payload_format, f, "payload_format", value, PayloadFormatIndicator);
            log_optional_string!(will.content_type, f, "content_type", value);
            log_optional_string!(will.response_topic, f, "response_topic", value);
            log_optional_binary_data!(will.correlation_data, f, "correlation_data", value);
            log_user_properties!(will.user_properties, f, "user_properties", value);
            write!(f, "  }}")?;
        }

        write!(f, " }}")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;
    use crate::validate::testing::*;

    #[test]
    fn connect_round_trip_encode_decode_default() {
        let packet = ConnectPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_basic() {
        let packet = ConnectPacket {
            keep_alive_interval_seconds : 1200,
            clean_start : true,
            client_id : Some("MyClient".to_string()),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_no_flags_all_optional_properties() {
        let packet = ConnectPacket {
            keep_alive_interval_seconds : 3600,
            clean_start : true,
            client_id : Some("MyClient2".to_string()),
            session_expiry_interval_seconds: Some(0xFFFFFFFFu32),
            request_response_information: Some(true),
            request_problem_information: Some(false),
            receive_maximum: Some(100),
            topic_alias_maximum: Some(20),
            maximum_packet_size_bytes: Some(128 * 1024),
            authentication_method: Some("Kerberos".to_string()),
            authentication_data: Some(vec![5, 4, 3, 2, 1]),
            user_properties: Some(vec!(
                UserProperty{name: "connecting".to_string(), value: "future".to_string()},
                UserProperty{name: "Iamabanana".to_string(), value: "Hamizilla".to_string()},
            )),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_username_only() {
        let packet = ConnectPacket {
            username : Some("SpaceUnicorn".to_string()),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_password_only() {
        let packet = ConnectPacket {
            password : Some("Marshmallow Lasers".as_bytes().to_vec()),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_all_non_will_properties() {
        let packet = ConnectPacket {
            keep_alive_interval_seconds : 3600,
            clean_start : true,
            client_id : Some("NotAHaxxor".to_string()),
            session_expiry_interval_seconds: Some(0x1234ABCDu32),
            request_response_information: Some(false),
            request_problem_information: Some(true),
            receive_maximum: Some(1000),
            topic_alias_maximum: Some(2),
            maximum_packet_size_bytes: Some(512 * 1024 - 1),
            authentication_method: Some("GSSAPI".to_string()),
            authentication_data: Some(vec![15, 14, 13, 12, 11]),
            user_properties: Some(vec!(
                UserProperty{name: "Another".to_string(), value: "brick".to_string()},
                UserProperty{name: "WhenIwas".to_string(), value: "ayoungboy".to_string()},
            )),
            username: Some("Gluten-free armada".to_string()),
            password: Some("PancakeRobot".as_bytes().to_vec()),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_default_will() {
        let packet = ConnectPacket {
            will : Some(PublishPacket {
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_simple_will() {
        let packet = ConnectPacket {
            will : Some(PublishPacket {
                topic : "in/rememberance".to_string(),
                qos: QualityOfService::ExactlyOnce,
                payload: Some("I'llbealright".as_bytes().to_vec()),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_round_trip_encode_decode_all_will_fields() {
        let packet = ConnectPacket {
            will_delay_interval_seconds : Some(60),
            will : Some(PublishPacket {
                topic : "in/rememberance/of/mrkrabs".to_string(),
                qos: QualityOfService::ExactlyOnce,
                payload: Some("Arrrrrrrrrrrrrrr".as_bytes().to_vec()),
                retain: true,
                payload_format : Some(PayloadFormatIndicator::Utf8),
                message_expiry_interval_seconds : Some(1800),
                content_type : Some("QueryXML".to_string()),
                response_topic : Some("forever/today".to_string()),
                correlation_data : Some("Request1".as_bytes().to_vec()),
                user_properties: Some(vec!(
                    UserProperty{name: "WillProp1".to_string(), value: "WillValue1".to_string()},
                )),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    fn create_connect_packet_all_properties() -> ConnectPacket {
        ConnectPacket {
            keep_alive_interval_seconds : 3600,
            clean_start : true,
            client_id : Some("NotAHaxxor".to_string()),
            session_expiry_interval_seconds: Some(0x1234ABCDu32),
            request_response_information: Some(false),
            request_problem_information: Some(true),
            receive_maximum: Some(1000),
            topic_alias_maximum: Some(2),
            maximum_packet_size_bytes: Some(512 * 1024 - 1),
            authentication_method: Some("GSSAPI".to_string()),
            authentication_data: Some(vec![15, 14, 13, 12, 11]),
            user_properties: Some(vec!(
                UserProperty{name: "Another".to_string(), value: "brick".to_string()},
                UserProperty{name: "WhenIwas".to_string(), value: "ayoungboy".to_string()},
            )),
            will_delay_interval_seconds : Some(60),
            will : Some(PublishPacket {
                topic : "in/rememberance/of/mrkrabs".to_string(),
                qos: QualityOfService::ExactlyOnce,
                payload: Some("Arrrrrrrrrrrrrrr".as_bytes().to_vec()),
                retain: true,
                payload_format : Some(PayloadFormatIndicator::Utf8),
                message_expiry_interval_seconds : Some(1800),
                content_type : Some("QueryXML".to_string()),
                response_topic : Some("forever/today".to_string()),
                correlation_data : Some("Request1".as_bytes().to_vec()),
                user_properties: Some(vec!(
                    UserProperty{name: "WillProp1".to_string(), value: "WillValue1".to_string()},
                )),
                ..Default::default()
            }),
            username: Some("Gluten-free armada".to_string()),
            password: Some("PancakeRobot X".as_bytes().to_vec()),
            ..Default::default()
        }
    }

    const CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH : usize = 259;
    const CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX : usize = 90;
    const CONNECT_PACKET_ALL_PROPERTIES_TEST_CONNECT_PROPERTY_LENGTH_INDEX : usize = 13;
    const CONNECT_PACKET_ALL_PROPERTIES_TEST_REQUEST_RESPONSE_INFORMATION_VALUE_INDEX : usize = 31;
    const CONNECT_PACKET_ALL_PROPERTIES_TEST_REQUEST_PROBLEM_INFORMATION_VALUE_INDEX : usize = 33;
    const CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_PROPERTY_LENGTH_INDEX : usize = 102;
    const CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_PAYLOAD_FORMAT_INDICATOR_VALUE_INDEX : usize = 109;
    const CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX : usize = 177;

    #[test]
    fn connect_round_trip_encode_decode_everything() {
        let packet  = create_connect_packet_all_properties();

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connect(packet)));
    }

    #[test]
    fn connect_decode_failure_bad_fixed_header() {
        let packet = ConnectPacket {
            will : Some(PublishPacket {
                topic : "in/rememberance".to_string(),
                qos: QualityOfService::ExactlyOnce,
                payload: Some("I'llbealright".as_bytes().to_vec()),
                ..Default::default()
            }),
            ..Default::default()
        };

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Connect(packet), 6);
    }

    #[test]
    fn connect_decode_failure_bad_protocol_name() {
        let packet = create_connect_packet_all_properties();

        let lets_do_http = | bytes: &[u8] | -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            clone[5] = 72;
            clone[6] = 84;
            clone[7] = 84;
            clone[8] = 80;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), lets_do_http);
    }

    #[test]
    fn connect_decode_failure_bad_protocol_version() {
        let packet = create_connect_packet_all_properties();

        let lets_do_mqtt3 = | bytes: &[u8] | -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            clone[9] = 3;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), lets_do_mqtt3);
    }

    #[test]
    fn connect_decode_failure_bad_reserved_flags() {
        let packet = create_connect_packet_all_properties();

        let lets_do_mqtt3 = | bytes: &[u8] | -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            clone[10] |= 0x01; // set the reserved bit of the connect flags

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), lets_do_mqtt3);
    }

    #[test]
    fn connect_decode_failure_bad_will_qos() {
        let packet = create_connect_packet_all_properties();

        let corrupt_will_qos = | bytes: &[u8] | -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            clone[10] |= 0x18; // will qos "3"

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), corrupt_will_qos);
    }

    #[test]
    fn connect_decode_failure_session_expiry_interval_duplicate() {
        let packet = create_connect_packet_all_properties();

        let duplicate_session_expiry_interval = | bytes: &[u8] | -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            // bump total remaining length
            clone[1] += 5;

            // bump connect property length
            clone[CONNECT_PACKET_ALL_PROPERTIES_TEST_CONNECT_PROPERTY_LENGTH_INDEX] += 5;

            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 0);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 0);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 0);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 0);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, PROPERTY_KEY_SESSION_EXPIRY_INTERVAL);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), duplicate_session_expiry_interval);
    }

    #[test]
    fn connect_decode_failure_receive_maximum_duplicate() {
        let packet = create_connect_packet_all_properties();

        let duplicate_receive_maximum = | bytes: &[u8] | -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            // bump total remaining length
            clone[1] += 3;

            // bump connect property length
            clone[CONNECT_PACKET_ALL_PROPERTIES_TEST_CONNECT_PROPERTY_LENGTH_INDEX] += 3;

            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 1);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 1);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, PROPERTY_KEY_RECEIVE_MAXIMUM);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), duplicate_receive_maximum);
    }

    #[test]
    fn connect_decode_failure_maximum_packet_size_duplicate() {
        let packet = create_connect_packet_all_properties();

        let duplicate_maximum_packet_size = | bytes: &[u8] | -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            // bump total remaining length
            clone[1] += 5;

            // bump connect property length
            clone[CONNECT_PACKET_ALL_PROPERTIES_TEST_CONNECT_PROPERTY_LENGTH_INDEX] += 5;

            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 1);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 2);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 3);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 4);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, PROPERTY_KEY_MAXIMUM_PACKET_SIZE);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), duplicate_maximum_packet_size);
    }

    #[test]
    fn connect_decode_failure_topic_alias_maximum_duplicate() {
        let packet = create_connect_packet_all_properties();

        let duplicate_topic_alias_maximum = | bytes: &[u8] | -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            // bump total remaining length
            clone[1] += 3;

            // bump connect property length
            clone[CONNECT_PACKET_ALL_PROPERTIES_TEST_CONNECT_PROPERTY_LENGTH_INDEX] += 3;

            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 1);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 1);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, PROPERTY_KEY_TOPIC_ALIAS_MAXIMUM);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), duplicate_topic_alias_maximum);
    }

    #[test]
    fn connect_decode_failure_request_response_information_duplicate() {
        let packet = create_connect_packet_all_properties();

        let duplicate_request_response_information = | bytes: &[u8] | -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            // bump total remaining length
            clone[1] += 2;

            // bump connect property length
            clone[CONNECT_PACKET_ALL_PROPERTIES_TEST_CONNECT_PROPERTY_LENGTH_INDEX] += 2;

            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 1);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, PROPERTY_KEY_REQUEST_RESPONSE_INFORMATION);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), duplicate_request_response_information);
    }

    #[test]
    fn connect_decode_failure_request_response_information_invalid() {
        let packet = create_connect_packet_all_properties();

        let invalidate_request_response_information = | bytes: &[u8] | -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            clone[CONNECT_PACKET_ALL_PROPERTIES_TEST_REQUEST_RESPONSE_INFORMATION_VALUE_INDEX] = 2;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), invalidate_request_response_information);
    }

    #[test]
    fn connect_decode_failure_request_problem_information_duplicate() {
        let packet = create_connect_packet_all_properties();

        let duplicate_request_problem_information = | bytes: &[u8] | -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            // bump total remaining length
            clone[1] += 2;

            // bump connect property length
            clone[CONNECT_PACKET_ALL_PROPERTIES_TEST_CONNECT_PROPERTY_LENGTH_INDEX] += 2;

            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 1);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, PROPERTY_KEY_REQUEST_PROBLEM_INFORMATION);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), duplicate_request_problem_information);
    }

    #[test]
    fn connect_decode_failure_request_problem_information_invalid() {
        let packet = create_connect_packet_all_properties();

        let invalidate_request_problem_information = | bytes: &[u8] | -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            clone[CONNECT_PACKET_ALL_PROPERTIES_TEST_REQUEST_PROBLEM_INFORMATION_VALUE_INDEX] = 2;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), invalidate_request_problem_information);
    }

    #[test]
    fn connect_decode_failure_authentication_method_duplicate() {
        let packet = create_connect_packet_all_properties();

        let duplicate_authentication_method = | bytes: &[u8] | -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            // bump total remaining length
            clone[1] += 5;

            // bump connect property length
            clone[CONNECT_PACKET_ALL_PROPERTIES_TEST_CONNECT_PROPERTY_LENGTH_INDEX] += 5;

            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 65);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 65);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 2);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 0);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, PROPERTY_KEY_AUTHENTICATION_METHOD);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), duplicate_authentication_method);
    }

    #[test]
    fn connect_decode_failure_authentication_data_duplicate() {
        let packet = create_connect_packet_all_properties();

        let duplicate_authentication_data = | bytes: &[u8] | -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            // bump total remaining length
            clone[1] += 4;

            // bump connect property length
            clone[CONNECT_PACKET_ALL_PROPERTIES_TEST_CONNECT_PROPERTY_LENGTH_INDEX] += 4;

            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 255);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 1);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, 0);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_CLIENT_ID_INDEX, PROPERTY_KEY_AUTHENTICATION_DATA);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), duplicate_authentication_data);
    }

    #[test]
    fn connect_decode_failure_will_delay_interval_duplicate() {
        let packet = create_connect_packet_all_properties();

        let duplicate_will_delay_interval = | bytes: &[u8] | -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            // bump total remaining length
            clone[1] += 5;

            // bump will property length
            clone[CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_PROPERTY_LENGTH_INDEX] += 5;

            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 1);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 1);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 1);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 0);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, PROPERTY_KEY_WILL_DELAY_INTERVAL);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), duplicate_will_delay_interval);
    }

    #[test]
    fn connect_decode_failure_will_payload_format_indicator_duplicate() {
        let packet = create_connect_packet_all_properties();

        let duplicate_will_payload_format_indicator = | bytes: &[u8] | -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            // bump total remaining length
            clone[1] += 2;

            // bump will property length
            clone[CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_PROPERTY_LENGTH_INDEX] += 2;

            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 0);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), duplicate_will_payload_format_indicator);
    }

    #[test]
    fn connect_decode_failure_will_message_expiry_interval_duplicate() {
        let packet = create_connect_packet_all_properties();

        let duplicate_will_message_expiry_interval = |bytes: &[u8]| -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            // bump total remaining length
            clone[1] += 5;

            // bump will property length
            clone[CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_PROPERTY_LENGTH_INDEX] += 5;

            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 1);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 2);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 3);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 4);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), duplicate_will_message_expiry_interval);
    }

    #[test]
    fn connect_decode_failure_will_content_type_duplicate() {
        let packet = create_connect_packet_all_properties();

        let duplicate_will_content_type = |bytes: &[u8]| -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            // bump total remaining length
            clone[1] += 5;

            // bump will property length
            clone[CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_PROPERTY_LENGTH_INDEX] += 5;

            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 65);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 66);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 2);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 0);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, PROPERTY_KEY_CONTENT_TYPE);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), duplicate_will_content_type);
    }

    #[test]
    fn connect_decode_failure_will_response_topic_duplicate() {
        let packet = create_connect_packet_all_properties();

        let duplicate_will_response_topic = |bytes: &[u8]| -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            // bump total remaining length
            clone[1] += 6;

            // bump will property length
            clone[CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_PROPERTY_LENGTH_INDEX] += 6;

            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 65);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 47);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 66);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 3);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 0);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, PROPERTY_KEY_RESPONSE_TOPIC);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), duplicate_will_response_topic);
    }

    #[test]
    fn connect_decode_failure_will_correlation_data_duplicate() {
        let packet = create_connect_packet_all_properties();

        let duplicate_will_correlation_data = |bytes: &[u8]| -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            // bump total remaining length
            clone[1] += 6;

            // bump will property length
            clone[CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_PROPERTY_LENGTH_INDEX] += 6;

            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 4);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 3);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 2);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 3);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, 0);
            clone.insert(CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_TOPIC_INDEX, PROPERTY_KEY_CORRELATION_DATA);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), duplicate_will_correlation_data);
    }

    #[test]
    fn connect_decode_failure_will_payload_format_indicator_invalid() {
        let packet = create_connect_packet_all_properties();

        let invalidate_will_payload_format_indicator = | bytes: &[u8] | -> Vec<u8> {
            assert_eq!(bytes.len(), CONNECT_PACKET_ALL_PROPERTIES_TEST_ENCODE_LENGTH); // it's critical this packet stays stable
            let mut clone = bytes.to_vec();

            clone[CONNECT_PACKET_ALL_PROPERTIES_TEST_WILL_PAYLOAD_FORMAT_INDICATOR_VALUE_INDEX] = 254;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connect(packet), invalidate_will_payload_format_indicator);
    }

    #[test]
    fn connect_decode_failure_packet_size() {
        let packet = create_connect_packet_all_properties();

        do_inbound_size_decode_failure_test(&MqttPacket::Connect(packet));
    }

    #[test]
    fn connect_validate_success_all_properties() {
        let packet = MqttPacket::Connect(create_connect_packet_all_properties());

        assert!(validate_packet_outbound(&packet).is_ok());

        let test_validation_context = create_pinned_validation_context();
        let validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);

        assert!(validate_packet_outbound_internal(&packet, &validation_context).is_ok());
    }

    #[test]
    fn connect_validate_failure_client_id_length() {
        let mut packet = create_connect_packet_all_properties();
        packet.client_id = Some("noooooo".repeat(10000));

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Connect(packet)), PacketType::Connect);
    }

    #[test]
    fn connect_validate_failure_receive_maximum_zero() {
        let mut packet = create_connect_packet_all_properties();
        packet.receive_maximum = Some(0);

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Connect(packet)), PacketType::Connect);
    }

    #[test]
    fn connect_validate_failure_maximum_packet_size_zero() {
        let mut packet = create_connect_packet_all_properties();
        packet.maximum_packet_size_bytes = Some(0);

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Connect(packet)), PacketType::Connect);
    }

    #[test]
    fn connect_validate_failure_auth_data_no_auth_method() {
        let mut packet = create_connect_packet_all_properties();
        packet.authentication_method = None;

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Connect(packet)), PacketType::Connect);
    }

    #[test]
    fn connect_validate_failure_authentication_method_length() {
        let mut packet = create_connect_packet_all_properties();
        packet.authentication_method = Some("hello".repeat(20000));

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Connect(packet)), PacketType::Connect);
    }

    #[test]
    fn connect_validate_failure_authentication_data_length() {
        let mut packet = create_connect_packet_all_properties();
        packet.authentication_data = Some(vec![0; 70 * 1024]);

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Connect(packet)), PacketType::Connect);
    }

    #[test]
    fn connect_validate_failure_username_length() {
        let mut packet = create_connect_packet_all_properties();
        packet.username = Some("ladeeda".repeat(20000));

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Connect(packet)), PacketType::Connect);
    }

    #[test]
    fn connect_validate_failure_password_length() {
        let mut packet = create_connect_packet_all_properties();
        packet.password = Some(vec![0; 80 * 1024]);

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Connect(packet)), PacketType::Connect);
    }

    #[test]
    fn connect_validate_failure_invalid_user_properties() {
        let mut packet = create_connect_packet_all_properties();
        packet.user_properties = Some(create_invalid_user_properties());

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Connect(packet)), PacketType::Connect);
    }

    #[test]
    fn connect_validate_failure_will_content_type_length() {
        let mut packet = create_connect_packet_all_properties();
        let will = packet.will.as_mut();
        will.unwrap().content_type = Some("NotJson".repeat(10000));

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Connect(packet)), PacketType::Connect);
    }

    #[test]
    fn connect_validate_failure_will_response_topic_length() {
        let mut packet = create_connect_packet_all_properties();
        let will = packet.will.as_mut();
        will.unwrap().response_topic = Some("NotJson".repeat(10000));

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Connect(packet)), PacketType::Connect);
    }

    #[test]
    fn connect_validate_failure_will_correlation_data_length() {
        let mut packet = create_connect_packet_all_properties();
        let will = packet.will.as_mut();
        will.unwrap().correlation_data = Some(vec![0; 80 * 1024]);

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Connect(packet)), PacketType::Connect);
    }

    #[test]
    fn connect_validate_failure_will_invalid_user_properties() {
        let mut packet = create_connect_packet_all_properties();
        let will = packet.will.as_mut();
        will.unwrap().user_properties = Some(create_invalid_user_properties());

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Connect(packet)), PacketType::Connect);
    }

    #[test]
    fn connect_validate_failure_will_topic_length() {
        let mut packet = create_connect_packet_all_properties();
        let will = packet.will.as_mut();
        will.unwrap().topic = "Terrible".repeat(10000);

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Connect(packet)), PacketType::Connect);
    }

    #[test]
    fn connect_validate_failure_will_payload_length() {
        let mut packet = create_connect_packet_all_properties();
        let will = packet.will.as_mut();
        will.unwrap().payload = Some(vec![0; 80 * 1024]);

        verify_validation_failure!(validate_packet_outbound(&MqttPacket::Connect(packet)), PacketType::Connect);
    }
}