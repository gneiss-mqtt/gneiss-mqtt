/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */


use crate::encode::utils::*;
use crate::error::{MqttError, MqttResult};
use crate::logging::*;
use crate::mqtt::*;
use crate::mqtt::utils::*;

use crate::mqtt::auth::*;
use crate::mqtt::connack::*;
use crate::mqtt::connect::*;
use crate::mqtt::disconnect::*;
use crate::mqtt::pingreq::*;
use crate::mqtt::pingresp::*;
use crate::mqtt::puback::*;
use crate::mqtt::pubcomp::*;
use crate::mqtt::publish::*;
use crate::mqtt::pubrec::*;
use crate::mqtt::pubrel::*;
use crate::mqtt::suback::*;
use crate::mqtt::subscribe::*;
use crate::mqtt::unsuback::*;
use crate::mqtt::unsubscribe::*;

use log::*;

use std::collections::*;

const DECODE_BUFFER_DEFAULT_SIZE : usize = 16 * 1024;

macro_rules! define_ack_packet_decode_properties_function {
    ($function_name: ident, $packet_type: ident, $packet_type_as_string: expr) => {
        fn $function_name(property_bytes: &[u8], packet : &mut $packet_type) -> MqttResult<()> {
            let mut mutable_property_bytes = property_bytes;

            while mutable_property_bytes.len() > 0 {
                let property_key = mutable_property_bytes[0];
                mutable_property_bytes = &mutable_property_bytes[1..];

                match property_key {
                    PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
                    PROPERTY_KEY_REASON_STRING => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.reason_string)?; }
                    _ => {
                        error!("{}Packet Decode - Invalid property type ({})", $packet_type_as_string, property_key);
                        return Err(MqttError::new_decoding_failure("invalid ack packet property type"));
                    }
                }
            }

            Ok(())
        }
    };
}

pub(crate) use define_ack_packet_decode_properties_function;

macro_rules! define_ack_packet_decode_function {
    ($function_name: ident, $mqtt_packet_type:ident, $packet_type: ident, $packet_type_as_string: expr, $first_byte: expr, $reason_code_converter_function_name: ident, $decode_properties_function_name: ident) => {
        pub(crate) fn $function_name(first_byte: u8, packet_body: &[u8]) -> MqttResult<Box<MqttPacket>> {
            if first_byte != $first_byte {
                error!("{}Packet Decode - invalid first byte", $packet_type_as_string);
                return Err(MqttError::new_decoding_failure("invalid first byte for ack packet"));
            }

            let mut box_packet = Box::new(MqttPacket::$mqtt_packet_type($packet_type { ..Default::default() }));

            if let MqttPacket::$mqtt_packet_type(packet) = box_packet.as_mut() {
                let mut mutable_body = packet_body;
                mutable_body = decode_u16(mutable_body, &mut packet.packet_id)?;
                if mutable_body.len() == 0 {
                    /* Success is the default, so nothing to do */
                    return Ok(box_packet);
                }

                mutable_body = decode_u8_as_enum(mutable_body, &mut packet.reason_code, $reason_code_converter_function_name)?;
                if mutable_body.len() == 0 {
                    return Ok(box_packet);
                }

                /* it's a mystery why the specification adds this field; it's completely unnecessary */
                let mut properties_length = 0;
                mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
                if properties_length != mutable_body.len() {
                    error!("{}Packet Decode - property length does not match remaining packet length", $packet_type_as_string);
                    return Err(MqttError::new_decoding_failure("mismatch between property length and remaining packet length for ack packet"));
                }

                $decode_properties_function_name(mutable_body, packet)?;

                return Ok(box_packet)
            }

            panic!("{}Packet Decode - Internal error", $packet_type_as_string);
        }
    };
}

pub(crate) use define_ack_packet_decode_function;

#[derive(Copy, Clone, Eq, PartialEq)]
enum DecoderState {
    ReadPacketType,
    ReadTotalRemainingLength,
    ReadPacketBody,
    TerminalError
}

//#[derive(Copy, Clone, Eq, PartialEq)]
enum DecoderDirective {
    OutOfData,
    Continue,
    TerminalError(MqttError)
}

pub(crate) struct DecodingContext<'a> {
    pub(crate) maximum_packet_size : u32,

    pub(crate) decoded_packets: &'a mut VecDeque<Box<MqttPacket>>
}

pub(crate) struct Decoder {
    state: DecoderState,

    scratch: Vec<u8>,

    first_byte: Option<u8>,

    remaining_length : Option<usize>,
}

fn decode_packet(first_byte: u8, packet_body: &[u8]) -> MqttResult<Box<MqttPacket>> {
    let packet_type = first_byte >> 4;

    info!("Decoding a packet of type {}", packet_type_to_str(packet_type));

    match packet_type {
        PACKET_TYPE_CONNECT => { decode_connect_packet(first_byte, packet_body) }
        PACKET_TYPE_CONNACK => { decode_connack_packet(first_byte, packet_body) }
        PACKET_TYPE_PUBLISH => { decode_publish_packet(first_byte, packet_body) }
        PACKET_TYPE_PUBACK => { decode_puback_packet(first_byte, packet_body) }
        PACKET_TYPE_PUBREC => { decode_pubrec_packet(first_byte, packet_body) }
        PACKET_TYPE_PUBREL => { decode_pubrel_packet(first_byte, packet_body) }
        PACKET_TYPE_PUBCOMP => { decode_pubcomp_packet(first_byte, packet_body) }
        PACKET_TYPE_SUBSCRIBE => { decode_subscribe_packet(first_byte, packet_body) }
        PACKET_TYPE_SUBACK => { decode_suback_packet(first_byte, packet_body) }
        PACKET_TYPE_UNSUBSCRIBE => { decode_unsubscribe_packet(first_byte, packet_body) }
        PACKET_TYPE_UNSUBACK => { decode_unsuback_packet(first_byte, packet_body) }
        PACKET_TYPE_PINGREQ => { decode_pingreq_packet(first_byte, packet_body) }
        PACKET_TYPE_PINGRESP => { decode_pingresp_packet(first_byte, packet_body) }
        PACKET_TYPE_DISCONNECT => { decode_disconnect_packet(first_byte, packet_body) }
        PACKET_TYPE_AUTH => { decode_auth_packet(first_byte, packet_body) }
        _ => {
            Err(MqttError::new_decoding_failure("invalid packet type value"))
        }
    }
}

impl Decoder {
    pub fn new() -> Decoder {
        Decoder {
            state: DecoderState::ReadPacketType,
            scratch : Vec::<u8>::with_capacity(DECODE_BUFFER_DEFAULT_SIZE),
            first_byte : None,
            remaining_length : None,
        }
    }

    pub fn reset_for_new_connection(&mut self) {
        self.reset();
    }

    fn process_read_packet_type<'a>(&mut self, bytes: &'a [u8]) -> (DecoderDirective, &'a[u8]) {
        if bytes.is_empty() {
            return (DecoderDirective::OutOfData, bytes);
        }

        self.first_byte = Some(bytes[0]);
        self.state = DecoderState::ReadTotalRemainingLength;

        (DecoderDirective::Continue, &bytes[1..])
    }

    fn process_read_total_remaining_length<'a>(&mut self, bytes: &'a[u8], context: &DecodingContext) -> (DecoderDirective, &'a[u8]) {
        if bytes.is_empty() {
            return (DecoderDirective::OutOfData, bytes);
        }

        self.scratch.push(bytes[0]);
        let remaining_bytes = &bytes[1..];

        let decode_vli_result = decode_vli(&self.scratch);
        if let Ok(DecodeVliResult::Value(remaining_length, _)) = decode_vli_result {
            let mut maximum_size = context.maximum_packet_size;
            if maximum_size == 0 {
                maximum_size = MAXIMUM_VARIABLE_LENGTH_INTEGER as u32;
            }

            let total_packet_size = remaining_length + 1 + self.scratch.len() as u32;
            if total_packet_size <= maximum_size {
                self.remaining_length = Some(remaining_length as usize);
                self.state = DecoderState::ReadPacketBody;
                self.scratch.clear();
                (DecoderDirective::Continue, remaining_bytes)
            } else {
                (DecoderDirective::TerminalError(MqttError::new_decoding_failure("packet size exceeds negotiated maximum")), remaining_bytes)
            }
        } else if self.scratch.len() >= 4 {
            (DecoderDirective::TerminalError(MqttError::new_decoding_failure("invalid remaining length vli value")), remaining_bytes)
        } else if !remaining_bytes.is_empty() {
            (DecoderDirective::Continue, remaining_bytes)
        } else {
            (DecoderDirective::OutOfData, remaining_bytes)
        }
    }

    fn process_read_packet_body<'a>(&mut self, bytes: &'a[u8], context: &mut DecodingContext) -> (DecoderDirective, &'a[u8]) {
        let read_so_far = self.scratch.len();
        let bytes_needed = self.remaining_length.unwrap() - read_so_far;
        if bytes_needed > bytes.len() {
            self.scratch.extend_from_slice(bytes);
            return (DecoderDirective::OutOfData, &[]);
        }

        let packet_slice : &[u8] =
            if !self.scratch.is_empty() {
                self.scratch.extend_from_slice(&bytes[..bytes_needed]);
                &self.scratch
            } else {
                &bytes[..bytes_needed]
            };

        match decode_packet(self.first_byte.unwrap(), packet_slice) {
            Ok(packet) => {
                log_packet("Successfully decoded incoming packet: ", &packet);
                context.decoded_packets.push_back(packet);

                self.reset_for_new_packet();
                (DecoderDirective::Continue, &bytes[bytes_needed..])
            }
            Err(error) => {
                (DecoderDirective::TerminalError(error), &[])
            }
        }
    }

    pub fn decode_bytes(&mut self, bytes: &[u8], context: &mut DecodingContext) -> MqttResult<()> {
        let mut current_slice = bytes;

        let mut decode_result = DecoderDirective::Continue;
        while let DecoderDirective::Continue = decode_result {
            match self.state {
                DecoderState::ReadPacketType => {
                    (decode_result, current_slice) = self.process_read_packet_type(current_slice);
                }

                DecoderState::ReadTotalRemainingLength => {
                    (decode_result, current_slice) = self.process_read_total_remaining_length(current_slice, context);
                }

                DecoderState::ReadPacketBody => {
                    (decode_result, current_slice) = self.process_read_packet_body(current_slice, context);
                }

                _ => {
                    decode_result = DecoderDirective::TerminalError(MqttError::new_decoding_failure("decoder already in a terminal failure state"));
                }
            }
        }

        if let DecoderDirective::TerminalError(error) = decode_result {
            self.state = DecoderState::TerminalError;
            return Err(error);
        }

        Ok(())
    }

    fn reset_for_new_packet(&mut self) {
        if self.state != DecoderState::TerminalError {
            self.reset();
        }
    }

    fn reset(&mut self) {
        self.state = DecoderState::ReadPacketType;
        self.scratch.clear();
        self.first_byte = None;
        self.remaining_length = None;
    }
}

#[derive(Eq, PartialEq, Debug)]
pub(crate) enum DecodeVliResult<'a> {
    InsufficientData,
    Value(u32, &'a[u8]), /* (decoded value, remaining bytes) */
}

pub(crate) fn decode_vli(buffer: &[u8]) -> MqttResult<DecodeVliResult> {
    let mut value: u32 = 0;
    let mut needs_data: bool;
    let mut shift: u32 = 0;
    let data_len = buffer.len();

    for i in 0..4 {
        if i >= data_len {
            return Ok(DecodeVliResult::InsufficientData);
        }

        let byte = buffer[i];
        value |= ((byte & 0x7F) as u32) << shift;
        shift += 7;

        needs_data = (byte & 0x80) != 0;
        if !needs_data {
            return Ok(DecodeVliResult::Value(value, &buffer[(i + 1)..]));
        }
    }

    error!("Packet Decode - invalid variable length integer");
    Err(MqttError::new_decoding_failure("invalid variable length integer"))
}

pub(crate) fn decode_vli_into_mutable<'a>(buffer: &'a[u8], value: &mut usize) -> MqttResult<&'a[u8]> {
    let decode_result = decode_vli(buffer)?;
    match decode_result {
        DecodeVliResult::InsufficientData => {
            error!("Packet Decode - invalid variable length integer");
            Err(MqttError::new_decoding_failure("invalid variable length integer"))
        }
        DecodeVliResult::Value(vli, remaining_slice) => {
            *value = vli as usize;
            Ok(remaining_slice)
        }
    }
}

pub(crate) fn decode_length_prefixed_string<'a>(bytes: &'a[u8], value: &mut String) -> MqttResult<&'a[u8]> {
    if bytes.len() < 2 {
        error!("Packet Decode - Utf-8 string value does not have a full length prefix");
        return Err(MqttError::new_decoding_failure("utf-8 string value does not have a full length prefix"));
    }

    let value_length : usize = u16::from_be_bytes(bytes[..2].try_into().unwrap()) as usize;
    let mutable_bytes = &bytes[2..];
    if value_length > mutable_bytes.len() {
        error!("Packet Decode - Utf-8 string value has length larger than remaining packet bytes");
        return Err(MqttError::new_decoding_failure("utf-8 string value has length longer than remaining packet bytes"));
    }

    let decode_utf8_result = std::str::from_utf8(&mutable_bytes[..value_length])?;
    *value = decode_utf8_result.to_string();
    Ok(&mutable_bytes[(value_length)..])
}

pub(crate) fn decode_optional_length_prefixed_string<'a>(bytes: &'a[u8], value: &mut Option<String>) -> MqttResult<&'a[u8]> {
    if bytes.len() < 2 {
        error!("Packet Decode - Utf-8 string value does not have a full length prefix");
        return Err(MqttError::new_decoding_failure("utf-8 string value does not have a full length prefix"));
    }

    if value.is_some() {
        error!("Packet Decode - Invalid duplicate optional string property");
        return Err(MqttError::new_decoding_failure("optional string property already set earlier"));
    }

    let value_length : usize = u16::from_be_bytes(bytes[..2].try_into().unwrap()) as usize;
    let mutable_bytes = &bytes[2..];
    if value_length > mutable_bytes.len() {
        error!("Packet Decode - Utf-8 string value has length larger than remaining packet bytes");
        return Err(MqttError::new_decoding_failure("utf-8 string value has length longer than remaining packet bytes"));
    }

    let decode_utf8_result = std::str::from_utf8(&mutable_bytes[..value_length])?;
    *value = Some(decode_utf8_result.to_string());
    Ok(&mutable_bytes[(value_length)..])
}

#[cfg(test)]
pub(crate) fn decode_length_prefixed_optional_string<'a>(bytes: &'a[u8], value: &mut Option<String>) -> MqttResult<&'a[u8]> {
    if bytes.len() < 2 {
        error!("Packet Decode - Utf-8 string value does not have a full length prefix");
        return Err(MqttError::new_decoding_failure("utf-8 string value does not have a full length prefix"));
    }

    if value.is_some() {
        error!("Packet Decode - Invalid duplicate optional string property");
        return Err(MqttError::new_decoding_failure("optional string property already set earlier"));
    }

    let value_length : usize = u16::from_be_bytes(bytes[..2].try_into().unwrap()) as usize;
    let mutable_bytes = &bytes[2..];

    if value_length == 0 {
        *value = None;
        return Ok(mutable_bytes);
    }

    if value_length > mutable_bytes.len() {
        error!("Packet Decode - Utf-8 string value has length larger than remaining packet bytes");
        return Err(MqttError::new_decoding_failure("utf-8 string value has length longer than remaining packet bytes"));
    }

    let decode_utf8_result = std::str::from_utf8(&mutable_bytes[..value_length])?;
    *value = Some(decode_utf8_result.to_string());
    Ok(&mutable_bytes[(value_length)..])
}

pub(crate) fn decode_optional_length_prefixed_bytes<'a>(bytes: &'a[u8], value: &mut Option<Vec<u8>>) -> MqttResult<&'a[u8]> {
    if bytes.len() < 2 {
        error!("Packet Decode - Binary data value does not have a full length prefix");
        return Err(MqttError::new_decoding_failure("binary value does not have a full length prefix"));
    }

    if value.is_some() {
        error!("Packet Decode - Invalid duplicate optional binary data property");
        return Err(MqttError::new_decoding_failure("optional binary property already set earlier"));
    }

    let value_length : usize = u16::from_be_bytes(bytes[..2].try_into().unwrap()) as usize;
    let mutable_bytes = &bytes[2..];
    if value_length > mutable_bytes.len() {
        error!("Packet Decode - Binary data value has length larger than remaining packet bytes");
        return Err(MqttError::new_decoding_failure("binary value has length longer than remaining packet bytes"));
    }

    *value = Some(Vec::from(&mutable_bytes[..value_length]));
    Ok(&mutable_bytes[(value_length)..])
}

#[cfg(test)]
pub(crate) fn decode_length_prefixed_optional_bytes<'a>(bytes: &'a[u8], value: &mut Option<Vec<u8>>) -> MqttResult<&'a[u8]> {
    if bytes.len() < 2 {
        error!("Packet Decode - Binary data value does not have a full length prefix");
        return Err(MqttError::new_decoding_failure("binary value does not have a full length prefix"));
    }

    if value.is_some() {
        error!("Packet Decode - Invalid duplicate optional binary data property");
        return Err(MqttError::new_decoding_failure("optional binary property already set earlier"));
    }

    let value_length : usize = u16::from_be_bytes(bytes[..2].try_into().unwrap()) as usize;
    let mutable_bytes = &bytes[2..];

    if value_length == 0 {
        *value = None;
        return Ok(mutable_bytes);
    }

    if value_length > mutable_bytes.len() {
        error!("Packet Decode - Binary data value has length larger than remaining packet bytes");
        return Err(MqttError::new_decoding_failure("binary value has length longer than remaining packet bytes"));
    }

    *value = Some(Vec::from(&mutable_bytes[..value_length]));
    Ok(&mutable_bytes[(value_length)..])
}

pub(crate) fn decode_user_property<'a>(bytes: &'a[u8], properties: &mut Option<Vec<UserProperty>>) -> MqttResult<&'a[u8]> {
    let mut property : UserProperty = UserProperty { ..Default::default() };

    let mut mutable_bytes = bytes;
    mutable_bytes = decode_length_prefixed_string(mutable_bytes, &mut property.name)?;
    mutable_bytes = decode_length_prefixed_string(mutable_bytes, &mut property.value)?;

    if properties.is_none() {
        *properties = Some(Vec::new());
    }

    properties.as_mut().unwrap().push(property);

    Ok(mutable_bytes)
}

#[cfg(test)]
pub(crate) fn decode_u8<'a>(bytes: &'a[u8], value: &mut u8) -> MqttResult<&'a[u8]> {
    if bytes.is_empty() {
        return Err(MqttError::new_decoding_failure("insufficient packet data for u8 property value"));
    }

    *value = bytes[0];

    Ok(&bytes[1..])
}

pub(crate) fn decode_optional_u8_as_bool<'a>(bytes: &'a[u8], value: &mut Option<bool>) -> MqttResult<&'a[u8]> {
    if bytes.is_empty() {
        error!("Packet Decode - Insufficent packet bytes for boolean property");
        return Err(MqttError::new_decoding_failure("insufficient packet data for boolean property value"));
    }

    if value.is_some() {
        error!("Packet Decode - Invalid duplicate optional boolean property");
        return Err(MqttError::new_decoding_failure("optional boolean property already set earlier"));
    }

    if bytes[0] == 0 {
        *value = Some(false);
    } else if bytes[0] == 1 {
        *value = Some(true);
    } else {
        error!("Packet Decode - Invalid byte value for boolean property");
        return Err(MqttError::new_decoding_failure("invalid value for boolean property"));
    }

    Ok(&bytes[1..])
}

pub(crate) fn decode_u8_as_enum<'a, T>(bytes: &'a[u8], value: &mut T, converter: fn(u8) -> MqttResult<T>) -> MqttResult<&'a[u8]> {
    if bytes.is_empty() {
        error!("Packet Decode - Insufficent packet bytes for enum property");
        return Err(MqttError::new_decoding_failure("insufficient packet data for enum property value"));
    }

    *value = converter(bytes[0])?;

    Ok(&bytes[1..])
}

pub(crate) fn decode_optional_u8_as_enum<'a, T>(bytes: &'a[u8], value: &mut Option<T>, converter: fn(u8) -> MqttResult<T>) -> MqttResult<&'a[u8]> {
    if bytes.is_empty() {
        error!("Packet Decode - Insufficent packet bytes for enum property");
        return Err(MqttError::new_decoding_failure("insufficient packet data for enum property value"));
    }

    if value.is_some() {
        error!("Packet Decode - Invalid duplicate optional enum property");
        return Err(MqttError::new_decoding_failure("optional enum property already set earlier"));
    }

    *value = Some(converter(bytes[0])?);

    Ok(&bytes[1..])
}

pub(crate) fn decode_u16<'a>(bytes: &'a[u8], value: &mut u16) -> MqttResult<&'a[u8]> {
    if bytes.len() < 2 {
        error!("Packet Decode - Insufficent packet bytes for u16 property");
        return Err(MqttError::new_decoding_failure("insufficient packet data for u16 property value"));
    }

    *value = u16::from_be_bytes(bytes[..2].try_into().unwrap());

    Ok(&bytes[2..])
}

pub(crate) fn decode_optional_u16<'a>(bytes: &'a[u8], value: &mut Option<u16>) -> MqttResult<&'a[u8]> {
    if bytes.len() < 2 {
        error!("Packet Decode - Insufficent packet bytes for u16 property");
        return Err(MqttError::new_decoding_failure("insufficient packet data for u16 property value"));
    }

    if value.is_some() {
        error!("Packet Decode - Invalid duplicate optional u16 property");
        return Err(MqttError::new_decoding_failure("optional u16 property already set earlier"));
    }

    *value = Some(u16::from_be_bytes(bytes[..2].try_into().unwrap()));

    Ok(&bytes[2..])
}

pub(crate) fn decode_optional_u32<'a>(bytes: &'a[u8], value: &mut Option<u32>) -> MqttResult<&'a[u8]> {
    if bytes.len() < 4 {
        error!("Packet Decode - Insufficent packet bytes for u32 property");
        return Err(MqttError::new_decoding_failure("insufficient packet data for u32 property value"));
    }

    if value.is_some() {
        error!("Packet Decode - Invalid duplicate optional u32 property");
        return Err(MqttError::new_decoding_failure("optional u32 property already set earlier"));
    }

    *value = Some(u32::from_be_bytes(bytes[..4].try_into().unwrap()));

    Ok(&bytes[4..])
}


#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::alias::*;
    use crate::encode::*;
    use assert_matches::assert_matches;

    pub(crate) fn do_single_encode_decode_test(packet : &MqttPacket, encode_size : usize, decode_size : usize, encode_repetitions : u32) -> bool {

        let mut encoder = Encoder::new();

        let mut full_encoded_stream = Vec::with_capacity( 128 * 1024);
        let mut encode_buffer = Vec::with_capacity(encode_size);

        let mut outbound_resolver  = (OutboundAliasResolverFactory::new_manual_factory())();
        outbound_resolver.reset_for_new_connection(65535);

        /* encode 5 copies of the packet */
        for _ in 0..encode_repetitions {
            let mut encoding_context = EncodingContext {
                ..Default::default()
            };

            if let MqttPacket::Publish(publish) = &packet {
                encoding_context.outbound_alias_resolution = outbound_resolver.resolve_and_apply_topic_alias(&publish.topic_alias, &publish.topic);
            }

            assert!(!encoder.reset(&packet, &encoding_context).is_err());

            let mut cumulative_result : EncodeResult = EncodeResult::Full;
            while cumulative_result == EncodeResult::Full {
                encode_buffer.clear();
                let encode_result = encoder.encode(packet, &mut encode_buffer);
                if let Err(_) = encode_result {
                    break;
                }

                cumulative_result = encode_result.unwrap();
                full_encoded_stream.extend_from_slice(encode_buffer.as_slice());
            }

            assert_eq!(cumulative_result, EncodeResult::Complete);
        }

        let mut decoder = Decoder::new();
        decoder.reset_for_new_connection();

        let mut decoded_packets : VecDeque<Box<MqttPacket>> = VecDeque::new();

        let mut decoding_context = DecodingContext {
            maximum_packet_size: MAXIMUM_VARIABLE_LENGTH_INTEGER as u32,
            decoded_packets: &mut decoded_packets
        };

        let mut decode_stream_slice = full_encoded_stream.as_slice();
        while decode_stream_slice.len() > 0 {
            let fragment_size : usize = usize::min(decode_size, decode_stream_slice.len());
            let decode_slice = &decode_stream_slice[..fragment_size];
            decode_stream_slice = &decode_stream_slice[fragment_size..];

            let decode_result = decoder.decode_bytes(decode_slice, &mut decoding_context);
            assert!(!decode_result.is_err());
        }

        let mut matching_packets : u32 = 0;

        let mut inbound_alias_resolver = InboundAliasResolver::new(65535);

        for mut received_packet in decoded_packets {
            matching_packets += 1;

            if let MqttPacket::Publish(publish) = &mut(*received_packet) {
                if let Err(_) = inbound_alias_resolver.resolve_topic_alias(&publish.topic_alias, &mut publish.topic) {
                    return false;
                }
            }

            assert_eq!(*packet, *received_packet);
        }

        assert_eq!(encode_repetitions, matching_packets);

        return true;
    }

    pub(crate) fn do_round_trip_encode_decode_test(packet : &MqttPacket) -> bool {
        let encode_buffer_sizes : Vec<usize> = vec!(4, 5, 7, 11, 17, 31, 47, 71, 131);
        let decode_fragment_sizes : Vec<usize> = vec!(1, 2, 3, 5, 7, 11, 17, 31, 47, 71, 131, 1023);

        for encode_size in encode_buffer_sizes.iter() {
            for decode_size in decode_fragment_sizes.iter() {
                assert!(do_single_encode_decode_test(&packet, *encode_size, *decode_size, 5));
            }
        }

        return true;
    }

    pub(crate) fn encode_packet_for_test(packet: &MqttPacket) -> Vec<u8> {
        let mut encoder = Encoder::new();

        let mut encoded_buffer = Vec::with_capacity( 128 * 1024);

        let mut encoding_context = EncodingContext { ..Default::default() };

        assert!(!encoder.reset(&packet, &mut encoding_context).is_err());

        let encode_result = encoder.encode(packet, &mut encoded_buffer);
        assert_matches!(encode_result, Ok(EncodeResult::Complete));

        encoded_buffer
    }

    /*
     * verifies that the packet encodes/decodes correctly, but applying the supplied mutator
     * to the encoding leads to a decode failure.  Useful to verify specification requirements
     * with respect to decode failures like reserved bits, headers, duplicate properties, etc...
     */
    pub(crate) fn do_mutated_decode_failure_test<F>(packet: &MqttPacket, mutator: F ) where F : Fn(&[u8]) -> Vec<u8> {
        let good_encoded_bytes = encode_packet_for_test(packet);

        let mut decoder = Decoder::new();
        decoder.reset_for_new_connection();

        let mut decoded_packets : VecDeque<Box<MqttPacket>> = VecDeque::new();

        let mut decoding_context = DecodingContext {
            maximum_packet_size: MAXIMUM_VARIABLE_LENGTH_INTEGER as u32,
            decoded_packets: &mut decoded_packets
        };

        let decode_result = decoder.decode_bytes(good_encoded_bytes.as_slice(), &mut decoding_context);
        assert!(decode_result.is_ok());
        assert_eq!(1, decoded_packets.len());

        let receive_result = &decoded_packets[0];
        assert_eq!(*packet, **receive_result);

        let bad_encoded_bytes = mutator(good_encoded_bytes.as_slice());

        assert_ne!(good_encoded_bytes.as_slice(), bad_encoded_bytes.as_slice());

        // verify that the packet now fails to decode
        decoded_packets.clear();
        decoder.reset_for_new_connection();

        let mut decoding_context = DecodingContext {
            maximum_packet_size: MAXIMUM_VARIABLE_LENGTH_INTEGER as u32,
            decoded_packets: &mut decoded_packets
        };

        let decode_result = decoder.decode_bytes(bad_encoded_bytes.as_slice(), &mut decoding_context);
        assert_matches!(decode_result, Err(MqttError::DecodingFailure(_)));
        assert_eq!(0, decoded_packets.len());
    }

    pub(crate) fn do_inbound_size_decode_failure_test(packet: &MqttPacket) {
        let encoded_bytes = encode_packet_for_test(packet);

        let mut decoder = Decoder::new();
        decoder.reset_for_new_connection();

        let mut decoded_packets : VecDeque<Box<MqttPacket>> = VecDeque::new();

        let mut decoding_context = DecodingContext {
            maximum_packet_size: MAXIMUM_VARIABLE_LENGTH_INTEGER as u32,
            decoded_packets: &mut decoded_packets
        };

        let decode_result = decoder.decode_bytes(encoded_bytes.as_slice(), &mut decoding_context);
        assert!(decode_result.is_ok());
        assert_eq!(1, decoded_packets.len());

        let receive_result = &decoded_packets[0];
        assert_eq!(*packet, **receive_result);

        decoded_packets.clear();

        // verify that the packet now fails to decode
        decoder.reset_for_new_connection();

        let mut decoding_context = DecodingContext {
            maximum_packet_size: (encoded_bytes.len() - 1) as u32,
            decoded_packets: &mut decoded_packets
        };

        let decode_result = decoder.decode_bytes(encoded_bytes.as_slice(), &mut decoding_context);
        assert_matches!(decode_result, Err(MqttError::DecodingFailure(_)));
        assert_eq!(0, decoded_packets.len());
    }

    pub(crate) fn do_fixed_header_flag_decode_failure_test(packet: &MqttPacket, flags_mask: u8) {
        let reserved_mutator = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();
            clone[0] |= flags_mask;
            clone
        };

        do_mutated_decode_failure_test(packet, reserved_mutator);
    }
}