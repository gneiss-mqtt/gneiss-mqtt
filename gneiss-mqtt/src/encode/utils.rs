/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

///
/// Internal utilities to encode MQTT5 packets, based on the MQTT5 spec
use std::collections::VecDeque;

use crate::error::{MqttError, MqttResult};
use crate::spec::*;

pub(crate) enum EncodingStep {
    Uint8(u8),
    Uint16(u16),
    Uint32(u32),
    Vli(u32),
    StringSlice(fn(&MqttPacket) -> &str, usize),
    BytesSlice(fn(&MqttPacket) -> &[u8], usize),
    IndexedString(fn(&MqttPacket, usize) -> &str, usize, usize),
    UserPropertyName(fn(&MqttPacket, usize) -> &UserProperty, usize, usize),
    UserPropertyValue(fn(&MqttPacket, usize) -> &UserProperty, usize, usize),
}

macro_rules! get_packet_field {
    ($target: expr, $pat: path, $field_name: ident) => {
        if let $pat(a) = $target {
            &a.$field_name
        } else {
            panic!("Packet variant mismatch");
        }
    };
}

pub(crate) use get_packet_field;

macro_rules! get_optional_packet_field {
    ($target: expr, $pat: path, $field_name: ident) => {
        if let $pat(a) = $target {
            &a.$field_name.as_ref().unwrap()
        } else {
            panic!("Packet variant mismatch");
        }
    };
}

pub(crate) use get_optional_packet_field;

macro_rules! encode_integral_expression {
    ($target: ident, $enum_variant: ident, $value: expr) => {
        $target.push_back(EncodingStep::$enum_variant($value));
    };
}

pub(crate) use encode_integral_expression;

macro_rules! encode_optional_property {
    ($target: ident, $enum_variant: ident, $property_key: expr, $optional_value: expr) => {
        if let Some(val) = $optional_value {
            $target.push_back(EncodingStep::Uint8($property_key));
            $target.push_back(EncodingStep::$enum_variant(val));
        }
    };
}

pub(crate) use encode_optional_property;

macro_rules! encode_optional_enum_property {
    ($target: ident, $enum_variant: ident, $property_key: expr, $int_type: ty, $optional_value: expr) => {
        if let Some(val) = $optional_value {
            $target.push_back(EncodingStep::Uint8($property_key));
            $target.push_back(EncodingStep::$enum_variant(val as $int_type));
        }
    };
}

pub(crate) use encode_optional_enum_property;

macro_rules! encode_optional_boolean_property {
    ($target: ident, $property_key: expr, $optional_value: expr) => {
        if let Some(val) = $optional_value {
            $target.push_back(EncodingStep::Uint8($property_key));
            $target.push_back(EncodingStep::Uint8(if val { 1u8 } else { 0u8 }));
        }
    };
}

pub(crate) use encode_optional_boolean_property;

macro_rules! encode_length_prefixed_string {
    ($target: ident, $getter: ident, $value: expr) => {
        $target.push_back(EncodingStep::Uint16($value.len() as u16));
        $target.push_back(EncodingStep::StringSlice(
            $getter as fn(&MqttPacket) -> &str,
            0,
        ));
    };
}

pub(crate) use encode_length_prefixed_string;

macro_rules! encode_length_prefixed_optional_string {
    ($target: ident, $getter: ident, $optional_value: expr) => {
        if let Some(val) = &$optional_value {
            $target.push_back(EncodingStep::Uint16(val.len() as u16));
            $target.push_back(EncodingStep::StringSlice(
                $getter as fn(&MqttPacket) -> &str,
                0,
            ));
        } else {
            $target.push_back(EncodingStep::Uint16(0));
        }
    };
}

pub(crate) use encode_length_prefixed_optional_string;

macro_rules! encode_optional_string_property {
    ($target: ident, $getter: ident, $property_key: expr, $field_value: expr) => {
        if let Some(val) = &$field_value {
            $target.push_back(EncodingStep::Uint8($property_key));
            $target.push_back(EncodingStep::Uint16(val.len() as u16));
            $target.push_back(EncodingStep::StringSlice(
                $getter as fn(&MqttPacket) -> &str,
                0,
            ));
        }
    };
}

pub(crate) use encode_optional_string_property;

macro_rules! encode_raw_bytes {
    ($target: ident, $getter: ident) => {
        $target.push_back(EncodingStep::BytesSlice(
            $getter as fn(&MqttPacket) -> &[u8],
            0,
        ));
    };
}

pub(crate) use encode_raw_bytes;

macro_rules! encode_length_prefixed_optional_bytes {
    ($target: ident, $getter: ident, $optional_value: expr) => {
        if let Some(val) = &$optional_value {
            $target.push_back(EncodingStep::Uint16(val.len() as u16));
            $target.push_back(EncodingStep::BytesSlice(
                $getter as fn(&MqttPacket) -> &[u8],
                0,
            ));
        } else {
            $target.push_back(EncodingStep::Uint16(0));
        }
    };
}

pub(crate) use encode_length_prefixed_optional_bytes;

macro_rules! encode_optional_bytes_property {
    ($target: ident, $getter: ident, $property_key: expr, $field_value: expr) => {
        if let Some(val) = &$field_value {
            $target.push_back(EncodingStep::Uint8($property_key));
            $target.push_back(EncodingStep::Uint16(val.len() as u16));
            $target.push_back(EncodingStep::BytesSlice(
                $getter as fn(&MqttPacket) -> &[u8],
                0,
            ));
        }
    };
}

pub(crate) use encode_optional_bytes_property;

macro_rules! encode_user_property {
    ($target: ident, $user_property_getter: ident, $value: ident, $index: expr) => {{
        $target.push_back(EncodingStep::Uint8(PROPERTY_KEY_USER_PROPERTY));
        $target.push_back(EncodingStep::Uint16($value.name.len() as u16));
        $target.push_back(EncodingStep::UserPropertyName(
            $user_property_getter as fn(&MqttPacket, usize) -> &UserProperty,
            $index,
            0,
        ));
        $target.push_back(EncodingStep::Uint16($value.value.len() as u16));
        $target.push_back(EncodingStep::UserPropertyValue(
            $user_property_getter as fn(&MqttPacket, usize) -> &UserProperty,
            $index,
            0,
        ));
    }};
}

pub(crate) use encode_user_property;

macro_rules! encode_user_properties {
    ($target: ident, $user_property_getter: ident, $properties_ref: expr) => {{
        if let Some(properties) = &$properties_ref {
            for (i, user_property) in properties.iter().enumerate() {
                encode_user_property!($target, $user_property_getter, user_property, i);
            }
        }
    }};
}

pub(crate) use encode_user_properties;

macro_rules! encode_indexed_string {
    ($target: ident, $indexed_string_getter: ident, $value: expr, $index: expr) => {{
        $target.push_back(EncodingStep::Uint16($value.len() as u16));
        $target.push_back(EncodingStep::IndexedString(
            $indexed_string_getter as fn(&MqttPacket, usize) -> &str,
            $index,
            0,
        ));
    }};
}

pub(crate) use encode_indexed_string;

macro_rules! encode_enum {
    ($target: ident, $enum_variant: ident, $int_type: ty, $value: expr) => {
        $target.push_back(EncodingStep::$enum_variant($value as $int_type));
    };
}

pub(crate) use encode_enum;

/*****************************************************/

macro_rules! define_ack_packet_lengths_function {
    ($function_name: ident, $packet_type: ident, $reason_code_type: ident) => {
        fn $function_name(packet: &$packet_type) -> MqttResult<(u32, u32)> {
            let mut property_section_length = compute_user_properties_length(&packet.user_properties);

            add_optional_string_property_length!(property_section_length, packet.reason_string);

            if property_section_length == 0 {
                if packet.reason_code == $reason_code_type::Success {
                    return Ok((2, 0));
                } else {
                    return Ok((3, 0));
                }
            }

            Ok(((3 + property_section_length + compute_variable_length_integer_encode_size(property_section_length)?) as u32, property_section_length as u32))
        }
    };
}

pub(crate) use define_ack_packet_lengths_function;

macro_rules! define_ack_packet_reason_string_accessor {
    ($accessor_name: ident, $packet_type: ident) => {
        fn $accessor_name(packet: &MqttPacket) -> &str {
            get_optional_packet_field!(packet, MqttPacket::$packet_type, reason_string)
        }
    };
}

pub(crate) use define_ack_packet_reason_string_accessor;

macro_rules! define_ack_packet_user_property_accessor {
    ($accessor_name: ident, $packet_type: ident) => {
        fn $accessor_name(packet: &MqttPacket, index: usize) -> &UserProperty {
            if let MqttPacket::$packet_type(ack) = packet {
                if let Some(properties) = &ack.user_properties {
                    return &properties[index];
                }
            }

            panic!("Internal encoding error: invalid user property state");
        }
    };
}

pub(crate) use define_ack_packet_user_property_accessor;

macro_rules! define_ack_packet_encoding_impl {
    ($function_name: ident, $packet_type: ident, $reason_code_type: ident, $first_byte: expr, $length_function: ident, $reason_string_accessor: ident, $user_property_accessor: ident) => {
        pub(crate) fn $function_name(packet: &$packet_type, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> MqttResult<()> {
            let (total_remaining_length, property_length) = $length_function(packet)?;

            encode_integral_expression!(steps, Uint8, $first_byte);
            encode_integral_expression!(steps, Vli, total_remaining_length);

            /* Variable header */
            encode_integral_expression!(steps, Uint16, packet.packet_id);

            /* per spec: empty properties + success = allowed to drop the reason code */
            if packet.reason_code == $reason_code_type::Success && property_length == 0 {
                assert_eq!(2, total_remaining_length);
                return Ok(());
            }

            encode_enum!(steps, Uint8, u8, packet.reason_code);

            /* slightly speculative: empty properties = allowed to drop the property length vli */
            if property_length == 0 {
                assert_eq!(3, total_remaining_length);
                return Ok(());
            }

            encode_integral_expression!(steps, Vli, property_length);
            encode_optional_string_property!(steps, $reason_string_accessor, PROPERTY_KEY_REASON_STRING, packet.reason_string);
            encode_user_properties!(steps, $user_property_accessor, packet.user_properties);

            Ok(())
        }
    };
}

pub(crate) use define_ack_packet_encoding_impl;

/*****************************************************/

macro_rules! add_optional_u8_property_length {
    ($target: ident, $optional_value: expr) => {
        if $optional_value.is_some() {
            $target += 2;
        }
    };
}

pub(crate) use add_optional_u8_property_length;

macro_rules! add_optional_u16_property_length {
    ($target: ident, $optional_value: expr) => {
        if $optional_value.is_some() {
            $target += 3;
        }
    };
}

pub(crate) use add_optional_u16_property_length;

macro_rules! add_optional_u32_property_length {
    ($target: ident, $optional_value: expr) => {
        if $optional_value.is_some() {
            $target += 5;
        }
    };
}

pub(crate) use add_optional_u32_property_length;

macro_rules! add_optional_string_property_length {
    ($target: ident, $optional_value: expr) => {
        if let Some(val) = &$optional_value {
            $target += 3 + val.len();
        }
    };
}

pub(crate) use add_optional_string_property_length;

macro_rules! add_optional_bytes_property_length {
    ($target: ident, $optional_value: expr) => {
        if let Some(val) = &$optional_value {
            $target += 3 + val.len();
        }
    };
}

pub(crate) use add_optional_bytes_property_length;

macro_rules! add_optional_string_length {
    ($target: ident, $optional_value: expr) => {
        $target += 2;
        if let Some(val) = &$optional_value {
            $target += val.len();
        }
    };
}

pub(crate) use add_optional_string_length;

macro_rules! add_optional_bytes_length {
    ($target: ident, $optional_value: expr) => {
        $target += 2;
        if let Some(val) = &$optional_value {
            $target += val.len();
        }
    };
}

pub(crate) use add_optional_bytes_length;

pub static MAXIMUM_VARIABLE_LENGTH_INTEGER: usize = (1 << 28) - 1;

pub fn compute_user_properties_length(properties: &Option<Vec<UserProperty>>) -> usize {
    let mut total = 0;
    if let Some(props) = properties {
        let property_count = props.len();
        total += property_count * 5; // 4 bytes of length-prefix per property, 1 byte for property key
        for property in props {
            total += property.name.len();
            total += property.value.len();
        }
    }

    total
}

pub fn compute_variable_length_integer_encode_size(value: usize) -> MqttResult<usize> {
    if value < 1usize << 7 {
        Ok(1)
    } else if value < 1usize << 14 {
        Ok(2)
    } else if value < 1usize << 21 {
        Ok(3)
    } else if value < 1usize << 28 {
        Ok(4)
    } else {
        Err(MqttError::VariableLengthIntegerMaximumExceeded)
    }
}

fn encode_vli(value: u32, dest: &mut Vec<u8>) -> MqttResult<()> {
    if value > MAXIMUM_VARIABLE_LENGTH_INTEGER as u32 {
        return Err(MqttError::VariableLengthIntegerMaximumExceeded);
    }

    let mut done = false;
    let mut val = value;
    while !done {
        let mut byte: u8 = (val & 0x7F) as u8;
        val /= 128;

        if val != 0 {
            byte |= 128;
        }

        dest.push(byte);

        done = val == 0;
    }

    Ok(())
}

fn process_byte_slice_encoding(bytes: &[u8], offset: usize, dest: &mut Vec<u8>) -> usize {
    let dest_space_in_bytes = dest.capacity() - dest.len();
    let remaining_slice_bytes = bytes.len() - offset;
    let encodable_length = usize::min(dest_space_in_bytes, remaining_slice_bytes);
    let end_offset = offset + encodable_length;
    let encodable_slice = bytes.get(offset..end_offset).unwrap();
    dest.extend_from_slice(encodable_slice);

    if encodable_length < remaining_slice_bytes {
        end_offset
    } else {
        0
    }
}

pub(crate) fn process_encoding_step(
    steps: &mut VecDeque<EncodingStep>,
    step: EncodingStep,
    packet: &MqttPacket,
    dest: &mut Vec<u8>,
) -> MqttResult<()> {
    match step {
        EncodingStep::Uint8(val) => {
            dest.push(val);
        }
        EncodingStep::Uint16(val) => {
            dest.extend_from_slice(&val.to_be_bytes());
        }
        EncodingStep::Uint32(val) => {
            dest.extend_from_slice(&val.to_be_bytes());
        }
        EncodingStep::Vli(val) => {
            return encode_vli(val, dest);
        }
        EncodingStep::StringSlice(getter, offset) => {
            let slice = getter(packet).as_bytes();
            let end_offset = process_byte_slice_encoding(slice, offset, dest);
            if end_offset > 0 {
                steps.push_front(EncodingStep::StringSlice(getter, end_offset));
            }
        }
        EncodingStep::BytesSlice(getter, offset) => {
            let slice = getter(packet);
            let end_offset = process_byte_slice_encoding(slice, offset, dest);
            if end_offset > 0 {
                steps.push_front(EncodingStep::BytesSlice(getter, end_offset));
            }
        }
        EncodingStep::IndexedString(getter, index, offset) => {
            let slice = getter(packet, index).as_bytes();
            let end_offset = process_byte_slice_encoding(slice, offset, dest);
            if end_offset > 0 {
                steps.push_front(EncodingStep::IndexedString(getter, index, end_offset));
            }
        }
        EncodingStep::UserPropertyName(getter, index, offset) => {
            let slice = getter(packet, index).name.as_bytes();
            let end_offset = process_byte_slice_encoding(slice, offset, dest);
            if end_offset > 0 {
                steps.push_front(EncodingStep::UserPropertyName(getter, index, end_offset));
            }
        }
        EncodingStep::UserPropertyValue(getter, index, offset) => {
            let slice = getter(packet, index).value.as_bytes();
            let end_offset = process_byte_slice_encoding(slice, offset, dest);
            if end_offset > 0 {
                steps.push_front(EncodingStep::UserPropertyValue(getter, index, end_offset));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::utils::*;

    macro_rules! assert_vli_encoding_equals {
        ($target: ident, $value: expr, $expected_result: expr) => {{
            let mut $target = Vec::<u8>::with_capacity(4);
            assert!(encode_vli($value, &mut $target).is_ok());
            assert_eq!($expected_result, &$target[..]);
        }};
    }

    macro_rules! assert_vli_encoding_fails {
        ($target: ident, $value: expr) => {{
            let mut $target = Vec::<u8>::with_capacity(4);
            assert!(encode_vli($value, &mut $target).is_err());
        }};
    }

    macro_rules! assert_vli_round_trip_success {
        ($value: expr) => {{
            let mut dest = Vec::<u8>::with_capacity(4);
            assert!(encode_vli($value, &mut dest).is_ok());

            for i in 1..dest.len() {
                let insufficient_data_result = decode_vli(&dest[..i]);
                assert!(insufficient_data_result.is_ok());
                assert_eq!(
                    DecodeVliResult::InsufficientData,
                    insufficient_data_result.unwrap()
                );
            }

            let final_result = decode_vli(&dest);
            let expected_bytes =
                compute_variable_length_integer_encode_size($value as usize).unwrap();
            assert!(final_result.is_ok());
            assert_eq!(
                DecodeVliResult::Value($value, &dest[expected_bytes..]),
                final_result.unwrap()
            );
        }};
    }

    #[test]
    fn vli_round_trips() {
        assert_vli_round_trip_success!(0);
        assert_vli_round_trip_success!(1);
        assert_vli_round_trip_success!(47);
        assert_vli_round_trip_success!(127);
        assert_vli_round_trip_success!(128);
        assert_vli_round_trip_success!(129);
        assert_vli_round_trip_success!(511);
        assert_vli_round_trip_success!(8000);
        assert_vli_round_trip_success!(16383);
        assert_vli_round_trip_success!(16384);
        assert_vli_round_trip_success!(16385);
        assert_vli_round_trip_success!(100000);
        assert_vli_round_trip_success!(4200000);
        assert_vli_round_trip_success!(34200000);
        assert_vli_round_trip_success!(MAXIMUM_VARIABLE_LENGTH_INTEGER as u32);
    }

    #[test]
    fn encode_vli_successes() {
        assert_vli_encoding_equals!(dest, 0, [0u8]);
        assert_vli_encoding_equals!(dest, 1, [1u8]);
        assert_vli_encoding_equals!(dest, 127, [127u8]);
        assert_vli_encoding_equals!(dest, 128, [0x80u8, 1u8]);
        assert_vli_encoding_equals!(dest, 129, [0x81u8, 1u8]);
    }

    #[test]
    fn encode_vli_failures() {
        assert_vli_encoding_fails!(dest, MAXIMUM_VARIABLE_LENGTH_INTEGER as u32 + 1);
        assert_vli_encoding_fails!(dest, 0x80000000u32);
        assert_vli_encoding_fails!(dest, 0xFFFFFFFFu32);
    }

    #[test]
    #[rustfmt::skip]
    fn compute_vli_encoding_size_successes() {
        assert_eq!(1, compute_variable_length_integer_encode_size(0).unwrap());
        assert_eq!(1, compute_variable_length_integer_encode_size(1).unwrap());
        assert_eq!(1, compute_variable_length_integer_encode_size(127).unwrap());
        assert_eq!(2, compute_variable_length_integer_encode_size(128).unwrap());
        assert_eq!(2, compute_variable_length_integer_encode_size(256).unwrap());
        assert_eq!(2, compute_variable_length_integer_encode_size(16383).unwrap());
        assert_eq!(3, compute_variable_length_integer_encode_size(16384).unwrap());
        assert_eq!(3, compute_variable_length_integer_encode_size(16385).unwrap());
        assert_eq!(3, compute_variable_length_integer_encode_size(2097151).unwrap());
        assert_eq!(4, compute_variable_length_integer_encode_size(2097152).unwrap());
        assert_eq!(4, compute_variable_length_integer_encode_size(MAXIMUM_VARIABLE_LENGTH_INTEGER).unwrap());
    }

    #[test]
    #[rustfmt::skip]
    fn compute_vli_encoding_size_failures() {
        assert!(compute_variable_length_integer_encode_size(MAXIMUM_VARIABLE_LENGTH_INTEGER + 1).is_err());
        assert!(compute_variable_length_integer_encode_size(u32::MAX as usize).is_err());
        assert!(compute_variable_length_integer_encode_size(usize::MAX).is_err());
    }
}
