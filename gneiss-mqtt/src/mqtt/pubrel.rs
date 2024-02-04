/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::decode::*;
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
define_ack_packet_lengths_function!(compute_pubrel_packet_length_properties, PubrelPacket, PubrelReasonCode);
define_ack_packet_reason_string_accessor!(get_pubrel_packet_reason_string, Pubrel);
define_ack_packet_user_property_accessor!(get_pubrel_packet_user_property, Pubrel);

#[rustfmt::skip]
define_ack_packet_encoding_impl!(write_pubrel_encoding_steps, PubrelPacket, PubrelReasonCode, PUBREL_FIRST_BYTE, compute_pubrel_packet_length_properties, get_pubrel_packet_reason_string, get_pubrel_packet_user_property);

define_ack_packet_decode_properties_function!(decode_pubrel_properties, PubrelPacket, "Pubrel");
define_ack_packet_decode_function!(decode_pubrel_packet, Pubrel, PubrelPacket, "Pubrel", PUBREL_FIRST_BYTE, convert_u8_to_pubrel_reason_code, decode_pubrel_properties);

validate_ack_outbound!(validate_pubrel_packet_outbound, PubrelPacket, PacketType::Pubrel, "Pubrel");
validate_ack_outbound_internal!(validate_pubrel_packet_outbound_internal, PubrelPacket, PacketType::Pubrel, compute_pubrel_packet_length_properties, "Pubrel");
validate_ack_inbound_internal!(validate_pubrel_packet_inbound_internal, PubrelPacket, PacketType::Pubrel, "Pubrel");

define_ack_packet_display_trait!(PubrelPacket, "PubrelPacket", pubrel_reason_code_to_str);

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    #[test]
    fn pubrel_round_trip_encode_decode_default() {
        let packet = PubrelPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet)));
    }

    #[test]
    fn pubrel_round_trip_encode_decode_success_no_props() {

        let packet = PubrelPacket {
            packet_id: 12,
            reason_code: PubrelReasonCode::Success,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet)));
    }

    #[test]
    fn pubrel_round_trip_encode_decode_failure_no_props() {

        let packet = PubrelPacket {
            packet_id: 8193,
            reason_code: PubrelReasonCode::PacketIdentifierNotFound,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet)));
    }

    #[test]
    fn pubrel_round_trip_encode_decode_success_with_props() {

        let packet = PubrelPacket {
            packet_id: 10253,
            reason_code: PubrelReasonCode::Success,
            reason_string: Some("Qos2, I can do this.  Believe in me.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "pubrel1".to_string(), value: "value1".to_string()},
                UserProperty{name: "pubrel2".to_string(), value: "value2".to_string()},
                UserProperty{name: "pubrel2".to_string(), value: "value3".to_string()},
            ))
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet)));
    }

    fn create_pubrel_with_all_properties() -> PubrelPacket {
        PubrelPacket {
            packet_id: 12500,
            reason_code: PubrelReasonCode::PacketIdentifierNotFound,
            reason_string: Some("Aw shucks, I forgot what I was doing.  Sorry!".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "hello1".to_string(), value: "squidward".to_string()},
                UserProperty{name: "patrick".to_string(), value: "star".to_string()},
            ))
        }
    }

    #[test]
    fn pubrel_round_trip_encode_decode_failure_with_props() {

        let packet = create_pubrel_with_all_properties();

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet)));
    }

    #[test]
    fn pubrel_decode_failure_bad_fixed_header() {
        let packet = create_pubrel_with_all_properties();

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Pubrel(packet), 15);
    }

    #[test]
    fn pubrel_decode_failure_bad_reason_code() {
        let packet = create_pubrel_with_all_properties();

        let corrupt_reason_code = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // for acks, the reason code is in byte 4
            clone[4] = 15;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Pubrel(packet), corrupt_reason_code);
    }

    #[test]
    fn pubrel_decode_failure_duplicate_reason_string() {
        let packet = create_pubrel_with_all_properties();

        let duplicate_reason_string = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length
            clone[1] += 5;

            // increase property section length
            clone[5] += 5;

            // add the duplicate property
            clone.push(PROPERTY_KEY_REASON_STRING);
            clone.push(0);
            clone.push(2);
            clone.push(67);
            clone.push(67);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Pubrel(packet), duplicate_reason_string);
    }

    #[test]
    fn pubrel_decode_failure_packet_size() {
        let packet = create_pubrel_with_all_properties();

        do_inbound_size_decode_failure_test(&MqttPacket::Pubrel(packet));
    }

    use crate::validate::testing::*;
    use crate::validate::utils::testing::*;

    test_ack_validate_success!(pubrel_validate_success, Pubrel, create_pubrel_with_all_properties);
    test_ack_validate_failure_reason_string_length!(pubrel_validate_failure_reason_string_length, Pubrel, create_pubrel_with_all_properties, PacketType::Pubrel);
    test_ack_validate_failure_invalid_user_properties!(pubrel_validate_failure_invalid_user_properties, Pubrel, create_pubrel_with_all_properties, PacketType::Pubrel);
    test_ack_validate_failure_outbound_size!(pubrel_validate_failure_outbound_size, Pubrel, create_pubrel_with_all_properties, PacketType::Pubrel);
    test_ack_validate_failure_packet_id_zero!(pubrel_validate_failure_packet_id_zero, Pubrel, create_pubrel_with_all_properties, PacketType::Pubrel);
}