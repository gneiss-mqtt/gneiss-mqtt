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
define_ack_packet_lengths_function!(compute_pubrel_packet_length_properties, PubrelPacket, PubrelReasonCode);
define_ack_packet_reason_string_accessor!(get_pubrel_packet_reason_string, Pubrel);
define_ack_packet_user_property_accessor!(get_pubrel_packet_user_property, Pubrel);

#[rustfmt::skip]
define_ack_packet_encoding_impl5!(write_pubrel_encoding_steps5, PubrelPacket, PubrelReasonCode, PUBREL_FIRST_BYTE, compute_pubrel_packet_length_properties, get_pubrel_packet_reason_string, get_pubrel_packet_user_property);
define_ack_packet_encoding_impl311!(write_pubrel_encoding_steps311, PubrelPacket, PUBREL_FIRST_BYTE);

define_ack_packet_decode_properties_function!(decode_pubrel_properties, PubrelPacket, "decode_pubrel_properties");
define_ack_packet_decode_function5!(decode_pubrel_packet5, Pubrel, PubrelPacket, "decode_pubrel_packet5", PUBREL_FIRST_BYTE, PubrelReasonCode, decode_pubrel_properties);
define_ack_packet_decode_function311!(decode_pubrel_packet311, Pubrel, PubrelPacket, "decode_pubrel_packet311", PUBREL_FIRST_BYTE);

validate_ack_outbound!(validate_pubrel_packet_outbound, PubrelPacket, PacketType::Pubrel, "validate_pubrel_packet_outbound");
validate_ack_outbound_internal!(validate_pubrel_packet_outbound_internal, PubrelPacket, PacketType::Pubrel, compute_pubrel_packet_length_properties, "validate_pubrel_packet_outbound_internal");
validate_ack_inbound_internal!(validate_pubrel_packet_inbound_internal, PubrelPacket, PacketType::Pubrel, "validate_pubrel_packet_inbound_internal");

define_ack_packet_display_trait!(PubrelPacket, "PubrelPacket", PubrelReasonCode);

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    fn do_pubrel_round_trip_encode_decode_default_test(protocol_version: ProtocolVersion) {
        let packet = PubrelPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet), protocol_version));
    }

    #[test]
    fn pubrel_round_trip_encode_decode_default5() {
        do_pubrel_round_trip_encode_decode_default_test(ProtocolVersion::Mqtt5);
    }

    #[test]
    fn pubrel_round_trip_encode_decode_default311() {
        do_pubrel_round_trip_encode_decode_default_test(ProtocolVersion::Mqtt311);
    }

    fn do_pubrel_round_trip_encode_decode_success_rc_no_props_test(protocol_version: ProtocolVersion) {
        let packet = PubrelPacket {
            packet_id: 12,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet), protocol_version));
    }

    #[test]
    fn pubrel_round_trip_encode_decode_success_rc_no_props5() {
        do_pubrel_round_trip_encode_decode_success_rc_no_props_test(ProtocolVersion::Mqtt5);
    }

    #[test]
    fn pubrel_round_trip_encode_decode_success_rc_no_props311() {
        do_pubrel_round_trip_encode_decode_success_rc_no_props_test(ProtocolVersion::Mqtt311);
    }

    #[test]
    fn pubrel_round_trip_encode_decode_failure_rc_no_props5() {

        let packet = PubrelPacket {
            packet_id: 8193,
            reason_code: PubrelReasonCode::PacketIdentifierNotFound,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet), ProtocolVersion::Mqtt5));
    }

    #[test]
    fn pubrel_round_trip_encode_decode_success_rc_with_props5() {

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

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet), ProtocolVersion::Mqtt5));
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
    fn pubrel_round_trip_encode_decode_failure_rc_with_props5() {
        let packet = create_pubrel_with_all_properties();

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrel(packet), ProtocolVersion::Mqtt5));
    }

    #[test]
    fn pubrel_round_trip_encode_decode_failure_rc_with_props311() {
        let packet = create_pubrel_with_all_properties();
        let expected_packet = PubrelPacket {
            packet_id : packet.packet_id,
            ..Default::default()
        };

        assert!(do_311_filter_encode_decode_test(&MqttPacket::Pubrel(packet), &MqttPacket::Pubrel(expected_packet)));
    }

    #[test]
    fn pubrel_decode_failure_bad_fixed_header5() {
        let packet = create_pubrel_with_all_properties();
        do_fixed_header_flag_decode_failure_test(&MqttPacket::Pubrel(packet), ProtocolVersion::Mqtt5, 15);
    }

    #[test]
    fn pubrel_decode_failure_bad_fixed_header311() {
        let packet = create_pubrel_with_all_properties();
        do_fixed_header_flag_decode_failure_test(&MqttPacket::Pubrel(packet), ProtocolVersion::Mqtt311, 15);
    }

    #[test]
    fn pubrel_decode_failure_bad_reason_code5() {
        let packet = create_pubrel_with_all_properties();

        let corrupt_reason_code = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // for acks, the reason code is in byte 4
            clone[4] = 15;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Pubrel(packet), ProtocolVersion::Mqtt5, corrupt_reason_code);
    }

    #[test]
    fn pubrel_decode_failure_duplicate_reason_string5() {
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

        do_mutated_decode_failure_test(&MqttPacket::Pubrel(packet), ProtocolVersion::Mqtt5, duplicate_reason_string);
    }

    #[test]
    fn pubrel_decode_failure_packet_size5() {
        let packet = create_pubrel_with_all_properties();

        do_inbound_size_decode_failure_test(&MqttPacket::Pubrel(packet), ProtocolVersion::Mqtt5);
    }

    use crate::validate::testing::*;

    test_ack_validate_success!(pubrel_validate_success, Pubrel, create_pubrel_with_all_properties);
    test_ack_validate_failure_reason_string_length!(pubrel_validate_failure_reason_string_length, Pubrel, create_pubrel_with_all_properties, PacketType::Pubrel);
    test_ack_validate_failure_invalid_user_properties!(pubrel_validate_failure_invalid_user_properties, Pubrel, create_pubrel_with_all_properties, PacketType::Pubrel);
    test_ack_validate_failure_outbound_size!(pubrel_validate_failure_outbound_size5, Pubrel, create_pubrel_with_all_properties, PacketType::Pubrel, ProtocolVersion::Mqtt5);
    test_ack_validate_failure_outbound_size!(pubrel_validate_failure_outbound_size311, Pubrel, create_pubrel_with_all_properties, PacketType::Pubrel, ProtocolVersion::Mqtt311);
    test_ack_validate_failure_packet_id_zero!(pubrel_validate_failure_packet_id_zero, Pubrel, create_pubrel_with_all_properties, PacketType::Pubrel);
}