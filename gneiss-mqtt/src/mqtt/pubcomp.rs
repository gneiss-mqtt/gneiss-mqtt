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
define_ack_packet_lengths_function!(compute_pubcomp_packet_length_properties, PubcompPacket, PubcompReasonCode);
define_ack_packet_reason_string_accessor!(get_pubcomp_packet_reason_string, Pubcomp);
define_ack_packet_user_property_accessor!(get_pubcomp_packet_user_property, Pubcomp);

#[rustfmt::skip]
define_ack_packet_encoding_impl5!(write_pubcomp_encoding_steps5, PubcompPacket, PubcompReasonCode, PUBCOMP_FIRST_BYTE, compute_pubcomp_packet_length_properties, get_pubcomp_packet_reason_string, get_pubcomp_packet_user_property);
define_ack_packet_encoding_impl311!(write_pubcomp_encoding_steps311, PubcompPacket, PUBCOMP_FIRST_BYTE);

define_ack_packet_decode_properties_function!(decode_pubcomp_properties, PubcompPacket, "decode_pubcomp_properties");
define_ack_packet_decode_function5!(decode_pubcomp_packet5, Pubcomp, PubcompPacket, "decode_pubcomp_packet5", PUBCOMP_FIRST_BYTE, PubcompReasonCode, decode_pubcomp_properties);
define_ack_packet_decode_function311!(decode_pubcomp_packet311, Pubcomp, PubcompPacket, "decode_pubcomp_packet311", PUBCOMP_FIRST_BYTE);

validate_ack_outbound!(validate_pubcomp_packet_outbound, PubcompPacket, PacketType::Pubcomp, "validate_pubcomp_packet_outbound");
validate_ack_outbound_internal!(validate_pubcomp_packet_outbound_internal, PubcompPacket, PacketType::Pubcomp, compute_pubcomp_packet_length_properties, "validate_pubcomp_packet_outbound_internal");
validate_ack_inbound_internal!(validate_pubcomp_packet_inbound_internal, PubcompPacket, PacketType::Pubcomp, "validate_pubcomp_packet_inbound_internal");

define_ack_packet_display_trait!(PubcompPacket, "PubcompPacket", PubcompReasonCode);

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    fn do_pubcomp_round_trip_encode_decode_default_test(protocol_version: ProtocolVersion) {
        let packet = PubcompPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubcomp(packet), protocol_version));
    }

    #[test]
    fn pubcomp_round_trip_encode_decode_default5() {
        do_pubcomp_round_trip_encode_decode_default_test(ProtocolVersion::Mqtt5);
    }

    #[test]
    fn pubcomp_round_trip_encode_decode_default311() {
        do_pubcomp_round_trip_encode_decode_default_test(ProtocolVersion::Mqtt311);
    }

    fn do_pubcomp_round_trip_encode_decode_success_rc_no_props_test(protocol_version: ProtocolVersion) {

        let packet = PubcompPacket {
            packet_id: 132,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubcomp(packet), protocol_version));
    }

    #[test]
    fn pubcomp_round_trip_encode_decode_success_rc_no_props5() {
        do_pubcomp_round_trip_encode_decode_success_rc_no_props_test(ProtocolVersion::Mqtt5);
    }

    #[test]
    fn pubcomp_round_trip_encode_decode_success_rc_no_props311() {
        do_pubcomp_round_trip_encode_decode_success_rc_no_props_test(ProtocolVersion::Mqtt311);
    }

    #[test]
    fn pubcomp_round_trip_encode_decode_failure_rc_no_props5() {

        let packet = PubcompPacket {
            packet_id: 4095,
            reason_code: PubcompReasonCode::PacketIdentifierNotFound,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubcomp(packet), ProtocolVersion::Mqtt5));
    }

    #[test]
    fn pubcomp_round_trip_encode_decode_success_rc_with_props5() {

        let packet = PubcompPacket {
            packet_id: 1253,
            reason_code: PubcompReasonCode::Success,
            reason_string: Some("We did it!  High five.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "pubcomp1".to_string(), value: "value1".to_string()},
                UserProperty{name: "pubcomp2".to_string(), value: "value2".to_string()},
                UserProperty{name: "pubcomp2".to_string(), value: "value3".to_string()},
            ))
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubcomp(packet), ProtocolVersion::Mqtt5));
    }

    fn create_pubcomp_with_all_properties() -> PubcompPacket {
        PubcompPacket {
            packet_id: 1500,
            reason_code: PubcompReasonCode::PacketIdentifierNotFound,
            reason_string: Some("I tried so hard, and got so far, but in the end, we totally face-planted".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "uf".to_string(), value: "dah".to_string()},
                UserProperty{name: "velkomen".to_string(), value: "stanwood".to_string()},
            ))
        }
    }

    #[test]
    fn pubcomp_round_trip_encode_decode_failure_rc_with_props5() {

        let packet = create_pubcomp_with_all_properties();

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubcomp(packet), ProtocolVersion::Mqtt5));
    }

    #[test]
    fn pubcomp_round_trip_encode_decode_failure_rc_with_props311() {

        let packet = create_pubcomp_with_all_properties();
        let expected_Packet = PubcompPacket {
            packet_id : packet.packet_id,
            ..Default::default()
        };

        assert!(do_311_filter_encode_decode_test(&MqttPacket::Pubcomp(packet), &MqttPacket::Pubcomp(expected_packet)));
    }

    #[test]
    fn pubcomp_decode_failure_bad_fixed_header5() {
        let packet = create_pubcomp_with_all_properties();

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Pubcomp(packet), ProtocolVersion::Mqtt5, 10);
    }

    #[test]
    fn pubcomp_decode_failure_bad_fixed_header311() {
        let packet = create_pubcomp_with_all_properties();

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Pubcomp(packet), ProtocolVersion::Mqtt311, 10);
    }


    #[test]
    fn pubcomp_decode_failure_bad_reason_code5() {
        let packet = create_pubcomp_with_all_properties();

        let corrupt_reason_code = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // for acks, the reason code is in byte 4
            clone[4] = 232;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Pubcomp(packet), ProtocolVersion::Mqtt5, corrupt_reason_code);
    }

    #[test]
    fn pubcomp_decode_failure_duplicate_reason_string5() {
        let packet = create_pubcomp_with_all_properties();

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

        do_mutated_decode_failure_test(&MqttPacket::Pubcomp(packet), ProtocolVersion::Mqtt5, duplicate_reason_string);
    }

    #[test]
    fn pubcomp_decode_failure_packet_size5() {
        let packet = create_pubcomp_with_all_properties();

        do_inbound_size_decode_failure_test(&MqttPacket::Pubcomp(packet), ProtocolVersion::Mqtt5);
    }

    use crate::validate::testing::*;

    test_ack_validate_success!(pubcomp_validate_success, Pubcomp, create_pubcomp_with_all_properties);
    test_ack_validate_failure_reason_string_length!(pubcomp_validate_failure_reason_string_length, Pubcomp, create_pubcomp_with_all_properties, PacketType::Pubcomp);
    test_ack_validate_failure_invalid_user_properties!(pubcomp_validate_failure_invalid_user_properties, Pubcomp, create_pubcomp_with_all_properties, PacketType::Pubcomp);
    test_ack_validate_failure_outbound_size!(pubcomp_validate_failure_outbound_size5, Pubcomp, create_pubcomp_with_all_properties, PacketType::Pubcomp, ProtocolVersion::Mqtt5);
    test_ack_validate_failure_outbound_size!(pubcomp_validate_failure_outbound_size311, Pubcomp, create_pubcomp_with_all_properties, PacketType::Pubcomp, ProtocolVersion::Mqtt311);
    test_ack_validate_failure_packet_id_zero!(pubcomp_validate_failure_packet_id_zero, Pubcomp, create_pubcomp_with_all_properties, PacketType::Pubcomp);
}