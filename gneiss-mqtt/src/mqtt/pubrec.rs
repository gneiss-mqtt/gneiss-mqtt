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
define_ack_packet_lengths_function!(compute_pubrec_packet_length_properties, PubrecPacket, PubrecReasonCode);
define_ack_packet_reason_string_accessor!(get_pubrec_packet_reason_string, Pubrec);
define_ack_packet_user_property_accessor!(get_pubrec_packet_user_property, Pubrec);

#[rustfmt::skip]
define_ack_packet_encoding_impl5!(write_pubrec_encoding_steps5, PubrecPacket, PubrecReasonCode, PUBREC_FIRST_BYTE, compute_pubrec_packet_length_properties, get_pubrec_packet_reason_string, get_pubrec_packet_user_property);
define_ack_packet_encoding_impl311!(write_pubrec_encoding_steps311, PubrecPacket, PUBREC_FIRST_BYTE);

define_ack_packet_decode_properties_function!(decode_pubrec_properties, PubrecPacket, "decode_pubrec_properties");
define_ack_packet_decode_function5!(decode_pubrec_packet5, Pubrec, PubrecPacket, "decode_pubrec_packet5", PUBREC_FIRST_BYTE, PubrecReasonCode, decode_pubrec_properties);
define_ack_packet_decode_function311!(decode_pubrec_packet311, Pubrec, PubrecPacket, "decode_pubrec_packet311", PUBREC_FIRST_BYTE);

validate_ack_outbound!(validate_pubrec_packet_outbound, PubrecPacket, PacketType::Pubrec, "validate_pubrec_packet_outbound");
validate_ack_outbound_internal!(validate_pubrec_packet_outbound_internal, PubrecPacket, PacketType::Pubrec, compute_pubrec_packet_length_properties, "validate_pubrec_packet_outbound_internal");
validate_ack_inbound_internal!(validate_pubrec_packet_inbound_internal, PubrecPacket, PacketType::Pubrec, "validate_pubrec_packet_inbound_internal");

define_ack_packet_display_trait!(PubrecPacket, "PubrecPacket", PubrecReasonCode);

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    fn do_pubrec_round_trip_encode_decode_default_test(protocol_version: ProtocolVersion) {
        let packet = PubrecPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet), protocol_version));
    }

    #[test]
    fn pubrec_round_trip_encode_decode_default5() {
        do_pubrec_round_trip_encode_decode_default_test(ProtocolVersion::Mqtt5);
    }

    #[test]
    fn pubrec_round_trip_encode_decode_default311() {
        do_pubrec_round_trip_encode_decode_default_test(ProtocolVersion::Mqtt311);
    }

    fn do_pubrec_round_trip_encode_decode_success_rc_no_props_test(protocol_version: ProtocolVersion) {

        let packet = PubrecPacket {
            packet_id: 1234,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet), protocol_version));
    }

    #[test]
    fn pubrec_round_trip_encode_decode_success_rc_no_props5() {
        do_pubrec_round_trip_encode_decode_success_rc_no_props_test(ProtocolVersion::Mqtt5);
    }

    #[test]
    fn pubrec_round_trip_encode_decode_success_rc_no_props311() {
        do_pubrec_round_trip_encode_decode_success_rc_no_props_test(ProtocolVersion::Mqtt311);
    }

    #[test]
    fn pubrec_round_trip_encode_decode_failure_rc_no_props5() {

        let packet = PubrecPacket {
            packet_id: 8191,
            reason_code: PubrecReasonCode::PacketIdentifierInUse,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet), ProtocolVersion::Mqtt5));
    }

    #[test]
    fn pubrec_round_trip_encode_decode_success_rc_with_props5() {

        let packet = PubrecPacket {
            packet_id: 10253,
            reason_code: PubrecReasonCode::Success,
            reason_string: Some("Whoa, qos2.  Brave and inspired.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "pubrec1".to_string(), value: "value1".to_string()},
                UserProperty{name: "pubrec2".to_string(), value: "value2".to_string()},
                UserProperty{name: "pubrec2".to_string(), value: "value3".to_string()},
            ))
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet), ProtocolVersion::Mqtt5));
    }

    fn create_pubrec_with_all_properties() -> PubrecPacket {
        PubrecPacket {
            packet_id: 125,
            reason_code: PubrecReasonCode::UnspecifiedError,
            reason_string: Some("Qos2?  Get that nonsense outta here.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "pubwreck1".to_string(), value: "krabbypatty".to_string()},
                UserProperty{name: "pubwreck2".to_string(), value: "spongebob".to_string()},
            ))
        }
    }

    #[test]
    fn pubrec_round_trip_encode_decode_failure_with_props5() {
        let packet = create_pubrec_with_all_properties();

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet), ProtocolVersion::Mqtt5));
    }

    #[test]
    fn pubrec_round_trip_encode_decode_failure_with_props311() {
        let packet = create_pubrec_with_all_properties();
        let expected_packet = PubrecPacket {
            packet_id : packet.packet_id,
            ..Default::default()
        };

        assert!(do_311_filter_encode_decode_test(&MqttPacket::Pubrec(packet), &MqttPacket::Pubrec(expected_packet)));
    }

    #[test]
    fn pubrec_decode_failure_bad_fixed_header5() {
        let packet = create_pubrec_with_all_properties();
        do_fixed_header_flag_decode_failure_test(&MqttPacket::Pubrec(packet), ProtocolVersion::Mqtt5, 12);
    }

    #[test]
    fn pubrec_decode_failure_bad_fixed_header311() {
        let packet = create_pubrec_with_all_properties();
        do_fixed_header_flag_decode_failure_test(&MqttPacket::Pubrec(packet), ProtocolVersion::Mqtt311, 12);
    }

    #[test]
    fn pubrec_decode_failure_bad_reason_code5() {
        let packet = create_pubrec_with_all_properties();

        let corrupt_reason_code = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // for acks, the reason code is in byte 4
            clone[4] = 3;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Pubrec(packet), ProtocolVersion::Mqtt5, corrupt_reason_code);
    }

    #[test]
    fn pubrec_decode_failure_duplicate_reason_string5() {
        let packet = create_pubrec_with_all_properties();

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

        do_mutated_decode_failure_test(&MqttPacket::Pubrec(packet), ProtocolVersion::Mqtt5, duplicate_reason_string);
    }

    #[test]
    fn pubrec_decode_failure_packet_size() {
        let packet = create_pubrec_with_all_properties();

        do_inbound_size_decode_failure_test(&MqttPacket::Pubrec(packet), ProtocolVersion::Mqtt5);
    }

    use crate::validate::testing::*;

    test_ack_validate_success!(pubrec_validate_success, Pubrec, create_pubrec_with_all_properties);
    test_ack_validate_failure_reason_string_length!(pubrec_validate_failure_reason_string_length, Pubrec, create_pubrec_with_all_properties, PacketType::Pubrec);
    test_ack_validate_failure_invalid_user_properties!(pubrec_validate_failure_invalid_user_properties, Pubrec, create_pubrec_with_all_properties, PacketType::Pubrec);
    test_ack_validate_failure_outbound_size!(pubrec_validate_failure_outbound_size5, Pubrec, create_pubrec_with_all_properties, PacketType::Pubrec, ProtocolVersion::Mqtt5);
    test_ack_validate_failure_outbound_size!(pubrec_validate_failure_outbound_size311, Pubrec, create_pubrec_with_all_properties, PacketType::Pubrec, ProtocolVersion::Mqtt311);
    test_ack_validate_failure_packet_id_zero!(pubrec_validate_failure_packet_id_zero, Pubrec, create_pubrec_with_all_properties, PacketType::Pubrec);
}