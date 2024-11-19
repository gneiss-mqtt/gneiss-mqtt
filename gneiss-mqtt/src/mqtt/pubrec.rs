/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::decode::*;
use crate::encode::*;
use crate::error::{MqttError, GneissResult};
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
define_ack_packet_encoding_impl!(write_pubrec_encoding_steps, PubrecPacket, PubrecReasonCode, PUBREC_FIRST_BYTE, compute_pubrec_packet_length_properties, get_pubrec_packet_reason_string, get_pubrec_packet_user_property);

define_ack_packet_decode_properties_function!(decode_pubrec_properties, PubrecPacket, "Pubrec");
define_ack_packet_decode_function!(decode_pubrec_packet, Pubrec, PubrecPacket, "Pubrec", PUBREC_FIRST_BYTE, convert_u8_to_pubrec_reason_code, decode_pubrec_properties);

validate_ack_outbound!(validate_pubrec_packet_outbound, PubrecPacket, PacketType::Pubrec, "Pubrec");
validate_ack_outbound_internal!(validate_pubrec_packet_outbound_internal, PubrecPacket, PacketType::Pubrec, compute_pubrec_packet_length_properties, "Pubrec");
validate_ack_inbound_internal!(validate_pubrec_packet_inbound_internal, PubrecPacket, PacketType::Pubrec, "Pubrec");

define_ack_packet_display_trait!(PubrecPacket, "PubrecPacket", pubrec_reason_code_to_str);

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    #[test]
    fn pubrec_round_trip_encode_decode_default() {
        let packet = PubrecPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet)));
    }

    #[test]
    fn pubrec_round_trip_encode_decode_success_no_props() {

        let packet = PubrecPacket {
            packet_id: 1234,
            reason_code: PubrecReasonCode::Success,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet)));
    }

    #[test]
    fn pubrec_round_trip_encode_decode_failure_no_props() {

        let packet = PubrecPacket {
            packet_id: 8191,
            reason_code: PubrecReasonCode::PacketIdentifierInUse,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet)));
    }

    #[test]
    fn pubrec_round_trip_encode_decode_success_with_props() {

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

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet)));
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
    fn pubrec_round_trip_encode_decode_failure_with_props() {

        let packet = create_pubrec_with_all_properties();

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pubrec(packet)));
    }

    #[test]
    fn pubrec_decode_failure_bad_fixed_header() {
        let packet = create_pubrec_with_all_properties();

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Pubrec(packet), 12);
    }

    #[test]
    fn pubrec_decode_failure_bad_reason_code() {
        let packet = create_pubrec_with_all_properties();

        let corrupt_reason_code = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // for acks, the reason code is in byte 4
            clone[4] = 3;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Pubrec(packet), corrupt_reason_code);
    }

    #[test]
    fn pubrec_decode_failure_duplicate_reason_string() {
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

        do_mutated_decode_failure_test(&MqttPacket::Pubrec(packet), duplicate_reason_string);
    }

    #[test]
    fn pubrec_decode_failure_packet_size() {
        let packet = create_pubrec_with_all_properties();

        do_inbound_size_decode_failure_test(&MqttPacket::Pubrec(packet));
    }

    use crate::validate::testing::*;

    test_ack_validate_success!(pubrec_validate_success, Pubrec, create_pubrec_with_all_properties);
    test_ack_validate_failure_reason_string_length!(pubrec_validate_failure_reason_string_length, Pubrec, create_pubrec_with_all_properties, PacketType::Pubrec);
    test_ack_validate_failure_invalid_user_properties!(pubrec_validate_failure_invalid_user_properties, Pubrec, create_pubrec_with_all_properties, PacketType::Pubrec);
    test_ack_validate_failure_outbound_size!(pubrec_validate_failure_outbound_size, Pubrec, create_pubrec_with_all_properties, PacketType::Pubrec);
    test_ack_validate_failure_packet_id_zero!(pubrec_validate_failure_packet_id_zero, Pubrec, create_pubrec_with_all_properties, PacketType::Pubrec);
}