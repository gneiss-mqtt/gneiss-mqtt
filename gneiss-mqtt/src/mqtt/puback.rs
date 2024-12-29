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
define_ack_packet_lengths_function!(compute_puback_packet_length_properties, PubackPacket, PubackReasonCode);
define_ack_packet_reason_string_accessor!(get_puback_packet_reason_string, Puback);
define_ack_packet_user_property_accessor!(get_puback_packet_user_property, Puback);

#[rustfmt::skip]
define_ack_packet_encoding_impl5!(write_puback_encoding_steps5, PubackPacket, PubackReasonCode, PUBACK_FIRST_BYTE, compute_puback_packet_length_properties, get_puback_packet_reason_string, get_puback_packet_user_property);
define_ack_packet_encoding_impl311!(write_puback_encoding_steps311, PubackPacket, PUBACK_FIRST_BYTE);


define_ack_packet_decode_properties_function!(decode_puback_properties, PubackPacket, "decode_puback_properties");
define_ack_packet_decode_function5!(decode_puback_packet5, Puback, PubackPacket, "decode_puback_packet5", PUBACK_FIRST_BYTE, PubackReasonCode, decode_puback_properties);
define_ack_packet_decode_function311!(decode_puback_packet311, Puback, PubackPacket, "decode_puback_packet311", PUBACK_FIRST_BYTE);

validate_ack_outbound!(validate_puback_packet_outbound, PubackPacket, PacketType::Puback, "validate_puback_packet_outbound");
validate_ack_outbound_internal!(validate_puback_packet_outbound_internal, PubackPacket, PacketType::Puback, compute_puback_packet_length_properties, "validate_puback_packet_outbound_internal");
validate_ack_inbound_internal!(validate_puback_packet_inbound_internal, PubackPacket, PacketType::Puback, "validate_puback_packet_inbound_internal");

define_ack_packet_display_trait!(PubackPacket, "PubackPacket", PubackReasonCode);

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;
    use crate::validate::testing::*;

    #[test]
    fn puback_round_trip_encode_decode_default() {
        let packet = PubackPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Puback(packet)));
    }

    #[test]
    fn puback_round_trip_encode_decode_success_no_props() {

        let packet = PubackPacket {
            packet_id: 123,
            reason_code: PubackReasonCode::Success,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Puback(packet)));
    }

    #[test]
    fn puback_round_trip_encode_decode_failure_no_props() {

        let packet = PubackPacket {
            packet_id: 16384,
            reason_code: PubackReasonCode::NotAuthorized,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Puback(packet)));
    }

    #[test]
    fn puback_round_trip_encode_decode_success_with_props() {

        let packet = PubackPacket {
            packet_id: 1025,
            reason_code: PubackReasonCode::Success,
            reason_string: Some("This was the best publish I've ever seen.  Take a bow.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "puback1".to_string(), value: "value1".to_string()},
                UserProperty{name: "puback2".to_string(), value: "value2".to_string()},
                UserProperty{name: "puback2".to_string(), value: "value3".to_string()},
            ))
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Puback(packet)));
    }

    fn create_puback_with_all_properties() -> PubackPacket {
         PubackPacket {
            packet_id: 1025,
            reason_code: PubackReasonCode::ImplementationSpecificError,
            reason_string: Some("Wow!  What a terrible publish.  You should be ashamed.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "puback1".to_string(), value: "value1".to_string()},
                UserProperty{name: "puback2".to_string(), value: "value2".to_string()},
            ))
        }
    }

    #[test]
    fn puback_round_trip_encode_decode_failure_with_props() {
        let packet = create_puback_with_all_properties();
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Puback(packet)));
    }

    #[test]
    fn puback_decode_failure_bad_fixed_header() {
        let packet = create_puback_with_all_properties();
        do_fixed_header_flag_decode_failure_test(&MqttPacket::Puback(packet), 7);
    }

    #[test]
    fn puback_decode_failure_bad_reason_code() {
        let packet = create_puback_with_all_properties();

        let corrupt_reason_code = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // for acks, the reason code is in byte 4
            clone[4] = 241;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Puback(packet), corrupt_reason_code);
    }

    #[test]
    fn puback_decode_failure_duplicate_reason_string() {
        let packet = create_puback_with_all_properties();

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
            clone.push(68);
            clone.push(68);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Puback(packet), duplicate_reason_string);
    }

    #[test]
    fn puback_decode_failure_packet_size() {
        let packet = create_puback_with_all_properties();

        do_inbound_size_decode_failure_test(&MqttPacket::Puback(packet));
    }

    test_ack_validate_success!(puback_validate_success, Puback, create_puback_with_all_properties);
    test_ack_validate_failure_reason_string_length!(puback_validate_failure_reason_string_length, Puback, create_puback_with_all_properties, PacketType::Puback);
    test_ack_validate_failure_invalid_user_properties!(puback_validate_failure_invalid_user_properties, Puback, create_puback_with_all_properties, PacketType::Puback);
    test_ack_validate_failure_outbound_size!(puback_validate_failure_outbound_size, Puback, create_puback_with_all_properties, PacketType::Puback);
    test_ack_validate_failure_packet_id_zero!(puback_validate_failure_packet_id_zero, Puback, create_puback_with_all_properties, PacketType::Puback);
}