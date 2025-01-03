/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::encode::*;
use crate::error::{GneissError, GneissResult};
use crate::mqtt::*;
use crate::mqtt::utils::*;

use std::collections::VecDeque;
use std::fmt;

#[rustfmt::skip]
pub(crate) fn write_pingreq_encoding_steps(_: &PingreqPacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> GneissResult<()> {
    encode_integral_expression!(steps, Uint8, PACKET_TYPE_PINGREQ << 4);
    encode_integral_expression!(steps, Uint8, 0);

    Ok(())
}

#[cfg(test)]
const PINGREQ_FIRST_BYTE : u8 = PACKET_TYPE_PINGREQ << 4;

#[cfg(test)]
pub(crate) fn decode_pingreq_packet(first_byte: u8, packet_body: &[u8]) -> GneissResult<Box<MqttPacket>> {
    if !packet_body.is_empty() {
        let message = "decode_pingreq_packet - nonzero remaining length";
        error!("{}", message);
        return Err(GneissError::new_decoding_failure(message));
    }

    if first_byte != PINGREQ_FIRST_BYTE {
        let message = "decode_pingreq_packet - invalid first byte";
        error!("{}", message);
        return Err(GneissError::new_decoding_failure(message));
    }

    Ok(Box::new(MqttPacket::Pingreq(PingreqPacket{})))
}

#[cfg(not(test))]
pub(crate) fn decode_pingreq_packet(_: u8, _: &[u8]) -> GneissResult<Box<MqttPacket>> {
    Err(GneissError::new_unimplemented("decode_pingreq_packet - test-only functionality"))
}

impl fmt::Display for PingreqPacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PingreqPacket {{ }}")
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::testing::*;

    #[test]
    fn pingreq_round_trip_encode_decode5() {
        let packet = PingreqPacket {};
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pingreq(packet), ProtocolVersion::Mqtt5));
    }

    #[test]
    fn pingreq_round_trip_encode_decode311() {
        let packet = PingreqPacket {};
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pingreq(packet), ProtocolVersion::Mqtt311));
    }

    #[test]
    fn pingreq_decode_failure_bad_fixed_header5() {
        let packet = PingreqPacket {};
        do_fixed_header_flag_decode_failure_test(&MqttPacket::Pingreq(packet), ProtocolVersion::Mqtt5, 1);
    }

    #[test]
    fn pingreq_decode_failure_bad_fixed_header311() {
        let packet = PingreqPacket {};
        do_fixed_header_flag_decode_failure_test(&MqttPacket::Pingreq(packet), ProtocolVersion::Mqtt311, 1);
    }

    fn do_pingreq_decode_failure_bad_length_test(protocol_version : ProtocolVersion) {
        let packet = PingreqPacket {};

        let extend_length = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase remaining length by 2 and add some garbage bytes
            clone[1] = 2;
            clone.push(5);
            clone.push(6);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Pingreq(packet), protocol_version, extend_length);
    }

    #[test]
    fn pingreq_decode_failure_bad_length5() {
        do_pingreq_decode_failure_bad_length_test(ProtocolVersion::Mqtt5);
    }

    #[test]
    fn pingreq_decode_failure_bad_length311() {
        do_pingreq_decode_failure_bad_length_test(ProtocolVersion::Mqtt311);
    }
}