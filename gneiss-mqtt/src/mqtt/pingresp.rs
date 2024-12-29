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
#[cfg(test)]
pub(crate) fn write_pingresp_encoding_steps(_: &PingrespPacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> GneissResult<()> {
    encode_integral_expression!(steps, Uint8, PACKET_TYPE_PINGRESP << 4);
    encode_integral_expression!(steps, Uint8, 0);

    Ok(())
}

#[cfg(not(test))]
pub(crate) fn write_pingresp_encoding_steps(_: &PingrespPacket, _: &EncodingContext, _: &mut VecDeque<EncodingStep>) -> GneissResult<()> {
    Err(GneissError::new_unimplemented("write_pingresp_encoding_steps - test-only functionality"))
}

const PINGRESP_FIRST_BYTE : u8 = PACKET_TYPE_PINGRESP << 4;

pub(crate) fn decode_pingresp_packet(first_byte: u8, packet_body: &[u8]) -> GneissResult<Box<MqttPacket>> {
    if !packet_body.is_empty() {
        let message = "decode_pingresp_packet - non-zero remaining length";
        error!("{}", message);
        return Err(GneissError::new_decoding_failure(message));
    }

    if first_byte != PINGRESP_FIRST_BYTE {
        let message = "decode_pingresp_packet - invalid first byte";
        error!("{}", message);
        return Err(GneissError::new_decoding_failure(message));
    }

    Ok(Box::new(MqttPacket::Pingresp(PingrespPacket{})))
}

impl fmt::Display for PingrespPacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PingrespPacket {{ }}")
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    #[test]
    fn pingresp_round_trip_encode_decode5() {
        let packet = PingrespPacket {};
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pingresp(packet), ProtocolVersion::Mqtt5));
    }

    #[test]
    fn pingresp_round_trip_encode_decode311() {
        let packet = PingrespPacket {};
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pingresp(packet), ProtocolVersion::Mqtt311));
    }

    #[test]
    fn pingresp_decode_failure_bad_fixed_header5() {
        let packet = PingrespPacket {};

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Pingresp(packet), ProtocolVersion::Mqtt5, 2);
    }

    #[test]
    fn pingresp_decode_failure_bad_fixed_header311() {
        let packet = PingrespPacket {};

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Pingresp(packet), ProtocolVersion::Mqtt311, 2);
    }

    fn do_pingresp_decode_failure_bad_length_test(protocol_version: ProtocolVersion) {
        let packet = PingrespPacket {};

        let extend_length = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // extend the length and add an appropriate amount of garbage bytes
            clone[1] = 4;
            clone.push(1);
            clone.push(2);
            clone.push(5);
            clone.push(6);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Pingresp(packet), protocol_version, extend_length);
    }

    #[test]
    fn pingresp_decode_failure_bad_length5() {
        do_pingresp_decode_failure_bad_length_test(ProtocolVersion::Mqtt5);
    }

    #[test]
    fn pingresp_decode_failure_bad_length311() {
        do_pingresp_decode_failure_bad_length_test(ProtocolVersion::Mqtt311);
    }
}