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
        error!("Packet Decode - Pingreq packet with non-zero remaining length");
        return Err(GneissError::new_decoding_failure("nonzero remaining length for pingreq packet"));
    }

    if first_byte != PINGREQ_FIRST_BYTE {
        error!("Packet Decode - Pingreq packet with invalid first byte");
        return Err(GneissError::new_decoding_failure("invalid first byte for pingreq packet"));
    }

    Ok(Box::new(MqttPacket::Pingreq(PingreqPacket{})))
}

#[cfg(not(test))]
pub(crate) fn decode_pingreq_packet(_: u8, _: &[u8]) -> GneissResult<Box<MqttPacket>> {
    Err(GneissError::new_unimplemented("Test-only functionality"))
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
    fn pingreq_round_trip_encode_decode() {
        let packet = PingreqPacket {};
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pingreq(packet)));
    }

    #[test]
    fn pingreq_decode_failure_bad_fixed_header() {
        let packet = PingreqPacket {};

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Pingreq(packet), 1);
    }

    #[test]
    fn pingreq_decode_failure_bad_length() {
        let packet = PingreqPacket {};

        let extend_length = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // for this packet, the reason code is in byte 2
            clone[1] = 2;
            clone.push(5);
            clone.push(6);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Pingreq(packet), extend_length);
    }

}