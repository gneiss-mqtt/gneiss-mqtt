/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate log;

use crate::*;
use crate::encode::*;
use crate::encode::utils::*;
use crate::spec::*;
use crate::spec::utils::*;

use log::*;
use std::collections::VecDeque;
use std::fmt;

/// Data model of an [MQTT5 PINGRESP](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901200) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PingrespPacket {}

#[rustfmt::skip]
pub(crate) fn write_pingresp_encoding_steps(_: &PingrespPacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<()> {
    encode_integral_expression!(steps, Uint8, PACKET_TYPE_PINGRESP << 4);
    encode_integral_expression!(steps, Uint8, 0);

    Ok(())
}

const PINGRESP_FIRST_BYTE : u8 = PACKET_TYPE_PINGRESP << 4;

pub(crate) fn decode_pingresp_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<Box<MqttPacket>> {
    if !packet_body.is_empty() {
        error!("Packet Decode - Pingresp packet with non-zero remaining length");
        return Err(Mqtt5Error::MalformedPacket);
    }

    if first_byte != PINGRESP_FIRST_BYTE {
        error!("Packet Decode - Pingresp packet with invalid first byte");
        return Err(Mqtt5Error::MalformedPacket);
    }

    Ok(Box::new(MqttPacket::Pingresp(PingrespPacket{})))
}

impl fmt::Display for PingrespPacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PingrespPacket {{}}")
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;

    #[test]
    fn pingresp_round_trip_encode_decode() {
        let packet = PingrespPacket {};
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Pingresp(packet)));
    }

    #[test]
    fn pingresp_decode_failure_bad_fixed_header() {
        let packet = PingrespPacket {};

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Pingresp(packet), 2);
    }

    #[test]
    fn pingresp_decode_failure_bad_length() {
        let packet = PingrespPacket {};

        let extend_length = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // for this packet, the reason code is in byte 2
            clone[1] = 4;
            clone.push(1);
            clone.push(2);
            clone.push(5);
            clone.push(6);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Pingresp(packet), extend_length);
    }
}