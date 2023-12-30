/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

pub(crate) mod utils;

extern crate log;

use crate::*;
use crate::decode::utils::*;
use crate::encode::utils::*;
use crate::logging::*;
use crate::spec::*;
use crate::spec::utils::*;

use crate::spec::auth::*;
use crate::spec::connack::*;
use crate::spec::connect::*;
use crate::spec::disconnect::*;
use crate::spec::pingreq::*;
use crate::spec::pingresp::*;
use crate::spec::puback::*;
use crate::spec::pubcomp::*;
use crate::spec::publish::*;
use crate::spec::pubrec::*;
use crate::spec::pubrel::*;
use crate::spec::suback::*;
use crate::spec::subscribe::*;
use crate::spec::unsuback::*;
use crate::spec::unsubscribe::*;

use log::*;

use std::collections::*;

const DECODE_BUFFER_DEFAULT_SIZE : usize = 16 * 1024;

#[derive(Copy, Clone, Eq, PartialEq)]
enum DecoderState {
    ReadPacketType,
    ReadTotalRemainingLength,
    ReadPacketBody,
    TerminalError
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum DecoderDirective {
    OutOfData,
    Continue,
    TerminalError
}

pub(crate) struct DecodingContext<'a> {
    pub(crate) maximum_packet_size : u32,

    pub(crate) decoded_packets: &'a mut VecDeque<Box<MqttPacket>>
}

pub(crate) struct Decoder {
    state: DecoderState,

    scratch: Vec<u8>,

    first_byte: Option<u8>,

    remaining_length : Option<usize>,
}

fn decode_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<Box<MqttPacket>> {
    let packet_type = first_byte >> 4;

    info!("Decoding a packet of type {}", packet_type_to_str(packet_type));

    match packet_type {
        PACKET_TYPE_CONNECT => { decode_connect_packet(first_byte, packet_body) }
        PACKET_TYPE_CONNACK => { decode_connack_packet(first_byte, packet_body) }
        PACKET_TYPE_PUBLISH => { decode_publish_packet(first_byte, packet_body) }
        PACKET_TYPE_PUBACK => { decode_puback_packet(first_byte, packet_body) }
        PACKET_TYPE_PUBREC => { decode_pubrec_packet(first_byte, packet_body) }
        PACKET_TYPE_PUBREL => { decode_pubrel_packet(first_byte, packet_body) }
        PACKET_TYPE_PUBCOMP => { decode_pubcomp_packet(first_byte, packet_body) }
        PACKET_TYPE_SUBSCRIBE => { decode_subscribe_packet(first_byte, packet_body) }
        PACKET_TYPE_SUBACK => { decode_suback_packet(first_byte, packet_body) }
        PACKET_TYPE_UNSUBSCRIBE => { decode_unsubscribe_packet(first_byte, packet_body) }
        PACKET_TYPE_UNSUBACK => { decode_unsuback_packet(first_byte, packet_body) }
        PACKET_TYPE_PINGREQ => { decode_pingreq_packet(first_byte, packet_body) }
        PACKET_TYPE_PINGRESP => { decode_pingresp_packet(first_byte, packet_body) }
        PACKET_TYPE_DISCONNECT => { decode_disconnect_packet(first_byte, packet_body) }
        PACKET_TYPE_AUTH => { decode_auth_packet(first_byte, packet_body) }
        _ => {
            Err(Mqtt5Error::MalformedPacket)
        }
    }
}

impl Decoder {
    pub fn new() -> Decoder {
        Decoder {
            state: DecoderState::ReadPacketType,
            scratch : Vec::<u8>::with_capacity(DECODE_BUFFER_DEFAULT_SIZE),
            first_byte : None,
            remaining_length : None,
        }
    }

    pub fn reset_for_new_connection(&mut self) {
        self.reset();
    }

    fn process_read_packet_type<'a>(&mut self, bytes: &'a [u8]) -> (DecoderDirective, &'a[u8]) {
        if bytes.is_empty() {
            return (DecoderDirective::OutOfData, bytes);
        }

        self.first_byte = Some(bytes[0]);
        self.state = DecoderState::ReadTotalRemainingLength;

        (DecoderDirective::Continue, &bytes[1..])
    }

    fn process_read_total_remaining_length<'a>(&mut self, bytes: &'a[u8], context: &DecodingContext) -> (DecoderDirective, &'a[u8]) {
        if bytes.is_empty() {
            return (DecoderDirective::OutOfData, bytes);
        }

        self.scratch.push(bytes[0]);
        let remaining_bytes = &bytes[1..];

        let decode_vli_result = decode_vli(&self.scratch);
        if let Ok(DecodeVliResult::Value(remaining_length, _)) = decode_vli_result {
            let mut maximum_size = context.maximum_packet_size;
            if maximum_size == 0 {
                maximum_size = MAXIMUM_VARIABLE_LENGTH_INTEGER as u32;
            }

            let total_packet_size = remaining_length + 1 + self.scratch.len() as u32;
            if total_packet_size <= maximum_size {
                self.remaining_length = Some(remaining_length as usize);
                self.state = DecoderState::ReadPacketBody;
                self.scratch.clear();
                (DecoderDirective::Continue, remaining_bytes)
            } else {
                (DecoderDirective::TerminalError, remaining_bytes)
            }
        } else if self.scratch.len() >= 4 {
            (DecoderDirective::TerminalError, remaining_bytes)
        } else if !remaining_bytes.is_empty() {
            (DecoderDirective::Continue, remaining_bytes)
        } else {
            (DecoderDirective::OutOfData, remaining_bytes)
        }
    }

    fn process_read_packet_body<'a>(&mut self, bytes: &'a[u8], context: &mut DecodingContext) -> (DecoderDirective, &'a[u8]) {
        let read_so_far = self.scratch.len();
        let bytes_needed = self.remaining_length.unwrap() - read_so_far;
        if bytes_needed > bytes.len() {
            self.scratch.extend_from_slice(bytes);
            return (DecoderDirective::OutOfData, &[]);
        }

        let packet_slice : &[u8] =
            if !self.scratch.is_empty() {
                self.scratch.extend_from_slice(&bytes[..bytes_needed]);
                &self.scratch
            } else {
                &bytes[..bytes_needed]
            };

        if let Ok(packet) = decode_packet(self.first_byte.unwrap(), packet_slice) {
            log_packet("Successfully decoded incoming packet: ", &packet);
            context.decoded_packets.push_back(packet);

            self.reset_for_new_packet();
            return (DecoderDirective::Continue, &bytes[bytes_needed..]);
        }

        (DecoderDirective::TerminalError, &[])
    }

    pub fn decode_bytes(&mut self, bytes: &[u8], context: &mut DecodingContext) -> Mqtt5Result<()> {
        let mut current_slice = bytes;

        let mut decode_result = DecoderDirective::Continue;
        while decode_result == DecoderDirective::Continue {
            match self.state {
                DecoderState::ReadPacketType => {
                    (decode_result, current_slice) = self.process_read_packet_type(current_slice);
                }

                DecoderState::ReadTotalRemainingLength => {
                    (decode_result, current_slice) = self.process_read_total_remaining_length(current_slice, context);
                }

                DecoderState::ReadPacketBody => {
                    (decode_result, current_slice) = self.process_read_packet_body(current_slice, context);
                }

                _ => {
                    decode_result = DecoderDirective::TerminalError;
                }
            }
        }

        if decode_result == DecoderDirective::TerminalError {
            self.state = DecoderState::TerminalError;
            return Err(Mqtt5Error::MalformedPacket);
        }

        Ok(())
    }

    fn reset_for_new_packet(&mut self) {
        if self.state != DecoderState::TerminalError {
            self.reset();
        }
    }

    fn reset(&mut self) {
        self.state = DecoderState::ReadPacketType;
        self.scratch.clear();
        self.first_byte = None;
        self.remaining_length = None;
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::alias::*;
    use crate::encode::*;

    pub(crate) fn do_single_encode_decode_test(packet : &MqttPacket, encode_size : usize, decode_size : usize, encode_repetitions : u32) -> bool {

        let mut encoder = Encoder::new();

        let mut full_encoded_stream = Vec::with_capacity( 128 * 1024);
        let mut encode_buffer = Vec::with_capacity(encode_size);

        let mut outbound_resolver : Box<dyn OutboundAliasResolver> = Box::new(ManualOutboundAliasResolver::new(65535));

        /* encode 5 copies of the packet */
        for _ in 0..encode_repetitions {
            let mut encoding_context = EncodingContext {
                ..Default::default()
            };

            if let MqttPacket::Publish(publish) = &packet {
                encoding_context.outbound_alias_resolution = outbound_resolver.resolve_and_apply_topic_alias(&publish.topic_alias, &publish.topic);
            }

            assert!(!encoder.reset(&packet, &encoding_context).is_err());

            let mut cumulative_result : EncodeResult = EncodeResult::Full;
            while cumulative_result == EncodeResult::Full {
                encode_buffer.clear();
                let encode_result = encoder.encode(packet, &mut encode_buffer);
                if let Err(_) = encode_result {
                    break;
                }

                cumulative_result = encode_result.unwrap();
                full_encoded_stream.extend_from_slice(encode_buffer.as_slice());
            }

            assert_eq!(cumulative_result, EncodeResult::Complete);
        }

        let mut decoder = Decoder::new();
        decoder.reset_for_new_connection();

        let mut decoded_packets : VecDeque<Box<MqttPacket>> = VecDeque::new();

        let mut decoding_context = DecodingContext {
            maximum_packet_size: MAXIMUM_VARIABLE_LENGTH_INTEGER as u32,
            decoded_packets: &mut decoded_packets
        };

        let mut decode_stream_slice = full_encoded_stream.as_slice();
        while decode_stream_slice.len() > 0 {
            let fragment_size : usize = usize::min(decode_size, decode_stream_slice.len());
            let decode_slice = &decode_stream_slice[..fragment_size];
            decode_stream_slice = &decode_stream_slice[fragment_size..];

            let decode_result = decoder.decode_bytes(decode_slice, &mut decoding_context);
            assert!(!decode_result.is_err());
        }

        let mut matching_packets : u32 = 0;

        let mut inbound_alias_resolver = InboundAliasResolver::new(65535);

        for mut received_packet in decoded_packets {
            matching_packets += 1;

            if let MqttPacket::Publish(publish) = &mut(*received_packet) {
                if let Err(_) = inbound_alias_resolver.resolve_topic_alias(&publish.topic_alias, &mut publish.topic) {
                    return false;
                }
            }

            assert_eq!(*packet, *received_packet);
        }

        assert_eq!(encode_repetitions, matching_packets);

        return true;
    }

    pub(crate) fn do_round_trip_encode_decode_test(packet : &MqttPacket) -> bool {
        let encode_buffer_sizes : Vec<usize> = vec!(4, 5, 7, 11, 17, 31, 47, 71, 131);
        let decode_fragment_sizes : Vec<usize> = vec!(1, 2, 3, 5, 7, 11, 17, 31, 47, 71, 131, 1023);

        for encode_size in encode_buffer_sizes.iter() {
            for decode_size in decode_fragment_sizes.iter() {
                assert!(do_single_encode_decode_test(&packet, *encode_size, *decode_size, 5));
            }
        }

        return true;
    }

    pub(crate) fn encode_packet_for_test(packet: &MqttPacket) -> Vec<u8> {
        let mut encoder = Encoder::new();

        let mut encoded_buffer = Vec::with_capacity( 128 * 1024);

        let mut encoding_context = EncodingContext { ..Default::default() };

        assert!(!encoder.reset(&packet, &mut encoding_context).is_err());

        let encode_result = encoder.encode(packet, &mut encoded_buffer);
        assert_eq!(encode_result, Ok(EncodeResult::Complete));

        encoded_buffer
    }

    /*
     * verifies that the packet encodes/decodes correctly, but applying the supplied mutator
     * to the encoding leads to a decode failure.  Useful to verify specification requirements
     * with respect to decode failures like reserved bits, headers, duplicate properties, etc...
     */
    pub(crate) fn do_mutated_decode_failure_test<F>(packet: &MqttPacket, mutator: F ) where F : Fn(&[u8]) -> Vec<u8> {
        let good_encoded_bytes = encode_packet_for_test(packet);

        let mut decoder = Decoder::new();
        decoder.reset_for_new_connection();

        let mut decoded_packets : VecDeque<Box<MqttPacket>> = VecDeque::new();

        let mut decoding_context = DecodingContext {
            maximum_packet_size: MAXIMUM_VARIABLE_LENGTH_INTEGER as u32,
            decoded_packets: &mut decoded_packets
        };

        let decode_result = decoder.decode_bytes(good_encoded_bytes.as_slice(), &mut decoding_context);
        assert_eq!(decode_result, Ok(()));
        assert_eq!(1, decoded_packets.len());

        let receive_result = &decoded_packets[0];
        assert_eq!(*packet, **receive_result);

        let bad_encoded_bytes = mutator(good_encoded_bytes.as_slice());

        assert_ne!(good_encoded_bytes.as_slice(), bad_encoded_bytes.as_slice());

        // verify that the packet now fails to decode
        decoded_packets.clear();
        decoder.reset_for_new_connection();

        let mut decoding_context = DecodingContext {
            maximum_packet_size: MAXIMUM_VARIABLE_LENGTH_INTEGER as u32,
            decoded_packets: &mut decoded_packets
        };

        let decode_result = decoder.decode_bytes(bad_encoded_bytes.as_slice(), &mut decoding_context);
        assert_eq!(decode_result, Err(Mqtt5Error::MalformedPacket));
        assert_eq!(0, decoded_packets.len());
    }

    pub(crate) fn do_inbound_size_decode_failure_test(packet: &MqttPacket) {
        let encoded_bytes = encode_packet_for_test(packet);

        let mut decoder = Decoder::new();
        decoder.reset_for_new_connection();

        let mut decoded_packets : VecDeque<Box<MqttPacket>> = VecDeque::new();

        let mut decoding_context = DecodingContext {
            maximum_packet_size: MAXIMUM_VARIABLE_LENGTH_INTEGER as u32,
            decoded_packets: &mut decoded_packets
        };

        let decode_result = decoder.decode_bytes(encoded_bytes.as_slice(), &mut decoding_context);
        assert_eq!(decode_result, Ok(()));
        assert_eq!(1, decoded_packets.len());

        let receive_result = &decoded_packets[0];
        assert_eq!(*packet, **receive_result);

        decoded_packets.clear();

        // verify that the packet now fails to decode
        decoder.reset_for_new_connection();

        let mut decoding_context = DecodingContext {
            maximum_packet_size: (encoded_bytes.len() - 1) as u32,
            decoded_packets: &mut decoded_packets
        };

        let decode_result = decoder.decode_bytes(encoded_bytes.as_slice(), &mut decoding_context);
        assert_eq!(decode_result, Err(Mqtt5Error::MalformedPacket));
        assert_eq!(0, decoded_packets.len());
    }

    pub(crate) fn do_fixed_header_flag_decode_failure_test(packet: &MqttPacket, flags_mask: u8) {
        let reserved_mutator = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();
            clone[0] |= flags_mask;
            clone
        };

        do_mutated_decode_failure_test(packet, reserved_mutator);
    }
}