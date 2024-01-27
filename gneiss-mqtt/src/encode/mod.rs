/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

pub(crate) mod utils;

use crate::alias::*;
use crate::encode::utils::*;
use crate::error::{MqttResult};
use crate::logging::*;
use crate::mqtt::*;
use crate::mqtt::auth::*;
use crate::mqtt::connack::*;
use crate::mqtt::connect::*;
use crate::mqtt::disconnect::*;
use crate::mqtt::pingreq::*;
use crate::mqtt::pingresp::*;
use crate::mqtt::puback::*;
use crate::mqtt::pubcomp::*;
use crate::mqtt::publish::*;
use crate::mqtt::pubrec::*;
use crate::mqtt::pubrel::*;
use crate::mqtt::suback::*;
use crate::mqtt::subscribe::*;
use crate::mqtt::unsuback::*;
use crate::mqtt::unsubscribe::*;

use std::collections::VecDeque;

#[derive(Default)]
pub(crate) struct EncodingContext {
    pub outbound_alias_resolution: OutboundAliasResolution,
}

fn write_encoding_steps(mqtt_packet: &MqttPacket, context: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> MqttResult<()> {
    log_packet("Writing encode steps for packet: ", mqtt_packet);

    match mqtt_packet {
        MqttPacket::Connect(packet) => { write_connect_encoding_steps(packet, context, steps) }
        MqttPacket::Connack(packet) => { write_connack_encoding_steps(packet, context, steps) }
        MqttPacket::Publish(packet) => { write_publish_encoding_steps(packet, context, steps) }
        MqttPacket::Puback(packet) => { write_puback_encoding_steps(packet, context, steps) }
        MqttPacket::Pubrec(packet) => { write_pubrec_encoding_steps(packet, context, steps) }
        MqttPacket::Pubrel(packet) => { write_pubrel_encoding_steps(packet, context, steps) }
        MqttPacket::Pubcomp(packet) => { write_pubcomp_encoding_steps(packet, context, steps) }
        MqttPacket::Subscribe(packet) => { write_subscribe_encoding_steps(packet, context, steps) }
        MqttPacket::Suback(packet) => { write_suback_encoding_steps(packet, context, steps) }
        MqttPacket::Unsubscribe(packet) => { write_unsubscribe_encoding_steps(packet, context, steps) }
        MqttPacket::Unsuback(packet) => { write_unsuback_encoding_steps(packet, context, steps) }
        MqttPacket::Pingreq(packet) => { write_pingreq_encoding_steps(packet, context, steps) }
        MqttPacket::Pingresp(packet) => {  write_pingresp_encoding_steps(packet, context, steps) }
        MqttPacket::Disconnect(packet) => { write_disconnect_encoding_steps(packet, context, steps) }
        MqttPacket::Auth(packet) => { write_auth_encoding_steps(packet, context, steps) }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum EncodeResult {
    Complete,
    Full,
}

pub(crate) struct Encoder {
    steps: VecDeque<EncodingStep>,
}

impl Encoder {
    pub fn new() -> Encoder {
        Encoder {
            steps: VecDeque::new(),
        }
    }

    pub fn reset(&mut self, packet: &MqttPacket, context: &EncodingContext) -> MqttResult<()> {
        self.steps.clear();

        write_encoding_steps(packet, context, &mut self.steps)
    }

    pub fn encode(
        &mut self,
        packet: &MqttPacket,
        dest: &mut Vec<u8>,
    ) -> MqttResult<EncodeResult> {
        let capacity = dest.capacity();
        if capacity < 4 {
            panic!("Encoder - target buffer too small");
        }

        while !self.steps.is_empty() && dest.len() + 4 <= dest.capacity() {
            let step = self.steps.pop_front().unwrap();
            process_encoding_step(&mut self.steps, step, packet, dest)?;
        }

        if capacity != dest.capacity() {
            panic!("Internal error: encoding logic resized dest buffer");
        }

        if self.steps.is_empty() {
            Ok(EncodeResult::Complete)
        } else {
            Ok(EncodeResult::Full)
        }
    }
}
