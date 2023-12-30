/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

pub(crate) mod utils;

extern crate log;

use crate::*;
use crate::alias::*;
use crate::client::*;
use crate::spec::*;
use crate::spec::auth::*;
use crate::spec::connack::*;
use crate::spec::connect::*;
use crate::spec::disconnect::*;
use crate::spec::puback::*;
use crate::spec::pubcomp::*;
use crate::spec::publish::*;
use crate::spec::pubrec::*;
use crate::spec::pubrel::*;
use crate::spec::suback::*;
use crate::spec::subscribe::*;
use crate::spec::unsubscribe::*;
use crate::spec::unsuback::*;
use crate::validate::utils::*;

use log::*;

pub(crate) const MAXIMUM_STRING_PROPERTY_LENGTH : usize = 65535;
pub(crate) const MAXIMUM_BINARY_PROPERTY_LENGTH : usize = 65535;

pub(crate) struct OutboundValidationContext<'a> {

    // Maximum packet size, maximum qos, retained, wildcard, sub ids, shared subs
    pub negotiated_settings : Option<&'a NegotiatedSettings>,

    // session_expiry_interval for disconnect constraints
    pub connect_options: Option<&'a ConnectOptions>,

    pub outbound_alias_resolution: Option<OutboundAliasResolution>
}

pub(crate) struct InboundValidationContext<'a> {

    // Maximum packet size, maximum qos, retained, wildcard, sub ids, shared subs
    pub negotiated_settings : Option<&'a NegotiatedSettings>,
}

fn validate_user_property(property: &UserProperty, error: Mqtt5Error, packet_type: &str) -> Mqtt5Result<()> {
    validate_string_length(&property.name, error, packet_type, "UserProperty Name")?;
    validate_string_length(&property.name, error, packet_type, "UserProperty Value")?;

    Ok(())
}

pub(crate) fn validate_user_properties(properties: &Option<Vec<UserProperty>>, error: Mqtt5Error, packet_type: &str) -> Mqtt5Result<()> {
    if let Some(props) = properties {
        for property in props {
            validate_user_property(property, error, packet_type)?;
        }
    }

    Ok(())
}

/// Validates client-outbound packets against the MQTT5 spec requirements.
///
/// The validation applied here does not take into account any connection-bound state
/// like maximum_qos, maximum_packet_size, etc...  Those constraints are checked in a different
/// function.  This function is called synchronously on submitted packets before even crossing
/// the async boundary into the client implementation.
///
/// Utf-8 codepoints are not currently checked by any validation function.
pub(crate) fn validate_packet_outbound(packet: &MqttPacket) -> Mqtt5Result<()> {
    match packet {
        MqttPacket::Auth(auth) => { validate_auth_packet_outbound(auth) }
        MqttPacket::Connect(connect) => { validate_connect_packet_outbound(connect) }
        MqttPacket::Disconnect(disconnect) => { validate_disconnect_packet_outbound(disconnect) }
        MqttPacket::Pingreq(_) => { Ok(()) }
        MqttPacket::Puback(puback) => { validate_puback_packet_outbound(puback) }
        MqttPacket::Pubcomp(pubcomp) => { validate_pubcomp_packet_outbound(pubcomp) }
        MqttPacket::Publish(publish) => { validate_publish_packet_outbound(publish) }
        MqttPacket::Pubrec(pubrec) => { validate_pubrec_packet_outbound(pubrec) }
        MqttPacket::Pubrel(pubrel) => { validate_pubrel_packet_outbound(pubrel) }
        MqttPacket::Subscribe(subscribe) => { validate_subscribe_packet_outbound(subscribe) }
        MqttPacket::Unsubscribe(unsubscribe) => { validate_unsubscribe_packet_outbound(unsubscribe) }
        _ => {
            error!("Packet Outbound Validation - unexpected packet type");
            Err(Mqtt5Error::ProtocolError)
        }
    }
}

/// Validates outbound packets against per-connection dynamic constraints.  Called internally
/// right before a packet is seated as the current operation of the client.
pub(crate) fn validate_packet_outbound_internal(packet: &MqttPacket, context: &OutboundValidationContext) -> Mqtt5Result<()> {
    match packet {
        MqttPacket::Auth(auth) => { validate_auth_packet_outbound_internal(auth, context) }
        MqttPacket::Connect(_) => { Ok(()) }
        MqttPacket::Disconnect(disconnect) => { validate_disconnect_packet_outbound_internal(disconnect, context) }
        MqttPacket::Pingreq(_) => { Ok(()) }
        MqttPacket::Puback(puback) => { validate_puback_packet_outbound_internal(puback, context) }
        MqttPacket::Pubcomp(pubcomp) => { validate_pubcomp_packet_outbound_internal(pubcomp, context) }
        MqttPacket::Publish(publish) => { validate_publish_packet_outbound_internal(publish, context) }
        MqttPacket::Pubrec(pubrec) => { validate_pubrec_packet_outbound_internal(pubrec, context) }
        MqttPacket::Pubrel(pubrel) => { validate_pubrel_packet_outbound_internal(pubrel, context) }
        MqttPacket::Subscribe(subscribe) => { validate_subscribe_packet_outbound_internal(subscribe, context) }
        MqttPacket::Unsubscribe(unsubscribe) => { validate_unsubscribe_packet_outbound_internal(unsubscribe, context) }
        _ => {
            error!("Packet Outbound Internal Validation - unexpected packet type");
            Err(Mqtt5Error::ProtocolError)
        }
    }
}

/// Validates client-inbound packets against the MQTT5 spec requirements.  Many things can be
/// skipped during inbound validation based on the fact that we assume the decoder created the
/// packet, and so problems like invalid string or binary lengths are impossible.
pub(crate) fn validate_packet_inbound_internal(packet: &MqttPacket, context: &InboundValidationContext) -> Mqtt5Result<()> {
    match packet {
        MqttPacket::Auth(auth) => { validate_auth_packet_inbound_internal(auth, context) }
        MqttPacket::Connack(connack) => { validate_connack_packet_inbound_internal(connack) }
        MqttPacket::Disconnect(disconnect) => { validate_disconnect_packet_inbound_internal(disconnect, context) }
        MqttPacket::Pingresp(_) => { Ok(()) }
        MqttPacket::Puback(puback) => { validate_puback_packet_inbound_internal(puback, context) }
        MqttPacket::Pubcomp(pubcomp) => { validate_pubcomp_packet_inbound_internal(pubcomp, context) }
        MqttPacket::Publish(publish) => { validate_publish_packet_inbound_internal(publish, context) }
        MqttPacket::Pubrec(pubrec) => { validate_pubrec_packet_inbound_internal(pubrec, context) }
        MqttPacket::Pubrel(pubrel) => { validate_pubrel_packet_inbound_internal(pubrel, context) }
        MqttPacket::Suback(suback) => { validate_suback_packet_inbound_internal(suback, context) }
        MqttPacket::Unsuback(unsuback) => { validate_unsuback_packet_inbound_internal(unsuback, context) }
        _ => {
            error!("Packet Inbound Validation - unexpected packet type");
            Err(Mqtt5Error::ProtocolError)
        }
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::encode::utils::MAXIMUM_VARIABLE_LENGTH_INTEGER;

    pub(crate) struct PinnedValidationContext{
        pub settings : NegotiatedSettings,
        pub connect_options : ConnectOptions,
    }

    pub(crate) fn create_pinned_validation_context() -> PinnedValidationContext {
        let mut pinned_context = PinnedValidationContext {
            settings : NegotiatedSettings {..Default::default() },
            connect_options : ConnectOptionsBuilder::new().build(),
        };

        pinned_context.settings.maximum_packet_size_to_server = MAXIMUM_VARIABLE_LENGTH_INTEGER as u32;
        pinned_context.settings.retain_available = true;
        pinned_context.settings.wildcard_subscriptions_available = true;
        pinned_context.settings.shared_subscriptions_available = true;

        pinned_context
    }

    pub(crate) fn create_outbound_validation_context_from_pinned(pinned: &PinnedValidationContext) -> OutboundValidationContext {
        OutboundValidationContext {
            negotiated_settings : Some(&pinned.settings),
            connect_options : Some(&pinned.connect_options),
            outbound_alias_resolution : None,
        }
    }

    pub(crate) fn create_inbound_validation_context_from_pinned(pinned: &PinnedValidationContext) -> InboundValidationContext {
        InboundValidationContext {
            negotiated_settings : Some(&pinned.settings),
        }
    }

    pub(crate) fn create_invalid_user_properties() -> Vec<UserProperty> {
        vec!(
            UserProperty{name: "GoodName".to_string(), value: "badvalue".repeat(20000)},
            UserProperty{name: "badname".repeat(10000), value: "goodvalue".to_string()},
        )
    }

    use crate::decode::testing::*;

    pub(crate) fn do_outbound_size_validate_failure_test(packet: &MqttPacket, error: Mqtt5Error) {
        let encoded_bytes = encode_packet_for_test(packet);

        let mut test_validation_context = create_pinned_validation_context();
        test_validation_context.settings.maximum_qos = QualityOfService::ExactlyOnce;

        let outbound_context1 = create_outbound_validation_context_from_pinned(&test_validation_context);

        assert_eq!(validate_packet_outbound_internal(packet, &outbound_context1), Ok(()));

        test_validation_context.settings.maximum_packet_size_to_server = (encoded_bytes.len() - 1) as u32;

        let outbound_context2 = create_outbound_validation_context_from_pinned(&test_validation_context);

        assert_eq!(validate_packet_outbound_internal(packet, &outbound_context2), Err(error));
    }
}