/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::mqtt::PacketType;
use crate::alias::*;
use crate::client::*;
use crate::client::config::*;
use crate::error::{GneissError, GneissResult};
use crate::mqtt::*;
use crate::mqtt::auth::*;
use crate::mqtt::connack::*;
use crate::mqtt::connect::*;
use crate::mqtt::disconnect::*;
use crate::mqtt::puback::*;
use crate::mqtt::pubcomp::*;
use crate::mqtt::publish::*;
use crate::mqtt::pubrec::*;
use crate::mqtt::pubrel::*;
use crate::mqtt::suback::*;
use crate::mqtt::subscribe::*;
use crate::mqtt::unsubscribe::*;
use crate::mqtt::unsuback::*;

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

pub(crate) fn validate_user_properties(properties: &Option<Vec<UserProperty>>, packet_type: PacketType, function_name: &str) -> GneissResult<()> {
    if let Some(props) = properties {
        for property in props {
            validate_string_length(property.name.as_str(), packet_type, function_name, "UserProperty Name")?;
            validate_string_length(property.name.as_str(), packet_type, function_name, "UserProperty Value")?;
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
pub(crate) fn validate_packet_outbound(packet: &MqttPacket) -> GneissResult<()> {
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
            let message = "validate_packet_outbound - unexpected packet type";
            error!("{}", message);
            Err(GneissError::new_protocol_error(message))
        }
    }
}

/// Validates outbound packets against per-connection dynamic constraints.  Called internally
/// right before a packet is seated as the current operation of the client.
pub(crate) fn validate_packet_outbound_internal(packet: &MqttPacket, context: &OutboundValidationContext) -> GneissResult<()> {
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
            let message = "validate_packet_outbound_internal - unexpected packet type";
            error!("{}", message);
            Err(GneissError::new_protocol_error(message))
        }
    }
}

/// Validates client-inbound packets against the MQTT5 spec requirements.  Many things can be
/// skipped during inbound validation based on the fact that we assume the decoder created the
/// packet, and so problems like invalid string or binary lengths are impossible.
pub(crate) fn validate_packet_inbound_internal(packet: &MqttPacket, context: &InboundValidationContext) -> GneissResult<()> {
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
            let message = "validate_packet_inbound_internal - unexpected packet type";
            error!("{}", message);
            Err(GneissError::new_protocol_error(message))
        }
    }
}


pub(crate) fn validate_string_length(value: &str, packet_type: PacketType, function_name: &str, field_name: &str) -> GneissResult<()> {
    if value.len() > MAXIMUM_STRING_PROPERTY_LENGTH {
        let message = format!("{} - {} string field too long", function_name, field_name);
        error!("{}", message);
        return Err(GneissError::new_packet_validation(packet_type, message));
    }

    Ok(())
}

pub(crate) fn validate_optional_string_length(optional_string: &Option<String>, packet_type: PacketType, function_name: &str, field_name: &str) -> GneissResult<()> {
    if let Some(value) = &optional_string {
        if value.len() > MAXIMUM_STRING_PROPERTY_LENGTH {
            let message = format!("{} - {} string field too long", function_name, field_name);
            error!("{}", message);
            return Err(GneissError::new_packet_validation(packet_type, message));
        }
    }

    Ok(())
}

pub(crate) fn validate_optional_binary_length(optional_data: &Option<Vec<u8>>, packet_type: PacketType, function_name: &str, field_name: &str) -> GneissResult<()> {
    if let Some(value) = &optional_data {
        if value.len() > MAXIMUM_BINARY_PROPERTY_LENGTH {
            let message = format!("{} - {} binary field too long", function_name, field_name);
            error!("{}", message);
            return Err(GneissError::new_packet_validation(packet_type, message));
        }
    }

    Ok(())
}

macro_rules! validate_optional_integer_non_zero {
    ($value_name: ident, $optional_integer_expr: expr, $packet_type: expr, $function_name: expr, $field_name: expr) => {
        if let Some($value_name) = $optional_integer_expr {
            if $value_name == 0 {
                let message = format!("{} - {} integer field is zero", $function_name, $field_name);
                error!("{}", message);
                return Err(GneissError::new_packet_validation($packet_type, message));
            }
        }
    };
}

pub(crate) use validate_optional_integer_non_zero;

macro_rules! validate_ack_outbound {
    ($function_name: ident, $packet_type_name: ident, $packet_type: expr, $validate_function_name: expr) => {
        pub(crate) fn $function_name(packet: &$packet_type_name) -> GneissResult<()> {

            validate_optional_string_length(&packet.reason_string, $packet_type, $validate_function_name, "reason_string")?;
            validate_user_properties(&packet.user_properties, $packet_type, $validate_function_name)?;

            Ok(())
        }
    };
}

pub(crate) use validate_ack_outbound;

macro_rules! validate_ack_outbound_internal {
    ($function_name: ident, $packet_type_name: ident, $packet_type: expr, $packet_length_function_name: ident, $validate_function_name: expr) => {
        pub(crate) fn $function_name(packet: &$packet_type_name, context: &OutboundValidationContext) -> GneissResult<()> {

            let (total_remaining_length, _) = $packet_length_function_name(packet)?;
            let total_packet_length = 1 + total_remaining_length + compute_variable_length_integer_encode_size(total_remaining_length as usize)? as u32;
            if total_packet_length > context.negotiated_settings.unwrap().maximum_packet_size_to_server {
                let message = format!("{} - packet length exceeds allowed maximum to server", $validate_function_name);
                error!("{}", message);
                return Err(GneissError::new_packet_validation($packet_type, message));
            }

            if packet.packet_id == 0 {
                let message = format!("{} - packet id is zero", $validate_function_name);
                error!("{}", message);
                return Err(GneissError::new_packet_validation($packet_type, message));
            }

            Ok(())
        }
    };
}

pub(crate) use validate_ack_outbound_internal;

macro_rules! validate_ack_inbound_internal {
    ($function_name: ident, $packet_type_name: ident, $packet_type: expr, $validate_function_name: expr) => {
        pub(crate) fn $function_name(packet: &$packet_type_name, _: &InboundValidationContext) -> GneissResult<()> {

            if packet.packet_id == 0 {
                let message = format!("{} - packet id is zero", $validate_function_name);
                error!("{}", message);
                return Err(GneissError::new_packet_validation($packet_type, message));
            }

            Ok(())
        }
    };
}

pub(crate) use validate_ack_inbound_internal;

pub(crate) fn is_valid_topic(topic: &str) -> bool {
    if topic.is_empty() || topic.len() > MAXIMUM_STRING_PROPERTY_LENGTH {
        return false;
    }

    if topic.contains(['#', '+']) {
        return false;
    }

    true
}

// if the topic filter is not valid, then the other fields are not to be trusted
pub(crate) struct TopicFilterProperties {
    pub is_valid: bool,
    pub is_shared: bool,
    pub has_wildcard: bool
}

fn compute_topic_filter_properties(topic: &str) -> TopicFilterProperties {
    let mut properties = TopicFilterProperties {
        is_valid: true,
        is_shared: false,
        has_wildcard: false
    };

    if topic.is_empty() || topic.len() > MAXIMUM_STRING_PROPERTY_LENGTH {
        properties.is_valid = false;
        return properties;
    }

    let mut has_share_prefix = false;
    let mut has_share_name = false;
    let mut seen_mlw = false;
    for (index, segment) in  topic.split('/').enumerate() {
        if seen_mlw {
            properties.is_valid = false;
            break;
        }

        let has_wildcard = segment.contains(['#', '+']);
        properties.has_wildcard |= has_wildcard;

        if index == 0 && segment == "$share" {
            has_share_prefix = true;
        }

        if index == 1 && has_share_prefix && !segment.is_empty() && !has_wildcard {
            has_share_name = true;
        }

        if has_share_name && ((index == 2 && !segment.is_empty()) || index > 2) {
            properties.is_shared = true;
        }

        if segment.len() == 1 {
            if segment == "#" {
                seen_mlw = true;
            }
        } else if has_wildcard {
            properties.is_valid = false;
            break;
        }
    }

    properties
}

pub(crate) fn is_valid_topic_filter_internal(filter: &str, context: &OutboundValidationContext, no_local: Option<bool>) -> bool {
    let topic_filter_properties = compute_topic_filter_properties(filter);

    if !topic_filter_properties.is_valid {
        return false;
    }

    if topic_filter_properties.is_shared {
        if !context.negotiated_settings.unwrap().shared_subscriptions_available {
            return false;
        }

        if let Some(no_local_value) = no_local {
            if no_local_value {
                return false;
            }
        }
    }

    if topic_filter_properties.has_wildcard && !context.negotiated_settings.unwrap().wildcard_subscriptions_available {
        return false;
    }

    true
}

#[cfg(feature = "testing")]
pub(crate) mod testing {
    use super::*;
    use crate::encode::MAXIMUM_VARIABLE_LENGTH_INTEGER;

    pub(crate) struct PinnedValidationContext{
        pub settings : NegotiatedSettings,
        pub connect_options : ConnectOptions,
    }

    pub(crate) fn create_pinned_validation_context() -> PinnedValidationContext {
        let mut pinned_context = PinnedValidationContext {
            settings : NegotiatedSettings {..Default::default() },
            connect_options : ConnectOptions::builder().build(),
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
    use assert_matches::assert_matches;

    pub(crate) fn do_outbound_size_validate_failure_test(packet: &MqttPacket, expected_packet_type: PacketType) {
        let encoded_bytes = encode_packet_for_test5(packet);

        let mut test_validation_context = create_pinned_validation_context();
        test_validation_context.settings.maximum_qos = QualityOfService::ExactlyOnce;

        let outbound_context1 = create_outbound_validation_context_from_pinned(&test_validation_context);

        assert!(validate_packet_outbound_internal(packet, &outbound_context1).is_ok());

        test_validation_context.settings.maximum_packet_size_to_server = (encoded_bytes.len() - 1) as u32;

        let outbound_context2 = create_outbound_validation_context_from_pinned(&test_validation_context);

        let validate_result = validate_packet_outbound_internal(packet, &outbound_context2);
        assert!(validate_result.is_err());
        assert_matches!(validate_result, Err(GneissError::PacketValidationFailure(_)));
        if let Err(GneissError::PacketValidationFailure(packet_validation_context)) = validate_result {
            assert_eq!(expected_packet_type, packet_validation_context.packet_type);
        }
    }

    macro_rules! verify_validation_failure {
        ($validation_expr: expr, $packet_type: expr) => {
            let validation_result = $validation_expr;
            if let Err(GneissError::PacketValidationFailure(packet_validation_context)) = validation_result {
                assert_eq!(packet_validation_context.packet_type, $packet_type)
            } else {
                panic!("expected validation error")
            }
        }
    }

    pub(crate) use verify_validation_failure;

    macro_rules! test_ack_validate_success {
        ($function_name: ident, $packet_type: ident, $packet_factory_function: ident) => {
            #[test]
            fn $function_name() {
                let packet = MqttPacket::$packet_type($packet_factory_function());

                assert!(validate_packet_outbound(&packet).is_ok());

                let test_validation_context = create_pinned_validation_context();

                let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);
                assert!(validate_packet_outbound_internal(&packet, &outbound_validation_context).is_ok());

                let inbound_validation_context = create_inbound_validation_context_from_pinned(&test_validation_context);
                assert!(validate_packet_inbound_internal(&packet, &inbound_validation_context).is_ok());
            }
        };
    }

    pub(crate) use test_ack_validate_success;

    macro_rules! test_ack_validate_failure_reason_string_length {
        ($function_name: ident, $packet_type_name: ident, $packet_factory_function: ident, $packet_type: expr) => {
            #[test]
            fn $function_name() {
                let mut packet = $packet_factory_function();
                packet.reason_string = Some("A".repeat(128 * 1024).to_string());

                verify_validation_failure!(validate_packet_outbound(&MqttPacket::$packet_type_name(packet)), $packet_type);
            }
        };
    }

    pub(crate) use test_ack_validate_failure_reason_string_length;

    macro_rules! test_ack_validate_failure_invalid_user_properties {
        ($function_name: ident, $packet_type_name: ident, $packet_factory_function: ident, $packet_type: expr) => {
            #[test]
            fn $function_name() {
                let mut packet = $packet_factory_function();
                packet.user_properties = Some(create_invalid_user_properties());

                verify_validation_failure!(validate_packet_outbound(&MqttPacket::$packet_type_name(packet)), $packet_type);
            }
        };
    }

    pub(crate) use test_ack_validate_failure_invalid_user_properties;

    macro_rules! test_ack_validate_failure_outbound_size {
        ($function_name: ident, $packet_type_name: ident, $packet_factory_function: ident, $packet_type: expr) => {
            #[test]
            fn $function_name() {
                let packet = $packet_factory_function();

                do_outbound_size_validate_failure_test(&MqttPacket::$packet_type_name(packet), $packet_type);
            }
        };
    }

    pub(crate) use test_ack_validate_failure_outbound_size;

    macro_rules! test_ack_validate_failure_packet_id_zero {
        ($function_name: ident, $packet_type_name: ident, $packet_factory_function: ident, $packet_type: expr) => {
            #[test]
            fn $function_name() {
                let mut ack = $packet_factory_function();
                ack.packet_id = 0;

                let packet = MqttPacket::$packet_type_name(ack);

                let test_validation_context = create_pinned_validation_context();

                let outbound_context = create_outbound_validation_context_from_pinned(&test_validation_context);
                verify_validation_failure!(validate_packet_outbound_internal(&packet, &outbound_context), $packet_type);

                let inbound_context = create_inbound_validation_context_from_pinned(&test_validation_context);
                verify_validation_failure!(validate_packet_inbound_internal(&packet, &inbound_context), $packet_type);
            }
        };
    }

    pub(crate) use test_ack_validate_failure_packet_id_zero;

    macro_rules! test_ack_validate_failure_inbound_packet_id_zero {
        ($function_name: ident, $packet_type_name: ident, $packet_factory_function: ident, $packet_type: expr) => {
            #[test]
            fn $function_name() {
                let mut ack = $packet_factory_function();
                ack.packet_id = 0;

                let packet = MqttPacket::$packet_type_name(ack);

                let test_validation_context = create_pinned_validation_context();
                let inbound_context = create_inbound_validation_context_from_pinned(&test_validation_context);
                verify_validation_failure!(validate_packet_inbound_internal(&packet, &inbound_context), $packet_type);
            }
        };
    }

    pub(crate) use test_ack_validate_failure_inbound_packet_id_zero;

    #[test]
    fn check_valid_topics() {
        assert_eq!(true, is_valid_topic("/"));
        assert_eq!(true, is_valid_topic("a/"));
        assert_eq!(true, is_valid_topic("/b"));
        assert_eq!(true, is_valid_topic("a/b/c"));
        assert_eq!(true, is_valid_topic("hippopotamus"));
    }

    #[test]
    fn check_invalid_topics() {
        assert_eq!(false, is_valid_topic("#"));
        assert_eq!(false, is_valid_topic("sport/tennis/#"));
        assert_eq!(false, is_valid_topic("sport/tennis#"));
        assert_eq!(false, is_valid_topic("sport/tennis/#/ranking"));
        assert_eq!(false, is_valid_topic(""));
        assert_eq!(false, is_valid_topic("+"));
        assert_eq!(false, is_valid_topic("+/tennis/#"));
        assert_eq!(false, is_valid_topic("sport/+/player1"));
        assert_eq!(false, is_valid_topic("sport+"));
        assert_eq!(false, is_valid_topic(&"s".repeat(70000).to_string()));
    }

    #[test]
    fn check_valid_topic_filters() {
        let default_settings = NegotiatedSettings {
            wildcard_subscriptions_available: true,
            shared_subscriptions_available: true,
            ..Default::default()
        };

        let context = OutboundValidationContext {
            negotiated_settings: Some(&default_settings),
            connect_options: None,
            outbound_alias_resolution: None,
        };

        assert_eq!(true, is_valid_topic_filter_internal("a/b/c", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("#", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("/#", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("sports/tennis/#", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("+", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("/+", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("+/a", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("+/tennis/#", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("port/+/player1", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("$share/derp/derp", &context, Some(false)));
        assert_eq!(true, is_valid_topic_filter_internal("$share/derp/derp", &context, None));
    }

    #[test]
    fn check_invalid_topic_filters() {
        let default_settings = NegotiatedSettings {
            wildcard_subscriptions_available: true,
            shared_subscriptions_available: true,
            ..Default::default()
        };

        let mut context = OutboundValidationContext {
            negotiated_settings: Some(&default_settings),
            connect_options: None,
            outbound_alias_resolution: None,
        };

        assert_eq!(false, is_valid_topic_filter_internal("", &context, None));
        assert_eq!(false, is_valid_topic_filter_internal("derp+", &context, None));
        assert_eq!(false, is_valid_topic_filter_internal("derp+/", &context, None));
        assert_eq!(false, is_valid_topic_filter_internal("derp#/", &context, None));
        assert_eq!(false, is_valid_topic_filter_internal("#/a", &context, None));
        assert_eq!(false, is_valid_topic_filter_internal("sport/tennis#", &context, None));
        assert_eq!(false, is_valid_topic_filter_internal("sport/tennis/#/ranking", &context, None));
        assert_eq!(false, is_valid_topic_filter_internal("sport+", &context, None));
        assert_eq!(false, is_valid_topic_filter_internal(&"s".repeat(70000).to_string(), &context, None));
        assert_eq!(false, is_valid_topic_filter_internal("$share/derp/derp", &context, Some(true)));

        let no_wildcard_settings = NegotiatedSettings {
            wildcard_subscriptions_available: false,
            shared_subscriptions_available: true,
            ..Default::default()
        };
        context.negotiated_settings = Some(&no_wildcard_settings);

        assert_eq!(false, is_valid_topic_filter_internal("#", &context, None));
        assert_eq!(false, is_valid_topic_filter_internal("/+", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("$share/derp/derp", &context, None));

        let no_shared_settings = NegotiatedSettings {
            wildcard_subscriptions_available: true,
            shared_subscriptions_available: false,
            ..Default::default()
        };
        context.negotiated_settings = Some(&no_shared_settings);

        assert_eq!(false, is_valid_topic_filter_internal(&"$share/derp/derp".to_string(), &context, None));
    }

    #[test]
    fn check_topic_properties() {
        assert_eq!(true, compute_topic_filter_properties("a/b/c").is_valid);
        assert_eq!(true, compute_topic_filter_properties("#").is_valid);
        assert_eq!(true, compute_topic_filter_properties("/#").is_valid);
        assert_eq!(true, compute_topic_filter_properties("sports/tennis/#").is_valid);
        assert_eq!(true, compute_topic_filter_properties("+").is_valid);
        assert_eq!(true, compute_topic_filter_properties("/+").is_valid);
        assert_eq!(true, compute_topic_filter_properties("+/a").is_valid);
        assert_eq!(true, compute_topic_filter_properties("+/tennis/#").is_valid);
        assert_eq!(true, compute_topic_filter_properties("port/+/player1").is_valid);
        assert_eq!(false, compute_topic_filter_properties("").is_valid);
        assert_eq!(false, compute_topic_filter_properties("derp+").is_valid);
        assert_eq!(false, compute_topic_filter_properties("derp+/").is_valid);
        assert_eq!(false, compute_topic_filter_properties("derp#/").is_valid);
        assert_eq!(false, compute_topic_filter_properties("#/a").is_valid);
        assert_eq!(false, compute_topic_filter_properties("sport/tennis#").is_valid);
        assert_eq!(false, compute_topic_filter_properties("sport/tennis/#/ranking").is_valid);
        assert_eq!(false, compute_topic_filter_properties("sport+").is_valid);
        assert_eq!(false, compute_topic_filter_properties(&"s".repeat(70000).to_string()).is_valid);

        assert_eq!(false, compute_topic_filter_properties("a/b/c").is_shared);
        assert_eq!(false, compute_topic_filter_properties("$share//c").is_shared);
        assert_eq!(false, compute_topic_filter_properties("$share/a").is_shared);
        assert_eq!(false, compute_topic_filter_properties("$share/+/a").is_shared);
        assert_eq!(false, compute_topic_filter_properties("$share/#/a").is_shared);
        assert_eq!(false, compute_topic_filter_properties("$share/b/").is_shared);
        assert_eq!(true, compute_topic_filter_properties("$share/b//").is_shared);
        assert_eq!(true, compute_topic_filter_properties("$share/a/b").is_shared);
        assert_eq!(true, compute_topic_filter_properties("$share/a/b/c").is_shared);

        assert_eq!(false, compute_topic_filter_properties("a/b/c").has_wildcard);
        assert_eq!(false, compute_topic_filter_properties("/").has_wildcard);
        assert_eq!(true, compute_topic_filter_properties("#").has_wildcard);
        assert_eq!(true, compute_topic_filter_properties("+").has_wildcard);
        assert_eq!(true, compute_topic_filter_properties("a/+/+").has_wildcard);
        assert_eq!(true, compute_topic_filter_properties("a/b/#").has_wildcard);
    }
}