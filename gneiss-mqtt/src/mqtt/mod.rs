/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Module containing a set of structured data types that model the MQTT5 specification.
 */

use std::fmt;
use log::error;
use crate::error::{MqttError};

pub(crate) mod auth;
pub(crate) mod connack;
pub(crate) mod connect;
pub(crate) mod disconnect;
pub(crate) mod pingreq;
pub(crate) mod pingresp;
pub(crate) mod puback;
pub(crate) mod pubcomp;
pub(crate) mod publish;
pub(crate) mod pubrec;
pub(crate) mod pubrel;
pub(crate) mod suback;
pub(crate) mod subscribe;
pub(crate) mod unsuback;
pub(crate) mod unsubscribe;
pub mod utils;

/// MQTT message delivery quality of service.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901234) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum QualityOfService {

    /// The message is delivered according to the capabilities of the underlying network. No response is sent by the
    /// receiver and no retry is performed by the sender. The message arrives at the receiver either once or not at all.
    #[default]
    AtMostOnce = 0,

    /// A level of service that ensures that the message arrives at the receiver at least once.
    AtLeastOnce = 1,

    /// A level of service that ensures that the message arrives at the receiver exactly once.
    ExactlyOnce = 2,
}

impl TryFrom<u8> for QualityOfService {
    type Error = MqttError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        utils::convert_u8_to_quality_of_service(value)
    }
}

/// Optional property describing a PUBLISH payload's format.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901111) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PayloadFormatIndicator {

    /// The payload is arbitrary binary data
    #[default]
    Bytes = 0,

    /// The payload is a well-formed utf-8 string value.
    Utf8 = 1,
}

/// Configures how retained messages should be handled when subscribing with a topic filter that matches topics with
/// associated retained messages.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RetainHandlingType {

    /// The server should always send all retained messages on topics that match a subscription's filter.
    #[default]
    SendOnSubscribe = 0,

    /// The server should send retained messages on topics that match the subscription's filter, but only for the
    /// first matching subscription, per session.
    SendOnSubscribeIfNew = 1,

    /// Subscriptions must not trigger any retained message publishes from the server.
    DontSend = 2,
}

/// Server return code for connection attempts.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901079) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ConnectReasonCode {

    /// Returned when the connection is accepted.
    #[default]
    Success = 0,

    /// Returned when the server has a failure but does not want to specify a reason or none
    /// of the other reason codes apply.
    UnspecifiedError = 128,

    /// Returned when data in the CONNECT packet could not be correctly parsed by the server.
    MalformedPacket = 129,

    /// Returned when data in the CONNECT packet does not conform to the MQTT5 specification requirements.
    ProtocolError = 130,

    /// Returned when the CONNECT packet is valid but was not accepted by the server.
    ImplementationSpecificError = 131,

    /// Returned when the server does not support MQTT5 protocol version specified in the connection.
    UnsupportedProtocolVersion = 132,

    /// Returned when the client identifier in the CONNECT packet is a valid string but not one that
    /// is allowed on the server.
    ClientIdentifierNotValid = 133,

    /// Returned when the server does not accept the username and/or password specified by the client
    /// in the connection packet.
    BadUsernameOrPassword = 134,

    /// Returned when the client is not authorized to connect to the server.
    NotAuthorized = 135,

    /// Returned when the MQTT5 server is not available.
    ServerUnavailable = 136,

    /// Returned when the server is too busy to make a connection. It is recommended that the client try again later.
    ServerBusy = 137,

    /// Returned when the client has been banned from the server.
    Banned = 138,

    /// Returned when the authentication method used in the connection is either not supported on the server or it does
    /// not match the authentication method currently in use in the CONNECT packet.
    BadAuthenticationMethod = 140,

    /// Returned when the Will topic name sent in the connection packet is correctly formed, but is not accepted by
    /// the server.
    TopicNameInvalid = 144,

    /// Returned when the connection packet exceeded the maximum permissible size on the server.
    PacketTooLarge = 149,

    /// Returned when the quota limits set on the server have been met and/or exceeded.
    QuotaExceeded = 151,

    /// Returned when the Will payload in the CONNECT packet does not match the specified payload format indicator.
    PayloadFormatInvalid = 153,

    /// Returned when the server does not retain messages but the connection packet on the client had Will retain enabled.
    RetainNotSupported = 154,

    /// Returned when the server does not support the QOS setting in the Will QOS in the connection packet.
    QosNotSupported = 155,

    /// Returned when the server is telling the client to temporarily use another server instead of the one they
    /// are trying to connect to.
    UseAnotherServer = 156,

    /// Returned when the server is telling the client to permanently use another server instead of the one they
    /// are trying to connect to.
    ServerMoved = 157,

    /// Returned when the server connection rate limit has been exceeded.
    ConnectionRateExceeded = 159,
}

/// Reason code inside PUBACK packets that indicates the result of the associated PUBLISH request.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901124) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PubackReasonCode {

    /// Returned when the (QoS 1) publish was accepted by the recipient.
    ///
    /// May be sent by the client or the server.
    #[default]
    Success = 0,

    /// Returned when the (QoS 1) publish was accepted but there were no matching subscribers.
    ///
    /// May only be sent by the server.
    NoMatchingSubscribers = 16,

    /// Returned when the (QoS 1) publish was not accepted and the receiver does not want to specify a reason or none
    /// of the other reason codes apply.
    ///
    /// May be sent by the client or the server.
    UnspecifiedError = 128,

    /// Returned when the (QoS 1) publish was valid but the receiver was not willing to accept it.
    ///
    /// May be sent by the client or the server.
    ImplementationSpecificError = 131,

    /// Returned when the (QoS 1) publish was not authorized by the receiver.
    ///
    /// May be sent by the client or the server.
    NotAuthorized = 135,

    /// Returned when the topic name was valid but the receiver was not willing to accept it.
    ///
    /// May be sent by the client or the server.
    TopicNameInvalid = 144,

    /// Returned when the packet identifier used in the associated PUBLISH was already in use.
    /// This can indicate a mismatch in the session state between client and server.
    ///
    /// May be sent by the client or the server.
    PacketIdentifierInUse = 145,

    /// Returned when the associated PUBLISH failed because an internal quota on the recipient was exceeded.
    ///
    /// May be sent by the client or the server.
    QuotaExceeded = 151,

    /// Returned when the PUBLISH packet's payload format did not match its payload format indicator property.
    ///
    /// May be sent by the client or the server.
    PayloadFormatInvalid = 153,
}

impl PubackReasonCode {
    /// Returns whether or not the reason code represents a successful publish
    pub fn is_success(&self) -> bool {
        matches!(self, PubackReasonCode::Success | PubackReasonCode::NoMatchingSubscribers)
    }
}


/// Reason code inside PUBREC packets that indicates the result of the associated QoS 2 PUBLISH request.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901134) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PubrecReasonCode {

    /// Returned when the (QoS 2) publish was accepted by the recipient.
    ///
    /// May be sent by the client or the server.
    #[default]
    Success = 0,

    /// Returned when the (QoS 2) publish was accepted but there were no matching subscribers.
    ///
    /// May only be sent by the server.
    NoMatchingSubscribers = 16,

    /// Returned when the (QoS 2) publish was not accepted and the receiver does not want to specify a reason or none
    /// of the other reason codes apply.
    ///
    /// May be sent by the client or the server.
    UnspecifiedError = 128,

    /// Returned when the (QoS 2) publish was valid but the receiver was not willing to accept it.
    ///
    /// May be sent by the client or the server.
    ImplementationSpecificError = 131,

    /// Returned when the (QoS 2) publish was not authorized by the receiver.
    ///
    /// May be sent by the client or the server.
    NotAuthorized = 135,

    /// Returned when the topic name was valid but the receiver was not willing to accept it.
    ///
    /// May be sent by the client or the server.
    TopicNameInvalid = 144,

    /// Returned when the packet identifier used in the associated PUBLISH was already in use.
    /// This can indicate a mismatch in the session state between client and server.
    ///
    /// May be sent by the client or the server.
    PacketIdentifierInUse = 145,

    /// Returned when the associated PUBLISH failed because an internal quota on the recipient was exceeded.
    ///
    /// May be sent by the client or the server.
    QuotaExceeded = 151,

    /// Returned when the PUBLISH packet's payload format did not match its payload format indicator property.
    ///
    /// May be sent by the client or the server.
    PayloadFormatInvalid = 153,
}

impl PubrecReasonCode {
    /// Returns whether or not the reason code represents a successful publish
    pub fn is_success(&self) -> bool {
        matches!(self, PubrecReasonCode::Success | PubrecReasonCode::NoMatchingSubscribers)
    }
}

/// Reason code inside PUBREL packets that indicates the result of receiving a PUBREC packet as part of the QoS 2 PUBLISH delivery process.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901144) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PubrelReasonCode {

    /// Returned when the associated PUBREC was successfully accepted by the recipient.
    ///
    /// May be sent by the client or the server.
    #[default]
    Success = 0,

    /// Returned when the associated PUBREC's packet id was not being tracked by the recipient as an in-progress QoS 2 delivery.
    ///
    /// May be sent by the client or the server.
    PacketIdentifierNotFound = 146,
}

/// Reason code inside PUBCOMP packets that indicates the result of receiving a PUBREL packet as part of the QoS 2 publish delivery process.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901154) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PubcompReasonCode {

    /// Returned when the associated PUBREL was successfully accepted by the recipient.  Marks a successful
    /// conclusion to the QoS 2 delivery sequence.
    ///
    /// May be sent by the client or the server.
    #[default]
    Success = 0,

    /// Returned when the associated PUBREL's packet id was not being tracked by the recipient as an in-progress QoS 2 delivery.
    ///
    /// May be sent by the client or the server.
    PacketIdentifierNotFound = 146,
}

/// Reason code inside DISCONNECT packets.  Helps determine why a connection was terminated.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901208) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DisconnectReasonCode {

    /// Returned when the remote endpoint wishes to disconnect normally. Will not trigger the publish of a Will message if a
    /// Will message was configured on the connection.
    ///
    /// May be sent by the client or server.
    #[default]
    NormalDisconnection = 0,

    /// Returns that the client wants to disconnect but requires that the server publish the Will message configured
    /// on the connection.
    ///
    /// May only be sent by the client.
    DisconnectWithWillMessage = 4,

    /// Returned when the connection was closed but the sender does not want to specify a reason or none
    /// of the other reason codes apply.
    ///
    /// May be sent by the client or the server.
    UnspecifiedError = 128,

    /// Indicates the remote endpoint received a packet that does not conform to the MQTT specification.
    ///
    /// May be sent by the client or the server.
    MalformedPacket = 129,

    /// Returned when an unexpected or out-of-order packet was received by the remote endpoint.
    ///
    /// May be sent by the client or the server.
    ProtocolError = 130,

    /// Returned when a valid packet was received by the remote endpoint, but could not be processed by the current implementation.
    ///
    /// May be sent by the client or the server.
    ImplementationSpecificError = 131,

    /// Returned when the remote endpoint received a packet that represented an operation that was not authorized within
    /// the current connection.
    ///
    /// May only be sent by the server.
    NotAuthorized = 135,

    /// Returned when the server is busy and cannot continue processing packets from the client.
    ///
    /// May only be sent by the server.
    ServerBusy = 137,

    /// Returned when the server is shutting down.
    ///
    /// May only be sent by the server.
    ServerShuttingDown = 139,

    /// Returned when the server closes the connection because no packet from the client has been received in
    /// 1.5 times the KeepAlive time set when the connection was established.
    ///
    /// May only be sent by the server.
    KeepAliveTimeout = 141,

    /// Returned when the server has established another connection with the same client ID as a client's current
    /// connection, causing the current client to become disconnected.
    ///
    /// May only be sent by the server.
    SessionTakenOver = 142,

    /// Returned when the topic filter name is correctly formed but not accepted by the server.
    ///
    /// May only be sent by the server.
    TopicFilterInvalid = 143,

    /// Returned when topic name is correctly formed, but is not accepted.
    ///
    /// May be sent by the client or the server.
    TopicNameInvalid = 144,

    /// Returned when the remote endpoint reached a state where there were more in-progress QoS1+ publishes then the
    /// limit it established for itself when the connection was opened.
    ///
    /// May be sent by the client or the server.
    ReceiveMaximumExceeded = 147,

    /// Returned when the remote endpoint receives a PUBLISH packet that contained a topic alias greater than the
    /// maximum topic alias limit that it established for itself when the connection was opened.
    ///
    /// May be sent by the client or the server.
    TopicAliasInvalid = 148,

    /// Returned when the remote endpoint received a packet whose size was greater than the maximum packet size limit
    /// it established for itself when the connection was opened.
    ///
    /// May be sent by the client or the server.
    PacketTooLarge = 149,

    /// Returned when the remote endpoint's incoming data rate was too high.
    ///
    /// May be sent by the client or the server.
    MessageRateTooHigh = 150,

    /// Returned when an internal quota of the remote endpoint was exceeded.
    ///
    /// May be sent by the client or the server.
    QuotaExceeded = 151,

    /// Returned when the connection was closed due to an administrative action.
    ///
    /// May be sent by the client or the server.
    AdministrativeAction = 152,

    /// Returned when the remote endpoint received a packet where payload format did not match the format specified
    /// by the payload format indicator.
    ///
    /// May be sent by the client or the server.
    PayloadFormatInvalid = 153,

    /// Returned when the server does not support retained messages.
    ///
    /// May only be sent by the server.
    RetainNotSupported = 154,

    /// Returned when the client sends a QOS that is greater than the maximum QOS established when the connection was
    /// opened.
    ///
    /// May only be sent by the server.
    QosNotSupported = 155,

    /// Returned by the server to tell the client to temporarily use a different server.
    ///
    /// May only be sent by the server.
    UseAnotherServer = 156,

    /// Returned by the server to tell the client to permanently use a different server.
    ///
    /// May only be sent by the server.
    ServerMoved = 157,

    /// Returned by the server to tell the client that shared subscriptions are not supported on the server.
    ///
    /// May only be sent by the server.
    SharedSubscriptionsNotSupported = 158,

    /// Returned when the server disconnects the client due to the connection rate being too high.
    ///
    /// May only be sent by the server.
    ConnectionRateExceeded = 159,

    /// Returned by the server when the maximum connection time authorized for the connection was exceeded.
    ///
    /// May only be sent by the server.
    MaximumConnectTime = 160,

    /// Returned by the server when it received a SUBSCRIBE packet with a subscription identifier, but the server does
    /// not support subscription identifiers.
    ///
    /// May only be sent by the server.
    SubscriptionIdentifiersNotSupported = 161,

    /// Returned by the server when it received a SUBSCRIBE packet with a wildcard topic filter, but the server does
    /// not support wildcard topic filters.
    ///
    /// May only be sent by the server.
    WildcardSubscriptionsNotSupported = 162,
}

impl TryFrom<u8> for DisconnectReasonCode {
    type Error = MqttError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        utils::convert_u8_to_disconnect_reason_code(value)
    }
}

/// Reason codes inside SUBACK packet payloads that specify the results for each subscription in the associated
/// SUBSCRIBE packet.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901178) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SubackReasonCode {

    /// Returned when the subscription was accepted and the maximum QOS sent will be QOS 0.
    #[default]
    GrantedQos0 = 0,

    /// Returned when the subscription was accepted and the maximum QOS sent will be QOS 1.
    GrantedQos1 = 1,

    /// Returned when the subscription was accepted and the maximum QOS sent will be QOS 2.
    GrantedQos2 = 2,

    /// Returned when the connection was closed but the sender does not want to specify a reason or none
    /// of the other reason codes apply.
    UnspecifiedError = 128,

    /// Returned when the subscription was valid but the server did not accept it.
    ImplementationSpecificError = 131,

    /// Returned when the client was not authorized to make the subscription on the server.
    NotAuthorized = 135,

    /// Returned when the subscription topic filter was correctly formed but not allowed for the client.
    TopicFilterInvalid = 143,

    /// Returned when the packet identifier was already in use on the server.
    PacketIdentifierInUse = 145,

    /// Returned when a subscribe-related quota set on the server was exceeded.
    QuotaExceeded = 151,

    /// Returned when the subscription's topic filter was a shared subscription and the server does not support
    /// shared subscriptions.
    SharedSubscriptionsNotSupported = 158,

    /// Returned when the SUBSCRIBE packet contained a subscription identifier and the server does not support
    /// subscription identifiers.
    SubscriptionIdentifiersNotSupported = 161,

    /// Returned when the subscription's topic filter contains a wildcard but the server does not support
    /// wildcard subscriptions.
    WildcardSubscriptionsNotSupported = 162,
}

impl SubackReasonCode {
    /// Returns whether or not the reason code represents a successful subscription
    pub fn is_success(&self) -> bool {
        matches!(self, SubackReasonCode::GrantedQos0 | SubackReasonCode::GrantedQos1 | SubackReasonCode::GrantedQos2)
    }
}

/// Reason codes inside UNSUBACK packet payloads that specify the results for each topic filter in the associated
/// UNSUBSCRIBE packet.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901194) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum UnsubackReasonCode {

    /// Returned when the unsubscribe was successful and the client is no longer subscribed to the topic filter on the server.
    #[default]
    Success = 0,

    /// Returned when the topic filter did not match one of the client's existing subscriptions on the server.
    NoSubscriptionExisted = 17,

    /// Returned when the unsubscribe of the topic filter was not accepted and the server does not want to specify a
    /// reason or none of the other reason codes apply.
    UnspecifiedError = 128,

    /// Returned when the topic filter was valid but the server does not accept an unsubscribe for it.
    ImplementationSpecificError = 131,

    /// Returned when the client was not authorized to unsubscribe from that topic filter on the server.
    NotAuthorized = 135,

    /// Returned when the topic filter was correctly formed but is not allowed for the client on the server.
    TopicNameInvalid = 144,

    /// Returned when the packet identifier was already in use on the server.
    PacketIdentifierInUse = 145,
}

impl UnsubackReasonCode {
    /// Returns whether or not the reason code represents a successful unsubscribe
    pub fn is_success(&self) -> bool {
        matches!(self, UnsubackReasonCode::Success | UnsubackReasonCode::NoSubscriptionExisted)
    }
}

/// Reason code that specifies the response to a received AUTH packet.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901220) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AuthenticateReasonCode {

    /// Notification that the authentication exchange is both complete and considered successful.
    ///
    /// Only the server may send this code.
    #[default]
    Success = 0,

    /// A request that the recipient should continue the authentication exchange.
    ///
    /// Either the client or server may send this code.
    ContinueAuthentication = 24,

    /// The associated packet represents an attempt to reauthenticate an established connection.
    ///
    /// Only the client may send this code.
    ReAuthenticate = 25,
}


/// Data model for MQTT5 user properties.
///
/// A user property is a name-value pair of utf-8 strings that can be added to mqtt5 packets. Names are
/// not unique; a given name value can appear more than once in a packet.
///
/// User properties are required to be a utf-8 string pair, as specified by the
/// [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901013).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UserProperty {

    /// User property name
    pub name: String,

    /// User property value
    pub value: String,
}

/// Specifies a single subscription within a Subscribe operation
///
/// See [MQTT5 Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Subscription {

    /// Topic filter to subscribe to
    ///
    /// See [MQTT5 Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
    pub topic_filter: String,

    /// Maximum QoS on which the subscriber will accept publish messages.  Negotiated QoS may be different.
    ///
    /// See [MQTT5 Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
    pub qos: QualityOfService,

    /// Should the server not send publishes to a client when that client was the one who sent the publish?
    ///
    /// See [MQTT5 Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
    pub no_local: bool,

    /// Should messages sent due to this subscription keep the retain flag preserved on the message?
    ///
    /// See [MQTT5 Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
    pub retain_as_published: bool,

    /// Should retained messages on matching topics be sent in reaction to this subscription?
    ///
    /// See [MQTT5 Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
    pub retain_handling_type: RetainHandlingType,
}

impl Subscription {

    /// Common-case constructor for Subscriptions that don't need specialized configuration
    pub fn new(topic_filter: &str, qos: QualityOfService) -> Self {
        Subscription {
            topic_filter: topic_filter.to_string(),
            qos,
            ..Default::default()
        }
    }
}

/// Data model of an [MQTT5 AUTH](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901217) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct AuthPacket {

    /// Specifies an endpoint's response to a previously-received AUTH packet as part of an authentication exchange.
    ///
    /// See [MQTT5 Authenticate Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901220)
    pub reason_code: AuthenticateReasonCode,

    /// Authentication method this packet corresponds to.  The authentication method must remain the
    /// same for the entirety of an authentication exchange.
    ///
    /// The MQTT5 specification lists the authentication method property as required, from a
    /// protocol perspective.  At the same time it specifies that it is valid to short-circuit the
    /// packet encoding if the reason is Success and there are no properties.  This is a bit
    /// self-contradictory, but we resolve it by modeling the authentication method as an optional
    /// string (so supporting the short-circuited encode/decode) but -- despite the option type --
    /// the packet fails validation if authentication_method is None.
    ///
    /// See [MQTT5 Authentication Method](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901223)
    pub authentication_method: Option<String>,

    /// Method-specific binary data included in this step of an authentication exchange.
    ///
    /// See [MQTT5 Authentication Data](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901224)
    pub authentication_data: Option<Vec<u8>>,

    /// Additional diagnostic information or context.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901225)
    pub reason_string: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901226)
    pub user_properties: Option<Vec<UserProperty>>,
}

/// Data model of an [MQTT5 CONNACK](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901074) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConnackPacket {

    /// True if the client rejoined an existing session on the server, false otherwise.
    ///
    /// See [MQTT5 Session Present](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901078)
    pub session_present: bool,

    /// Indicates either success or the reason for failure for the connection attempt.
    ///
    /// See [MQTT5 Connect Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901079)
    pub reason_code: ConnectReasonCode,

    /// A time interval, in seconds, that the server will persist this connection's MQTT session state
    /// for.  If present, this value overrides any session expiry specified in the preceding CONNECT packet.
    ///
    /// See [MQTT5 Session Expiry Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901082)
    pub session_expiry_interval: Option<u32>,

    /// The maximum amount of in-flight QoS 1 or 2 messages that the server is willing to handle at once.  If omitted,
    /// the limit is based on the valid MQTT packet id space (65535).
    ///
    /// See [MQTT5 Receive Maximum](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901083)
    pub receive_maximum: Option<u16>,

    /// The maximum message delivery quality of service that the server will allow on this connection.
    ///
    /// See [MQTT5 Maximum QoS](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901084)
    pub maximum_qos: Option<QualityOfService>,

    /// Indicates whether the server supports retained messages.  If undefined, retained messages are
    /// supported.
    ///
    /// See [MQTT5 Retain Available](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901085)
    pub retain_available: Option<bool>,

    /// Specifies the maximum packet size, in bytes, that the server is willing to accept.  If undefined, there
    /// is no limit beyond what is imposed by the MQTT spec itself.
    ///
    /// See [MQTT5 Maximum Packet Size](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901086)
    pub maximum_packet_size: Option<u32>,

    /// Specifies a client identifier assigned to this connection by the server.  Only valid when the client id of
    /// the preceding CONNECT packet was left empty.
    ///
    /// See [MQTT5 Assigned Client Identifier](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901087)
    pub assigned_client_identifier: Option<String>,

    /// Specifies the maximum topic alias value that the server will accept from the client.
    ///
    /// See [MQTT5 Topic Alias Maximum](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901088)
    pub topic_alias_maximum: Option<u16>,

    /// Additional diagnostic information about the result of the connection attempt.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901089)
    pub reason_string: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901090)
    pub user_properties: Option<Vec<UserProperty>>,

    /// Indicates whether the server supports wildcard subscriptions.  If undefined, wildcard subscriptions
    /// are supported.
    ///
    /// See [MQTT5 Wildcard Subscriptions Available](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901091)
    pub wildcard_subscriptions_available: Option<bool>,

    /// Indicates whether the server supports subscription identifiers.  If undefined, subscription identifiers
    /// are supported.
    ///
    /// See [MQTT5 Subscription Identifiers Available](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901092)
    pub subscription_identifiers_available: Option<bool>,

    /// Indicates whether the server supports shared subscription topic filters.  If undefined, shared subscriptions
    /// are supported.
    ///
    /// See [MQTT5 Shared Subscriptions Available](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901093)
    pub shared_subscriptions_available: Option<bool>,

    /// Server-requested override of the keep alive interval, in seconds.  If undefined, the keep alive value sent
    /// by the client should be used.
    ///
    /// See [MQTT5 Server Keep Alive](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901094)
    pub server_keep_alive: Option<u16>,

    /// A value that can be used in the creation of a response topic associated with this connection.  MQTT5-based
    /// request/response is outside the purview of the MQTT5 spec and this client.
    ///
    /// See [MQTT5 Response Information](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901095)
    pub response_information: Option<String>,

    /// Property indicating an alternate server that the client may temporarily or permanently attempt
    /// to connect to instead of the configured endpoint.  Will only be set if the reason code indicates another
    /// server may be used (ServerMoved, UseAnotherServer).
    ///
    /// See [MQTT5 Server Reference](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901096)
    pub server_reference: Option<String>,

    /// Authentication method used in the authentication exchange that led to this CONNACK packet being sent.
    ///
    /// See [MQTT5 Authentication Method](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901097)
    pub authentication_method: Option<String>,

    /// Authentication method specific binary data associated with the authentication exchange that led to this
    /// CONNACK packet being sent.
    ///
    /// Developer Note: It is likely that this field is only relevant in authentication exchanges that *DO NOT*
    /// need AUTH packets to reach a successful conclusion, otherwise the final server->client authentication
    /// data would have been sent with the final server->client AUTH packet that included the success reason code.
    ///
    /// See [MQTT5 Authentication Data](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901098)
    pub authentication_data: Option<Vec<u8>>,
}

/// Data model of an [MQTT5 CONNECT](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901033) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConnectPacket {

    /// The maximum time interval, in seconds, that is permitted to elapse between the point at which the client
    /// finishes transmitting one MQTT packet and the point it starts sending the next.  The client will use
    /// PINGREQ packets to maintain this property.
    ///
    /// If the responding CONNACK contains a keep alive property value, then that is the negotiated keep alive value.
    /// Otherwise, the keep alive sent by the client is the negotiated value.
    ///
    /// See [MQTT5 Keep Alive](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901045)
    pub keep_alive_interval_seconds: u16,

    /// Clean start is modeled but not under direct user control.  Instead it is controlled by client
    /// configuration that is outside the scope of the MQTT5 spec.
    pub clean_start: bool,

    /// A unique string identifying the client to the server.  Used to restore session state between connections.
    ///
    /// If left empty, the broker will auto-assign a unique client id.  When reconnecting, the mqtt5 client will
    /// always use the auto-assigned client id.
    ///
    /// See [MQTT5 Client Identifier](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901059)
    pub client_id: Option<String>,

    /// A string value that the server may use for client authentication and authorization.
    ///
    /// See [MQTT5 User Name](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901071)
    pub username: Option<String>,

    /// Opaque binary data that the server may use for client authentication and authorization.
    ///
    /// See [MQTT5 Password](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901072)
    pub password: Option<Vec<u8>>,

    /// A time interval, in seconds, that the client requests the server to persist this connection's MQTT session state
    /// for.  Has no meaning if the client has not been configured to rejoin sessions.  Must be non-zero in order to
    /// successfully rejoin a session.
    ///
    /// If the responding CONNACK contains a session expiry property value, then that is the negotiated session expiry
    /// value.  Otherwise, the session expiry sent by the client is the negotiated value.
    ///
    /// See [MQTT5 Session Expiry Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901048)
    pub session_expiry_interval_seconds: Option<u32>,

    /// If set to true, requests that the server send response information in the subsequent CONNACK.  This response
    /// information may be used to set up request-response implementations over MQTT, but doing so is outside
    /// the scope of the MQTT5 spec and client.
    ///
    /// See [MQTT5 Request Response Information](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901052)
    pub request_response_information: Option<bool>,

    /// If set to true, requests that the server send additional diagnostic information (via response string or
    /// user properties) in DISCONNECT or CONNACK packets from the server.
    ///
    /// See [MQTT5 Request Problem Information](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901053)
    pub request_problem_information: Option<bool>,

    /// Notifies the server of the maximum number of in-flight Qos 1 and 2 messages the client is willing to handle.  If
    /// omitted, then no limit is requested.
    ///
    /// See [MQTT5 Receive Maximum](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901049)
    pub receive_maximum: Option<u16>,

    /// Maximum number of topic aliases that the client will accept for incoming publishes.  An inbound topic alias larger than
    /// this number is a protocol error.  If this value is not specified, the client does not support inbound topic
    /// aliasing.
    ///
    /// See [MQTT5 Topic Alias Maximum](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901051)
    pub topic_alias_maximum: Option<u16>,

    /// Notifies the server of the maximum packet size the client is willing to handle.  If
    /// omitted, then no limit beyond the natural limits of MQTT packet size is requested.
    ///
    /// See [MQTT5 Maximum Packet Size](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901050)
    pub maximum_packet_size_bytes: Option<u32>,

    /// Notifies the server that the client wishes to use a specific authentication method as part of the connection
    /// process.  If this field is left empty, no authentication exchange should be performed as part of the connection
    /// process.
    ///
    /// See [MQTT5 Authentication Method](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901055)
    pub authentication_method: Option<String>,

    /// Additional authentication method specific binary data supplied as part of kicking off an authentication
    /// exchange.  This field may only be set if `authentication_method` is also set.
    ///
    /// See [MQTT5 Authentication Data](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901056)
    pub authentication_data: Option<Vec<u8>>,

    /// A time interval, in seconds, that the server should wait (for a session reconnection) before sending the
    /// will message associated with the connection's session.  If omitted, the server will send the will when the
    /// associated session is destroyed.  If the session is destroyed before a will delay interval has elapsed, then
    /// the will must be sent at the time of session destruction.
    ///
    /// See [MQTT5 Will Delay Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901062)
    pub will_delay_interval_seconds: Option<u32>,

    /// The definition of a message to be published when the connection's session is destroyed by the server or when
    /// the will delay interval has elapsed, whichever comes first.  If undefined, then nothing will be sent.
    ///
    /// See [MQTT5 Will](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901040)
    pub will: Option<PublishPacket>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901054)
    pub user_properties: Option<Vec<UserProperty>>,
}

/// Data model of an [MQTT5 DISCONNECT](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901205) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DisconnectPacket {

    /// Value indicating the reason that the sender is closing the connection
    ///
    /// See [MQTT5 Disconnect Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901208)
    pub reason_code: DisconnectReasonCode,

    /// Requests a change to the session expiry interval negotiated at connection time as part of the disconnect.  Only
    /// valid for  DISCONNECT packets sent from client to server.  It is not valid to attempt to change session expiry
    /// from zero to a non-zero value.
    ///
    /// See [MQTT5 Session Expiry Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901211)
    pub session_expiry_interval_seconds: Option<u32>,

    /// Additional diagnostic information about the reason that the sender is closing the connection
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901212)
    pub reason_string: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901213)
    pub user_properties: Option<Vec<UserProperty>>,

    /// Property indicating an alternate server that the client may temporarily or permanently attempt
    /// to connect to instead of the configured endpoint.  Will only be set if the reason code indicates another
    /// server may be used (ServerMoved, UseAnotherServer).
    ///
    /// See [MQTT5 Server Reference](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901214)
    pub server_reference: Option<String>,
}

/// Data model of an [MQTT5 PINGREQ](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901195) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PingreqPacket {}

/// Data model of an [MQTT5 PINGRESP](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901200) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PingrespPacket {}

/// Data model of an [MQTT5 PUBACK](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901121) packet
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PubackPacket {

    /// Id of the QoS 1 publish this packet is acknowledging
    pub packet_id: u16,

    /// Success indicator or failure reason for the associated PUBLISH packet.
    ///
    /// See [MQTT5 PUBACK Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901124)
    pub reason_code: PubackReasonCode,

    /// Additional diagnostic information about the result of the PUBLISH attempt.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901127)
    pub reason_string: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901128)
    pub user_properties: Option<Vec<UserProperty>>,
}

/// Data model of an [MQTT5 PUBCOMP](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901151) packet
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PubcompPacket {

    /// Id of the QoS 2 publish this packet corresponds to
    pub packet_id: u16,

    /// Success indicator or failure reason for the final step of a QoS 2 PUBLISH delivery.
    ///
    /// See [MQTT5 PUBCOMP Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901154)
    pub reason_code: PubcompReasonCode,

    /// Additional diagnostic information about the final step of a QoS 2 PUBLISH delivery.
    ///
    /// See [MQTT5 PUBCOMP Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901157)
    pub reason_string: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901158)
    pub user_properties: Option<Vec<UserProperty>>,
}

/// Data model of an [MQTT5 PUBLISH](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901100) packet
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PublishPacket {

    /// Packet Id of the publish.  Setting this value on an outbound publish has no effect on the
    /// actual packet id used by the client.
    pub packet_id: u16,

    /// Sent publishes - The topic this message should be published to.
    ///
    /// Received publishes - The topic this message was published to.
    ///
    /// See [MQTT5 Topic Name](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901107)
    pub topic: String,

    /// Sent publishes - The MQTT quality of service level this message should be delivered with.
    ///
    /// Received publishes - The MQTT quality of service level this message was delivered at.
    ///
    /// See [MQTT5 QoS](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901103)
    pub qos: QualityOfService,

    /// Indicates to the recipient that this packet is a resend of a previously-submitted
    /// Publish
    pub duplicate: bool,

    /// True if this is a retained message, false otherwise.
    ///
    /// Always set on received publishes; on sent publishes, undefined implies false.
    ///
    /// See [MQTT5 Retain](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901104)
    pub retain: bool,

    /// The payload of the publish message.
    ///
    /// See [MQTT5 Publish Payload](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901119)
    pub payload: Option<Vec<u8>>,

    /// Property specifying the format of the payload data.  The mqtt5 client does not enforce or use this
    /// value in a meaningful way.
    ///
    /// See [MQTT5 Payload Format Indicator](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901111)
    pub payload_format: Option<PayloadFormatIndicator>,

    /// Sent publishes - indicates the maximum amount of time allowed to elapse for message delivery before the server
    /// should instead delete the message (relative to a recipient).
    ///
    /// Received publishes - indicates the remaining amount of time (from the server's perspective) before the message would
    /// have been deleted relative to the subscribing client.
    ///
    /// If left undefined, indicates no expiration timeout.
    ///
    /// See [MQTT5 Message Expiry Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901112)
    pub message_expiry_interval_seconds: Option<u32>,

    /// If the topic field is non-empty:
    ///   Tells the recipient to bind this id to the topic field's value within its alias cache
    ///
    /// If the topic field is empty:
    ///   Tells the recipient to lookup the topic in their alias cache based on this id.
    ///
    /// See [MQTT5 Topic Alias](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901113)
    pub topic_alias: Option<u16>,

    /// Opaque topic string intended to assist with request/response implementations.  Not internally meaningful to
    /// MQTT5 or this client.
    ///
    /// See [MQTT5 Response Topic](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901114)
    pub response_topic: Option<String>,

    /// Opaque binary data used to correlate between publish messages, as a potential method for request-response
    /// implementation.  Not internally meaningful to MQTT5.
    ///
    /// See [MQTT5 Correlation Data](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901115)
    pub correlation_data: Option<Vec<u8>>,

    /// Sent publishes - setting this fails client-side packet validation
    ///
    /// Received publishes - the subscription identifiers of all the subscriptions this message matched.
    ///
    /// See [MQTT5 Subscription Identifier](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901117)
    pub subscription_identifiers: Option<Vec<u32>>,

    /// Property specifying the content type of the payload.  Not internally meaningful to MQTT5.
    ///
    /// See [MQTT5 Content Type](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901118)
    pub content_type: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901116)
    pub user_properties: Option<Vec<UserProperty>>,
}

/// Data model of an [MQTT5 PUBREC](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901131) packet
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PubrecPacket {

    /// Id of the QoS 2 publish this packet corresponds to
    pub packet_id: u16,

    /// Success indicator or failure reason for the initial step of the QoS 2 PUBLISH delivery process.
    ///
    /// See [MQTT5 PUBREC Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901134)
    pub reason_code: PubrecReasonCode,

    /// Additional diagnostic information about the result of the PUBLISH attempt.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901147)
    pub reason_string: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901138)
    pub user_properties: Option<Vec<UserProperty>>,
}

/// Data model of an [MQTT5 PUBREL](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901141) packet
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PubrelPacket {

    // packet id is modeled but internal to the client
    pub(crate) packet_id: u16,

    /// Success indicator or failure reason for the middle step of the QoS 2 PUBLISH delivery process.
    ///
    /// See [MQTT5 PUBREL Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901144)
    pub reason_code: PubrelReasonCode,

    /// Additional diagnostic information about the ongoing QoS 2 PUBLISH delivery process.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901147)
    pub reason_string: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901148)
    pub user_properties: Option<Vec<UserProperty>>,
}

/// Data model of an [MQTT5 SUBACK](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901171) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SubackPacket {

    /// Id of the unsubscribe this packet is acknowledging
    pub packet_id: u16,

    /// Additional diagnostic information about the result of the SUBSCRIBE attempt.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901176)
    pub reason_string: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901177)
    pub user_properties: Option<Vec<UserProperty>>,

    /// A list of reason codes indicating the result of each individual subscription entry in the
    /// associated SUBSCRIBE packet.
    ///
    /// See [MQTT5 Suback Payload](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901178)
    pub reason_codes: Vec<SubackReasonCode>,
}

/// Data model of an [MQTT5 SUBSCRIBE](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901161) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SubscribePacket {

    /// Packet Id of the subscribe.  Setting this value on an outbound subscribe has no effect on the
    /// actual packet id used by the client.
    pub packet_id: u16,

    /// List of topic filter subscriptions that the client wishes to listen to
    ///
    /// See [MQTT5 Subscribe Payload](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901168)
    pub subscriptions: Vec<Subscription>,

    /// A positive integer to associate with all subscriptions in this request.  Publish packets that match
    /// a subscription in this request should include this identifier in the resulting message.
    ///
    /// See [MQTT5 Subscription Identifier](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901166)
    pub subscription_identifier: Option<u32>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901167)
    pub user_properties: Option<Vec<UserProperty>>,
}

/// Data model of an [MQTT5 UNSUBACK](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901187) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UnsubackPacket {

    /// Id of the unsubscribe this packet is acknowledging
    pub packet_id: u16,

    /// Additional diagnostic information about the result of the UNSUBSCRIBE attempt.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901192)
    pub reason_string: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901193)
    pub user_properties: Option<Vec<UserProperty>>,

    /// A list of reason codes indicating the result of unsubscribing from each individual topic filter entry in the
    /// associated UNSUBSCRIBE packet.
    ///
    /// See [MQTT5 Unsuback Payload](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901194)
    pub reason_codes: Vec<UnsubackReasonCode>,
}

/// Data model of an [MQTT5 UNSUBSCRIBE](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901179) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UnsubscribePacket {

    /// Packet Id of the unsubscribe.  Setting this value on an outbound unsubscribe has no effect on the
    /// actual packet id used by the client.
    pub packet_id: u16,

    /// List of topic filters that the client wishes to unsubscribe from.
    ///
    /// See [MQTT5 Unsubscribe Payload](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901185)
    pub topic_filters: Vec<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901184)
    pub user_properties: Option<Vec<UserProperty>>,
}

/// Algebraic union of all MQTT5 packet types.
#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) enum MqttPacket {
    Connect(ConnectPacket),
    Connack(ConnackPacket),
    Publish(PublishPacket),
    Puback(PubackPacket),
    Pubrec(PubrecPacket),
    Pubrel(PubrelPacket),
    Pubcomp(PubcompPacket),
    Subscribe(SubscribePacket),
    Suback(SubackPacket),
    Unsubscribe(UnsubscribePacket),
    Unsuback(UnsubackPacket),
    Pingreq(PingreqPacket),
    Pingresp(PingrespPacket),
    Disconnect(DisconnectPacket),
    Auth(AuthPacket),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
/// An enum indicating the kind of MQTT packet
pub enum PacketType {
    /// A [Connect](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901033) packet
    Connect,

    /// A [Connack](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901074) packet
    Connack,

    /// A [Publish](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901100) packet
    Publish,

    /// A [Puback](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901121) packet
    Puback,

    /// A [Pubrec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901131) packet
    Pubrec,

    /// A [Pubrel](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901141) packet
    Pubrel,

    /// A [Pubcomp](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901151) packet
    Pubcomp,

    /// A [Subscribe](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901161) packet
    Subscribe,

    /// A [Suback](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901171) packet
    Suback,

    /// An [Unsubscribe](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901179) packet
    Unsubscribe,

    /// An [Unsuback](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901187) packet
    Unsuback,

    /// A [Pingreq](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901195) packet
    Pingreq,

    /// A [Pingresp](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901200) packet
    Pingresp,

    /// A [Disconnect](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901205) packet
    Disconnect,

    /// An [Auth](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901217) packet
    Auth,
}

impl fmt::Display for PacketType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PacketType::Connect => { write!(f, "ConnectPacket") }
            PacketType::Connack => { write!(f, "ConnackPacket") }
            PacketType::Publish => { write!(f, "PublishPacket") }
            PacketType::Puback => { write!(f, "PubackPacket") }
            PacketType::Pubrec => { write!(f, "PubrecPacket") }
            PacketType::Pubrel => { write!(f, "PubrelPacket") }
            PacketType::Pubcomp => { write!(f, "PubcompPacket") }
            PacketType::Subscribe => { write!(f, "SubscribePacket") }
            PacketType::Suback => { write!(f, "SubackPacket") }
            PacketType::Unsubscribe => { write!(f, "UnsubscribePacket") }
            PacketType::Unsuback => { write!(f, "UnsubackPacket") }
            PacketType::Pingreq => { write!(f, "PingreqPacket") }
            PacketType::Pingresp => { write!(f, "PingrespPacket") }
            PacketType::Disconnect => { write!(f, "DisconnectPacket") }
            PacketType::Auth => { write!(f, "AuthPacket") }
        }
    }
}