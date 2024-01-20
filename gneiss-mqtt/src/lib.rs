/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
This crate provides clients for communicating with a message broker using the MQTT5 protocol.

MQTT is a publish/subscribe protocol commonly used in IoT use cases.  MQTT5 is the latest
version of the protocol.  The previous version, MQTT311, has also seen widespread adoption
and will eventually be supported in an upcoming release.

# Usage

To use this crate, you'll first need to add it to your project's Cargo.toml:

```toml
[dependencies]
gneiss-mqtt = "0.2"
```

(Temporary) If your project does not include [`tokio`](https://crates.io/crates/tokio), you will need to add it too:

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
```

Future releases will support other async runtimes as well as a client that runs in a background
thread and does not need an async runtime.  For now, [`tokio`](https://crates.io/crates/tokio) is required.

This crate contains all the building blocks necessary to connect to most MQTT brokers, but
the configuration to do so can be confusing and delicate.  For complex cases, we recommend
using broker-specific crates that implement all the low-level connector details needed to
successfully establish an MQTT connection to a specific broker.  The documentation for these
specialized crates contains samples and information on how to connect in all of the ways
each broker supports.

Currently, these crates include:
* *[gneiss-mqtt-aws](https://crates.io/crates/gneiss-mqtt-aws)* - A crate with a builder that
supports all connection methods allowed by the AWS MQTT broker implementation,
[AWS IoT Core](https://docs.aws.amazon.com/iot/latest/developerguide/iot-gs.html).

# Example: Connect to a local Mosquitto server

Assuming a default Mosquitto installation, you can connect locally by plaintext on port 1883:

```no_run
use gneiss_mqtt::config::GenericClientBuilder;
use tokio::runtime::Handle;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // In the common case, you will not need a root CA certificate
    let client =
        GenericClientBuilder::new("127.0.0.1", 1883)
            .build(&Handle::current())?;

    // Once started, the client will recurrently maintain a connection to the endpoint until
    // stop() is invoked
    client.start()?;

    // <do stuff with the client>

    Ok(())
}
```

# Example: React to client events

TODO

# Additional Notes

The intention is that this crate will eventually be as agnostic as possible of underlying
implementation details (async runtimes, TLS/transport implementations, etc...)
but at present it has hard dependencies on tokio, rustls, and some
associated helper libraries.  These will get feature-flag-gated before GA, allowing the user
to pare the implementation down to their exact connection needs.  In Rust's current state,
there is a fundamental tension between trying to be transport/runtime agnostic and trying
to provide an easy-to-use interface for getting successful clients set up for the many
different combinations expected by users.

This crate's public API is expected to be very unstable until v0.5.0.  See the roadmap
in the README for more details.
*/

#![warn(missing_docs)]

pub mod alias;
pub mod client;
pub mod config;
mod decode;
mod encode;
pub mod features;
mod logging;
mod operation;
pub mod spec;
mod validate;

/* Re-export all spec types at the root level */
pub use spec::QualityOfService;
pub use spec::PayloadFormatIndicator;
pub use spec::RetainHandlingType;
pub use spec::ConnectReasonCode;
pub use spec::PubackReasonCode;
pub use spec::PubrecReasonCode;
pub use spec::PubrelReasonCode;
pub use spec::PubcompReasonCode;
pub use spec::DisconnectReasonCode;
pub use spec::SubackReasonCode;
pub use spec::UnsubackReasonCode;
pub use spec::AuthenticateReasonCode;
pub use spec::UserProperty;
pub use spec::Subscription;

pub use spec::auth::AuthPacket;
pub use spec::connack::ConnackPacket;
pub use spec::connect::ConnectPacket;
pub use spec::disconnect::DisconnectPacket;
pub use spec::pingreq::PingreqPacket;
pub use spec::pingresp::PingrespPacket;
pub use spec::puback::PubackPacket;
pub use spec::pubcomp::PubcompPacket;
pub use spec::publish::PublishPacket;
pub use spec::pubrec::PubrecPacket;
pub use spec::pubrel::PubrelPacket;
pub use spec::suback::SubackPacket;
pub use spec::subscribe::SubscribePacket;
pub use spec::unsuback::UnsubackPacket;
pub use spec::unsubscribe::UnsubscribePacket;
pub use spec::utils::{
    convert_u8_to_disconnect_reason_code,
    convert_u8_to_quality_of_service,
};

use std::error::Error;
use std::fmt;
use std::time::Instant;

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

#[derive(Debug)]
pub struct TestContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

/// Basic error type for the entire gneiss-mqtt crate.
#[derive(Debug)]
#[non_exhaustive]
pub enum MqttError {

    /// An error where no root cause could be determined.  Generally an indication of
    /// a bug in an interior system.  Emitted when the client encounters a state transition
    /// where its internal error field should have been set (earlier) but instead it
    /// is clear.
    Unknown,

    /// Functionality was invoked that has not yet been implemented.
    Unimplemented,

    /// A client encountered an error awaiting on its incoming operation channel.  This is usually
    /// due to the user dropping the client which in turn drops the channel sender.
    ///
    /// TODO: if this does not send the client into a terminal state, it should.
    /// TODO: add underlying error as source
    OperationChannelReceiveError,

    /// There was an error attempting to submit an operation to the client via the operation channel.
    /// This usually means the client was closed, which dropped the channel receiver.
    /// TODO: add underlying error as source
    OperationChannelSendError,

    /// Error emitted when packet's encoding would produce a VLI value that exceeds the protocol
    /// maximum (2 ^ 28 - 1)
    VariableLengthIntegerMaximumExceeded,

    /// Error emitted by the client when the encoder buffer does not have capacity to encode at
    /// least 4 bytes.  Should never be seen.
    EncodeBufferTooSmall,

    /// Error emitted when a packet is received with a variable length integer whose encoding
    /// does not conform to the MQTT spec
    DecoderInvalidVli,

    /// Error emitted when an invalid packet encoding is received.  This is distinct from
    /// errors that arise from packets that validate protocol behavior specifications.
    ///
    /// MalformedPacket examples include bad header flags, mismatches between remaining length
    /// fields and overall packet length, etc...
    MalformedPacket,

    /// Generic error emitted when the client encounters broker behavior that violates the MQTT
    /// specification in a way that cannot be safely ignored or recovered from.
    ///
    /// More specific errors exist for a variety of protocol violations.
    ProtocolError,

    /// Error emitted when an inbound publish arrives with an unknown topic alias.
    InboundTopicAliasNotValid(TestContext),

    /// Error emitted when an Auth packet is submitted or received that violates the MQTT
    /// specification.
    PacketValidation(PacketType),

    /// Error emitted by the client when something happens that should never happen.  Always indicates
    /// a bug in the client.
    InternalStateError,

    /// Error emitted when the broker explicitly rejects a connection attempt by sending a Connack
    /// packet with a failing reason code.  The connect failure client event will contain the
    /// full Connack packet and can be inspected for further diagnostics.
    ConnectionRejected,

    /// Error emitted by the client when the broker does not respond to a Connect packet within
    /// the configured timeout interval.
    /// TODO: Add duration?
    ConnackTimeout,

    /// Error emitted when the client shuts down a connection due to the broker not responding to
    /// a Pingreq packet.  Generally indicates that connectivity between the client and broker is
    /// broken.
    /// TODO: Add duration?
    PingTimeout,

    /// Error emitted when the client's connection gets closed for some external reason.  Usually
    /// this is the broker hanging up on the client, but intermediary network failures may trigger
    /// this as well.
    /// TODO: applied to multiple, cannot add context
    ConnectionClosed,

    /// Error applied to MQTT operaations that are failed because the client is offline and the
    /// configured offline policy rejects the operation.
    /// TODO: applied to multiple, cannot add context
    OfflineQueuePolicyFailed,

    /// Error emitted when the broker sends a Disconnect packet to indicate the connection is
    /// being shut down.
    ServerSideDisconnect,

    /// Error applied to user-submitted operations that indicates the operation failed because
    /// we did not receive an Ack packet within the operation's timeout interval.
    /// TODO: Add duration?
    AckTimeout,

    /// Error indicating no more packet ids are available for outbound packets.  Should never
    /// happen; indicates a bad client bug.
    PacketIdSpaceExhausted,

    /// Error applied to all unfinished client operations when the client is closed by the user.
    OperationalStateReset,

    /// Error emitted by the client after sending a user-submitted Disconnect packet as a part
    /// of a `stop()` invocation.  Does not indicate an actual failure.
    UserInitiatedDisconnect,

    /// Error emitted by the client when a connection attempt (the interval between starting
    /// the connection and it being ready for an MQTT Connect packet) times out
    /// TODO: Add duration?
    ConnectionTimeout,

    /// Error emitted by the client when a connection is rejected prior to the Connect <-> Connack
    /// handshake
    /// TODO: add underlying error as source
    ConnectionEstablishmentFailure,

    /// Error emitted by the client when it fails to write data to the socket.  This is a
    /// connection-fatal event; it does not represent socket-buffer-full.
    /// TODO: add underlying error as source
    StreamWriteFailure,

    /// Error emitted by the client when it fails to read data from the socket.  This is a
    /// connection-fatal event; it does not represent no-data-ready.
    /// TODO: add underlying error as source
    StreamReadFailure,

    /// Test Only - an operation's result should have been in the output channel for the
    /// operation and was not
    #[cfg(test)]
    OperationChannelEmpty,

    /// Generic error associated with reading TLS configuration data from the filesystem
    /// TODO: add underlying error as source, make more specific if desc is accurate
    IoError,

    /// Generic error associated with parsing TLS configuration from memory or applying it to a
    /// TLS context
    /// TODO: add underlying error as source
    TlsError
}

impl MqttError {
    pub fn new_inbound_topic_alias_not_valid(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        MqttError::InboundTopicAliasNotValid(
            TestContext{
                source : source.into()
            }
        )
    }
}

impl Error for MqttError {
}

impl fmt::Display for MqttError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MqttError::Unknown => { write!(f, "unknown - no root cause could be determined") }
            MqttError::Unimplemented => { write!(f, "unimplemented - attempt to invoke functionality that has not been completed") }
            MqttError::OperationChannelReceiveError => { write!(f, "operation channel receive error - client operation sender has been dropped") }
            MqttError::OperationChannelSendError => { write!(f, "operation channel send error - client has been closed") }
            MqttError::VariableLengthIntegerMaximumExceeded => { write!(f, "variable length integer maximum exceeded") }
            MqttError::EncodeBufferTooSmall => { write!(f, "encode buffer too small - mqtt encoder requires at least 4 bytes") }
            MqttError::DecoderInvalidVli => { write!(f, "decoder invalid vli - received a packet with an invalid vli encoding") }
            MqttError::MalformedPacket => { write!(f, "malformed packet - received a packet whose encoding properties violate the mqtt spec") }
            MqttError::ProtocolError => { write!(f, "protocol error - broker behavior disallowed by the mqtt spec") }
            MqttError::InboundTopicAliasNotValid(_) => { write!(f, "inbound topic alias not valid - incoming publish contained an invalid topic alias") }
            MqttError::PacketValidation(packet_type) => { write!(f, "{} contains a property that violates the mqtt spec", packet_type) }
            MqttError::InternalStateError => { write!(f, "internal state error - client reached an invalid internal state, almost certainly a client bug") }
            MqttError::ConnectionRejected => { write!(f, "connack rejected - the broker explicitly rejected the connect packet") }
            MqttError::ConnackTimeout => { write!(f, "connack timeout - the broker did not respond in time to the connect packet") }
            MqttError::PingTimeout => { write!(f, "ping timeout - the broker did not response in time to the pingreq packet; likely the connection was unhealthy") }
            MqttError::ConnectionClosed => { write!(f, "connection closed - the client's connection was closed due to some external reason") }
            MqttError::OfflineQueuePolicyFailed => { write!(f, "offline queue policy failed - operation failed due to the offline queue policy and the fact that the client is currently offline") }
            MqttError::ServerSideDisconnect => { write!(f, "server side disconnect - connection was shut down after receiving a disconnect packet from the broker") }
            MqttError::AckTimeout => { write!(f, "ack timeout - the operation's timeout triggered prior to receiving an ack from the broker") }
            MqttError::PacketIdSpaceExhausted => { write!(f, "packet id space exhausted - no packet ids remain; should never happen") }
            MqttError::OperationalStateReset => { write!(f, "operational state reset - the operation was not complete prior to the client being closed") }
            MqttError::UserInitiatedDisconnect => { write!(f, "user-initiated disconnect - connection was shut down by user action") }
            MqttError::ConnectionTimeout => { write!(f, "connection timeout - a transport-level connection to the broker could not be established before timeout") }
            MqttError::ConnectionEstablishmentFailure => { write!(f, "connection establishment failure - failure to establish a transport-level connection to the broker") }
            MqttError::StreamWriteFailure => { write!(f, "stream write failure - error attempting to write or flush a connection stream") }
            MqttError::StreamReadFailure => { write!(f, "stream read failure - error when attempting to read a connection stream") }
            #[cfg(test)]
            MqttError::OperationChannelEmpty => { write!(f, "operation channel empty - testing encountered a situation where an operation result was expected to be in the output channel and was not") }
            MqttError::IoError => { write!(f, "io error - generic error due to an error operating on the connection's network stream") }
            MqttError::TlsError => { write!(f, "tls error - generic error when setting up a tls context") }
        }
    }
}

impl From<std::io::Error> for MqttError {
    fn from(_: std::io::Error) -> Self {
        MqttError::IoError
    }
}

/// Crate-wide result type for functions that can fail
pub type MqttResult<T> = Result<T, MqttError>;

fn fold_mqtt_result<T>(base: MqttResult<T>, new_result: MqttResult<T>) -> MqttResult<T> {
    new_result?;
    base
}

fn fold_timepoint(base: &Option<Instant>, new: &Instant) -> Option<Instant> {
    if let Some(base_timepoint) = &base {
        if base_timepoint < new {
            return *base;
        }
    }

    Some(*new)
}

fn fold_optional_timepoint_min(base: &Option<Instant>, new: &Option<Instant>) -> Option<Instant> {
    if let Some(base_timepoint) = base {
        if let Some(new_timepoint) = new {
            if base_timepoint < new_timepoint {
                return *base;
            } else {
                return *new;
            }
        }

        return *base;
    }

    *new
}
