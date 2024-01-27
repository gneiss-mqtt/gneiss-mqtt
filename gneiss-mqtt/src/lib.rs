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
pub mod error;
pub mod features;
mod logging;
pub mod mqtt;
mod protocol;
mod validate;

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
