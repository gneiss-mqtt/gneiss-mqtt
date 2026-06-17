/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
This crate contains the foundational protocol implements for clients to communicate with a message broker using the MQTT protocol.
This is a support crate; actual clients are defined in downstream crates.

MQTT is a publish/subscribe protocol commonly chosen in IoT use cases.  This crate supports
both [MQTT5](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html) and
[MQTT311](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html).  We strongly
recommend using MQTT5 over 311 for the significant error handling and communication improvements.
MQTT specification links within crate documentation are made to the MQTT5 spec.

# Usage

To use this crate, you'll first need to add it to your project's Cargo.toml:

```toml
[dependencies]
gneiss-mqtt = { version = "<version>", features = [ ... ] }
```

*/

/*!
# Frequently Asked Questions
See [FAQ](https://github.com/gneiss-mqtt/gneiss-mqtt/blob/main/FAQ.md)

*/

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]
#![cfg_attr(feature = "strict", deny(warnings))]
#![allow(clippy::collapsible_match)]

pub mod alias;
pub mod client;
mod decode;
mod encode;
pub mod error;
mod logging;
pub mod mqtt;
mod protocol;
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod testing;
#[doc(hidden)]
pub mod validate;
