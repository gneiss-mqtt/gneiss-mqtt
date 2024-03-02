# Gneiss MQTT Change document
This document is currently hand-written and non-authoritative.

## 0.1.0 
* Initial release for name selection.

## 0.2.0 
* Generic Client builder (tokio only)
* * Tls config and support (rustls only)
* * Http proxy support
* * MQTT over websockets support
* * Initial crate documentation

## 0.3.0
* Refactoring/Chores
* * Errors refactored to be composable and include additional context
* * Errors consolidated into a much smaller set
* * Logging added to client, configuration, and connection establishment
* * Logging of packets and state changed to single-line
* * spec module renamed to mqtt
* * operation module renamed to protocol
* * removed all re-exports from top-level crate
* * added public Into implementations for a few spec enums
* * broker encode/decode functionality needed for testing wrapped in test feature checks
* * default client event listener injection moved from client config to the start() client method
* * packet model refactoring
* * * all packet fields crate-internal
* * * add accessors for user-facing packet fields
* * * builder types for all user-submitted packets: Disconnect, Publish, Subscribe, Unsubscribe
* * timeout polish
* * * remove connack timeout as a setting, connect timeout now covers the end-to-end interval from socket open to connack receipt
* * * fixed an edge case when ping timeout is larger than half the keep alive setting
* TLS
* * alpn values changed to a string representation
* * Rustls configuration build errors now a wrapped error
* Features
* * Rustls support feature-gated
* * Native-tls support added and feature-gated
