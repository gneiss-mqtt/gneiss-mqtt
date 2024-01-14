## gneiss-mqtt

A suite of MQTT-related projects in Rust.

Feedback is always welcome.

### Projects
* **[gneiss-mqtt](https://crates.io/crates/gneiss-mqtt)** - Async and thread-based MQTT5 clients
* **[gneiss-mqtt-aws](https://crates.io/crates/gneiss-mqtt-aws)** - Builder crate to connect gneiss MQTT clients to AWS IoT Core
* **elasti-gneiss** - (Unpublished, Workspace only) Interactive client test application using gneiss-mqtt
* **elasti-gneiss-aws** - (Unpublished, Workspace only) Interactive client test application using gneiss-mqtt-aws to connect to AWS IoT Core

### Tentative User-Facing Roadmap (dates non-binding)
* 0.2.0 Release (January 2024)
* * MQTT-over-websockets support
* * Builders for common transport options (mqtt/mqtts/ws/wss)
* * Builders for AWS IoT Core (mtls, websockets via sig4 signing, custom auth)
* * Http proxy support
* * Complete API documentation
* 0.3.0 Release (April 2024)
* * Internal refactoring and unit/integration tests that were delayed once "it worked"
* * Background thread client for no-async support
* * Support async-std as a runtime option
* * Support native-tls as a TLS option
* * CI/CD
* * Misc chores: error unification and polish
* * Misc chores: conversion impls
* * Misc chores: logging/display/debug polish
* 0.4.0 Release (July 2024)
* * MQTT311 support
* 1.0.0 Release (GA, September 2024)
* * Public API finalization
* * Performance measurements and tuning
* * Canaries, soak testing, samples
* * Non-AWS cloud provider builders (Azure, HiveMQ, etc...)
* 1.1.0 Release
* * Bridging support
* 1.2.0 Release
* * MQTT5 authentication exchange support
* 1.3.0 Release
* * Persistence support

### Additional post GA ideas
* Request-response client crates for known MQTT services (AWS Shadow, Jobs, Fleet Provisioning, etc...)
* gneiss-mqtt-embedded: Embedded implementation targeting highly-constrained environments

## License

All projects in this space are licensed under the Apache 2.0 License. 
