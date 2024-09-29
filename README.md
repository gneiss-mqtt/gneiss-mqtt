## gneiss-mqtt

A suite of MQTT-related projects in Rust.

Feedback is always welcome.

### Projects
* **[gneiss-mqtt](https://crates.io/crates/gneiss-mqtt)** - Async and thread-based MQTT5 clients
* **[gneiss-mqtt-aws](https://crates.io/crates/gneiss-mqtt-aws)** - Builder crate to connect gneiss MQTT clients to AWS IoT Core
* **elasti-gneiss** - (Unpublished, Workspace only) Interactive client test application using gneiss-mqtt
* **elasti-gneiss-aws** - (Unpublished, Workspace only) Interactive client test application using gneiss-mqtt-aws to connect to AWS IoT Core

### Roadmap
* 0.4.0 Release 
* * Background thread client for no-async support
* 0.5.0 Release
* * crate refactor/split to reduce complexity
* * Client features - throttled resubmit on reconnect, max retries on delivery
* 0.6.0 Release
* * MQTT311 support
* * MQTT5 authentication exchange support
* 0.7.0
* * Intern/reimplement unwanted/glue/wrapper dependencies
* * Non-AWS broker builders (Azure, HiveMQ, etc...)
* * Performance measurements and tuning
* 1.0.0 Release 
* * CI/CD
* * Canaries, soak testing, samples
* 1.1.0 Release
* * Request-response service clients for AWS MQTT services: shadow, jobs, identity
* 1.2.0 Release
* * Bridging support
* 1.3.0 Release
* * Persistence support

## License

All projects in this space are licensed under the Apache 2.0 License. 
