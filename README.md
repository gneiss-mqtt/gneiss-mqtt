## gneiss-mqtt

A suite of MQTT-related projects in Rust.

Feedback is always welcome.

### Projects
* **[gneiss-mqtt](https://crates.io/crates/gneiss-mqtt)** - Async and thread-based MQTT clients
* **[gneiss-mqtt-aws](https://crates.io/crates/gneiss-mqtt-aws)** - Builder-and-glue crate to connect gneiss MQTT clients to AWS IoT Core

### Tentative Roadmap
* 0.5.0 Release
* * Client features - throttled resubmit on reconnect, max retries on delivery
* * MQTT311 support
* 0.6.0 Release
* * MQTT5 authentication exchange support
* 0.7.0
* * Intern/reimplement/reevaluate unwanted/glue/wrapper dependencies
* * Performance measurements and tuning
* 1.0.0 Release 
* * CI/CD
* * Canaries, soak testing
* 1.1.0 Release
* * Request-response service clients for AWS MQTT services: shadow, jobs, identity
* 1.2.0 Release
* * Bridging support
* 1.3.0 Release
* * Persistence support

## License

All projects in this space are licensed under the Apache 2.0 License. 
