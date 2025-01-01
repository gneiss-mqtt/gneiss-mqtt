## gneiss-mqtt

A suite of MQTT-related projects in Rust.

### Projects
* **[gneiss-mqtt](https://crates.io/crates/gneiss-mqtt)** - Async and thread-based MQTT clients
* **[gneiss-mqtt-aws](https://crates.io/crates/gneiss-mqtt-aws)** - Builder-and-glue crate to connect gneiss MQTT clients to AWS IoT Core

### Roadmap
* gneiss-mqtt 0.5.0 Release
* * Client features - throttled resubmit on reconnect, max retries on delivery
* * MQTT311 support
* AWS Iot Request-Response service clients
* * New crates with service clients for AWS IoT Shadow, Jobs, Identity, and Command services 
* gneiss-mqtt 0.6.0
* * Intern/reimplement/reevaluate unwanted/glue/wrapper dependencies
* gneiss-mqtt 1.0.0 Release 
* * CI/CD
* * Performance measurements and tuning
* * Canaries, soak testing
* gneiss-mqtt 1.1.0 Release
* * Bridging support
* gneiss-mqtt 1.2.0 Release
* * MQTT5 authentication exchange support
* gneiss-mqtt 1.3.0 Release
* * Persistence support

## License

All projects in this space are licensed under the Apache 2.0 License. 
