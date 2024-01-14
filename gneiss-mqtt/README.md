## gneiss-mqtt

Rust MQTT5 client implementation.

This project is currently in pre-dev-preview.  You can use it, but there's a lot of friction and missing documentation/examples.

Feedback is always welcome.  Interested contributors are also welcome.

### Spec Compliance Notes
Gneiss-mqtt supports all aspects of the MQTT5 specification with the following exceptions:
* **Extended Utf-8 Validation** - No client-side utf-8 validation is done beyond what the Rust standard library does for strings.  The MQTT5 specification imposes additional constraints that the Rust standard library does not check.
* **Authentication** - There is no API for MQTT5 authentication exchanges.  Auth packet encode/decode/validation is implemented, but currently, authentication is assumed to be accomplished via lower-level or custom protocol details (mTLS, websocket upgrade signing, username/password schemes, etc...).  Crates for specific authentication styles are coming soon.  Authentication exchanges are a long-term roadmap item.
* **Client Queue Receive Maximum Blocking** - The MQTT5 spec requires clients to not block non-publish packets when the receive maximum threshold is met by the current session.  It is the opinion of the author that this is an unnecessary restriction that wants to express "don't delay acks or pings for receive maximum" but uses something coarser/clumsier because that notion is difficult to express in formal specification language.  In gneiss-mqtt, Subscribes and Unsubscribes are also blocked by the receive maximum state.  Contrary feedback is welcome here.
* **Broker Forgiveness** - The client is not 100% strict on broker behavior validation.  While many protocol violations will result in the client closing the connection, not all will.  In particular, violations that don't disrupt critical invariants or implementation configurations tend to be allowed.  For example, if the broker sends a larger packet size than what the client says was allowed, we do not disconnect.  On the other hand, if the broker sends a publish packet with an unknown alias and without a topic, then we do disconnect because we can't handle it.  We might revisit this forgiveness in the future and allow for a strict compliance mode, but it is the opinion of the author that rigid/uncompromising compliance validation leads to brittle applications.

### Tentative User-Facing Roadmap (dates non-binding)
* 0.2.0 Release (January 2024)
* * MQTT-over-websockets support
* * Builders for common transport options (mqtt/mqtts/ws/wss)
* * Builders for AWS IoT Core (mtls, websockets via sig4 signing, custom auth)
* * Http proxy support
* * Complete API documentation
* 0.3.0 Release (March 2024)
* * Internal refactoring and unit/integration tests that were delayed once "it worked"
* * Background thread client for no-async support
* * Support async-std as a runtime option
* * Support native-tls as a TLS option
* * CI/CD
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

### Release History
* 0.1.0 - Initial release for name selection.  

## License

This library is licensed under the Apache 2.0 License. 
