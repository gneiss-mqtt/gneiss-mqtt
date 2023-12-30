## gneiss-mqtt

Rust implementation of an MQTT 5 client.  

This project is currently in pre-dev-preview.  You can use it, but there's a lot of friction and missing documentation/examples.

Feedback is always welcome.

### Spec Compliance Notes
Gneiss-mqtt supports all aspects of the MQTT5 specification with the following exceptions:
* **Extended Utf-8 Validation** - No client-side utf-8 validation is done beyond what the Rust standard library does for strings.  The MQTT5 specification imposes additional constraints that the Rust standard library does not check.
* **Authentication** - There is no API for MQTT5 authentication exchanges.  Currently, authentication is assumed to be accomplished via lower-level or custom protocol details (mTLS, websocket upgrade signing, username/password schemes, etc...).  Crates for specific authentication styles are coming soon.  Authentication exchanges are a long-term roadmap item.
* **Client Queue Receive Maximum Blocking** - The MQTT5 spec requires clients to not block non-publish packets when the receive maximum threshold is met by the current session.  It is the opinion of the author that this is an unnecessary restriction that wants to express "don't delay acks or pings for receive maximum" but uses something coarser/clumsier because that notion is difficult to express in formal specification language.  In gneiss-mqtt, Subscribes and Unsubscribes are also blocked by the receive maximum state.  Contrary feedback is welcome here.
* **Broker Forgiveness** - The client is not 100% strict on broker behavior validation.  While many protocol violations will result in the client closing the connection, not all will.  In particular, violations that don't disrupt critical invariants or implementation configurations tend to be allowed.  For example, if the broker sends a larger packet size than what the client says was allowed, we do not disconnect.  We might revisit this forgiveness in the future and allow for a strict compliance mode, but it is the opinion of the author that rigid/uncompromising compliance validation leads to brittle applications.

### Tentative User-Facing Roadmap to dev preview (unordered)
* A lot of internal refactoring and unit/integration tests that were delayed once "it worked"
* Builder crates for connection modes (direct, websockets) and AWS-focused specializations (direct, websockets with sigv4 signing, IoT Core custom auth)
* HTTP proxy support
* Background thread client for no-async support
* Support async-std as a runtime option
* Public API and documentation polish

### Tentative Roadmp post dev preview (unordered)
* Consider/reject MQTT311 support.  No matter what, will be a backwards-incompatible breaking change to the API for obvious reasons.
* MQTT bridging (user-controlled ACKs) support.
* Auth packet/exchange support.

### Tentative Roadmap post GA
* Client crates for known MQTT services (AWS Shadow, Jobs, Fleet Provisioning)
* Crates for non-AWS cloud provider connectors (Azure, etc...)
* Embedded implementation targeting highly-constrained environments

### Release History
* 0.1.0 - Initial release for name selection.  Async client is dev-preview but there's still a ton of work to be done before a stable release, let alone GA.

## License

This library is licensed under the Apache 2.0 License. 
