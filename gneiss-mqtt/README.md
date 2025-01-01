## gneiss-mqtt

Tokio (async) and thread-based MQTT client implementations.

### Spec Compliance Notes
Gneiss-mqtt supports all aspects of the MQTT311 and MQTT5 specifications with the following exceptions:
* **Extended Utf-8 Validation** - No client-side utf-8 validation is done beyond what the Rust standard library does for strings.  The MQTT5 specification imposes additional constraints that the Rust standard library does not check.
* **Authentication** - There is no API for MQTT5 authentication exchanges.  Auth packet encode/decode/validation is implemented, but currently, authentication is assumed to be accomplished via lower-level or custom protocol details (mTLS, websocket upgrade signing, username/password schemes, etc...).  Crates for specific authentication styles are coming soon.  Authentication exchanges are a long-term roadmap item.
* **Client Queue Receive Maximum Blocking** - The MQTT5 spec requires clients to not block non-publish packets when the receive maximum threshold is met by the current session.  It is the opinion of the author that this is an unnecessary restriction that wants to express "don't delay acks or pings for receive maximum" but uses something coarser/clumsier because that notion is difficult to express in formal specification language.  In gneiss-mqtt, Subscribes and Unsubscribes are also blocked by the receive maximum state.  Contrary feedback is welcome here.
* **Broker Forgiveness** - The client is not 100% strict on broker behavior validation.  While many protocol violations will result in the client closing the connection, not all will.  In particular, violations that don't disrupt critical invariants or implementation configurations tend to be allowed.  For example, if the broker sends a larger packet size than what the client says was allowed, we do not disconnect.  On the other hand, if the broker sends a publish packet with an unknown alias and without a topic, then we do disconnect because we can't handle it.  We might revisit this forgiveness in the future and allow for a strict compliance mode, but it is the opinion of the author that rigid/uncompromising compliance validation leads to brittle applications.

### Roadmap
See [Gneiss MQTT Roadmap](https://github.com/gneiss-mqtt/gneiss-mqtt/blob/main/README.md#roadmap)

## License

This library is licensed under the Apache 2.0 License. 
