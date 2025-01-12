# Frequently Asked Questions

*__Jump To:__*
* [Where should I start?](#where-should-i-start)
* [How do I enable logging?](#how-do-i-enable-logging)
* [How do I use MQTT311?](#how-do-i-use-mqtt-311)
* [How can I connect to AWS IoT Core?](#how-can-i-connect-to-aws-iot-core)
* [How can I connect to AWS IoT Core using the threaded client and websockets?](#how-can-i-connect-to-aws-iot-core-using-the-threaded-client-and-websockets)
* [Why are there so many dependencies?](#why-are-there-so-many-dependencies)

### Where should I start?
This project contains a significant number of examples to get started from.  
* [Connection Examples](https://github.com/gneiss-mqtt/gneiss-mqtt/tree/main/gneiss-mqtt/examples) - show how to configure a variety of transport options (TLS, websockets, HTTP proxy) for establishing connections.
* [Pubsub Examples](https://github.com/gneiss-mqtt/gneiss-mqtt/tree/main/gneiss-mqtt/examples) - demonstrate how to subscribe, publish, and unsubscribe with both the threaded and tokio clients
* [AWS Connection Examples](https://github.com/gneiss-mqtt/gneiss-mqtt/tree/main/gneiss-mqtt-aws/examples) which demonstrate how to connect using each of AWS IoT Core's supported authentication approaches.

### How do I enable logging?
This project uses the [log](https://docs.rs/log/latest/log/) crate to log events of interest.  To enable logging you'll have to integrate
a logger implementation (see [logger suggestions](https://docs.rs/log/latest/log/#available-logging-implementations) for some suggestions)
into your project and then configure a logger instance at startup.

For example, the `elasti-gneiss` [command line executables](https://github.com/gneiss-mqtt/gneiss-mqtt/tree/main/elastigneiss) support 
logging to a file via the [simplelog](https://docs.rs/simplelog/latest/simplelog/) crate:

In Cargo.toml
```
[dependencies]
simplelog = { version = "..." }
```

and in code
```rust
let log_file = std::fs::File::create(log_file_path)?;
let log_config = simplelog::ConfigBuilder::new().build();
simplelog::WriteLogger::init(simplelog::LevelFilter::Debug, log_config, log_file)?;
```

### How do I use MQTT 311?
By default, gneiss clients use MQTT 5.  If your broker does not support MQTT5, you can use the `with_protocol_mode` API 
on `MqttClientOptionsBuilder` to configure clients to use MQTT 311 instead:

```rust
let client_options = MqttClientOptions::builder()
    .with_protocol_mode(ProtocolMode::Mqtt311)
    .build();

let client = ThreadedClientBuilder::new(&endpoint, port)
    .with_client_options(client_options)
    .build()?;
```

### How can I connect to AWS IoT Core?
The [gneiss-mqtt-aws](https://docs.rs/gneiss-mqtt-aws/latest/gneiss_mqtt_aws/) crate contains a client builder that 
supports all authentication mechanisms supported by AWS IoT Core.  See
the [Connection Examples](https://github.com/gneiss-mqtt/gneiss-mqtt/tree/main/gneiss-mqtt-aws/examples) for examples 
how to use each authentication approach.

### How can I connect to AWS IoT Core using the threaded client and websockets?
This is not currently supported.  The credentials providers used for sourcing AWS credentials are tokio-only.  This use
case may eventually be supported through an interface, but there are no plans to implement thread-based credentials
providers.

### Why are there so many dependencies?
This project supports a significant number of compile-time features, primarily relating to transport establishment.  We recommend only
enabling the features necessary for your transport needs.  In a future release, dependencies will be carefully evaluated and pruned as
needed to support versioning health and minimize compile times.

