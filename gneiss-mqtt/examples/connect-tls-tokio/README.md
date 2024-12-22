# connect-tls-tokio

[**Return to main sample list**](../README.md)

This example illustrates how to connect to a message broker over TCP using TLS with a tokio-based MQTT client.

## Prerequisites
You must have access to an MQTT broker configured to accept TLS connections.  Mosquitto is a common choice for
local installations.  Configuring mosquitto to accept TLS connections is beyond the scope of this example; tutorials
can be found on the web.  If you use a local mosquitto install, you will also need to supply the broker's root CA on 
the command line.

## How to run

```
cargo run -p connect-tls-tokio -- --rootca <path-to-broker-root-ca> <broker endpoint>
```

Assuming correct parameters and broker setup, you should see:

```
connect-tls-tokio - an example connecting to an MQTT broker with TLS over TCP using a tokio-based client

Connecting to <broker endpoint>...

Attempting to connect!
Connection attempt successful!
```
