# connect-plaintext-tokio

[**Return to main sample list**](../README.md)

This example illustrates how to connect to a message broker over TCP with a tokio-based MQTT client.

## Prerequisites
You must have access to an MQTT broker that accepts plaintext TCP connections.  Mosquitto is a common choice for
local installations; by default mosquitto's plaintext port is `1883`.  In that case, using either `localhost:1883` or `127.0.0.1:1883`
as the endpoint should lead to a successful connection.

## How to run

```
cargo run -p connect-plaintext-tokio -- <broker endpoint>
```

Assuming correct parameters and broker setup, you should see:

```
connect-plaintext-tokio - an example connecting to an MQTT broker over TCP using a tokio-based client

Connecting to <broker endpoint>...

Attempting to connect!
Connection attempt successful!
```
