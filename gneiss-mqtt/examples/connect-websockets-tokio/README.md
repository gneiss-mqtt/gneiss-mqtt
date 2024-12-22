# connect-websockets-tokio

[**Return to main sample list**](../README.md)

This example illustrates how to connect to a message broker over TCP using websockets with a tokio-based MQTT client.

## Prerequisites
You must have access to an MQTT broker configured to accept websocket connections.  Mosquitto is a common choice for
local installations.  Configuring mosquitto to accept websocket connections is beyond the scope of this example; tutorials
can be found on the web.  

## How to run

```
cargo run -p connect-websockets-tokio -- <broker endpoint>
```

Assuming correct parameters and broker setup, you should see:

```
connect-websockets-tokio - an example connecting to an MQTT broker with websockets over TCP using a tokio-based client

Connecting to <broker endpoint>...

Attempting to connect!
Connection attempt successful!
```
