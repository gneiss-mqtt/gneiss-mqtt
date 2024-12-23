# pubsub-tokio

[**Return to main sample list**](../README.md)

This example illustrates how to perform MQTT operations using a tokio-based client.

## Prerequisites
You must have access to an MQTT broker that accepts plaintext TCP connections.  Mosquitto is a common choice for
local installations; by default mosquitto's plaintext port is `1883`.  In that case, using either `localhost:1883` or `127.0.0.1:1883`
as the endpoint should lead to a successful connection.

## How to run

```
cargo run -p pubsub-tokio -- <broker endpoint>
```

After connecting, the example application subscribes to a topic and then publishes a sequence of messages to that
topic.  Once finished, the example unsubscribes from the topic and then cleans shuts the client down.

Assuming correct parameters and broker setup, you should see:

```
pubsub-tokio - an example that demonstrates publish-subscribe operations with an MQTT broker over a plaintext connection

Connecting to <broker endpoint>...

Attempting to connect!
Connection attempt successful!

Subscribing to the topic 'gneiss/examples'...
Subscribe succeeded!

Performing Qos1 publish #1 to topic 'gneiss/examples` with payload 'Gneiss-1'...
Publish received on topic 'gneiss/examples' with payload 'Gneiss-1'
Publish #1 succeeded!
Performing Qos1 publish #2 to topic 'gneiss/examples` with payload 'Gneiss-2'...
Publish received on topic 'gneiss/examples' with payload 'Gneiss-2'
Publish #2 succeeded!
Performing Qos1 publish #3 to topic 'gneiss/examples` with payload 'Gneiss-3'...
Publish received on topic 'gneiss/examples' with payload 'Gneiss-3'
Publish #3 succeeded!
Performing Qos1 publish #4 to topic 'gneiss/examples` with payload 'Gneiss-4'...
Publish received on topic 'gneiss/examples' with payload 'Gneiss-4'
Publish #4 succeeded!
Performing Qos1 publish #5 to topic 'gneiss/examples` with payload 'Gneiss-5'...
Publish received on topic 'gneiss/examples' with payload 'Gneiss-5'
Publish #5 succeeded!
Performing Qos1 publish #6 to topic 'gneiss/examples` with payload 'Gneiss-6'...
Publish received on topic 'gneiss/examples' with payload 'Gneiss-6'
Publish #6 succeeded!
Performing Qos1 publish #7 to topic 'gneiss/examples` with payload 'Gneiss-7'...
Publish received on topic 'gneiss/examples' with payload 'Gneiss-7'
Publish #7 succeeded!
Performing Qos1 publish #8 to topic 'gneiss/examples` with payload 'Gneiss-8'...
Publish received on topic 'gneiss/examples' with payload 'Gneiss-8'
Publish #8 succeeded!
Performing Qos1 publish #9 to topic 'gneiss/examples` with payload 'Gneiss-9'...
Publish received on topic 'gneiss/examples' with payload 'Gneiss-9'
Publish #9 succeeded!
Performing Qos1 publish #10 to topic 'gneiss/examples` with payload 'Gneiss-10'...
Publish received on topic 'gneiss/examples' with payload 'Gneiss-10'
Publish #10 succeeded!

Done publishing!  Taking a siesta.

Unsubscribing from topic 'gneiss/examples'...
Unsubscribe succeeded!

Stopping...
Stopped!
```
