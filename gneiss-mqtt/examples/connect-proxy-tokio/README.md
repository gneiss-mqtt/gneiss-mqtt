# connect-proxy-tokio

[**Return to main sample list**](../README.md)

This example illustrates how to connect to a message broker through an HTTP proxy with a tokio-based MQTT client.

## Prerequisites
You must have access to both an MQTT broker configured to accept plaintext connections and an HTTP proxy.  Mosquitto is 
a common choice for local MQTT broker installations and squid or burp are common choices for an HTTP proxy.  

## How to run

```
cargo run -p connect-proxy-tokio -- <http proxy endpoint> <broker endpoint>
```

Assuming correct parameters, proxy setup, and broker setup, you should see:

```
connect-proxy-tokio - an example connecting to an MQTT broker through an HTTP proxy using a tokio-based client

Connecting to <broker endpoint> through an HTTP proxy at <http proxy endpoint>...

Attempting to connect!
Connection attempt successful!
```
