# Gneiss MQTT AWS Change document
This document is currently hand-written and non-authoritative.

## 0.2.0 
* AWS Client builder (tokio + rustls only)
* * mtls support
* * websockets via AWS sig4 support
* * AWS IoT Custom Authentication support
* * Initial crate documentation

## 0.3.0
* Custom Auth refactoring
* * Reworked custom auth options to builder-based
* * Added SDK/Version params (may remove later)
* * URI encode custom authorizer signatures when it's safe to do so
* Features
* * Rustls support feature-gated
* * Native-tls support added and feature-gated
* * Websocket support feature-gated

## 0.4.0
* Threaded client support for mtls and custom auth

## 0.5.0
* When using MQTT311, apply some default settings designed to make the connection more reliable in the face of disconnect-triggering limit violations when using IoT Core