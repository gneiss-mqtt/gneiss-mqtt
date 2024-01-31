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