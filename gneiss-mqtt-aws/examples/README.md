# gneiss-mqtt-aws connection examples

### Tokio client examples
* [Connect with mTLS](./aws-mtls-tokio/README.md) - Shows how to connect to IoT Core using X509 certificate and key files.
* [Connect with websockets](./aws-websockets-tokio/README.md) - Shows how to connect to IoT Core using websockets by signing the websocket handshake with AWS Credentials.
* [Connect with an unsigned custom authorizer](./aws-custom-auth-unsigned-tokio/README.md) - Shows how to connect to IoT Core using an unsigned custom authorizer.  Requires significant resource setup beforehand.
* [Connect with an signed custom authorizer](./aws-custom-auth-signed-tokio/README.md) - Shows how to connect to IoT Core using a signed custom authorizer.  Requires significant resource setup beforehand.
### Threaded client examples
* [Connect with mTLS](./aws-mtls-threaded/README.md) - Shows how to connect to IoT Core using X509 certificate and key files.
* [Connect with an unsigned custom authorizer](./aws-custom-auth-unsigned-threaded/README.md) - Shows how to connect to IoT Core using an unsigned custom authorizer.  Requires significant resource setup beforehand.
* [Connect with an signed custom authorizer](./aws-custom-auth-signed-threaded/README.md) - Shows how to connect to IoT Core using a signed custom authorizer.  Requires significant resource setup beforehand.
