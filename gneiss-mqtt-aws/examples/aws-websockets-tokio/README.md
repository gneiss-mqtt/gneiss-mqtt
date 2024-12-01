# aws-websockets-tokio

[**Return to main sample list**](../README.md)

This example illustrates how to connect to the
[AWS IoT Message Broker](https://docs.aws.amazon.com/iot/latest/developerguide/iot-message-broker.html)
with a tokio-based MQTT client that uses websockets as transport.  AWS MQTT-over-websockets support requires the initial handshake request to be 
signed with the AWS Sigv4 signing algorithm.  Internally, gneiss-mqtt-aws uses the [AWS SDK for Rust](https://aws.amazon.com/sdk-for-rust/) to source credentials via the default
credentials provider chain and sign the websocket handshake.

## Prerequisites

The AWS IAM permission policy associated with the AWS credentials resolved by the default credentials provider chain must 
provide privileges for this sample to connect. Below is a sample policy will allow this sample to run as intended.

<details>
<summary>(see sample policy)</summary>
<pre>
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "iot:Connect"
      ],
      "Resource": [
        "arn:aws:iot:<b>region</b>:<b>account</b>:client/*"
      ]
    }
  ]
}
</pre>

Replace the following with the data from your AWS account:
* `<region>`: The AWS IoT Core region that you are connecting to. For example `us-east-1`.
* `<account>`: Your AWS IoT Core account ID. This is the set of numbers in the top right next to your AWS account name when using the AWS IoT Core website.

</details>

## How to run

```
cargo run -p aws-websockets-tokio -- --region <region> <AWS IoT endpoint>
```

With proper credentials sourcing and permission policy setup, you should see as output:

```
aws-websockets-tokio - an example connecting to AWS IoT Core using a tokio-based client and websockets

Attempting to connect!
Connection attempt successful!
```
