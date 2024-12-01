# aws-mtls-threaded

[**Return to main sample list**](../README.md)

This example illustrates how to connect to the
[AWS IoT Message Broker](https://docs.aws.amazon.com/iot/latest/developerguide/iot-message-broker.html)
with a thread-based MQTT client.

## Prerequisites

Your IoT Core Thing's [Policy](https://docs.aws.amazon.com/iot/latest/developerguide/iot-policies.html) must provide privileges for this sample to connect. Below is a sample policy that can be used on your IoT Core Thing that will allow this sample to run as intended.

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
* `<region>`: The AWS IoT Core region where you created your AWS IoT Core thing you wish to use with this sample. For example `us-east-1`.
* `<account>`: Your AWS IoT Core account ID. This is the set of numbers in the top right next to your AWS account name when using the AWS IoT Core website.

</details>

## How to run

```
cargo run -p aws-mtls-threaded -- --cert <path to cert file> --key <path to key file> <AWS IoT endpoint>
```

With proper resource (thing, permission policy) setup, you should see as output:

```
aws-mtls-threaded - an example connecting to AWS IoT Core using a thread-based client and MTLS

Attempting to connect!
Connection attempt successful!
```
