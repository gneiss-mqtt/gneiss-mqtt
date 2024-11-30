# aws-custom-auth-signed-tokio

[**Return to main sample list**](../README.md)

This example illustrates how to connect to the
[AWS IoT Message Broker](https://docs.aws.amazon.com/iot/latest/developerguide/iot-message-broker.html)
with a tokio-based MQTT client, by authenticating with a signed 
[custom authorizer Lambda function](https://docs.aws.amazon.com/iot/latest/developerguide/custom-auth-tutorial.html).

## Prerequisites

Using a custom authorizer with AWS IoT requires significant AWS resource setup first.  You need a Lambda function that will properly
act as an authorizer, as well as an authorizer resource that invokes your Lambda.  
See [Custom Auth Tutorial](https://docs.aws.amazon.com/iot/latest/developerguide/custom-auth-tutorial.html) for
an extensive tutorial that walks you through the process of creating and configuring a custom authorizer.  The created
resources must be in the same region that you wish to use the IoT message broker in.

The IAM [Policy](https://docs.aws.amazon.com/iot/latest/developerguide/iot-policies.html) returned by a successful invocation of the custom authorizer Lambda function must provide 
privileges for this example to connect. Below is a policy that will allow this example to run as intended.

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
* `<region>`: The AWS IoT Core region you wish to use with this sample. For example `us-east-1`.
* `<account>`: Your AWS account ID. This is the set of numbers in the top right next to your AWS account name when using the AWS IoT Core website.

</details>

## How to run

Signed custom authentication requires three additional values to be passed into the authentication context:
* **authorizer token key value** - An arbitrary string value chosen by the developer
* **authorizer signature** - The url-encoded, base64-encoded digital signature of the **authorizer token key value** using the private key from the key pair associated with the signed authorizer.
* **authorizer token key name** - A name used during the custom authentication exchange to pass the **authorizer token key value**.  Anything simple will work; avoid a value that needs url-encoding.

You can run the signed custom auth example like this:

```
cargo run -p aws-custom-auth-signed-tokio -- \
    --authorizer <authorizer name> \
    --username <username data, if any, used by the authorizer Lambda> \
    --password <password data, if any, used by the authorizer Lambda> \
    --authorizer-token-key-value <arbitrary developer-chosen value whose digital signature is also passed> \
    --authorizer-signature "<url-encoded base64-encoded digital signature of <authorizer-token-key-value>>" \
    --authorizer-token-key-name <arbitrary name used to pass the token key value during authentication> \
    <AWS IoT endpoint>
```

With proper resource (authorizer) setup, you should see as output:

```
aws-custom-auth-signed-tokio - an example connecting to AWS IoT Core using a tokio-based client and a signed custom authorizer

Attempting to connect!
Connection attempt successful!
```
