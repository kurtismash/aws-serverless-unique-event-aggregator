# AWS Serverless Event Aggregator

This repository contains a proof of concept application to aggregate events using AWS Lambda and DynamoDB. The batches of events created through this application do not contain duplicates, enabling the batches to be used for processing in non-idempotent situations.

![An AWS architecture diagram. Starts on the left with an SQS queue, named "input-queue", follows an arrow to the right to the "aggregator-lambda". The flow continues right from the "aggregator-lambda" into a DynamoDB table, "lock-table", and another SQS queue, "batch-queue". Both the table and the second queue have an arrow into the final element, the "batch-processor-lambda".](docs/images/architecture.png)

The aggregation uses Lambda's [built-in batching](https://aws.amazon.com/about-aws/whats-new/2020/11/aws-lambda-now-supports-batch-windows-of-up-to-5-minutes-for-functions/) functionality and in-memory caching to group events into batches of a specified size. DynamoDB is used to prevent the same message being included in multiple batches due to SQS's at-least-once delivery guarantee. The application is designed to handle a failure of the aggregation-lambda, ensuring that events do not become stuck within the DynamoDB table and preventing re-processing.

## Deployment

The application is written and deployed using AWS CDK. From the root of this repository, with AWS CDK installed, run the following to deploy to your AWS environment.

```sh
npm i src/aggregator-lambda/
(cd infrastructure/ && cdk deploy --all)
```

## Testing

The included script `send_messages.py` can be used to send messages to the `input-queue`. The logs from the batch-processor-lambda should show the size of the batches being created.
