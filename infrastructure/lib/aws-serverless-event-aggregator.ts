import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { Queue } from "aws-cdk-lib/aws-sqs";
import { Function, Runtime, Code } from "aws-cdk-lib/aws-lambda";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { AttributeType, Table, BillingMode } from "aws-cdk-lib/aws-dynamodb";
import path = require("path");

export class AwsServerlessEventAggregatorStack extends cdk.Stack {
	constructor(scope: Construct, id: string, props?: cdk.StackProps) {
		super(scope, id, props);

		const inputQueue = new Queue(this, `${id}-input-queue`, {
			visibilityTimeout: cdk.Duration.minutes(10),
			retentionPeriod: cdk.Duration.days(14),
			queueName: `${id}-input-queue`,
		});

		const batchQueue = new Queue(this, `${id}-batch-queue`, {
			visibilityTimeout: cdk.Duration.minutes(6),
			retentionPeriod: cdk.Duration.days(14),
			queueName: `${id}-batch-queue`,
		});

		const lockTable = new Table(this, `${id}-lock-table`, {
			partitionKey: {
				name: "messageId",
				type: AttributeType.STRING,
			},
			tableName: `${id}-lock-table`,
			billingMode: BillingMode.PAY_PER_REQUEST,
			removalPolicy: cdk.RemovalPolicy.DESTROY,
		});
		const batchGsiName = "batchId-index";
		lockTable.addGlobalSecondaryIndex({
			indexName: batchGsiName,
			partitionKey: { name: "batchId", type: AttributeType.STRING },
		});

		const aggregatorFunction = new Function(this, `${id}-aggregator-lambda`, {
			runtime: Runtime.NODEJS_18_X,
			handler: "src/index.handler",
			code: Code.fromAsset(path.join(__dirname, "../../src/aggregator-lambda/")),
			functionName: `${id}-aggregator-lambda`,
			environment: {
				DDB_LOCK_TABLE: lockTable.tableName,
				OUTPUT_SQS_URL: batchQueue.queueUrl,
			},
			memorySize: 1769,
			timeout: cdk.Duration.minutes(1),
		});
		aggregatorFunction.addEventSource(
			new SqsEventSource(inputQueue, {
				batchSize: 10000,
				maxBatchingWindow: cdk.Duration.minutes(5),
				maxConcurrency: 2,
				reportBatchItemFailures: true,
			})
		);
		batchQueue.grantSendMessages(aggregatorFunction);
		lockTable.grantReadWriteData(aggregatorFunction);

		const batchProcessorFunction = new Function(this, `${id}-batch-processor-lambda`, {
			runtime: Runtime.NODEJS_18_X,
			handler: "src/index.handler",
			code: Code.fromAsset(path.join(__dirname, "/../../src/batch-processor-lambda/")),
			functionName: `${id}-batch-processor-lambda`,
			environment: {
				DDB_LOCK_TABLE: lockTable.tableName,
				DDB_BATCH_INDEX: batchGsiName,
			},
			timeout: cdk.Duration.minutes(5),
		});
		batchProcessorFunction.addEventSource(
			new SqsEventSource(batchQueue, {
				batchSize: 1,
			})
		);
		lockTable.grantReadWriteData(batchProcessorFunction);
	}
}
