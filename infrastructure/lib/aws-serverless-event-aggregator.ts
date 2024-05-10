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

		const eventQueue = new Queue(this, `${id}-event-queue`, {
			visibilityTimeout: cdk.Duration.minutes(5),
			retentionPeriod: cdk.Duration.days(14),
			queueName: `${id}-event-queue`,
		});

		const batchQueue = new Queue(this, `${id}-batch-queue`, {
			visibilityTimeout: cdk.Duration.minutes(2),
			retentionPeriod: cdk.Duration.days(14),
			queueName: `${id}-batch-queue`,
		});

		const aggregationTable = new Table(this, `${id}-aggregation-table`, {
			partitionKey: {
				name: "eventId",
				type: AttributeType.STRING,
			},
			tableName: `${id}-aggregation-table`,
			billingMode: BillingMode.PAY_PER_REQUEST,
			removalPolicy: cdk.RemovalPolicy.DESTROY,
		});
		const batchGsiName = "batchId-index";
		aggregationTable.addGlobalSecondaryIndex({
			indexName: batchGsiName,
			partitionKey: { name: "batchId", type: AttributeType.STRING },
		});

		const aggregatorFunction = new Function(this, `${id}-aggregator-lambda`, {
			runtime: Runtime.NODEJS_18_X,
			handler: "src/index.handler",
			code: Code.fromAsset(
				path.join(__dirname, "/../../src/lambdas/aggregator/")
			),
			functionName: `${id}-aggregator-lambda`,
			environment: {
				DDB_TABLE_NAME: aggregationTable.tableName,
				OUTPUT_SQS_URL: batchQueue.queueUrl,
			},
			memorySize: 256,
			timeout: cdk.Duration.minutes(3),
		});
		aggregatorFunction.addEventSource(
			new SqsEventSource(eventQueue, {
				batchSize: 1500,
				maxBatchingWindow: cdk.Duration.minutes(5),
				maxConcurrency: 2,
			})
		);
		batchQueue.grantSendMessages(aggregatorFunction);
		aggregationTable.grantReadWriteData(aggregatorFunction);

		const batchHandlerFunction = new Function(
			this,
			`${id}-batch-handler-lambda`,
			{
				runtime: Runtime.NODEJS_18_X,
				handler: "src/index.handler",
				code: Code.fromAsset(
					path.join(__dirname, "/../../src/lambdas/batchHandler/")
				),
				functionName: `${id}-batch-handler-lambda`,
				environment: {
					DDB_TABLE_NAME: aggregationTable.tableName,
					DDB_INDEX: batchGsiName,
				},
				timeout: cdk.Duration.minutes(1),
			}
		);
		batchHandlerFunction.addEventSource(
			new SqsEventSource(batchQueue, {
				batchSize: 1,
			})
		);
		aggregationTable.grantReadData(batchHandlerFunction);
	}
}
