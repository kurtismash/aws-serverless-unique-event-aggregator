import { v4 } from "uuid";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { PutCommand, DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";

const DDB_LOCK_TABLE = process.env.DDB_LOCK_TABLE;
const OUTPUT_SQS_URL = process.env.OUTPUT_SQS_URL;
const MAX_SECONDARY_BATCHING_WINDOW = process.env.MAX_SECONDARY_BATCHING_WINDOW || 300;
const MAX_BATCH_SIZE = process.env.MAX_BATCH_SIZE || 50000;

const BATCH_CACHE = {};

const sqsClient = new SQSClient();
const dbClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dbClient);

const startNewBatch = async (sourceId, lambdaTimeoutTime) => {
	console.log(BATCH_CACHE);
	const batchId = v4();
	const expires = lambdaTimeoutTime + MAX_SECONDARY_BATCHING_WINDOW * 1000;
	BATCH_CACHE[sourceId] = {
		batchId,
		expires,
		items: 1,
	};
	const sqsCommand = new SendMessageCommand({
		QueueUrl: OUTPUT_SQS_URL,
		DelaySeconds: Math.ceil((expires - Date.now()) / 1000),
		MessageBody: JSON.stringify({ batchId }),
	});
	await sqsClient.send(sqsCommand);
	console.log(`STARTING BATCH ${batchId}`);
	return batchId;
};

const getBatchId = async (sourceId, lambdaTimeoutTime) => {
	const currentBatchForSource = BATCH_CACHE[sourceId];

	if (!currentBatchForSource) {
		console.log({
			action: "Start new batch.",
			reason: "No existing batch.",
			source: sourceId,
		});
		return startNewBatch(sourceId, lambdaTimeoutTime);
	} else if (lambdaTimeoutTime >= currentBatchForSource.expires) {
		console.log({
			action: "Start new batch.",
			reason: "Expiry before lambda timeout.",
			source: sourceId,
		});
		return startNewBatch(sourceId, lambdaTimeoutTime);
	} else if (currentBatchForSource.items >= MAX_BATCH_SIZE) {
		console.log({
			action: "Start new batch.",
			reason: "Max batch size exceeded.",
			source: sourceId,
		});
		return startNewBatch(sourceId, lambdaTimeoutTime);
	} else {
		currentBatchForSource.items += 1;
		return currentBatchForSource.batchId;
	}
};

export const handler = async (event, context) => {
	const lambdaTimeoutTime = Date.now() + context.getRemainingTimeInMillis();

	// Attempt to add each message to DynamoDB.
	const promises = event.Records.map(async (i) => {
		const batchId = await getBatchId(i.eventSourceARN, lambdaTimeoutTime);
		const { identifier } = JSON.parse(i.body);
		const ddbCommand = new PutCommand({
			TableName: DDB_LOCK_TABLE,
			Item: {
				messageId: identifier,
				batchId,
				message: i.body,
			},
			ConditionExpression: "attribute_not_exists(messageId)",
		});
		try {
			await docClient.send(ddbCommand);
		} catch (e) {
			return { success: false, sqsMessageId: i.messageId };
		}
		return { success: true };
	});

	const results = await Promise.all(promises);
	const successCount = results.filter((i) => i.success).length;
	const failures = results.filter((i) => !i.success);
	console.log(`Success: ${successCount}. Failures: ${failures.length}`);
	return {
		batchItemFailures: failures.map((i) => ({
			itemIdentifier: i.sqsMessageId,
		})),
	};
};
