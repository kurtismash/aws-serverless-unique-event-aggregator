import { v4 } from "uuid";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { PutCommand, DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { SendMessageCommand, SQSClient } from "@aws-sdk/client-sqs";

const DDB_TABLE_NAME = process.env.DDB_TABLE_NAME;
const OUTPUT_SQS_URL = process.env.OUTPUT_SQS_URL;

const sqsClient = new SQSClient();
const dbClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dbClient);

export const handler = async (event, context) => {
	const rawMessageCount = event.Records.length;
	let filteredMessageCount = 0;
	const batchId = v4();

	// Send a message to the batch queue with a batch identifier
	// Do this first to handle the batch even if this Lambda fails
	const sqsCommand = new SendMessageCommand({
		QueueUrl: OUTPUT_SQS_URL,
		DelaySeconds: context.getRemainingTimeInMillis() / 1000 + 60,
		MessageBody: JSON.stringify({ batchId }),
	});
	await sqsClient.send(sqsCommand);

	// Attempt to add each message to DynamoDB
	const promises = event.Records.map(async (i) => {
		const { identifier } = JSON.parse(i.body);
		const ddbCommand = new PutCommand({
			TableName: DDB_TABLE_NAME,
			Item: {
				eventId: identifier,
				batchId,
				message: i.body,
			},
			ConditionExpression: "attribute_not_exists(eventId)",
		});
		try {
			await docClient.send(ddbCommand);
			filteredMessageCount += 1;
		} catch (e) {
			// TODO: Passthrough any error but conditional not met
			// TODO: Return successes and failures so Lambda can delete successfuls
			return;
		}
	});

	await Promise.all(promises);

	console.log(
		`${rawMessageCount} -> ${filteredMessageCount} (Batch: ${batchId})`
	);
};
