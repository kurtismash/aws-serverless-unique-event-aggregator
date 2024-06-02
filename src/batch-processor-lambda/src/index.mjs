import {
	BatchWriteItemCommand,
	DynamoDBClient,
} from "@aws-sdk/client-dynamodb";
import { QueryCommand, DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

const DDB_LOCK_TABLE = process.env.DDB_LOCK_TABLE;
const DDB_BATCH_INDEX = process.env.DDB_BATCH_INDEX;

const dbClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dbClient);

async function deleteItems(ids) {
	const MAX_BATCH_SIZE = 25;
	const batches = [];
	for (let i = 0; i < ids.length; i += MAX_BATCH_SIZE) {
		batches.push(ids.slice(i, i + MAX_BATCH_SIZE));
	}
	const promises = batches.map((batch) => {
		console.log("Sending delete request for messages:", batch);
		return dbClient.send(
			new BatchWriteItemCommand({
				RequestItems: {
					[DDB_LOCK_TABLE]: batch.map((id) => ({
						DeleteRequest: {
							Key: {
								messageId: { S: id },
							},
						},
					})),
				},
			})
		);
	});
	await Promise.all(promises);
}

async function query(batchId) {
	const params = {
		TableName: DDB_LOCK_TABLE,
		IndexName: DDB_BATCH_INDEX,
		KeyConditionExpression: "batchId = :bid",
		ExpressionAttributeValues: { ":bid": batchId },
	};

	let items = [];
	let lastEvaluatedKey = null;
	do {
		if (lastEvaluatedKey) {
			params.ExclusiveStartKey = lastEvaluatedKey;
		}
		const command = new QueryCommand(params);
		const response = await docClient.send(command);
		items = items.concat(response.Items);
		lastEvaluatedKey = response.LastEvaluatedKey;
	} while (lastEvaluatedKey);

	return items;
}

export const handler = async (event) => {
	const { batchId } = JSON.parse(event.Records[0].body);

	// Fetch all the messages within the batch
	const result = await query(batchId);

	// This is where we can process the batch as one entity
	console.log(`Batch: ${batchId} => ${result.length} items`);

	// Delete the items from the lock-table to enable retries.
	await deleteItems(result.map((i) => i.messageId));
};
