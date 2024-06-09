import { BatchWriteItemCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { QueryCommand, DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

const DDB_LOCK_TABLE = process.env.DDB_LOCK_TABLE;
const DDB_BATCH_INDEX = process.env.DDB_BATCH_INDEX;

const dbClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dbClient);

const deleteBatchWithRetries = async (batch, retries = 0) => {
	const MAX_RETRIES = 5;
	const RETRY_DELAY_MS = 200;
	if (retries > MAX_RETRIES) {
		throw new Error(`Failed to delete batch after ${MAX_RETRIES} retries`);
	}

	try {
		const response = await dbClient.send(
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

		const unprocessedItems = response.UnprocessedItems || {};

		// Check if there are unprocessed items
		if (unprocessedItems[DDB_LOCK_TABLE] && unprocessedItems[DDB_LOCK_TABLE].length > 0) {
			console.log(
				`Unprocessed items found: ${unprocessedItems[DDB_LOCK_TABLE].map((item) => item.DeleteRequest.Key.messageId.S)}`
			);

			// Retry only unprocessed items
			await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
			await processBatchWithRetries(
				unprocessedItems[DDB_LOCK_TABLE].map((item) => item.DeleteRequest.Key.messageId.S),
				retries + 1
			);
		}
	} catch (error) {
		console.error(`Error deleting batch: ${error}`);
		// Retry in case of an error if the retry count is less than MAX_RETRIES
		if (retries < MAX_RETRIES) {
			await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
			await deleteBatchWithRetries(batch, retries + 1);
		} else {
			throw error;
		}
	}
};

const deleteItems = async (ids) => {
	const MAX_BATCH_SIZE = 25;
	const batches = [];
	for (let i = 0; i < ids.length; i += MAX_BATCH_SIZE) {
		batches.push(ids.slice(i, i + MAX_BATCH_SIZE));
	}
	const promises = batches.map((i) => processBatchWithRetries(i));
	await Promise.all(promises);
};

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
