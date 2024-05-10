import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { QueryCommand, DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

const DDB_TABLE_NAME = process.env.DDB_TABLE_NAME;
const DDB_INDEX = process.env.DDB_INDEX;

const dbClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dbClient);

export const handler = async (event) => {
	const { batchId } = JSON.parse(event.Records[0].body);

	const ddbCommand = new QueryCommand({
		TableName: DDB_TABLE_NAME,
		IndexName: DDB_INDEX,
		KeyConditionExpression: "batchId = :bid",
		ExpressionAttributeValues: { ":bid": batchId },
	});
	const result = await docClient.send(ddbCommand);
	console.log(`Batch: ${batchId} => ${result.Items.length} items`);
};
