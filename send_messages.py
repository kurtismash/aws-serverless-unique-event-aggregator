import boto3
import uuid
import random
import json

def send_messages_batch(queue_url, num_messages, collision_percentage):
    # Create SQS client
    sqs = boto3.client('sqs')

    # Generate messages
    messages = []
    for i in range(num_messages):
        # Generate UUID
        if collision_percentage == 0 or random.randint(1, 100) <= collision_percentage:
            # Generate a random UUID
            message_id = uuid.uuid4().hex
        else:
            # Generate a deterministic UUID
            message_id = uuid.uuid5(uuid.NAMESPACE_DNS, str(i)).hex

        message_body = {"identifier": str(message_id)}
        messages.append({'Id': str(i), 'MessageBody': json.dumps(message_body)})

    # Send messages in batches of 10
    for i in range(0, len(messages), 10):
        batch_messages = messages[i:i+10]
        response = sqs.send_message_batch(
            QueueUrl=queue_url,
            Entries=batch_messages
        )

        # Print any failed messages
        if 'Failed' in response:
            print(f"Failed to send messages: {response['Failed']}")

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 4:
        print("Usage: python script.py <queue_url> <num_messages> <collision_percentage>")
        sys.exit(1)

    queue_url = sys.argv[1]
    num_messages = int(sys.argv[2])
    collision_percentage = int(sys.argv[3])

    if collision_percentage < 0 or collision_percentage > 100:
        print("Collision percentage should be between 0 and 100")
        sys.exit(1)

    send_messages_batch(queue_url, num_messages, collision_percentage)
