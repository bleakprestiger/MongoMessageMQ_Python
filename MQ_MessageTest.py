from datetime import datetime
from MongoDBMessageQueue import MongoDBMessageQueue
# Initialize the queue using environment variables
queue = MongoDBMessageQueue()

# Producer
channel = "orders"
message_id = queue.publish_message(channel, {
    "order_id": 1001,
    "customer": "John Doe",
    "items": ["item1", "item2"],
    "timestamp": datetime.utcnow().isoformat()
})

# Consumer
consumer_id = "worker_1"
message = queue.get_message(channel, consumer_id, timeout=10)

if message:
    try:
        print(f"Processing order: {message['payload']['order_id']}")
        # Process the message...
        queue.complete_message(message["_id"], consumer_id)
    except Exception as e:
        print(f"Failed to process message: {str(e)}")
        queue.fail_message(message["_id"], consumer_id)

# Get statistics
stats = queue.get_channel_stats(channel)
print(f"Channel statistics: {stats}")

# Clean up
queue.close()