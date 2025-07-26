#####################OLD CODE START###################################################################################
# import os
# import pymongo
# from pymongo import MongoClient, errors
# from datetime import datetime, timezone, timedelta  # Add timezone import
# import time
# import uuid
# import logging
# from typing import Optional, Dict, Any, List
# from threading import Lock
# from enum import Enum
# from dotenv import load_dotenv

# # Load environment variables from .env file
# load_dotenv()

# # Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# class MessageStatus(Enum):
#     PENDING = "pending"
#     PROCESSING = "processing"
#     COMPLETED = "completed"
#     FAILED = "failed"

# class MongoDBMessageQueue:
#     def __init__(
#         self,
#         mongo_uri: Optional[str] = None,
#         db_name: Optional[str] = None,
#         max_retries: Optional[int] = None,
#         retry_delay: Optional[float] = None,
#         visibility_timeout: Optional[int] = None,
#         heartbeat_interval: Optional[int] = None,
#     ):
#         """
#         Initialize the MongoDB message queue with environment variable configuration.
        
#         Args (all optional, will fall back to environment variables):
#             mongo_uri: MongoDB connection URI
#             db_name: Database name to use
#             max_retries: Maximum number of retries for operations
#             retry_delay: Delay between retries in seconds
#             visibility_timeout: Time in seconds after which a message becomes visible again if not completed
#             heartbeat_interval: Interval in seconds to update the processing message's heartbeat
#         """
#         # Get configuration from environment variables with defaults
#         self.mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://localhost:27017")
#         self.db_name = db_name or os.getenv("MONGO_DB_NAME", "message_queue_db")
#         self.max_retries = max_retries or int(os.getenv("MQ_MAX_RETRIES", "3"))
#         self.retry_delay = retry_delay or float(os.getenv("MQ_RETRY_DELAY", "1.0"))
#         self.visibility_timeout = visibility_timeout or int(os.getenv("MQ_VISIBILITY_TIMEOUT", "300"))  # 5 minutes
#         self.heartbeat_interval = heartbeat_interval or int(os.getenv("MQ_HEARTBEAT_INTERVAL", "60"))  # 1 minute
        
#         # Validate configuration
#         self._validate_config()
        
#         # Thread-safe lock for operations that need synchronization
#         self._lock = Lock()
        
#         # Initialize MongoDB connection
#         self._init_mongo_connection()
        
#         # Ensure indexes exist
#         self._ensure_indexes()
        
#         # Track active consumers
#         self.active_consumers = {}
        
#     def get_current_utc_time(self):
#         """Get current UTC time in a timezone-aware manner."""
#         return datetime.now(timezone.utc)
    
#     def _validate_config(self):
#         """Validate the configuration."""
#         if not self.mongo_uri:
#             raise ValueError("MongoDB URI must be provided either through constructor or MONGO_URI environment variable")
        
#         if not self.db_name:
#             raise ValueError("Database name must be provided either through constructor or MONGO_DB_NAME environment variable")
        
#         if self.max_retries < 0:
#             raise ValueError("Max retries must be a non-negative integer")
        
#         if self.retry_delay <= 0:
#             raise ValueError("Retry delay must be a positive number")
        
#         if self.visibility_timeout <= 0:
#             raise ValueError("Visibility timeout must be a positive number")
        
#         if self.heartbeat_interval <= 0:
#             raise ValueError("Heartbeat interval must be a positive number")
        
#         logger.info("Configuration validated successfully")
    
#     def _init_mongo_connection(self):
#         """Initialize MongoDB connection with retry logic."""
#         logger.info(f"Connecting to MongoDB at {self.mongo_uri} (database: {self.db_name})")
#         tls_enabled = os.getenv("MONGO_TLS", "false").lower() == "true"
#         print("TLS Value - ",tls_enabled)
#         for attempt in range(self.max_retries + 1):
#             try:
#                 self.client = MongoClient(
#                     self.mongo_uri,
#                     retryWrites=True,
#                     retryReads=True,
#                     serverSelectionTimeoutMS=5000,
#                     connectTimeoutMS=30000,
#                     socketTimeoutMS=30000,
#                     appname=os.getenv("MONGO_DB_NAME", "message_queue_db"),
#                     tls=tls_enabled,  # Explicitly enable/disable TLS
#                     tlsCAFile=os.getenv("MONGO_CA_FILE") if tls_enabled else None,
#                     tlsCertificateKeyFile=os.getenv("MONGO_CERT_KEY_FILE") if tls_enabled else None,
#                     tlsAllowInvalidCertificates=os.getenv("MONGO_ALLOW_INVALID_CERTS") if tls_enabled else False,
#                     #tlsAllowInvalidCertificates=os.getenv("MONGO_ALLOW_INVALID_CERTS", "false").lower() == "true" if tls_enabled else False
#                 )
#                 # Test the connection
#                 self.client.admin.command('ping')
#                 self.db = self.client[self.db_name]
#                 self.messages_collection = self.db["messages"]
#                 self.channels_collection = self.db["channels"]
#                 logger.info("Successfully connected to MongoDB")
#                 return
#             except errors.PyMongoError as e:
#                 if attempt == self.max_retries:
#                     logger.error(f"Failed to connect to MongoDB after {self.max_retries} attempts")
#                     raise
#                 logger.warning(f"Attempt {attempt + 1} failed to connect to MongoDB: {str(e)}")
#                 time.sleep(self.retry_delay)
    
#     def _ensure_indexes(self):
#         """Ensure necessary indexes exist for efficient querying."""
#         try:
#             # Index for channel and status
#             self.messages_collection.create_index([
#                 ("channel", 1),
#                 ("status", 1),
#                 ("created_at", 1)
#             ], name="channel_status_created_idx")
            
#             # Index for processing messages with heartbeat
#             self.messages_collection.create_index([
#                 ("channel", 1),
#                 ("status", 1),
#                 ("heartbeat", 1)
#             ], name="channel_status_heartbeat_idx")
            
#             # Index for sequence number per channel
#             self.messages_collection.create_index([
#                 ("channel", 1),
#                 ("sequence_number", 1)
#             ], name="channel_sequence_unique_idx", unique=True)
            
#             # TTL index for automatic cleanup of completed messages
#             retention_days = int(os.getenv("MQ_RETENTION_DAYS", "7"))  # Default 7 days retention
#             if retention_days > 0:
#                 self.messages_collection.create_index(
#                     [("updated_at", 1)],
#                     name="completed_messages_ttl_idx",
#                     expireAfterSeconds=retention_days * 24 * 60 * 60,
#                     partialFilterExpression={"status": MessageStatus.COMPLETED.value}
#                 )
#                 logger.info(f"Created TTL index for completed messages with {retention_days} days retention")
            
#             logger.info("Ensured necessary indexes exist")
#         except errors.PyMongoError as e:
#             logger.error(f"Failed to create indexes: {str(e)}")
#             raise
    
#     def _get_next_sequence_number(self, channel: str) -> int:
#         """
#         Get the next sequence number for a channel using atomic operation.
        
#         Args:
#             channel: The channel name
            
#         Returns:
#             The next sequence number for the channel
#         """
#         try:
#             result = self.channels_collection.find_one_and_update(
#                 {"_id": channel},
#                 {"$inc": {"sequence_number": 1}},
#                 upsert=True,
#                 return_document=pymongo.ReturnDocument.AFTER,
#                 maxTimeMS=5000  # 5 second timeout
#             )
#             return result["sequence_number"]
#         except errors.PyMongoError as e:
#             logger.error(f"Failed to get next sequence number for channel {channel}: {str(e)}")
#             raise
    
#     def publish_message(self, channel: str, message: Dict[str, Any]) -> str:
#         """
#         Publish a message to a specific channel with guaranteed ordering.
        
#         Args:
#             channel: The channel to publish to
#             message: The message payload (as a dictionary)
            
#         Returns:
#             The message ID
#         """
#         message_id = str(uuid.uuid4())
#         sequence_number = self._get_next_sequence_number(channel)
        
#         message_doc = {
#             "_id": message_id,
#             "channel": channel,
#             "sequence_number": sequence_number,
#             "payload": message,
#             "status": MessageStatus.PENDING.value,
#             "created_at": self.get_current_utc_time(),
#             "updated_at": self.get_current_utc_time(),
#             "attempts": 0,
#             "heartbeat": None,
#             "consumer_id": None
#         }
        
#         for attempt in range(self.max_retries + 1):
#             try:
#                 self.messages_collection.insert_one(message_doc)
#                 logger.info(f"Published message {message_id} to channel {channel} with sequence {sequence_number}")
#                 return message_id
#             except errors.PyMongoError as e:
#                 if attempt == self.max_retries:
#                     logger.error(f"Failed to publish message after {self.max_retries} attempts: {str(e)}")
#                     raise
#                 logger.warning(f"Attempt {attempt + 1} failed to publish message: {str(e)}")
#                 time.sleep(self.retry_delay)
    
#     def get_message(self, channel: str, consumer_id: str, timeout: Optional[int] = None) -> Optional[Dict[str, Any]]:
#         """
#         Get the next pending message from the specified channel.
        
#         Args:
#             channel: The channel to consume from
#             consumer_id: Unique identifier for the consumer
#             timeout: Optional timeout in seconds to wait for a message
            
#         Returns:
#             The message document or None if no message is available
#         """
#         start_time = time.time()
        
#         while True:
#             try:
#                 # First, check for any processing messages that have expired (heartbeat too old)
#                 self._release_expired_messages(channel)
                
#                 # Find the oldest pending message for this channel
#                 query = {
#                     "channel": channel,
#                     "status": MessageStatus.PENDING.value
#                 }
                
#                 update = {
#                     "$set": {
#                         "status": MessageStatus.PROCESSING.value,
#                         "updated_at": self.get_current_utc_time(),
#                         "heartbeat": self.get_current_utc_time(),
#                         "consumer_id": consumer_id
#                     },
#                     "$inc": {"attempts": 1}
#                 }
                
#                 # Find and modify atomically to claim the message
#                 message = self.messages_collection.find_one_and_update(
#                     query,
#                     update,
#                     sort=[("sequence_number", pymongo.ASCENDING)],
#                     return_document=pymongo.ReturnDocument.AFTER,
#                     maxTimeMS=5000  # 5 second timeout
#                 )
                
#                 if message:
#                     logger.info(f"Consumer {consumer_id} claimed message {message['_id']} from channel {channel}")
                    
#                     # Start heartbeat for this message
#                     self._start_heartbeat(channel, message["_id"], consumer_id)
                    
#                     return message
                
#                 # No message available
#                 if timeout is None or (time.time() - start_time) >= timeout:
#                     return None
                
#                 # Wait before retrying
#                 time.sleep(min(1.0, self.retry_delay))
                
#             except errors.PyMongoError as e:
#                 logger.error(f"Error getting message from channel {channel}: {str(e)}")
#                 if timeout is None or (time.time() - start_time) >= timeout:
#                     return None
#                 time.sleep(self.retry_delay)
    
#     def _start_heartbeat(self, channel: str, message_id: str, consumer_id: str):
#         """Start a background heartbeat for a processing message."""
#         if consumer_id in self.active_consumers:
#             self._stop_heartbeat(consumer_id)
        
#         # Create a new heartbeat entry
#         self.active_consumers[consumer_id] = {
#             "channel": channel,
#             "message_id": message_id,
#             "last_heartbeat": time.time(),
#             "active": True
#         }
        
#         # Start heartbeat thread
#         import threading
#         def heartbeat_loop():
#             while self.active_consumers.get(consumer_id, {}).get("active", False):
#                 try:
#                     now = self.get_current_utc_time()
#                     result = self.messages_collection.update_one(
#                         {
#                             "_id": message_id,
#                             "status": MessageStatus.PROCESSING.value,
#                             "consumer_id": consumer_id
#                         },
#                         {
#                             "$set": {
#                                 "heartbeat": now,
#                                 "updated_at": now
#                             }
#                         },
#                         maxTimeMS=3000  # 3 second timeout
#                     )
                    
#                     if result.modified_count == 0:
#                         logger.warning(f"Heartbeat failed for message {message_id} - possibly completed or failed")
#                         break
                    
#                     self.active_consumers[consumer_id]["last_heartbeat"] = time.time()
#                 except errors.PyMongoError as e:
#                     logger.error(f"Heartbeat error for message {message_id}: {str(e)}")
                
#                 time.sleep(self.heartbeat_interval)
        
#         thread = threading.Thread(target=heartbeat_loop, daemon=True)
#         thread.start()
    
#     def _stop_heartbeat(self, consumer_id: str):
#         """Stop the heartbeat for a consumer."""
#         if consumer_id in self.active_consumers:
#             self.active_consumers[consumer_id]["active"] = False
#             del self.active_consumers[consumer_id]
    
#     def _release_expired_messages(self, channel: str):
#         """Release messages that have been processing for too long without a heartbeat."""
#         try:
#             cutoff = self.get_current_utc_time() - timedelta(seconds=self.visibility_timeout)
            
#             result = self.messages_collection.update_many(
#                 {
#                     "channel": channel,
#                     "status": MessageStatus.PROCESSING.value,
#                     "heartbeat": {"$lt": cutoff}
#                 },
#                 {
#                     "$set": {
#                         "status": MessageStatus.PENDING.value,
#                         "updated_at": self.get_current_utc_time(),
#                         "heartbeat": None,
#                         "consumer_id": None
#                     }
#                 },
#                 maxTimeMS=10000  # 10 second timeout
#             )
            
#             if result.modified_count > 0:
#                 logger.info(f"Released {result.modified_count} expired messages in channel {channel}")
        
#         except errors.PyMongoError as e:
#             logger.error(f"Error releasing expired messages: {str(e)}")
    
#     def complete_message(self, message_id: str, consumer_id: str):
#         """
#         Mark a message as completed.
        
#         Args:
#             message_id: The ID of the message to complete
#             consumer_id: The ID of the consumer completing the message
#         """
#         self._stop_heartbeat(consumer_id)
        
#         for attempt in range(self.max_retries + 1):
#             try:
#                 result = self.messages_collection.update_one(
#                     {
#                         "_id": message_id,
#                         "consumer_id": consumer_id,
#                         "status": MessageStatus.PROCESSING.value
#                     },
#                     {
#                         "$set": {
#                             "status": MessageStatus.COMPLETED.value,
#                             "updated_at": self.get_current_utc_time(),
#                             "heartbeat": None,
#                             "consumer_id": None
#                         }
#                     },
#                     maxTimeMS=5000  # 5 second timeout
#                 )
                
#                 if result.modified_count == 1:
#                     logger.info(f"Completed message {message_id}")
#                     return
#                 else:
#                     logger.warning(f"Message {message_id} not found or not in processing state")
#                     return
#             except errors.PyMongoError as e:
#                 if attempt == self.max_retries:
#                     logger.error(f"Failed to complete message {message_id} after {self.max_retries} attempts: {str(e)}")
#                     raise
#                 logger.warning(f"Attempt {attempt + 1} failed to complete message: {str(e)}")
#                 time.sleep(self.retry_delay)
    
#     def fail_message(self, message_id: str, consumer_id: str):
#         """
#         Mark a message as failed.
        
#         Args:
#             message_id: The ID of the message to mark as failed
#             consumer_id: The ID of the consumer failing the message
#         """
#         self._stop_heartbeat(consumer_id)
        
#         max_attempts = int(os.getenv("MQ_MAX_ATTEMPTS", "3"))
        
#         for attempt in range(self.max_retries + 1):
#             try:
#                 # First get the current attempt count
#                 message = self.messages_collection.find_one(
#                     {"_id": message_id},
#                     {"attempts": 1},
#                     maxTimeMS=3000  # 3 second timeout
#                 )
                
#                 if not message:
#                     logger.warning(f"Message {message_id} not found")
#                     return
                
#                 new_status = MessageStatus.FAILED.value if message.get("attempts", 0) >= max_attempts - 1 else MessageStatus.PENDING.value
                
#                 result = self.messages_collection.update_one(
#                     {
#                         "_id": message_id,
#                         "consumer_id": consumer_id,
#                         "status": MessageStatus.PROCESSING.value
#                     },
#                     {
#                         "$set": {
#                             "status": new_status,
#                             "updated_at": self.get_current_utc_time(),
#                             "heartbeat": None,
#                             "consumer_id": None if new_status == MessageStatus.PENDING.value else consumer_id
#                         }
#                     },
#                     maxTimeMS=5000  # 5 second timeout
#                 )
                
#                 if result.modified_count == 1:
#                     logger.info(f"Marked message {message_id} as {new_status}")
#                     return
#                 else:
#                     logger.warning(f"Message {message_id} not found or not in processing state")
#                     return
#             except errors.PyMongoError as e:
#                 if attempt == self.max_retries:
#                     logger.error(f"Failed to fail message {message_id} after {self.max_retries} attempts: {str(e)}")
#                     raise
#                 logger.warning(f"Attempt {attempt + 1} failed to fail message: {str(e)}")
#                 time.sleep(self.retry_delay)
    
#     def get_channel_stats(self, channel: str) -> Dict[str, int]:
#         """
#         Get statistics for a channel.
        
#         Args:
#             channel: The channel name
            
#         Returns:
#             Dictionary with message counts by status
#         """
#         try:
#             pipeline = [
#                 {"$match": {"channel": channel}},
#                 {"$group": {
#                     "_id": "$status",
#                     "count": {"$sum": 1}
#                 }},
#                 {"$project": {
#                     "status": "$_id",
#                     "count": 1,
#                     "_id": 0
#                 }}
#             ]
            
#             results = list(self.messages_collection.aggregate(
#                 pipeline,
#                 maxTimeMS=5000  # 5 second timeout
#             ))
            
#             stats = {
#                 "pending": 0,
#                 "processing": 0,
#                 "completed": 0,
#                 "failed": 0,
#                 "total": 0
#             }
            
#             for result in results:
#                 status = result["status"]
#                 if status in stats:
#                     stats[status] = result["count"]
            
#             # Calculate total
#             stats["total"] = sum(stats.values()) - stats["total"]  # Subtract the initial 0
            
#             return stats
#         except errors.PyMongoError as e:
#             logger.error(f"Error getting stats for channel {channel}: {str(e)}")
#             raise
    
#     def close(self):
#         """Clean up resources."""
#         try:
#             # Stop all heartbeats
#             for consumer_id in list(self.active_consumers.keys()):
#                 self._stop_heartbeat(consumer_id)
            
#             # Close MongoDB connection
#             if hasattr(self, 'client'):
#                 self.client.close()
#                 logger.info("Closed MongoDB connection")
#         except Exception as e:
#             logger.error(f"Error during close: {str(e)}")

#     def __enter__(self):
#         return self
    
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         self.close()
#####################OLD CODE END###################################################################################
import os
import pymongo
from pymongo import MongoClient, errors
from datetime import datetime, timezone, timedelta
import time
import uuid
import logging
from typing import Optional, Dict, Any, List
from threading import Lock
from enum import Enum
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MessageStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class MongoDBMessageQueue:
    def __init__(
        self,
        mongo_uri: Optional[str] = None,
        db_name: Optional[str] = None,
        max_retries: Optional[int] = None,
        retry_delay: Optional[float] = None,
        visibility_timeout: Optional[int] = None,
        heartbeat_interval: Optional[int] = None,
    ):
        """Initialize the MongoDB message queue with environment variable configuration."""
        self.mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self.db_name = db_name or os.getenv("MONGO_DB_NAME", "message_queue_db")
        self.max_retries = max_retries or int(os.getenv("MQ_MAX_RETRIES", "3"))
        self.retry_delay = retry_delay or float(os.getenv("MQ_RETRY_DELAY", "1.0"))
        self.visibility_timeout = visibility_timeout or int(os.getenv("MQ_VISIBILITY_TIMEOUT", "300"))
        self.heartbeat_interval = heartbeat_interval or int(os.getenv("MQ_HEARTBEAT_INTERVAL", "60"))
        
        self._validate_config()
        self._lock = Lock()
        self.active_consumers = {}
        self._init_mongo_connection()
        self._ensure_indexes()

    def _validate_config(self):
        """Validate configuration parameters."""
        if not self.mongo_uri:
            raise ValueError("MongoDB URI must be provided")
        if not self.db_name:
            raise ValueError("Database name must be provided")

    def _init_mongo_connection(self):
        """Initialize MongoDB connection with proper TLS handling."""
        tls_enabled = os.getenv("MONGO_TLS", "false").lower() == "true"
        
        for attempt in range(self.max_retries + 1):
            try:
                self.client = MongoClient(
                    self.mongo_uri,
                    retryWrites=True,
                    retryReads=True,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=30000,
                    socketTimeoutMS=30000,
                    tls=tls_enabled,
                    tlsCAFile=os.getenv("MONGO_CA_FILE") if tls_enabled else None,
                    tlsCertificateKeyFile=os.getenv("MONGO_CERT_KEY_FILE") if tls_enabled else None,
                    tlsAllowInvalidCertificates=os.getenv("MONGO_ALLOW_INVALID_CERTS", "false").lower() == "true" if tls_enabled else False
                )
                self.client.admin.command('ping')
                self.db = self.client[self.db_name]
                self.messages_collection = self.db["messages"]
                self.channels_collection = self.db["channels"]
                logger.info("MongoDB connection established")
                break
            except errors.PyMongoError as e:
                if attempt == self.max_retries:
                    logger.error(f"Connection failed after {self.max_retries} attempts")
                    raise
                logger.warning(f"Connection attempt {attempt + 1} failed: {str(e)}")
                time.sleep(self.retry_delay)

    def _ensure_indexes(self):
        """Create required indexes with proper timeouts."""
        try:
            with self.messages_collection.with_options(max_time_ms=10000) as collection:
                collection.create_index([
                    ("channel", 1),
                    ("status", 1),
                    ("created_at", 1)
                ], name="channel_status_created_idx")
                
                collection.create_index([
                    ("channel", 1),
                    ("status", 1),
                    ("heartbeat", 1)
                ], name="channel_status_heartbeat_idx")
                
                collection.create_index([
                    ("channel", 1),
                    ("sequence_number", 1)
                ], name="channel_sequence_unique_idx", unique=True)
                
                retention_days = int(os.getenv("MQ_RETENTION_DAYS", "7"))
                if retention_days > 0:
                    collection.create_index(
                        [("updated_at", 1)],
                        name="completed_messages_ttl_idx",
                        expireAfterSeconds=retention_days * 24 * 60 * 60,
                        partialFilterExpression={"status": MessageStatus.COMPLETED.value}
                    )
        except errors.PyMongoError as e:
            logger.error(f"Index creation failed: {str(e)}")
            raise

    def _get_current_time(self):
        """Get current UTC time with timezone awareness."""
        return datetime.now(timezone.utc)

    def _get_next_sequence_number(self, channel: str) -> int:
        """Atomically increment and get sequence number."""
        try:
            result = self.channels_collection.find_one_and_update(
                {"_id": channel},
                {"$inc": {"sequence_number": 1}},
                upsert=True,
                return_document=pymongo.ReturnDocument.AFTER,
                max_time_ms=5000
            )
            return result["sequence_number"]
        except errors.PyMongoError as e:
            logger.error(f"Sequence number error: {str(e)}")
            raise

    def publish_message(self, channel: str, message: Dict[str, Any]) -> str:
        """Publish message to channel with ordering guarantee."""
        message_id = str(uuid.uuid4())
        sequence_number = self._get_next_sequence_number(channel)
        current_time = self._get_current_time()
        
        message_doc = {
            "_id": message_id,
            "channel": channel,
            "sequence_number": sequence_number,
            "payload": message,
            "status": MessageStatus.PENDING.value,
            "created_at": current_time,
            "updated_at": current_time,
            "attempts": 0,
            "heartbeat": None,
            "consumer_id": None
        }
        
        try:
            self.messages_collection.insert_one(message_doc)
            logger.info(f"Message published to {channel} (ID: {message_id})")
            return message_id
        except errors.PyMongoError as e:
            logger.error(f"Publish failed: {str(e)}")
            raise

    def get_message(self, channel: str, consumer_id: str, timeout: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Get next message from channel with timeout support."""
        start_time = time.time()
        
        while True:
            try:
                self._release_expired_messages(channel)
                
                message = self.messages_collection.find_one_and_update(
                    {
                        "channel": channel,
                        "status": MessageStatus.PENDING.value
                    },
                    {
                        "$set": {
                            "status": MessageStatus.PROCESSING.value,
                            "updated_at": self._get_current_time(),
                            "heartbeat": self._get_current_time(),
                            "consumer_id": consumer_id
                        },
                        "$inc": {"attempts": 1}
                    },
                    sort=[("sequence_number", pymongo.ASCENDING)],
                    return_document=pymongo.ReturnDocument.AFTER,
                    max_time_ms=5000
                )
                
                if message:
                    self._start_heartbeat(channel, message["_id"], consumer_id)
                    return message
                
                if timeout is None or (time.time() - start_time) >= timeout:
                    return None
                
                time.sleep(min(1.0, self.retry_delay))
                
            except errors.PyMongoError as e:
                logger.error(f"Message retrieval error: {str(e)}")
                if timeout is None or (time.time() - start_time) >= timeout:
                    return None
                time.sleep(self.retry_delay)

    def _start_heartbeat(self, channel: str, message_id: str, consumer_id: str):
        """Start heartbeat monitoring for processing message."""
        if consumer_id in self.active_consumers:
            self._stop_heartbeat(consumer_id)
        
        self.active_consumers[consumer_id] = {
            "channel": channel,
            "message_id": message_id,
            "last_heartbeat": time.time(),
            "active": True
        }
        
        import threading
        def heartbeat_loop():
            while self.active_consumers.get(consumer_id, {}).get("active", False):
                try:
                    now = self._get_current_time()
                    result = self.messages_collection.update_one(
                        {
                            "_id": message_id,
                            "status": MessageStatus.PROCESSING.value,
                            "consumer_id": consumer_id
                        },
                        {
                            "$set": {
                                "heartbeat": now,
                                "updated_at": now
                            }
                        },
                        max_time_ms=3000
                    )
                    
                    if result.modified_count == 0:
                        logger.warning("Heartbeat failed - message may be completed")
                        break
                    
                    self.active_consumers[consumer_id]["last_heartbeat"] = time.time()
                    time.sleep(self.heartbeat_interval)
                except errors.PyMongoError as e:
                    logger.error(f"Heartbeat error: {str(e)}")
                    time.sleep(self.retry_delay)
        
        threading.Thread(target=heartbeat_loop, daemon=True).start()

    def _release_expired_messages(self, channel: str):
        """Release messages that exceeded visibility timeout."""
        try:
            cutoff = self._get_current_time() - timedelta(seconds=self.visibility_timeout)
            
            with self.messages_collection.with_options(max_time_ms=10000) as collection:
                result = collection.update_many(
                    {
                        "channel": channel,
                        "status": MessageStatus.PROCESSING.value,
                        "heartbeat": {"$lt": cutoff}
                    },
                    {
                        "$set": {
                            "status": MessageStatus.PENDING.value,
                            "updated_at": self._get_current_time(),
                            "heartbeat": None,
                            "consumer_id": None
                        }
                    }
                )
                
                if result.modified_count > 0:
                    logger.info(f"Released {result.modified_count} expired messages")
        
        except errors.PyMongoError as e:
            logger.error(f"Message release error: {str(e)}")

    def complete_message(self, message_id: str, consumer_id: str):
        """Mark message as successfully processed."""
        self._stop_heartbeat(consumer_id)
        
        try:
            result = self.messages_collection.update_one(
                {
                    "_id": message_id,
                    "consumer_id": consumer_id,
                    "status": MessageStatus.PROCESSING.value
                },
                {
                    "$set": {
                        "status": MessageStatus.COMPLETED.value,
                        "updated_at": self._get_current_time(),
                        "heartbeat": None,
                        "consumer_id": None
                    }
                },
                max_time_ms=5000
            )
            
            if result.modified_count == 0:
                logger.warning("Message completion failed - may already be processed")
        except errors.PyMongoError as e:
            logger.error(f"Completion error: {str(e)}")
            raise

    def fail_message(self, message_id: str, consumer_id: str):
        """Mark message as failed with retry logic."""
        self._stop_heartbeat(consumer_id)
        max_attempts = int(os.getenv("MQ_MAX_ATTEMPTS", "3"))
        
        try:
            message = self.messages_collection.find_one(
                {"_id": message_id},
                {"attempts": 1},
                max_time_ms=3000
            )
            
            if not message:
                logger.warning("Message not found")
                return
            
            new_status = MessageStatus.FAILED.value if message.get("attempts", 0) >= max_attempts - 1 else MessageStatus.PENDING.value
            
            result = self.messages_collection.update_one(
                {
                    "_id": message_id,
                    "consumer_id": consumer_id,
                    "status": MessageStatus.PROCESSING.value
                },
                {
                    "$set": {
                        "status": new_status,
                        "updated_at": self._get_current_time(),
                        "heartbeat": None,
                        "consumer_id": None if new_status == MessageStatus.PENDING.value else consumer_id
                    }
                },
                max_time_ms=5000
            )
            
            if result.modified_count == 0:
                logger.warning("Message failure update failed")
        except errors.PyMongoError as e:
            logger.error(f"Failure handling error: {str(e)}")
            raise

    def get_channel_stats(self, channel: str) -> Dict[str, int]:
        """Get statistics for a message channel."""
        try:
            pipeline = [
                {"$match": {"channel": channel}},
                {"$group": {"_id": "$status", "count": {"$sum": 1}}},
                {"$project": {"status": "$_id", "count": 1, "_id": 0}}
            ]
            
            results = list(self.messages_collection.aggregate(
                pipeline,
                max_time_ms=5000
            ))
            
            stats = {status.value: 0 for status in MessageStatus}
            stats["total"] = 0
            
            for result in results:
                stats[result["status"]] = result["count"]
                stats["total"] += result["count"]
            
            return stats
        except errors.PyMongoError as e:
            logger.error(f"Stats error: {str(e)}")
            raise

    def _stop_heartbeat(self, consumer_id: str):
        """Stop heartbeat for a consumer."""
        if consumer_id in self.active_consumers:
            self.active_consumers[consumer_id]["active"] = False
            del self.active_consumers[consumer_id]

    def close(self):
        """Clean up resources."""
        try:
            for consumer_id in list(self.active_consumers.keys()):
                self._stop_heartbeat(consumer_id)
            if hasattr(self, 'client'):
                self.client.close()
        except Exception as e:
            logger.error(f"Cleanup error: {str(e)}")

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()