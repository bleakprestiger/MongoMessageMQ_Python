import os
import pymongo
from pymongo import MongoClient, errors
from datetime import datetime, timezone, timedelta
import time
import uuid
import logging
from typing import Optional, Dict, Any, List
from threading import Lock, Thread
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
        """Initialize the MongoDB message queue."""
        self.mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self.db_name = db_name or os.getenv("MONGO_DB_NAME", "message_queue_db")
        self.max_retries = max_retries or int(os.getenv("MQ_MAX_RETRIES", "3"))
        self.retry_delay = retry_delay or float(os.getenv("MQ_RETRY_DELAY", "1.0"))
        self.visibility_timeout = visibility_timeout or int(os.getenv("MQ_VISIBILITY_TIMEOUT", "300"))
        self.heartbeat_interval = heartbeat_interval or int(os.getenv("MQ_HEARTBEAT_INTERVAL", "60"))
        
        self._lock = Lock()
        self.active_consumers = {}
        self._init_mongo_connection()
        self._ensure_indexes()

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
                self.client.admin.command('ping', socketTimeoutMS=5000)
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
        """Create required indexes with proper timeout handling."""
        try:
            self.messages_collection.create_index(
                [("channel", 1), ("status", 1), ("created_at", 1)],
                name="channel_status_created_idx",
                maxTimeMS=10000
            )
            
            self.messages_collection.create_index(
                [("channel", 1), ("status", 1), ("heartbeat", 1)],
                name="channel_status_heartbeat_idx",
                maxTimeMS=10000
            )
            
            self.messages_collection.create_index(
                [("channel", 1), ("sequence_number", 1)],
                name="channel_sequence_unique_idx",
                unique=True,
                maxTimeMS=10000
            )
            
            retention_days = int(os.getenv("MQ_RETENTION_DAYS", "7"))
            if retention_days > 0:
                self.messages_collection.create_index(
                    [("updated_at", 1)],
                    name="completed_messages_ttl_idx",
                    expireAfterSeconds=retention_days * 24 * 60 * 60,
                    partialFilterExpression={"status": MessageStatus.COMPLETED.value},
                    maxTimeMS=10000
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
                return_document=pymongo.ReturnDocument.AFTER
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
                    return_document=pymongo.ReturnDocument.AFTER
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
        with self._lock:
            if consumer_id in self.active_consumers:
                self._stop_heartbeat(consumer_id)
            
            self.active_consumers[consumer_id] = {
                "channel": channel,
                "message_id": message_id,
                "last_heartbeat": time.time(),
                "active": True,
                "thread": None
            }
            
            def heartbeat_loop():
                while True:
                    with self._lock:
                        if not self.active_consumers.get(consumer_id, {}).get("active", False):
                            break
                    
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
                            }
                        )
                        
                        with self._lock:
                            if result.modified_count == 0 or consumer_id not in self.active_consumers:
                                break
                            self.active_consumers[consumer_id]["last_heartbeat"] = time.time()
                            
                    except errors.PyMongoError as e:
                        logger.error(f"Heartbeat error: {str(e)}")
                    
                    time.sleep(self.heartbeat_interval)
            
            thread = Thread(target=heartbeat_loop, daemon=True)
            self.active_consumers[consumer_id]["thread"] = thread
            thread.start()

    def _release_expired_messages(self, channel: str):
        """Release messages that exceeded visibility timeout."""
        try:
            cutoff = self._get_current_time() - timedelta(seconds=self.visibility_timeout)
            
            result = self.messages_collection.update_many(
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
                }
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
                {"attempts": 1}
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
                }
            )
            
            if result.modified_count == 0:
                logger.warning("Message failure update failed")
        except errors.PyMongoError as e:
            logger.error(f"Failure handling error: {str(e)}")
            raise

    def _stop_heartbeat(self, consumer_id: str):
        """Stop heartbeat for a consumer."""
        with self._lock:
            if consumer_id in self.active_consumers:
                self.active_consumers[consumer_id]["active"] = False
                thread = self.active_consumers[consumer_id].get("thread")
                if thread and thread.is_alive():
                    thread.join(timeout=1.0)
                del self.active_consumers[consumer_id]

    def get_channel_stats(self, channel: str) -> Dict[str, int]:
        """Get statistics for a message channel."""
        try:
            pipeline = [
                {"$match": {"channel": channel}},
                {"$group": {"_id": "$status", "count": {"$sum": 1}}},
                {"$project": {"status": "$_id", "count": 1, "_id": 0}}
            ]
            
            results = list(self.messages_collection.aggregate(pipeline))
            
            stats = {status.value: 0 for status in MessageStatus}
            stats["total"] = 0
            
            for result in results:
                stats[result["status"]] = result["count"]
                stats["total"] += result["count"]
            
            return stats
        except errors.PyMongoError as e:
            logger.error(f"Stats error: {str(e)}")
            raise

    def close(self):
        """Clean up resources."""
        try:
            with self._lock:
                consumers = list(self.active_consumers.keys())
                for consumer_id in consumers:
                    self._stop_heartbeat(consumer_id)
            if hasattr(self, 'client'):
                self.client.close()
        except Exception as e:
            logger.error(f"Cleanup error: {str(e)}")

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()