import asyncio
from typing import Optional, Any, Dict, List
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
from pymongo.errors import PyMongoError, DuplicateKeyError
import logging

logger = logging.getLogger(__name__)


class MongoConnectionError(Exception):
    """MongoDB connection error"""
    pass


class RobustMongo:
    """Robust MongoDB client with connection retry and error handling"""
    
    def __init__(self, client: AsyncIOMotorClient, database: AsyncIOMotorDatabase):
        self.client = client
        self.database = database
        self._collections: Dict[str, AsyncIOMotorCollection] = {}
    
    def get_collection(self, collection_name: str) -> AsyncIOMotorCollection:
        """Get or create a collection with caching"""
        if collection_name not in self._collections:
            self._collections[collection_name] = self.database[collection_name]
        return self._collections[collection_name]
    
    async def store_payload(self, collection_name: str, job_id: str, payload: bytes, 
                           metadata: Optional[Dict] = None) -> bool:
        """Store payload in MongoDB with retry logic"""
        collection = self.get_collection(collection_name)
        
        document = {
            "_id": job_id,
            "payload": payload,
            "created_at": asyncio.get_event_loop().time(),
            "metadata": metadata or {}
        }
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await collection.insert_one(document)
                return True
            except DuplicateKeyError:
                # If document already exists, update it
                try:
                    await collection.replace_one({"_id": job_id}, document)
                    return True
                except PyMongoError as e:
                    logger.warning(f"Failed to update existing payload for job {job_id}: {e}")
                    if attempt == max_retries - 1:
                        raise MongoConnectionError(f"Failed to store payload after {max_retries} attempts: {e}")
                    await asyncio.sleep(0.1 * (2 ** attempt))  # Exponential backoff
            except PyMongoError as e:
                logger.warning(f"Failed to store payload for job {job_id} (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    raise MongoConnectionError(f"Failed to store payload after {max_retries} attempts: {e}")
                await asyncio.sleep(0.1 * (2 ** attempt))  # Exponential backoff
        
        return False
    
    async def retrieve_payload(self, collection_name: str, job_id: str) -> Optional[bytes]:
        """Retrieve payload from MongoDB with retry logic"""
        collection = self.get_collection(collection_name)
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                document = await collection.find_one({"_id": job_id})
                if document and "payload" in document:
                    return document["payload"]
                return None
            except PyMongoError as e:
                logger.warning(f"Failed to retrieve payload for job {job_id} (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    raise MongoConnectionError(f"Failed to retrieve payload after {max_retries} attempts: {e}")
                await asyncio.sleep(0.1 * (2 ** attempt))  # Exponential backoff
        
        return None
    
    async def delete_payload(self, collection_name: str, job_id: str) -> bool:
        """Delete payload from MongoDB with retry logic"""
        collection = self.get_collection(collection_name)
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = await collection.delete_one({"_id": job_id})
                return result.deleted_count > 0
            except PyMongoError as e:
                logger.warning(f"Failed to delete payload for job {job_id} (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    raise MongoConnectionError(f"Failed to delete payload after {max_retries} attempts: {e}")
                await asyncio.sleep(0.1 * (2 ** attempt))  # Exponential backoff
        
        return False
    
    async def bulk_delete_payloads(self, collection_name: str, job_ids: List[str]) -> int:
        """Bulk delete multiple payloads with retry logic"""
        if not job_ids:
            return 0
            
        collection = self.get_collection(collection_name)
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = await collection.delete_many({"_id": {"$in": job_ids}})
                return result.deleted_count
            except PyMongoError as e:
                logger.warning(f"Failed to bulk delete payloads (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    raise MongoConnectionError(f"Failed to bulk delete payloads after {max_retries} attempts: {e}")
                await asyncio.sleep(0.1 * (2 ** attempt))  # Exponential backoff
        
        return 0
    
    async def health_check(self) -> bool:
        """Check MongoDB connection health"""
        try:
            await self.database.command("ping")
            return True
        except PyMongoError:
            return False
    
    async def close(self):
        """Close MongoDB connection"""
        try:
            self.client.close()
        except Exception as e:
            logger.warning(f"Error closing MongoDB connection: {e}")


class MongoManager:
    """MongoDB connection manager for singleton pattern"""
    
    _instance = None
    _client: Optional[AsyncIOMotorClient] = None
    _database: Optional[AsyncIOMotorDatabase] = None
    _robust_mongo: Optional[RobustMongo] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoManager, cls).__new__(cls)
        return cls._instance
    
    @classmethod
    async def initialize(cls, connection_string: str, database_name: str):
        """Initialize MongoDB connection"""
        if cls._client is None:
            try:
                cls._client = AsyncIOMotorClient(connection_string)
                cls._database = cls._client[database_name]
                cls._robust_mongo = RobustMongo(cls._client, cls._database)
                
                # Test connection
                await cls._robust_mongo.health_check()
                logger.info(f"MongoDB connected successfully to {database_name}")
                
            except Exception as e:
                logger.error(f"Failed to initialize MongoDB connection: {e}")
                raise MongoConnectionError(f"Failed to initialize MongoDB: {e}")
    
    @classmethod
    def get_client(cls) -> RobustMongo:
        """Get MongoDB client instance"""
        if cls._robust_mongo is None:
            raise MongoConnectionError("MongoDB not initialized. Call initialize() first.")
        return cls._robust_mongo
    
    @classmethod
    async def close(cls):
        """Close MongoDB connection"""
        if cls._robust_mongo:
            await cls._robust_mongo.close()
            cls._client = None
            cls._database = None
            cls._robust_mongo = None 