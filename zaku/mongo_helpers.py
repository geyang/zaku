import asyncio
from typing import Optional, Any, Dict, List
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
from pymongo.errors import PyMongoError, DuplicateKeyError
import logging
from bson import ObjectId

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

    async def insert_payload_auto_id(self, collection_name: str, payload: Optional[bytes], 
                                     metadata: Optional[Dict] = None) -> str:
        """Insert payload without specifying _id and return the generated ObjectId as a string.
        Retries on transient failures with exponential backoff.
        """
        collection = self.get_collection(collection_name)
        document = {
            "payload": payload,
            "created_at": asyncio.get_event_loop().time(),
            "metadata": metadata or {}
        }
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = await collection.insert_one(document)
                return str(result.inserted_id)
            except PyMongoError as e:
                logger.warning(f"Failed to insert payload (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    raise MongoConnectionError(f"Failed to insert payload after {max_retries} attempts: {e}")
                await asyncio.sleep(0.1 * (2 ** attempt))

    async def retrieve_payload(self, collection_name: str, job_id: str) -> Optional[bytes]:
        """Retrieve payload from MongoDB with retry logic"""
        collection = self.get_collection(collection_name)
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                document = None
                # Try ObjectId form first if applicable, then fall back to string id for backward compatibility
                if isinstance(job_id, str) and ObjectId.is_valid(job_id):
                    document = await collection.find_one({"_id": ObjectId(job_id)})
                    if document is None:
                        document = await collection.find_one({"_id": job_id})
                else:
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
                deleted = 0
                if isinstance(job_id, str) and ObjectId.is_valid(job_id):
                    result = await collection.delete_one({"_id": ObjectId(job_id)})
                    deleted = result.deleted_count
                    if deleted == 0:
                        result = await collection.delete_one({"_id": job_id})
                        deleted = result.deleted_count
                else:
                    result = await collection.delete_one({"_id": job_id})
                    deleted = result.deleted_count
                return deleted > 0
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
                object_ids = [ObjectId(j) for j in job_ids if isinstance(j, str) and ObjectId.is_valid(j)]
                string_ids = [j for j in job_ids if not (isinstance(j, str) and ObjectId.is_valid(j))]
                filters: List[Dict[str, Any]] = []
                if object_ids:
                    filters.append({"_id": {"$in": object_ids}})
                if string_ids:
                    filters.append({"_id": {"$in": string_ids}})
                if not filters:
                    return 0
                query: Dict[str, Any] = filters[0] if len(filters) == 1 else {"$or": filters}
                result = await collection.delete_many(query)
                return result.deleted_count
            except PyMongoError as e:
                logger.warning(f"Failed to bulk delete payloads (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    raise MongoConnectionError(f"Failed to bulk delete payloads after {max_retries} attempts: {e}")
                await asyncio.sleep(0.1 * (2 ** attempt))  # Exponential backoff
        
        return 0
    
    async def clear_collection(self, collection_name: str) -> bool:
        """Clear entire collection with retry logic"""
        collection = self.get_collection(collection_name)
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = await collection.delete_many({})
                logger.info(f"Cleared collection '{collection_name}': {result.deleted_count} documents removed")
                return True
            except PyMongoError as e:
                logger.warning(f"Failed to clear collection '{collection_name}' (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    raise MongoConnectionError(f"Failed to clear collection after {max_retries} attempts: {e}")
                await asyncio.sleep(0.1 * (2 ** attempt))  # Exponential backoff
        
        return False
    
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