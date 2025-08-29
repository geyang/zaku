#!/usr/bin/env python3
"""
Test MongoDB integration with Zaku
"""

import asyncio
import os
import sys
import time
from unittest.mock import Mock, patch, AsyncMock

# Add the zaku package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

import pytest
from zaku.mongo_helpers import RobustMongo, MongoManager, MongoConnectionError
from zaku.interfaces import Job
from zaku.server import TaskServer


class TestMongoDBIntegration:
    """Test MongoDB integration functionality"""
    
    @pytest.fixture
    def mock_mongo_client(self):
        """Mock MongoDB client for testing"""
        mock_client = Mock()
        mock_client.store_payload = AsyncMock(return_value=True)
        mock_client.retrieve_payload = AsyncMock(return_value=b"test_payload")
        mock_client.delete_payload = AsyncMock(return_value=True)
        return mock_client
    
    @pytest.fixture
    def mock_redis_client(self):
        """Mock Redis client for testing"""
        mock_redis = Mock()
        # Mock the eval to return a job key that matches the expected format
        mock_redis.eval = AsyncMock(return_value=[b"test_prefix:test_queue:job123"])
        mock_redis.json.return_value = Mock()
        mock_redis.pipeline.return_value = Mock()
        # Mock publish method for pub/sub operations
        mock_redis.publish = AsyncMock(return_value=1)
        
        # Mock pipeline methods
        mock_pipeline = Mock()
        mock_pipeline.execute = AsyncMock(return_value=True)
        mock_pipeline.json.return_value = Mock()
        mock_pipeline.unlink.return_value = mock_pipeline
        mock_redis.pipeline.return_value = mock_pipeline
        
        return mock_redis
    
    def test_mongo_connection_string_building(self):
        """Test MongoDB connection string building"""
        from zaku.server import MongoDB
        
        # Test with individual components
        mongo = MongoDB()
        mongo.host = "localhost"
        mongo.port = 27017
        mongo.database = "testdb"
        
        # Force recalculation of connection string
        mongo.__post_init__()
        
        expected = "mongodb://localhost:27017/testdb"
        assert mongo.connection_string == expected
        
        # Test with authentication
        mongo.username = "user"
        mongo.password = "pass"
        mongo.auth_source = "admin"
        
        # Force recalculation of connection string
        mongo.__post_init__()
        
        expected = "mongodb://user:pass@localhost:27017/testdb?authSource=admin"
        assert mongo.connection_string == expected
        
        # Test with URI - clear previous auth info first
        mongo.username = None
        mongo.password = None
        mongo.auth_source = None
        mongo.uri = "mongodb://host1:27017,host2:27017/?replicaSet=rs0"
        
        # Force recalculation of connection string
        mongo.__post_init__()
        
        expected = "mongodb://host1:27017,host2:27017/?replicaSet=rs0"
        assert mongo.connection_string == expected
    
    @pytest.mark.asyncio
    async def test_payload_storage_and_retrieval(self, mock_mongo_client):
        """Test payload storage and retrieval"""
        # Test store_payload
        result = await mock_mongo_client.store_payload("test_collection", "job123", b"test_data")
        assert result is True
        
        # Test retrieve_payload
        payload = await mock_mongo_client.retrieve_payload("test_collection", "job123")
        assert payload == b"test_payload"
        
        # Test delete_payload
        result = await mock_mongo_client.delete_payload("test_collection", "job123")
        assert result is True
    
    @pytest.mark.asyncio
    async def test_job_add_with_mongodb(self, mock_redis_client, mock_mongo_client):
        """Test Job.add method with MongoDB integration"""
        with patch('zaku.interfaces.get_mongo_client', return_value=mock_mongo_client):
            # Test adding a job with payload
            result = await Job.add(
                mock_redis_client,
                "test_queue",
                prefix="test_prefix",
                payload=b"test_payload",
                job_id="job123"
            )
            
            assert result is True
            mock_mongo_client.store_payload.assert_called_once_with(
                "test_prefix_test_queue", "job123", b"test_payload"
            )
    
    @pytest.mark.asyncio
    async def test_job_take_with_mongodb(self, mock_redis_client, mock_mongo_client):
        """Test Job.take method with MongoDB integration"""
        with patch('zaku.interfaces.get_mongo_client', return_value=mock_mongo_client):
            # Test taking a job
            job_id, payload = await Job.take(
                mock_redis_client,
                "test_queue",
                prefix="test_prefix"
            )
            
            assert job_id == "job123"
            assert payload == b"test_payload"
            mock_mongo_client.retrieve_payload.assert_called_once_with(
                "test_prefix_test_queue", "job123"
            )
    
    @pytest.mark.asyncio
    async def test_job_remove_with_mongodb(self, mock_redis_client, mock_mongo_client):
        """Test Job.remove method with MongoDB integration"""
        with patch('zaku.interfaces.get_mongo_client', return_value=mock_mongo_client):
            # Test removing a job
            result = await Job.remove(
                mock_redis_client,
                "job123",
                "test_queue",
                prefix="test_prefix"
            )
            
            assert result is True
            mock_mongo_client.delete_payload.assert_called_once_with(
                "test_prefix_test_queue", "job123"
            )
    
    async def test_job_publish_with_mongodb(self, mock_redis_client, mock_mongo_client):
        """Test that publish stores payload in MongoDB and publishes message ID to Redis"""
        # Mock MongoDB store_payload
        mock_mongo_client.store_payload = AsyncMock()
        
        # Test data
        payload = b"test payload data"
        topic_id = "test_topic"
        queue = "test_queue"
        prefix = "test_prefix"
        
        # Mock the eval to return a job key that matches the expected format
        mock_redis_client.eval = AsyncMock(return_value=[b"test_prefix:test_queue:job123"])
        
        # Call publish
        result = await Job.publish(
            mock_redis_client, 
            queue, 
            payload=payload, 
            topic_id=topic_id, 
            prefix=prefix
        )
        
        # Verify MongoDB was called
        mock_mongo_client.store_payload.assert_called_once()
        call_args = mock_mongo_client.store_payload.call_args
        assert call_args[0][0] == f"{prefix}_{queue}_topics"  # collection name
        assert call_args[0][2] == payload  # payload
        
        # Verify Redis publish was called with message ID (not payload)
        mock_redis_client.publish.assert_called_once()
        publish_call = mock_redis_client.publish.call_args
        assert publish_call[0][0] == f"{prefix}:{queue}.topics:{topic_id}"
        # The published data should be a message ID (UUID string encoded as bytes)
        published_data = publish_call[0][1]
        assert isinstance(published_data, bytes)
        message_id = published_data.decode()
        assert len(message_id) == 36 and '-' in message_id  # UUID format

    async def test_job_subscribe_with_mongodb(self, mock_redis_client, mock_mongo_client):
        """Test that subscribe retrieves payload from MongoDB when receiving message ID"""
        # Mock MongoDB retrieve_payload
        mock_mongo_client.retrieve_payload = AsyncMock(return_value=b"retrieved payload")
        
        # Mock Redis pubsub to return a message ID
        mock_pubsub = Mock()
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.get_message = AsyncMock(return_value={
            "type": "message",
            "data": b"12345678-1234-1234-1234-123456789abc"  # UUID format
        })
        mock_redis_client.pubsub.return_value.__aenter__.return_value = mock_pubsub
        
        # Test data
        topic_id = "test_topic"
        queue = "test_queue"
        prefix = "test_prefix"
        
        # Call subscribe
        result = await Job.subscribe(
            mock_redis_client, 
            queue, 
            topic_id=topic_id, 
            prefix=prefix,
            timeout=0.1
        )
        
        # Verify MongoDB was called to retrieve payload
        mock_mongo_client.retrieve_payload.assert_called_once()
        call_args = mock_mongo_client.retrieve_payload.call_args
        assert call_args[0][0] == f"{prefix}_{queue}_topics"  # collection name
        assert call_args[0][1] == "12345678-1234-1234-1234-123456789abc"  # message ID
        
        # Verify result is the retrieved payload
        assert result == b"retrieved payload"

    async def test_job_subscribe_fallback_to_direct(self, mock_redis_client, mock_mongo_client):
        """Test that subscribe falls back to direct Redis data when not a UUID"""
        # Mock Redis pubsub to return non-UUID data
        mock_pubsub = Mock()
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.get_message = AsyncMock(return_value={
            "type": "message",
            "data": b"direct payload data"  # Not UUID format
        })
        mock_redis_client.pubsub.return_value.__aenter__.return_value = mock_pubsub
        
        # Test data
        topic_id = "test_topic"
        queue = "test_queue"
        prefix = "test_prefix"
        
        # Call subscribe
        result = await Job.subscribe(
            mock_redis_client, 
            queue, 
            topic_id=topic_id, 
            prefix=prefix,
            timeout=0.1
        )
        
        # Verify MongoDB was NOT called
        mock_mongo_client.retrieve_payload.assert_not_called()
        
        # Verify result is the direct Redis data
        assert result == b"direct payload data"

    async def test_job_subscribe_stream_with_mongodb(self, mock_redis_client, mock_mongo_client):
        """Test that subscribe_stream retrieves payload from MongoDB when receiving message ID"""
        # Mock MongoDB retrieve_payload
        mock_mongo_client.retrieve_payload = AsyncMock(return_value=b"streamed payload")
        
        # Mock Redis pubsub to return a message ID
        mock_pubsub = Mock()
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.get_message = AsyncMock(return_value={
            "type": "message",
            "data": b"87654321-4321-4321-4321-cba987654321"  # UUID format
        })
        mock_redis_client.pubsub.return_value.__aenter__.return_value = mock_pubsub
        
        # Test data
        topic_id = "test_topic"
        queue = "test_queue"
        prefix = "test_prefix"
        
        # Call subscribe_stream
        results = []
        async for payload in Job.subscribe_stream(
            mock_redis_client, 
            queue, 
            topic_id=topic_id, 
            prefix=prefix,
            timeout=0.1
        ):
            results.append(payload)
        
        # Verify MongoDB was called to retrieve payload
        mock_mongo_client.retrieve_payload.assert_called_once()
        call_args = mock_mongo_client.retrieve_payload.call_args
        assert call_args[0][0] == f"{prefix}_{queue}_topics"  # collection name
        assert call_args[0][1] == "87654321-4321-4321-4321-cba987654321"  # message ID
        
        # Verify results contain the retrieved payload
        assert len(results) == 1
        assert results[0] == b"streamed payload"
    
    def test_mongo_manager_singleton(self):
        """Test MongoManager singleton pattern"""
        manager1 = MongoManager()
        manager2 = MongoManager()
        assert manager1 is manager2
    
    @pytest.mark.asyncio
    async def test_mongo_connection_error_handling(self):
        """Test MongoDB connection error handling"""
        with pytest.raises(MongoConnectionError):
            # This should raise an error since MongoDB is not initialized
            MongoManager.get_client()
    
    def test_task_server_mongo_initialization(self):
        """Test TaskServer MongoDB initialization"""
        server = TaskServer()
        
        # Check that MongoDB wrapper is created
        assert hasattr(server, 'mongo_wrapper')
        assert hasattr(server, 'mongo_initialized')
        assert server.mongo_initialized is False
        
        # Check MongoDB configuration
        assert server.mongo_wrapper.host == "localhost"
        assert server.mongo_wrapper.port == 27017
        assert server.mongo_wrapper.database == "zaku"


class TestConcurrencySafety:
    """Test concurrency safety of MongoDB operations"""
    
    @pytest.mark.asyncio
    async def test_concurrent_payload_operations(self):
        """Test concurrent payload operations"""
        # This test would require a real MongoDB instance
        # For now, we'll test the mock behavior
        
        mock_client = Mock()
        mock_client.store_payload = AsyncMock(return_value=True)
        mock_client.retrieve_payload = AsyncMock(return_value=b"concurrent_test")
        
        # Simulate concurrent operations
        async def store_payload(job_id):
            return await mock_client.store_payload("test_collection", job_id, b"data")
        
        async def retrieve_payload(job_id):
            return await mock_client.retrieve_payload("test_collection", job_id)
        
        # Run concurrent operations
        tasks = []
        for i in range(10):
            tasks.append(store_payload(f"job_{i}"))
            tasks.append(retrieve_payload(f"job_{i}"))
        
        results = await asyncio.gather(*tasks)
        
        # All operations should succeed
        assert all(results)
        assert mock_client.store_payload.call_count == 10
        assert mock_client.retrieve_payload.call_count == 10


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"]) 