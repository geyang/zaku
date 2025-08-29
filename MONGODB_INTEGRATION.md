# MongoDB Integration for Zaku Task Queue

This document describes the MongoDB integration that has been added to Zaku, allowing payload storage in MongoDB while keeping job metadata in Redis.

## Overview

The integration separates concerns:
- **Redis**: Stores job metadata (status, timestamps, etc.) and handles job queue operations
- **MongoDB**: Stores job payloads (binary data, large objects, etc.)

This architecture provides:
- Better performance for large payloads
- Scalable storage for binary data
- Maintained Redis performance for job metadata
- Fault tolerance with retry mechanisms

## Configuration

### Environment Variables

#### Single MongoDB Server
```bash
# MongoDB connection
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_USERNAME=admin
MONGO_PASSWORD=your_password
MONGO_DATABASE=zaku
MONGO_AUTH_SOURCE=admin

# Redis connection (for job metadata)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=your_redis_password
REDIS_DB=0
```

#### MongoDB Replica Set
```bash
# MongoDB replica set connection
MONGO_URI=mongodb://host1:27017,host2:27017,host3:27017/?replicaSet=rs0
MONGO_DATABASE=zaku
MONGO_USERNAME=admin
MONGO_PASSWORD=your_password
MONGO_AUTH_SOURCE=admin

# Redis connection (for job metadata)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=your_redis_password
REDIS_DB=0
```

### CLI Options

```bash
python -m zaku.server \
    --mongo.host localhost \
    --mongo.port 27017 \
    --mongo.database zaku \
    --redis.host localhost \
    --redis.port 6379
```

## Architecture

### Components

1. **MongoDB Configuration Class** (`zaku.server.MongoDB`)
   - Handles connection string building
   - Supports both single server and replica set configurations
   - Environment variable integration

2. **MongoDB Helpers** (`zaku.mongo_helpers`)
   - `RobustMongo`: Client with retry logic and error handling
   - `MongoManager`: Singleton connection manager
   - Connection pooling and health checks

3. **Modified Job Interface** (`zaku.interfaces.Job`)
   - `add()`: Stores payload in MongoDB, metadata in Redis
   - `take()`: Retrieves payload from MongoDB, metadata from Redis
   - `remove()`: Deletes payload from MongoDB, metadata from Redis

### Data Flow

```
Job Creation:
1. Client sends job with payload
2. Job metadata stored in Redis (JSON)
3. Payload stored in MongoDB (Binary)
4. Job ID links both stores

Job Processing:
1. Worker requests job from queue
2. Redis returns job metadata
3. MongoDB returns job payload
4. Worker processes complete job

Job Completion:
1. Worker marks job as done
2. Redis metadata updated
3. MongoDB payload optionally cleaned up
```

## Usage Examples

### Basic Usage

```python
from zaku import TaskQ

# Create queue (automatically initializes MongoDB)
queue = TaskQ()

# Add job with payload
queue.add_job({
    "model": "resnet50",
    "epochs": 100,
    "batch_size": 32
})

# Process job
with queue.pop() as job:
    if job:
        print(f"Processing job: {job}")
        # Job automatically marked as done
```

### Server Setup

```python
from zaku.server import TaskServer

# Server automatically initializes MongoDB
server = TaskServer()
server.run()
```

## Error Handling

### MongoDB Connection Failures

- **Graceful Degradation**: If MongoDB is unavailable, jobs can still be processed (metadata only)
- **Retry Logic**: Automatic retries with exponential backoff
- **Logging**: Comprehensive error logging for debugging

### Fallback Behavior

```python
# If MongoDB fails, payload operations are logged but don't crash the system
try:
    await mongo_client.store_payload(collection, job_id, payload)
except Exception as e:
    logging.warning(f"Failed to store payload in MongoDB: {e}")
    # Job metadata still stored in Redis
```

## Performance Considerations

### MongoDB Collections

- Collections are named as `{prefix}_{queue_name}`
- Automatic collection creation and caching
- Connection pooling for optimal performance

### Concurrent Operations

- All MongoDB operations are thread-safe
- Async/await support for high concurrency
- Connection reuse across requests

### Memory Management

- Payloads are streamed to/from MongoDB
- No in-memory payload caching
- Automatic cleanup of completed jobs

## Testing

Run the MongoDB integration tests:

```bash
# Install test dependencies
pip install pytest pytest-asyncio

# Run tests
python test_mongodb_integration.py

# Or with pytest
pytest test_mongodb_integration.py -v
```

## Migration from Redis-Only

### Existing Deployments

1. **Install MongoDB**: Set up MongoDB server or replica set
2. **Update Environment**: Add MongoDB environment variables
3. **Restart Services**: Restart Zaku servers
4. **Verify**: Check logs for MongoDB connection success

### Data Migration

- Existing Redis data remains intact
- New jobs automatically use MongoDB for payloads
- Old jobs continue to work (payloads remain in Redis)

## Troubleshooting

### Common Issues

1. **MongoDB Connection Failed**
   - Check MongoDB service status
   - Verify connection string and credentials
   - Check network connectivity

2. **Payload Not Found**
   - Verify MongoDB collections exist
   - Check job ID consistency
   - Review error logs for MongoDB failures

3. **Performance Issues**
   - Monitor MongoDB query performance
   - Check connection pool settings
   - Verify indexes on collections

### Debug Mode

Enable verbose logging:

```bash
python -m zaku.server --verbose
```

### Health Checks

```python
from zaku.mongo_helpers import MongoManager

# Check MongoDB health
is_healthy = await MongoManager.get_client().health_check()
print(f"MongoDB healthy: {is_healthy}")
```
