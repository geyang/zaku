#!/usr/bin/env python3
"""
🚀 Redis + MongoDB 混合存储系统
==============================

自动内存压力检测，智能数据迁移，无缝CRUD操作
"""

import json
import time
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from enum import Enum

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StorageType(Enum):
    """存储类型枚举"""
    REDIS = "redis"
    MONGODB = "mongodb"

@dataclass
class StorageConfig:
    """存储配置"""
    redis_uri: str = "redis://localhost:6379"
    mongodb_uri: str = "mongodb://localhost:27017"
    database: str = "zaku_hybrid"
    collection: str = "task_queue"
    memory_threshold: float = 0.8  # Redis内存使用率阈值 (80%)
    memory_check_interval: int = 5  # 内存检查间隔(秒)
    batch_size: int = 100  # 批量操作大小

class MemoryMonitor:
    """Redis内存监控器"""
    
    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.last_check = 0
        self.cached_memory_usage = 0.0
    
    def get_memory_usage(self) -> float:
        """获取Redis内存使用率"""
        current_time = time.time()
        
        # 缓存内存使用率，避免频繁查询
        if current_time - self.last_check > 5:
            try:
                info = self.redis_client.info('memory')
                used_memory = info.get('used_memory', 0)
                max_memory = info.get('maxmemory', 0)
                
                if max_memory > 0:
                    self.cached_memory_usage = used_memory / max_memory
                else:
                    # 如果没有设置maxmemory，使用used_memory_human估算
                    self.cached_memory_usage = 0.5  # 默认50%
                
                self.last_check = current_time
                logger.debug(f"Redis内存使用率: {self.cached_memory_usage:.2%}")
                
            except Exception as e:
                logger.warning(f"获取Redis内存信息失败: {e}")
                self.cached_memory_usage = 0.5  # 默认值
        
        return self.cached_memory_usage
    
    def is_memory_pressure_high(self) -> bool:
        """判断内存压力是否高"""
        return self.get_memory_usage() > 0.8

class BaseStorage(ABC):
    """存储基类抽象接口"""
    
    @abstractmethod
    def add(self, key: str, data: Any) -> bool:
        """添加数据"""
        pass
    
    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """获取数据"""
        pass
    
    @abstractmethod
    def delete(self, key: str) -> bool:
        """删除数据"""
        pass
    
    @abstractmethod
    def exists(self, key: str) -> bool:
        """检查数据是否存在"""
        pass
    
    @abstractmethod
    def clear(self) -> bool:
        """清空所有数据"""
        pass
    
    @abstractmethod
    def get_size(self) -> int:
        """获取数据量"""
        pass
    
    @abstractmethod
    def scan_keys(self, pattern: str = "*") -> List[str]:
        """扫描键"""
        pass

class RedisStorage(BaseStorage):
    """Redis存储实现"""
    
    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.memory_monitor = MemoryMonitor(redis_client)
    
    def add(self, key: str, data: Any) -> bool:
        """添加数据到Redis"""
        try:
            if isinstance(data, (dict, list)):
                serialized_data = json.dumps(data, ensure_ascii=False)
            else:
                serialized_data = str(data)
            
            self.redis_client.set(key, serialized_data)
            return True
        except Exception as e:
            logger.error(f"Redis添加数据失败: {e}")
            return False
    
    def get(self, key: str) -> Optional[Any]:
        """从Redis获取数据"""
        try:
            data = self.redis_client.get(key)
            if data:
                try:
                    return json.loads(data)
                except json.JSONDecodeError:
                    return data.decode('utf-8')
            return None
        except Exception as e:
            logger.error(f"Redis获取数据失败: {e}")
            return None
    
    def delete(self, key: str) -> bool:
        """从Redis删除数据"""
        try:
            return bool(self.redis_client.delete(key))
        except Exception as e:
            logger.error(f"Redis删除数据失败: {e}")
            return False
    
    def exists(self, key: str) -> bool:
        """检查Redis中是否存在数据"""
        try:
            return bool(self.redis_client.exists(key))
        except Exception as e:
            logger.error(f"Redis检查数据存在性失败: {e}")
            return False
    
    def clear(self) -> bool:
        """清空Redis数据"""
        try:
            self.redis_client.flushdb()
            return True
        except Exception as e:
            logger.error(f"Redis清空数据失败: {e}")
            return False
    
    def get_size(self) -> int:
        """获取Redis数据量"""
        try:
            return self.redis_client.dbsize()
        except Exception as e:
            logger.error(f"Redis获取数据量失败: {e}")
            return 0
    
    def scan_keys(self, pattern: str = "*") -> List[str]:
        """扫描Redis键"""
        try:
            keys = []
            cursor = 0
            
            while True:
                cursor, batch_keys = self.redis_client.scan(
                    cursor=cursor, 
                    match=pattern, 
                    count=100
                )
                keys.extend(batch_keys)
                
                if cursor == 0:
                    break
            
            return [key.decode('utf-8') if isinstance(key, bytes) else key for key in keys]
        except Exception as e:
            logger.error(f"Redis扫描键失败: {e}")
            return []
    
    def is_memory_pressure_high(self) -> bool:
        """检查内存压力"""
        return self.memory_monitor.is_memory_pressure_high()

class MongoDBStorage(BaseStorage):
    """MongoDB存储实现"""
    
    def __init__(self, mongodb_client, database: str, collection: str):
        self.db = mongodb_client[database]
        self.collection = self.db[collection]
        self._ensure_indexes()
    
    def _ensure_indexes(self):
        """确保必要的索引存在"""
        try:
            # 创建唯一索引
            self.collection.create_index("key", unique=True)
            # 创建时间索引
            self.collection.create_index("created_at")
            logger.info("MongoDB索引创建成功")
        except Exception as e:
            logger.warning(f"MongoDB索引创建失败: {e}")
    
    def add(self, key: str, data: Any) -> bool:
        """添加数据到MongoDB"""
        try:
            document = {
                "key": key,
                "data": data,
                "created_at": time.time(),
                "updated_at": time.time()
            }
            
            # 使用upsert模式，如果key存在则更新
            result = self.collection.replace_one(
                {"key": key}, 
                document, 
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"MongoDB添加数据失败: {e}")
            return False
    
    def get(self, key: str) -> Optional[Any]:
        """从MongoDB获取数据"""
        try:
            document = self.collection.find_one({"key": key})
            if document:
                return document.get("data")
            return None
        except Exception as e:
            logger.error(f"MongoDB获取数据失败: {e}")
            return None
    
    def delete(self, key: str) -> bool:
        """从MongoDB删除数据"""
        try:
            result = self.collection.delete_one({"key": key})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"MongoDB删除数据失败: {e}")
            return False
    
    def exists(self, key: str) -> bool:
        """检查MongoDB中是否存在数据"""
        try:
            return bool(self.collection.count_documents({"key": key}))
        except Exception as e:
            logger.error(f"MongoDB检查数据存在性失败: {e}")
            return False
    
    def clear(self) -> bool:
        """清空MongoDB数据"""
        try:
            result = self.collection.delete_many({})
            return True
        except Exception as e:
            logger.error(f"MongoDB清空数据失败: {e}")
            return False
    
    def get_size(self) -> int:
        """获取MongoDB数据量"""
        try:
            return self.collection.count_documents({})
        except Exception as e:
            logger.error(f"MongoDB获取数据量失败: {e}")
            return 0
    
    def scan_keys(self, pattern: str = "*") -> List[str]:
        """扫描MongoDB键"""
        try:
            # MongoDB不支持通配符模式匹配，这里返回所有键
            # 实际使用时可以根据需要实现更复杂的查询
            cursor = self.collection.find({}, {"key": 1})
            keys = [doc["key"] for doc in cursor]
            return keys
        except Exception as e:
            logger.error(f"MongoDB扫描键失败: {e}")
            return []
    
    def batch_get(self, keys: List[str]) -> Dict[str, Any]:
        """批量获取数据"""
        try:
            documents = self.collection.find({"key": {"$in": keys}})
            result = {}
            for doc in documents:
                result[doc["key"]] = doc["data"]
            return result
        except Exception as e:
            logger.error(f"MongoDB批量获取数据失败: {e}")
            return {}
    
    def batch_delete(self, keys: List[str]) -> int:
        """批量删除数据"""
        try:
            result = self.collection.delete_many({"key": {"$in": keys}})
            return result.deleted_count
        except Exception as e:
            logger.error(f"MongoDB批量删除数据失败: {e}")
            return 0

class HybridStorage:
    """Redis + MongoDB 混合存储系统"""
    
    def __init__(self, config: StorageConfig):
        self.config = config
        self.redis_storage = None
        self.mongodb_storage = None
        self._init_storages()
    
    def _init_storages(self):
        """初始化存储系统"""
        try:
            # 初始化Redis
            import redis
            redis_client = redis.from_url(self.config.redis_uri)
            self.redis_storage = RedisStorage(redis_client)
            logger.info("Redis存储初始化成功")
        except Exception as e:
            logger.error(f"Redis存储初始化失败: {e}")
            self.redis_storage = None
        
        try:
            # 初始化MongoDB
            from pymongo import MongoClient
            mongodb_client = MongoClient(self.config.mongodb_uri)
            self.mongodb_storage = MongoDBStorage(
                mongodb_client, 
                self.config.database, 
                self.config.collection
            )
            logger.info("MongoDB存储初始化成功")
        except Exception as e:
            logger.error(f"MongoDB存储初始化失败: {e}")
            self.mongodb_storage = None
    
    def add(self, key: str, data: Any) -> bool:
        """智能添加数据"""
        # 优先尝试Redis
        if self.redis_storage and not self.redis_storage.is_memory_pressure_high():
            if self.redis_storage.add(key, data):
                logger.debug(f"数据已添加到Redis: {key}")
                return True
        
        # Redis不可用或内存压力大，使用MongoDB
        if self.mongodb_storage:
            if self.mongodb_storage.add(key, data):
                logger.info(f"数据已添加到MongoDB: {key}")
                return True
        
        logger.error(f"添加数据失败: {key}")
        return False
    
    def get(self, key: str) -> Optional[Any]:
        """智能获取数据"""
        # 优先从Redis获取
        if self.redis_storage:
            data = self.redis_storage.get(key)
            if data is not None:
                logger.debug(f"从Redis获取数据: {key}")
                return data
        
        # Redis中没有，从MongoDB获取
        if self.mongodb_storage:
            data = self.mongodb_storage.get(key)
            if data is not None:
                logger.info(f"从MongoDB获取数据: {key}")
                # 如果Redis可用且内存压力不大，尝试回填到Redis
                if self.redis_storage and not self.redis_storage.is_memory_pressure_high():
                    self.redis_storage.add(key, data)
                return data
        
        logger.debug(f"数据不存在: {key}")
        return None
    
    def delete(self, key: str) -> bool:
        """智能删除数据"""
        success = False
        
        # 从Redis删除
        if self.redis_storage:
            if self.redis_storage.delete(key):
                success = True
        
        # 从MongoDB删除
        if self.mongodb_storage:
            if self.mongodb_storage.delete(key):
                success = True
        
        if success:
            logger.debug(f"数据删除成功: {key}")
        else:
            logger.warning(f"数据删除失败: {key}")
        
        return success
    
    def exists(self, key: str) -> bool:
        """检查数据是否存在"""
        # 先检查Redis
        if self.redis_storage and self.redis_storage.exists(key):
            return True
        
        # 再检查MongoDB
        if self.mongodb_storage and self.mongodb_storage.exists(key):
            return True
        
        return False
    
    def clear(self) -> bool:
        """清空所有数据"""
        success = True
        
        if self.redis_storage:
            success &= self.redis_storage.clear()
        
        if self.mongodb_storage:
            success &= self.mongodb_storage.clear()
        
        if success:
            logger.info("所有数据清空成功")
        else:
            logger.warning("部分数据清空失败")
        
        return success
    
    def get_size(self) -> int:
        """获取总数据量"""
        total_size = 0
        
        if self.redis_storage:
            total_size += self.redis_storage.get_size()
        
        if self.mongodb_storage:
            total_size += self.mongodb_storage.get_size()
        
        return total_size
    
    def scan_keys(self, pattern: str = "*") -> List[str]:
        """扫描所有键"""
        all_keys = []
        
        # 扫描Redis键
        if self.redis_storage:
            redis_keys = self.redis_storage.scan_keys(pattern)
            all_keys.extend(redis_keys)
        
        # 扫描MongoDB键
        if self.mongodb_storage:
            mongodb_keys = self.mongodb_storage.scan_keys(pattern)
            all_keys.extend(mongodb_keys)
        
        # 去重
        return list(set(all_keys))
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """获取存储统计信息"""
        stats = {
            "redis_available": self.redis_storage is not None,
            "mongodb_available": self.mongodb_storage is not None,
            "total_size": self.get_size()
        }
        
        if self.redis_storage:
            stats.update({
                "redis_size": self.redis_storage.get_size(),
                "redis_memory_pressure": self.redis_storage.is_memory_pressure_high()
            })
        
        if self.mongodb_storage:
            stats.update({
                "mongodb_size": self.mongodb_storage.get_size()
            })
        
        return stats
    
    def migrate_to_redis(self, keys: List[str]) -> int:
        """将MongoDB中的数据迁移到Redis"""
        if not self.redis_storage or not self.mongodb_storage:
            return 0
        
        if self.redis_storage.is_memory_pressure_high():
            logger.warning("Redis内存压力大，无法迁移数据")
            return 0
        
        migrated_count = 0
        batch_keys = keys[:self.config.batch_size]
        
        for key in batch_keys:
            data = self.mongodb_storage.get(key)
            if data and self.redis_storage.add(key, data):
                migrated_count += 1
        
        if migrated_count > 0:
            logger.info(f"成功迁移 {migrated_count} 条数据到Redis")
        
        return migrated_count
    
    def migrate_to_mongodb(self, keys: List[str]) -> int:
        """将Redis中的数据迁移到MongoDB"""
        if not self.redis_storage or not self.mongodb_storage:
            return 0
        
        migrated_count = 0
        batch_keys = keys[:self.config.batch_size]
        
        for key in batch_keys:
            data = self.redis_storage.get(key)
            if data and self.mongodb_storage.add(key, data):
                migrated_count += 1
        
        if migrated_count > 0:
            logger.info(f"成功迁移 {migrated_count} 条数据到MongoDB")
        
        return migrated_count 