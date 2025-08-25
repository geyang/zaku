#!/usr/bin/env python3
"""
🚀 增强版TaskQ - 集成Redis + MongoDB混合存储
============================================

自动内存压力检测，智能数据迁移，无缝CRUD操作
"""

import time
import json
import logging
from typing import Any, Dict, List, Optional, Union
from hybrid_storage import HybridStorage, StorageConfig

logger = logging.getLogger(__name__)

class HybridTaskQ:
    """增强版TaskQ，支持Redis + MongoDB混合存储"""
    
    def __init__(self, 
                 name: str, 
                 uri: str = "http://localhost:9000",
                 redis_uri: str = "redis://localhost:6379",
                 mongodb_uri: str = "mongodb://localhost:27017",
                 database: str = "zaku_hybrid",
                 collection: str = None,
                 memory_threshold: float = 0.8,
                 memory_check_interval: int = 5,
                 batch_size: int = 100):
        """
        初始化混合存储TaskQ
        
        Args:
            name: 队列名称
            uri: Zaku服务器URI
            redis_uri: Redis连接URI
            mongodb_uri: MongoDB连接URI
            database: MongoDB数据库名
            collection: MongoDB集合名（默认使用队列名）
            memory_threshold: Redis内存使用率阈值
            memory_check_interval: 内存检查间隔
            batch_size: 批量操作大小
        """
        self.name = name
        self.uri = uri
        self.collection = collection or f"queue_{name}"
        
        # 配置混合存储
        config = StorageConfig(
            redis_uri=redis_uri,
            mongodb_uri=mongodb_uri,
            database=database,
            collection=self.collection,
            memory_threshold=memory_threshold,
            memory_check_interval=memory_check_interval,
            batch_size=batch_size
        )
        
        # 初始化混合存储
        self.storage = HybridStorage(config)
        
        # 队列相关
        self.prefix = f"zaku:queue:{name}:"
        self.task_prefix = f"{self.prefix}task:"
        self.meta_prefix = f"{self.prefix}meta:"
        
        logger.info(f"混合存储TaskQ初始化成功: {name}")
    
    def _generate_key(self, task_id: str = None) -> str:
        """生成存储键"""
        if task_id:
            return f"{self.task_prefix}{task_id}"
        return f"{self.meta_prefix}counter"
    
    def _generate_task_id(self) -> str:
        """生成任务ID"""
        return f"{int(time.time() * 1000)}_{id(self)}"
    
    def add(self, data: Any, priority: int = 0) -> str:
        """
        添加任务到队列
        
        Args:
            data: 任务数据
            priority: 任务优先级（数字越小优先级越高）
            
        Returns:
            任务ID
        """
        task_id = self._generate_task_id()
        task_data = {
            "id": task_id,
            "data": data,
            "priority": priority,
            "created_at": time.time(),
            "status": "pending"
        }
        
        key = self._generate_key(task_id)
        if self.storage.add(key, task_data):
            logger.info(f"任务添加成功: {task_id}")
            return task_id
        else:
            raise Exception(f"任务添加失败: {task_id}")
    
    def take(self, timeout: float = None) -> Optional[Any]:
        """
        从队列中取出任务
        
        Args:
            timeout: 超时时间（秒）
            
        Returns:
            任务数据，如果没有任务返回None
        """
        start_time = time.time()
        
        while True:
            # 获取所有任务键
            task_keys = self._get_all_task_keys()
            
            if not task_keys:
                if timeout and (time.time() - start_time) > timeout:
                    logger.debug("等待任务超时")
                    return None
                time.sleep(0.1)  # 短暂等待
                continue
            
            # 按优先级排序任务
            sorted_tasks = self._sort_tasks_by_priority(task_keys)
            
            for task_key in sorted_tasks:
                task_data = self.storage.get(task_key)
                if task_data and task_data.get("status") == "pending":
                    # 标记任务为处理中
                    task_data["status"] = "processing"
                    task_data["taken_at"] = time.time()
                    self.storage.add(task_key, task_data)
                    
                    logger.info(f"任务取出成功: {task_data['id']}")
                    return task_data["data"]
            
            # 没有可用的任务
            if timeout and (time.time() - start_time) > timeout:
                logger.debug("等待任务超时")
                return None
            
            time.sleep(0.1)  # 短暂等待
    
    def _get_all_task_keys(self) -> List[str]:
        """获取所有任务键"""
        try:
            # 使用混合存储的键扫描功能
            all_keys = self.storage.scan_keys(f"{self.task_prefix}*")
            return all_keys
        except Exception as e:
            logger.error(f"获取任务键失败: {e}")
            return []
    
    def _sort_tasks_by_priority(self, task_keys: List[str]) -> List[str]:
        """按优先级排序任务"""
        tasks_with_priority = []
        
        for key in task_keys:
            task_data = self.storage.get(key)
            if task_data:
                tasks_with_priority.append((key, task_data.get("priority", 0)))
        
        # 按优先级排序（数字越小优先级越高）
        sorted_tasks = sorted(tasks_with_priority, key=lambda x: x[1])
        return [task[0] for task in sorted_tasks]
    
    def complete(self, task_id: str) -> bool:
        """
        标记任务为完成
        
        Args:
            task_id: 任务ID
            
        Returns:
            是否成功
        """
        key = self._generate_key(task_id)
        task_data = self.storage.get(key)
        
        if task_data:
            task_data["status"] = "completed"
            task_data["completed_at"] = time.time()
            
            if self.storage.add(key, task_data):
                logger.info(f"任务标记完成: {task_id}")
                return True
        
        logger.warning(f"任务标记完成失败: {task_id}")
        return False
    
    def fail(self, task_id: str, error: str = None) -> bool:
        """
        标记任务为失败
        
        Args:
            task_id: 任务ID
            error: 错误信息
            
        Returns:
            是否成功
        """
        key = self._generate_key(task_id)
        task_data = self.storage.get(key)
        
        if task_data:
            task_data["status"] = "failed"
            task_data["failed_at"] = time.time()
            task_data["error"] = error
            
            if self.storage.add(key, task_data):
                logger.info(f"任务标记完成: {task_id}")
                return True
        
        logger.warning(f"任务标记失败失败: {task_id}")
        return False
    
    def delete(self, task_id: str) -> bool:
        """
        删除任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            是否成功
        """
        key = self._generate_key(task_id)
        if self.storage.delete(key):
            logger.info(f"任务删除成功: {task_id}")
            return True
        
        logger.warning(f"任务删除失败: {task_id}")
        return False
    
    def get_task_info(self, task_id: str) -> Optional[Dict]:
        """
        获取任务信息
        
        Args:
            task_id: 任务ID
            
        Returns:
            任务信息字典
        """
        key = self._generate_key(task_id)
        return self.storage.get(key)
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """
        获取队列统计信息
        
        Returns:
            队列统计信息
        """
        stats = self.storage.get_storage_stats()
        stats.update({
            "queue_name": self.name,
            "queue_uri": self.uri
        })
        return stats
    
    def get_task_count_by_status(self) -> Dict[str, int]:
        """
        获取各状态的任务数量
        
        Returns:
            状态统计字典
        """
        status_counts = {"pending": 0, "processing": 0, "completed": 0, "failed": 0}
        
        try:
            task_keys = self._get_all_task_keys()
            for key in task_keys:
                task_data = self.storage.get(key)
                if task_data:
                    status = task_data.get("status", "unknown")
                    status_counts[status] = status_counts.get(status, 0) + 1
        except Exception as e:
            logger.error(f"获取任务状态统计失败: {e}")
        
        return status_counts
    
    def reset(self) -> bool:
        """
        重置队列（清空所有数据）
        
        Returns:
            是否成功
        """
        if self.storage.clear():
            logger.info(f"队列重置成功: {self.name}")
            return True
        
        logger.warning(f"队列重置失败: {self.name}")
        return False
    
    def cleanup_completed_tasks(self, max_age_hours: int = 24) -> int:
        """
        清理已完成的任务
        
        Args:
            max_age_hours: 最大保留时间（小时）
            
        Returns:
            清理的任务数量
        """
        cleaned_count = 0
        cutoff_time = time.time() - (max_age_hours * 3600)
        
        try:
            task_keys = self._get_all_task_keys()
            for key in task_keys:
                task_data = self.storage.get(key)
                if task_data:
                    status = task_data.get("status")
                    completed_at = task_data.get("completed_at", 0)
                    
                    # 清理已完成且超过保留时间的任务
                    if (status in ["completed", "failed"] and 
                        completed_at < cutoff_time):
                        if self.storage.delete(key):
                            cleaned_count += 1
            
            if cleaned_count > 0:
                logger.info(f"清理了 {cleaned_count} 个已完成的任务")
                
        except Exception as e:
            logger.error(f"清理已完成任务失败: {e}")
        
        return cleaned_count
    
    def migrate_tasks(self, target_storage: str = "auto") -> Dict[str, int]:
        """
        迁移任务到指定存储
        
        Args:
            target_storage: 目标存储 ("redis", "mongodb", "auto")
            
        Returns:
            迁移统计信息
        """
        migration_stats = {"to_redis": 0, "to_mongodb": 0}
        
        try:
            if target_storage == "auto":
                # 自动迁移：根据内存压力决定
                if self.storage.redis_storage and self.storage.redis_storage.is_memory_pressure_high():
                    # Redis内存压力大，迁移到MongoDB
                    task_keys = self._get_all_task_keys()
                    migration_stats["to_mongodb"] = self.storage.migrate_to_mongodb(task_keys)
                else:
                    # Redis内存压力不大，迁移到Redis
                    task_keys = self._get_all_task_keys()
                    migration_stats["to_redis"] = self.storage.migrate_to_redis(task_keys)
            
            elif target_storage == "redis":
                task_keys = self._get_all_task_keys()
                migration_stats["to_redis"] = self.storage.migrate_to_redis(task_keys)
            
            elif target_storage == "mongodb":
                task_keys = self._get_all_task_keys()
                migration_stats["to_mongodb"] = self.storage.migrate_to_mongodb(task_keys)
            
            logger.info(f"任务迁移完成: {migration_stats}")
            
        except Exception as e:
            logger.error(f"任务迁移失败: {e}")
        
        return migration_stats
    
    def publish(self, message: Any, topic: str) -> int:
        """
        发布消息到主题（兼容原有API）
        
        Args:
            message: 消息内容
            topic: 主题名称
            
        Returns:
            订阅者数量
        """
        # 这里简化实现，实际应该使用Zaku的发布订阅功能
        logger.info(f"发布消息到主题: {topic}")
        return 1
    
    def subscribe_one(self, topic: str, timeout: float = None) -> Optional[Any]:
        """
        订阅主题的单条消息（兼容原有API）
        
        Args:
            topic: 主题名称
            timeout: 超时时间
            
        Returns:
            消息内容
        """
        # 这里简化实现，实际应该使用Zaku的发布订阅功能
        logger.info(f"订阅主题: {topic}")
        return None
    
    def subscribe_stream(self, topic: str, timeout: float = None):
        """
        订阅主题的消息流（兼容原有API）
        
        Args:
            topic: 主题名称
            timeout: 超时时间
            
        Yields:
            消息内容
        """
        # 这里简化实现，实际应该使用Zaku的发布订阅功能
        logger.info(f"订阅主题流: {topic}")
        return []

class HybridTaskQFactory:
    """混合存储TaskQ工厂类"""
    
    @staticmethod
    def create(name: str, 
               redis_uri: str = "redis://localhost:6379",
               mongodb_uri: str = "mongodb://localhost:27017",
               **kwargs) -> HybridTaskQ:
        """
        创建混合存储TaskQ实例
        
        Args:
            name: 队列名称
            redis_uri: Redis连接URI
            mongodb_uri: MongoDB连接URI
            **kwargs: 其他配置参数
            
        Returns:
            HybridTaskQ实例
        """
        return HybridTaskQ(
            name=name,
            redis_uri=redis_uri,
            mongodb_uri=mongodb_uri,
            **kwargs
        )

# 兼容原有TaskQ的别名
TaskQ = HybridTaskQ 