#!/usr/bin/env python3
"""
🚀 Redis + MongoDB 混合存储系统演示
==================================

展示自动内存压力检测、智能数据迁移、无缝CRUD操作
"""

import time
import threading
import random
from hybrid_taskq import HybridTaskQ, HybridTaskQFactory

def demo_basic_operations():
    """演示基本操作"""
    print("🔧 基本操作演示")
    print("=" * 40)
    
    # 创建混合存储队列
    queue = HybridTaskQ(
        name="demo-queue",
        redis_uri="redis://localhost:6379",
        mongodb_uri="mongodb://localhost:27017"
    )
    
    try:
        # 添加任务
        print("📝 添加任务...")
        task_ids = []
        for i in range(5):
            task_id = queue.add({
                "task_number": i + 1,
                "description": f"演示任务 {i + 1}",
                "priority": random.randint(1, 5)
            }, priority=random.randint(1, 5))
            task_ids.append(task_id)
            print(f"  ✅ 任务 {i + 1} 添加成功: {task_id}")
        
        # 获取队列统计
        print("\n📊 队列统计信息:")
        stats = queue.get_queue_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        # 获取任务状态统计
        print("\n📈 任务状态统计:")
        status_stats = queue.get_task_count_by_status()
        for status, count in status_stats.items():
            print(f"  {status}: {count}")
        
        # 取出任务
        print("\n📤 取出任务...")
        for i in range(3):
            task_data = queue.take(timeout=5.0)
            if task_data:
                print(f"  ✅ 取出任务: {task_data}")
                # 标记任务完成
                queue.complete(task_ids[i])
            else:
                print(f"  ❌ 取出任务失败")
        
        # 再次获取状态统计
        print("\n📈 更新后的任务状态统计:")
        status_stats = queue.get_task_count_by_status()
        for status, count in status_stats.items():
            print(f"  {status}: {count}")
        
        # 清理已完成的任务
        print("\n🧹 清理已完成的任务...")
        cleaned_count = queue.cleanup_completed_tasks(max_age_hours=0)  # 立即清理
        print(f"  ✅ 清理了 {cleaned_count} 个任务")
        
        # 重置队列
        print("\n🔄 重置队列...")
        if queue.reset():
            print("  ✅ 队列重置成功")
        else:
            print("  ❌ 队列重置失败")
            
    except Exception as e:
        print(f"❌ 基本操作演示失败: {e}")

def demo_memory_pressure_simulation():
    """演示内存压力模拟"""
    print("\n💾 内存压力模拟演示")
    print("=" * 40)
    
    # 创建队列
    queue = HybridTaskQ(
        name="pressure-queue",
        redis_uri="redis://localhost:6379",
        mongodb_uri="mongodb://localhost:27017",
        memory_threshold=0.6  # 降低阈值以便演示
    )
    
    try:
        # 添加大量任务模拟内存压力
        print("📝 添加大量任务模拟内存压力...")
        task_ids = []
        
        for i in range(20):
            # 创建较大的任务数据
            large_data = {
                "task_id": f"large_task_{i}",
                "data": "x" * 1000,  # 1KB数据
                "timestamp": time.time(),
                "metadata": {
                    "size": 1000,
                    "type": "large_task",
                    "priority": random.randint(1, 10)
                }
            }
            
            task_id = queue.add(large_data, priority=random.randint(1, 10))
            task_ids.append(task_id)
            
            if (i + 1) % 5 == 0:
                print(f"  ✅ 已添加 {i + 1} 个任务")
        
        # 获取存储统计
        print("\n📊 存储统计信息:")
        stats = queue.get_queue_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        # 模拟内存压力检测
        print("\n🔍 内存压力检测:")
        if queue.storage.redis_storage:
            memory_usage = queue.storage.redis_storage.memory_monitor.get_memory_usage()
            is_high = queue.storage.redis_storage.is_memory_pressure_high()
            print(f"  Redis内存使用率: {memory_usage:.2%}")
            print(f"  内存压力状态: {'高' if is_high else '正常'}")
        
        # 手动迁移任务
        print("\n🚚 手动迁移任务...")
        migration_stats = queue.migrate_tasks(target_storage="auto")
        print(f"  迁移统计: {migration_stats}")
        
        # 再次获取统计
        print("\n📊 迁移后的存储统计:")
        stats = queue.get_queue_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        # 重置队列
        queue.reset()
        
    except Exception as e:
        print(f"❌ 内存压力模拟演示失败: {e}")

def demo_concurrent_operations():
    """演示并发操作"""
    print("\n⚡ 并发操作演示")
    print("=" * 40)
    
    # 创建队列
    queue = HybridTaskQ(
        name="concurrent-queue",
        redis_uri="redis://localhost:6379",
        mongodb_uri="mongodb://localhost:27017"
    )
    
    # 生产者函数
    def producer(producer_id: int, task_count: int):
        """生产者：添加任务"""
        for i in range(task_count):
            task_data = {
                "producer_id": producer_id,
                "task_number": i + 1,
                "timestamp": time.time()
            }
            
            try:
                task_id = queue.add(task_data, priority=random.randint(1, 5))
                print(f"  🏭 生产者 {producer_id} 添加任务: {task_id}")
                time.sleep(random.uniform(0.1, 0.3))  # 随机延迟
            except Exception as e:
                print(f"  ❌ 生产者 {producer_id} 添加任务失败: {e}")
    
    # 消费者函数
    def consumer(consumer_id: int, task_count: int):
        """消费者：处理任务"""
        processed_count = 0
        
        while processed_count < task_count:
            try:
                task_data = queue.take(timeout=2.0)
                if task_data:
                    print(f"  🏪 消费者 {consumer_id} 处理任务: {task_data}")
                    processed_count += 1
                    time.sleep(random.uniform(0.1, 0.2))  # 模拟处理时间
                else:
                    time.sleep(0.1)
            except Exception as e:
                print(f"  ❌ 消费者 {consumer_id} 处理任务失败: {e}")
                time.sleep(0.1)
        
        print(f"  ✅ 消费者 {consumer_id} 完成，处理了 {processed_count} 个任务")
    
    try:
        # 启动生产者和消费者
        print("🚀 启动并发操作...")
        
        producer_threads = []
        consumer_threads = []
        
        # 启动3个生产者，每个生产5个任务
        for i in range(3):
            thread = threading.Thread(
                target=producer, 
                args=(i + 1, 5)
            )
            producer_threads.append(thread)
            thread.start()
        
        # 启动2个消费者，每个消费8个任务
        for i in range(2):
            thread = threading.Thread(
                target=consumer, 
                args=(i + 1, 8)
            )
            consumer_threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in producer_threads:
            thread.join()
        
        for thread in consumer_threads:
            thread.join()
        
        print("✅ 所有并发操作完成")
        
        # 获取最终统计
        print("\n📊 最终队列统计:")
        stats = queue.get_queue_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        # 重置队列
        queue.reset()
        
    except Exception as e:
        print(f"❌ 并发操作演示失败: {e}")

def demo_factory_pattern():
    """演示工厂模式"""
    print("\n🏭 工厂模式演示")
    print("=" * 40)
    
    try:
        # 使用工厂创建队列
        queue1 = HybridTaskQFactory.create(
            name="factory-queue-1",
            redis_uri="redis://localhost:6379",
            mongodb_uri="mongodb://localhost:27017"
        )
        
        queue2 = HybridTaskQFactory.create(
            name="factory-queue-2",
            redis_uri="redis://localhost:6379",
            mongodb_uri="mongodb://localhost:27017"
        )
        
        print("✅ 使用工厂模式创建队列成功")
        print(f"  队列1: {queue1.name}")
        print(f"  队列2: {queue2.name}")
        
        # 测试队列功能
        task_id = queue1.add({"test": "factory pattern"})
        print(f"  ✅ 队列1添加任务成功: {task_id}")
        
        task_data = queue1.take(timeout=5.0)
        if task_data:
            print(f"  ✅ 队列1取出任务成功: {task_data}")
        
        # 清理
        queue1.reset()
        queue2.reset()
        
    except Exception as e:
        print(f"❌ 工厂模式演示失败: {e}")

def main():
    """主函数"""
    print("🚀 Redis + MongoDB 混合存储系统演示")
    print("=" * 60)
    print()
    
    try:
        # 运行各种演示
        demo_basic_operations()
        demo_memory_pressure_simulation()
        demo_concurrent_operations()
        demo_factory_pattern()
        
        print("\n🎉 所有演示完成！")
        print("\n📚 系统特性总结:")
        print("• ✅ 自动内存压力检测")
        print("• ✅ 智能数据迁移")
        print("• ✅ 无缝CRUD操作")
        print("• ✅ 高并发支持")
        print("• ✅ 任务优先级管理")
        print("• ✅ 自动清理机制")
        print("• ✅ 工厂模式支持")
        
    except Exception as e:
        print(f"\n❌ 演示运行失败: {e}")
        print("\n🔧 请确保:")
        print("1. Redis服务正在运行")
        print("2. MongoDB服务正在运行")
        print("3. 已安装必要的依赖包")

if __name__ == "__main__":
    main() 