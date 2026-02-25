

import json
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import redis.asyncio as redis

from app.core.config import settings


class RedisService:


    def __init__(self):
        """Initialize Redis service"""
        self._pool = None
        self._client = None

    async def connect(self):

        if self._pool is None:
            self._pool = redis.ConnectionPool.from_url(
                settings.redis.url,
                max_connections=settings.redis.max_connections,
                decode_responses=settings.redis.decode_responses,
            )
            self._client = redis.Redis(connection_pool=self._pool)
            print("✓ Redis connected successfully")

    async def disconnect(self):
        """Close Redis connections"""
        if self._client:
            await self._client.close()
            self._client = None
        if self._pool:
            await self._pool.disconnect()
            self._pool = None
            print("✓ Redis disconnected")

    @property
    def client(self):
        """Get Redis client"""
        if self._client is None:
            raise RuntimeError("Redis not connected. Call connect() first.")
        return self._client

    async def health_check(self) -> bool:

        try:
            await self.client.ping()
            return True
        except Exception as e:
            print(f"Redis health check failed: {e}")
            return False



    async def cache_job(self, job_id: str, job_data: Dict[str, Any], ttl: int = 3600):

        key = f"job:{job_id}"
        await self.client.setex(
            key,
            ttl,
            json.dumps(job_data, default=str)
        )

    async def get_cached_job(self, job_id: str) -> Optional[Dict[str, Any]]:

        key = f"job:{job_id}"
        data = await self.client.get(key)
        if data:
            return json.loads(data)
        return None

    async def delete_cached_job(self, job_id: str):

        key = f"job:{job_id}"
        await self.client.delete(key)



    async def increment_queue_size(self, priority: str, amount: int = 1):

        key = f"queue:{priority}"
        await self.client.incrby(key, amount)

    async def decrement_queue_size(self, priority: str, amount: int = 1):

        key = f"queue:{priority}"
        current = await self.client.get(key)
        if current:
            new_val = max(0, int(current) - amount)
            await self.client.set(key, new_val)
        else:
            await self.client.set(key, 0)

    async def get_queue_size(self, priority: str) -> int:

        key = f"queue:{priority}"
        size = await self.client.get(key)
        return int(size) if size else 0

    async def reset_queue_sizes(self):
        """Reset all queue size counters to zero"""
        for priority in ["HIGH", "MEDIUM", "LOW"]:
            key = f"queue:{priority}"
            await self.client.set(key, 0)



    async def register_worker(
            self,
            worker_id: str,
            priority: str,
            status: str = "idle"
    ):

        key = f"worker:{priority}:{worker_id}"
        data = {
            "worker_id": worker_id,
            "priority": priority,
            "status": status,
            "last_heartbeat": datetime.utcnow().isoformat()
        }
        await self.client.setex(key, 300, json.dumps(data))  # 5 min TTL

    async def update_worker_status(
            self,
            worker_id: str,
            priority: str,
            status: str,
            current_job_id: Optional[str] = None
    ):

        key = f"worker:{priority}:{worker_id}"
        data = {
            "worker_id": worker_id,
            "priority": priority,
            "status": status,
            "current_job_id": current_job_id,
            "last_heartbeat": datetime.utcnow().isoformat()
        }
        await self.client.setex(key, 300, json.dumps(data))

    async def get_active_workers(self, priority: str) -> List[Dict[str, Any]]:

        pattern = f"worker:{priority}:*"
        workers = []

        async for key in self.client.scan_iter(match=pattern):
            data = await self.client.get(key)
            if data:
                workers.append(json.loads(data))

        return workers

    async def unregister_worker(self, worker_id: str, priority: str):

        key = f"worker:{priority}:{worker_id}"
        await self.client.delete(key)



    async def update_stats(self, stats: Dict[str, Any]):

        key = "stats:jobs"
        await self.client.setex(
            key,
            600,  # 10 min TTL
            json.dumps(stats, default=str)
        )

    async def get_stats(self) -> Optional[Dict[str, Any]]:

        key = "stats:jobs"
        data = await self.client.get(key)
        if data:
            return json.loads(data)
        return None



    async def set_with_ttl(self, key: str, value: Any, ttl: int):

        await self.client.setex(key, ttl, json.dumps(value, default=str))

    async def get_value(self, key: str) -> Optional[Any]:

        data = await self.client.get(key)
        if data:
            return json.loads(data)
        return None

    async def delete_key(self, key: str):
        """Delete a key"""
        await self.client.delete(key)

    async def clear_all(self):
        
        await self.client.flushdb()


# Global Redis service instance
redis_service = RedisService()


async def init_redis():
    """Initialize Redis connection"""
    await redis_service.connect()


async def close_redis():
    """Close Redis connection"""
    await redis_service.disconnect()