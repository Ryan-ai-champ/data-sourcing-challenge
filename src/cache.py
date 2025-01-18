import redis
from typing import Any, Optional
import json
import logging
from datetime import timedelta

logger = logging.getLogger(__name__)

class Cache:
    def __init__(
        self,
        host: str = 'localhost',
        port: int = 6379,
        db: int = 0,
        expire_time: int = 3600
    ):
        """Initialize Redis cache connection.
        
        Args:
            host: Redis host address
            port: Redis port
            db: Redis database number
            expire_time: Cache expiration time in seconds
        """
        self.redis_client = redis.Redis(host=host, port=port, db=db)
        self.expire_time = expire_time
    
    def get(self, key: str) -> Optional[Any]:
        """Retrieve value from cache."""
        try:
            value = self.redis_client.get(key)
            return json.loads(value) if value else None
        except Exception as e:
            logger.error(f"Cache get error: {str(e)}")
            return None
    
    def set(self, key: str, value: Any) -> bool:
        """Store value in cache."""
        try:
            self.redis_client.setex(
                key,
                timedelta(seconds=self.expire_time),
                json.dumps(value)
            )
            return True
        except Exception as e:
            logger.error(f"Cache set error: {str(e)}")
            return False
    
    def delete(self, key: str) -> bool:
        """Remove value from cache."""
        try:
            self.redis_client.delete(key)
            return True
        except Exception as e:
            logger.error(f"Cache delete error: {str(e)}")
            return False

