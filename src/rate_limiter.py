import time
import redis
from functools import wraps
from typing import Optional
from datetime import datetime

class RateLimiter:
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379):
        self.redis = redis.Redis(host=redis_host, port=redis_port)
        
    def limit_requests(self, key_prefix: str, max_requests: int, period: int):
        def decorator(f):
            @wraps(f)
            def wrapper(*args, **kwargs):
                current_time = int(time.time())
                key = f"{key_prefix}:{current_time // period}"
                
                # Use pipeline for atomic operations
                pipe = self.redis.pipeline()
                pipe.incr(key)
                pipe.expire(key, period)
                request_count = pipe.execute()[0]
                
                if request_count > max_requests:
                    raise Exception(
                        f"Rate limit exceeded. Maximum {max_requests} requests per {period} seconds."
                    )
                    
                return f(*args, **kwargs)
            return wrapper
        return decorator

