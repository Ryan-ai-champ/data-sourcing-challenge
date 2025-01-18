import logging
from prometheus_client import Counter, Histogram, start_http_server
from functools import wraps
from time import time
from typing import Callable

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
REQUEST_COUNT = Counter(
    'app_request_count',
    'Application Request Count',
    ['endpoint', 'method', 'status']
)

REQUEST_LATENCY = Histogram(
    'app_request_latency_seconds',
    'Application Request Latency',
    ['endpoint', 'method']
)

def start_metrics_server(port: int = 8000):
    """Start Prometheus metrics server"""
    start_http_server(port)
    logger.info(f"Metrics server started on port {port}")

def monitor_request(endpoint: str, method: str) -> Callable:
    """Decorator to monitor request metrics"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time()
            try:
                result = await func(*args, **kwargs)
                REQUEST_COUNT.labels(endpoint=endpoint, method=method, status="success").inc()
                return result
            except Exception as e:
                REQUEST_COUNT.labels(endpoint=endpoint, method=method, status="error").inc()
                logger.error(f"Error in {endpoint}: {str(e)}")
                raise
            finally:
                duration = time() - start_time
                REQUEST_LATENCY.labels(endpoint=endpoint, method=method).observe(duration)
        return wrapper
    return decorator

