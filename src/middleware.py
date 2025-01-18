import logging
import time
from functools import wraps
from typing import Callable, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class APIError(Exception):
    """Custom exception for API-related errors."""
    pass

def log_request(func: Callable) -> Callable:
    """Decorator to log API requests and responses."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f"Starting request to {func.__name__}")
        
        try:
            result = func(*args, **kwargs)
            logger.info(
                f"Request to {func.__name__} completed successfully "
                f"in {time.time() - start_time:.2f}s"
            )
            return result
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            raise
    return wrapper

def error_handler(func: Callable) -> Callable:
    """Decorator to handle API errors."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except APIError as e:
            logger.error(f"API Error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise APIError(f"An unexpected error occurred: {str(e)}")
    return wrapper

