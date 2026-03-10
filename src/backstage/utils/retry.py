"""
Retry decorator for EUR-Lex operations.
"""

import time
from typing import Callable, Any, Optional
from functools import wraps

import requests


def with_retry(max_attempts: int = 3, base_delay: float = 1.0):
    """
    Simple retry decorator.

    Args:
        max_attempts: Maximum number of retry attempts
        base_delay: Base delay between retries in seconds
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except (requests.exceptions.Timeout,
                        requests.exceptions.ConnectionError,
                        requests.exceptions.HTTPError) as e:
                    last_exception = e

                    if attempt < max_attempts - 1:
                        time.sleep(base_delay * (attempt + 1))
                    else:
                        raise

            if last_exception:
                raise last_exception

        return wrapper
    return decorator
