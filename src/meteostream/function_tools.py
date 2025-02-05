"""
Module to provide utilities and tools for functions 
"""

import psutil
import time
from functools import wraps


def monitor_resources(func):
    """
    Monitor CPU, memory, and disk metrics during function
    runtime
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Record initial system stats
        process = psutil.Process()
        cpu_start = process.cpu_times()
        memory_start = process.memory_info().rss

        # Start time tracking
        start_time = time.time()

        # Run the function
        result = func(*args, **kwargs)

        # End time tracking
        end_time = time.time()

        # Record final system stats
        cpu_end = process.cpu_times()
        memory_end = process.memory_info().rss

        # Calculate usage
        cpu_usage = cpu_end.user - cpu_start.user
        memory_usage = (memory_end - memory_start) / (1024 * 1024)  # Convert to MB
        execution_time = end_time - start_time

        # Print the resource usage report
        print(f"Function '{func.__name__}' completed:")
        print(f"  CPU Time Used: {cpu_usage:.2f} seconds")
        print(f"  Memory Used: {memory_usage:.2f} MB")
        print(f"  Execution Time: {execution_time:.2f} seconds")

        return result
    return wrapper