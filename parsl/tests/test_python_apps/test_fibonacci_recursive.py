# from parsl.app.app import join_app, python_app


# @python_app
# def add(*args):
#     """Add all of the arguments together. If no arguments, then
#     zero is returned (the neutral element of +)
#     """
#     accumulator = 0
#     for v in args:
#         accumulator += v
#     return accumulator


# @join_app
# def fibonacci(n):
#     if n == 0:
#         return add()
#     elif n == 1:
#         return add(1)
#     else:
#         return add(fibonacci(n - 1), fibonacci(n - 2))


# def test_fibonacci():
#     print("Testing fibonacci(0):", fibonacci(0).result())
#     assert fibonacci(0).result() == 0
    
#     print("Testing fibonacci(4):", fibonacci(4).result())
#     assert fibonacci(4).result() == 3
    
#     print("Testing fibonacci(6):", fibonacci(6).result())
#     assert fibonacci(6).result() == 8

import time
import psutil
import pytest
from parsl.app.app import join_app, python_app

@python_app
def add(*args):
    accumulator = 0
    for v in args:
        accumulator += v
    return accumulator

@join_app
def fibonacci(n):
    if n == 0:
        return add()
    elif n == 1:
        return add(1)
    else:
        return add(fibonacci(n - 1), fibonacci(n - 2))

@pytest.fixture
def measure_performance():
    def _measure_performance(n):
        process = psutil.Process()
        
        # Measure start time and CPU times
        start_time = time.time()
        start_cpu_times = process.cpu_times()
        
        # Run the fibonacci function
        result = fibonacci(n).result()
        
        # Measure end time and CPU times
        end_time = time.time()
        end_cpu_times = process.cpu_times()
        
        # Calculate elapsed time and CPU usage
        elapsed_time = end_time - start_time
        cpu_time_user = end_cpu_times.user - start_cpu_times.user
        cpu_time_system = end_cpu_times.system - start_cpu_times.system
        
        return result, elapsed_time, cpu_time_user, cpu_time_system
    
    return _measure_performance

def test_fibonacci_performance(measure_performance):
    for n in [0, 4, 6, 10, 20]:
        result, elapsed_time, cpu_time_user, cpu_time_system = measure_performance(n)
        print(f"Fibonacci({n}) = {result}")
        print(f"Elapsed time: {elapsed_time:.4f} seconds")
        print(f"User CPU time: {cpu_time_user:.4f} seconds")
        print(f"System CPU time: {cpu_time_system:.4f} seconds")
