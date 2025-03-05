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
    FIBONACCI_RESULTS_20 = [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181]
    for n in [0, 4, 6, 10, 20]:
        result, elapsed_time, cpu_time_user, cpu_time_system = measure_performance(n)
        print(f"Fibonacci({n}) = {result}")
        print(f"Elapsed time: {elapsed_time:.4f} seconds")
        print(f"User CPU time: {cpu_time_user:.4f} seconds")
        print(f"System CPU time: {cpu_time_system:.4f} seconds")
        assert result == FIBONACCI_RESULTS_20[n]
