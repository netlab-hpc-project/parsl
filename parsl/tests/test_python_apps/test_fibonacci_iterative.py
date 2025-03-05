import time
import pytest
import psutil
import os

from parsl.app.app import python_app

@python_app
def get_num(first, second):
    return first + second

def calculate_fibonacci(num):
    """Calculate Fibonacci sequence up to the nth number using Parsl."""
    counter = 0
    results = []
    results.append(0)
    results.append(1)
    while counter < num - 2:
        counter += 1
        results.append(get_num(results[counter - 1], results[counter]))
    
    # Convert futures to values
    final_results = []
    for i in range(len(results)):
        if isinstance(results[i], int):
            final_results.append(results[i])
        else:
            final_results.append(results[i].result())
    
    return final_results

class TestFibonacci:
    
    # Known first 20 Fibonacci numbers
    FIBONACCI_30 = [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765, 10946, 17711, 28657, 46368, 75025, 121393, 196418, 317811, 514229]
    
    @pytest.fixture
    def performance_metrics(self):
        """Fixture that provides a function to measure performance."""
        def measure_function():
            process = psutil.Process(os.getpid())
            start_time = time.time()
            start_memory = process.memory_info()
            start_io = process.io_counters() if hasattr(process, 'io_counters') else None
            start_cpu = process.cpu_times()
            
            # 返回一个函数用于计算结束时的指标
            def end_measurement():
                end_time = time.time()
                end_memory = process.memory_info()
                end_io = process.io_counters() if hasattr(process, 'io_counters') else None
                end_cpu = process.cpu_times()
                
                metrics = {
                    "elapsed_time": end_time - start_time,
                    "memory_used_mb": (end_memory.rss - start_memory.rss) / (1024 * 1024),
                    "cpu_user": end_cpu.user - start_cpu.user,
                    "cpu_system": end_cpu.system - start_cpu.system
                }
                
                if start_io and end_io:
                    metrics["io_read_bytes"] = end_io.read_bytes - start_io.read_bytes
                    metrics["io_write_bytes"] = end_io.write_bytes - start_io.write_bytes
                
                return metrics
            
            return start_time, end_measurement
        
        # 直接返回一个包含测量指标的字典，而不是使用yield后再计算
        metrics = {}
        yield metrics
        
        # 测试完成后不需要再做任何事情，因为metrics已经被测试函数填充
    
    @pytest.mark.parametrize("num_terms", [5, 10, 15, 20, 25, 30])
    def test_fibonacci_sequence_correctness(self, num_terms):
        """Test that the calculated Fibonacci sequence is correct."""
        result = calculate_fibonacci(num_terms)
        assert result == self.FIBONACCI_30[:num_terms]
    
    @pytest.mark.parametrize("num_terms", [5, 10, 15, 20, 25, 30])
    def test_fibonacci_performance(self, num_terms, performance_metrics):
        """Test performance metrics when calculating Fibonacci sequence."""
        # 手动测量性能
        process = psutil.Process(os.getpid())
        start_time = time.time()
        start_memory = process.memory_info()
        start_cpu = process.cpu_times()
        
        # 运行计算
        result = calculate_fibonacci(num_terms)
        
        # 收集结束指标
        end_time = time.time()
        end_memory = process.memory_info()
        end_cpu = process.cpu_times()
        
        # 填充性能指标字典
        performance_metrics["elapsed_time"] = end_time - start_time
        performance_metrics["memory_used_mb"] = (end_memory.rss - start_memory.rss) / (1024 * 1024)
        performance_metrics["cpu_user"] = end_cpu.user - start_cpu.user
        performance_metrics["cpu_system"] = end_cpu.system - start_cpu.system
        
        # 验证结果
        assert result == self.FIBONACCI_30[:num_terms]
        
        # 输出性能指标
        print(f"\nPerformance metrics for {num_terms} Fibonacci numbers:")
        print(f"Time elapsed: {performance_metrics['elapsed_time']:.4f} seconds")
        print(f"Memory used: {performance_metrics['memory_used_mb']:.2f} MB")
        print(f"CPU time: User {performance_metrics['cpu_user']:.4f}s, System {performance_metrics['cpu_system']:.4f}s")
        
        if "io_read_bytes" in performance_metrics:
            print(f"I/O: Read {performance_metrics['io_read_bytes']} bytes, Write {performance_metrics['io_write_bytes']} bytes")

# # 为了向后兼容，添加原始测试函数
# def test_fibonacci(num=3):
#     """Original test function that prints Fibonacci numbers."""
#     result = calculate_fibonacci(num)
#     for i in result:
#         print(i)