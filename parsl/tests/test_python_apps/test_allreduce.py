import time
import pytest
import psutil
import os
import numpy as np
from functools import wraps
from parsl.app.app import python_app

def monitor_resources(func):
    """资源监控装饰器"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        metrics = kwargs.get('metrics', {})
        process = psutil.Process(os.getpid())
        
        # 收集初始指标
        start = {
            'time': time.time(),
            'memory': process.memory_info().rss,
            'cpu': process.cpu_times(),
            'io': psutil.disk_io_counters(),
            'net': psutil.net_io_counters()
        }
        
        result = func(*args, **kwargs)
        
        # 收集结束指标并计算差值
        end = {
            'time': time.time(),
            'memory': process.memory_info().rss,
            'cpu': process.cpu_times(),
            'io': psutil.disk_io_counters(),
            'net': psutil.net_io_counters()
        }
        
        # 更新metrics
        metrics.update({
            'elapsed_time': end['time'] - start['time'],
            'memory_delta_mb': (end['memory'] - start['memory']) / (1024 * 1024),
            'cpu_user': end['cpu'].user - start['cpu'].user,
            'cpu_system': end['cpu'].system - start['cpu'].system,
            'io_read_kb': (end['io'].read_bytes - start['io'].read_bytes) / 1024,
            'io_write_kb': (end['io'].write_bytes - start['io'].write_bytes) / 1024,
            'net_sent_kb': (end['net'].bytes_sent - start['net'].bytes_sent) / 1024,
            'net_recv_kb': (end['net'].bytes_recv - start['net'].bytes_recv) / 1024
        })
        
        return result
    return wrapper

@python_app
def compute_partial_sum(data_part):
    sum = 0
    for value in data_part:
        sum += value
    return sum

@python_app
def sum_two_values(a, b):
    return a + b

class TestAllReduce:
    
    @pytest.fixture
    def performance_metrics(self):
        """性能指标fixture"""
        return {}
        
    @monitor_resources
    def run_allreduce_test(self, data_size, num_workers, metrics):
        """执行AllReduce测试"""
        # 生成测试数据
        data = np.random.rand(data_size)
        partitions = np.array_split(data, num_workers)
        
        # 提交任务
        futures = [compute_partial_sum(p) for p in partitions]
        
        # 树状归约
        while len(futures) > 1:
            new_futures = []
            for i in range(0, len(futures), 2):
                if i + 1 < len(futures):
                    new_futures.append(sum_two_values(futures[i], futures[i+1]))
                else:
                    new_futures.append(futures[i])
            futures = new_futures
            
        result = futures[0].result()
        expected = np.sum(data)
        assert np.isclose(result, expected)
        
        return result

    @pytest.mark.parametrize("data_size", [1_000_000_000])
    @pytest.mark.parametrize("num_workers", [64, 32, 16, 8, 4, 2, 1])
    def test_allreduce_performance(self, data_size, num_workers, performance_metrics):
        """性能测试用例"""
        try:
            self.run_allreduce_test(
                data_size=data_size,
                num_workers=num_workers,
                metrics=performance_metrics
            )
            
            # 打印详细性能报告
            print(f"\n{'='*20} Performance Report {'='*20}")
            print(f"Test Configuration:")
            print(f"  Data Size: {data_size:,}")
            print(f"  Workers: {num_workers}")
            print("\nMetrics:")
            for key, value in performance_metrics.items():
                if 'time' in key:
                    print(f"  {key:15s}: {value:.3f} sec")
                elif 'memory' in key:
                    print(f"  {key:15s}: {value:.2f} MB")
                elif any(x in key for x in ['io', 'net']):
                    print(f"  {key:15s}: {value:.2f} KB")
                else:
                    print(f"  {key:15s}: {value:.3f}")
            print('='*60)
            
        except Exception as e:
            print(f"Error during test execution: {str(e)}")
            raise

if __name__ == "__main__":
    # 直接运行示例
    test = TestAllReduce()
    metrics = {}
    test.run_allreduce_test(100000, 8, metrics=metrics)
    
    print("\nQuick Test Results:")
    for key, value in metrics.items():
        print(f"{key:15s}: {value}")
