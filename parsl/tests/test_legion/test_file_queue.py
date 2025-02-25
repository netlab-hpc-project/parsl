import pytest
import time
import json
import shutil
import multiprocessing
from parsl.executors.legion.file_queue import RobustFsQueue

# Test that the push method creates a file with the correct message
def test_push_creates_file_with_correct_message():
    try:
        queue = RobustFsQueue("./parsl/tests/test_legion/test_queue")
        message = {"key": "value"}
        queue.push(message)
        assert queue.queue_dir.exists()
        if queue.queue_dir.exists():
            # 读取并打印所有后缀为".msg"的文件
            for file in queue.queue_dir.glob("*.msg"):
                with open(file, 'r') as f:
                    print(file, ": ", json.load(f))
    finally:
        if queue.queue_dir.exists():
            shutil.rmtree(queue.queue_dir)
    
    

# Test that the push method handles concurrent access correctly
def test_push_handles_concurrent_access():
    try:
        queue = RobustFsQueue("./parsl/tests/test_legion/test_queue")
        messages = [{"key": "value1"}, {"key": "value2"}]
        
        processes: list[multiprocessing.Process] = []
        for message in messages:
            process = multiprocessing.Process(
                target=queue.push, 
                args=(message,),
                name=f"PushProcess-{message['key']}"
            )
            processes.append(process)
            process.start()
        
        for process in processes:
            process.join(timeout=30)  # 设置30秒超时
            if process.exitcode is None:  # 检查是否超时
                process.terminate()
                pytest.fail(f"Process {process.name} timed out")
        
        assert queue.queue_dir.exists()
        if queue.queue_dir.exists():
            # 读取并打印所有后缀为".msg"的文件
            for file in queue.queue_dir.glob("*.msg"):
                with open(file, 'r') as f:
                    print(file, ": ", json.load(f))
    finally:
        if queue.queue_dir.exists():
            shutil.rmtree(queue.queue_dir)

def test_pop():
    try:
        queue = RobustFsQueue("./parsl/tests/test_legion/test_queue")
        messages = [{"key": "value1"}, {"key": "value2"}]
        
        processes: list[multiprocessing.Process] = []
        
        def get_pop_result():
            count = 0
            while count < len(messages):
                result = queue.pop(timeout=0.1)
                if result is not None:
                    count += 1
                    print(result)
                    
        pop_process = multiprocessing.Process(
            target=get_pop_result,
            args=(),
            name="PopProcess"
        )
        processes.append(pop_process)
        pop_process.start()
        
        for message in messages:
            process = multiprocessing.Process(
                target=queue.push, 
                args=(message,),
                name=f"PushProcess-{message['key']}"
            )
            processes.append(process)
            process.start()
        
        for process in processes:
            process.join(timeout=5)  # 设置30秒超时
            if process.exitcode is None:  # 检查是否超时
                process.terminate()
                pytest.fail(f"Process {process.name} timed out")
        
    finally:
        if queue.queue_dir.exists():
            shutil.rmtree(queue.queue_dir)