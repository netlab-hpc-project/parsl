import os
import threading
from parsl.executors.legion.executor import submit_task_to_legion


# 测试正常情况(只写入单行)
def test_submit_task_to_legion_normal():
    path = "./parsl/tests/test_legion/test_args.json"
    # 删除path和path的文件锁
    if os.path.exists(path):
        os.remove(path)
        os.remove(path + ".lock")
    
    input_files = ["input1.txt", "input2.txt"]
    output_files = ["output1.txt", "output2.txt"]
    resource_specification = {"cpu": 4, "memory": 8}
    function_file = "function.py"
    argument_file = "argument.txt"
    result_file = "result.txt"
    map_file = "map.txt"
    

    total_lines, legion_task_info = submit_task_to_legion(
        input_files, output_files, resource_specification,
        function_file, argument_file, result_file, map_file, path
    )

    assert total_lines == 0  # 假设初始文件行数为 0
    assert legion_task_info.executor_id == 0  # 假设 executor_id 为 0
    assert legion_task_info.cmd == ["mamba run -n parsl_py38 --no-capture-output python./playground/exec_parsl_function.py"]
    assert legion_task_info.input_files == input_files
    assert legion_task_info.output_files == output_files
    assert legion_task_info.map_file == map_file
    assert legion_task_info.function_file == function_file
    assert legion_task_info.argument_file == argument_file
    assert legion_task_info.result_file == result_file
    assert legion_task_info.resource_specification == resource_specification
    
    # 打印path的全部内容
    with open(path, 'r') as f:
        print(f.read())
    
    # 删除path和path的文件锁
    os.remove(path)
    os.remove(path + ".lock")
    
    
# 测试正常情况(并行写入多行数据)
def test_submit_task_to_legion_par():
    path = "./parsl/tests/test_legion/test_args.json"
    # 删除path和path的文件锁
    if os.path.exists(path):
        os.remove(path)
        os.remove(path + ".lock")
    
    results = []  # 新增共享结果列表
    lock = threading.Lock()  # 新增线程锁
    threads = []
    
    
    for i in range(3):
        input_files = [f"input{i}.txt", f"input2{i+3}.txt"]
        output_files = [f"output{i}.txt", f"output{i+3}.txt"]
        resource_specification = {"cpu": i, "memory": i*2}
        function_file = f"function{i}.py"
        argument_file = f"argument{i}.txt"
        result_file = f"result{i}.txt"
        map_file = f"map{i}.txt"
        
        def thread_task():
            total_lines, legion_task_info = submit_task_to_legion(
                input_files, output_files, resource_specification,
                function_file, argument_file, result_file, map_file, path
            )
            with lock:
                results.append((total_lines, legion_task_info))
                 
        t = threading.Thread(target=thread_task)
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    assert len(results) == 3
    for thread_id, (total_lines, task_info) in enumerate(results):
        print(thread_id, total_lines, task_info)

    # 打印path的全部内容
    with open(path, 'r') as f:
        print(f.read())
    
    # 删除path和path的文件锁
    os.remove(path)
    os.remove(path + ".lock")
