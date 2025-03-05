import os
import sys
import time
import subprocess
import signal
from pathlib import Path

def modify_run_x86_config(cpu_nums, np_nums):
    """修改run_x86.py中的配置"""
    file_path = "./parsl/executors/legion/flexflow/scripts/runner_helper/run_x86.py"
    with open(file_path, 'r') as f:
        lines = f.readlines()
    
    # 修改legion_cpu_nums和mpi_run_args
    for i, line in enumerate(lines):
        if "self.legion_cpu_nums: int = " in line:
            lines[i] = f"        self.legion_cpu_nums: int = {cpu_nums}\n"
        elif "self.mpi_run_args: List[str] = [" in line:
            lines[i] = f'        self.mpi_run_args: List[str] = ["-np {np_nums}"]\n'
    
    with open(file_path, 'w') as f:
        f.writelines(lines)

def run_legion_server():
    """启动legion server进程,隐藏输出"""
    with open(os.devnull, 'w') as devnull:
        process = subprocess.Popen(
            ['make', 'legion_server'],
            stdout=devnull,
            stderr=devnull
        )
    return process


def run_test():
    """在mamba环境中运行测试"""
    cmd = "mamba run -n parsl_py38 pytest parsl/tests/test_python_apps/test_allreduce.py --config parsl/tests/configs/legion.py -v -s"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout, result.returncode


def send_feishu_notification(test_output, cpu_nums, np_nums, status):
    """发送飞书通知"""
    title = "Legion Test Report"
    subtitle = f"CPU Nums: {cpu_nums}, NP Nums: {np_nums}"
    content = f"Test Output:\n{test_output}" if status == 0 else test_output
    if status == 0:
        tag = "Finish"
    elif status == -256:
        tag = "Start"
    else:
        tag = "Error"
    
    cmd = f'python utils/send_feishu.py "{title}" "{subtitle}" "{content}" "{tag}"'
    subprocess.run(cmd, shell=True)

def main():
    # 测试配置组合
    # (CPU nums, NP nums)
    configs = [
        (24, 24),
        (16, 16),
        (8, 8),
        (4, 4),
        (2, 2),
        (1, 1),
    ]
    
    for cpu_nums, np_nums in configs:
        print(f"\nRunning test with CPU nums: {cpu_nums}, NP nums: {np_nums}")
        
        # 修改配置文件
        modify_run_x86_config(cpu_nums, np_nums)
        
        # 启动legion server
        server_process = run_legion_server()
        time.sleep(10)  # 等待服务器启动
        
        try:
            # 发送通知
            send_feishu_notification("Legion server started", cpu_nums, np_nums, -256)
            
            # 运行测试
            test_output, status = run_test()
            print(test_output)
            
            # 发送通知
            # ```bash\n + test_output + \n```
            # test_output = f"```bash\n{test_output}\n```"
            send_feishu_notification(test_output, cpu_nums, np_nums, status)
            
        finally:
            # 终止legion server进程
            if server_process:
                server_process.terminate()
                try:
                    server_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    server_process.kill()
                
if __name__ == "__main__":
    # 确保在正确的conda环境中运行
    if not os.environ.get('CONDA_DEFAULT_ENV') == 'parsl_py38':
        print("Please run this script in parsl_py38 conda environment")
        sys.exit(1)
    
    # 确保在项目根目录运行
    if not Path('parsl').exists():
        print("Please run this script from project root directory")
        sys.exit(1)
        
    main()
