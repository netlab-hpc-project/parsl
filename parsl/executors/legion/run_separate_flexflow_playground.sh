#!/bin/bash

set -xe

platform="x86"
compute_backend="torch_cpu"
conda_env="parsl_py38"
runtime_task_args_path="./playground/task_args.json"
runtime_task_queue_dir="./playground/queue"

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        --platform)
            platform="$2"
            shift 2
            ;;
        --compute-backend)
            compute_backend="$2"
            shift 2
            ;;
        --conda_env)
            conda_env="$2"
            shift 2
            ;;
        --runtime_task_args_path)
            runtime_task_args_path="$2"
            shift 2
            ;;
        --runtime_task_queue_dir)
            runtime_task_queue_dir="$2"
            shift 2
            ;;
        *)
            echo "未知参数: $1"
            exit 1
            ;;
    esac
done

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
echo "当前脚本位置：$SCRIPT_DIR"

cd "$SCRIPT_DIR/flexflow"

./scripts/runner_cli run $platform --compute-backend $compute_backend \
    --runtime_task_args_path $runtime_task_args_path \
    --runtime_task_queue_dir $runtime_task_queue_dir playground