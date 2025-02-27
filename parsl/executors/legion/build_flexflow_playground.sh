#!/bin/bash

set -xe


platform="x86"
compute_backend="torch_cpu"
torch_dir="/home/netlab/xlc_interview/project/outside_torch/libtorch"
conda_env="parsl_py38"
build_from_empty="false"

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
        --torch-dir)
            torch_dir="$2"
            shift 2
            ;;
        --conda_env)
            conda_env="$2"
            shift 2
            ;;
        --build-from-empty)
            build_from_empty="true"
            shift 1
            ;;
        *)
            echo "未知参数: $1"
            exit 1
            ;;
    esac
done

# 获取脚本所在目录的绝对路径
SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
echo "当前脚本位置：$SCRIPT_DIR"

# 检查"$SCRIPT_DIR/flexflow"是否存在，如果不存在则直接结束脚本，报错
if [ ! -d "$SCRIPT_DIR/flexflow" ]; then
    echo "Error: $SCRIPT_DIR/flexflow 不存在"
    exit 1
fi
# 切换到脚本所在目录
cd "$SCRIPT_DIR/flexflow"

if [ $build_from_empty -eq "true" ]; then
    rm -rf ./target
fi

./scripts/runner_cli build $platform --compute-backend $compute_backend --torch-dir $torch_dir