from parsl.config import Config
from parsl.executors import LegionExecutor

def fresh_config():
    return Config(
        executors=[
            LegionExecutor(
                label="legion",
                legion_runtime_json_path="/home/netlab/xlc_interview/project/flexflow/playground/task_args.json",
                legion_queue_dir="/home/netlab/xlc_interview/project/flexflow/playground/queue",
            )
        ],
    )
