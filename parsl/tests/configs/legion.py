from parsl.config import Config
from parsl.executors import LegionExecutor

def fresh_config():
    return Config(
        executors=[
            LegionExecutor(
                label="legion",
                legion_runtime_json_path="parsl/executors/legion/flexflow/playground/task_args.json",
                legion_queue_dir="parsl/executors/legion/flexflow/playground/queue",
            )
        ],
    )
