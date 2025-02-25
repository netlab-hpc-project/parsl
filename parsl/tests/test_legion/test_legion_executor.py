from parsl.executors.legion.executor import LegionExecutor

def multiply(x, y):
    return x * y

def test_start_submit():
    with LegionExecutor(
        label="legion",
        legion_runtime_json_path="/home/netlab/xlc_interview/project/flexflow/playground/task_args.json",
        legion_queue_dir="/home/netlab/xlc_interview/project/flexflow/playground/queue",
    ) as executor:
        executor.start()
        futures = [executor.submit(multiply, {}, i, 7) for i in range(1)]
        for i, future in enumerate(futures):
            assert future.result() == i * 7
            assert future.done()
            assert future.exception() is None
