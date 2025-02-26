import concurrent.futures as cf
import logging
from typing import List, Optional

import typeguard
import multiprocessing
import threading
import os
import queue
import itertools
import tempfile
import getpass
import fcntl
from datetime import datetime
from typing import Optional, Union, Dict
import json

import parsl.utils as putils
from parsl.utils import setproctitle
from parsl.data_provider.files import File
from parsl.executors.status_handling import BlockProviderExecutor
from parsl.executors.errors import ExecutorError
from parsl.executors.legion.errors import LegionTaskFailure, LegionRuntimeFailure
from parsl.executors.legion.file_queue import RobustFsQueue
from parsl.process_loggers import wrap_with_logs
from parsl.utils import RepresentationMixin
from parsl.serialize import deserialize, serialize
from parsl.providers import LocalProvider
from parsl.providers.base import ExecutionProvider

from concurrent.futures import Future


logger = logging.getLogger(__name__)


class ParslFinishTask:
    def __init__(self,
                executor_id: int,              # executor id of task
                result_received: bool,         # whether result is received or not
                result_file: Optional[str],    # path to file that contains the serialized result object
                reason: Optional[str],         # string describing why execution fails
                status: Optional[int]          # exit code of execution of task) -> None:
                ):
        self.parsl_executor_id: int = executor_id
        self.result_received: bool = result_received
        self.result_file: Optional[str] = result_file
        self.reason: Optional[str] = reason
        self.status: Optional[int] = status
    
class ParslReadyTask:
    def __init__(self, executor_id: int,
                func,
                resource_specification,
                args,
                kwargs) -> None:
        self.executor_id = executor_id
        self.serialize_func = serialize(func, buffer_threshold=1024 * 1024) # 序列化 
        self.resource_specification = resource_specification
        self.args = args
        self.kwargs = kwargs
    
class LegionReadyTask:
    def __init__(self, 
                executor_id: int,                # executor id of task
                parsl_executor_id: int,
                cmd: list,                       # command to execute the task
                input_files: list,                # list of input files to this function
                output_files: list,               # list of output files to this function
                map_file: Optional[str],          # pickled file containing mapping of local to remote names of files
                function_file: Optional[str],     # pickled file containing the function information
                argument_file: Optional[str],     # pickled file containing the arguments to the function call
                result_file: Optional[str],       # path to the pickled result object of the function execution
                resource_specification: Optional[Dict], # 资源类型
                ) -> None:
        self.executor_id: int = executor_id
        self.parsl_executor_id: int = parsl_executor_id
        self.cmd: list = cmd
        self.input_files: list[ParslFileToLegion] = input_files
        self.output_files: list[ParslFileToLegion] = output_files
        self.map_file: Optional[str] = map_file
        self.function_file: Optional[str] = function_file
        self.argument_file: Optional[str] = argument_file
        self.result_file: Optional[str] = result_file
        self.resource_specification: Optional[Dict] = resource_specification
    
class LegionFinishTask:
    def __init__(self, executor_id: int,
                parsl_executor_id: int,
                result_file: str):
        self.executor_id: int = executor_id
        self.parsl_executor_id: int = parsl_executor_id
        self.result_file: str = result_file

class ParslFileToLegion:
    """
    Support structure to report Parsl filenames to Legion Runtime.
    parsl_name is the local_name or filepath attribute of a Parsl file object.
    """
    def __init__(self, parsl_name: str):
        self.parsl_name = parsl_name


    # 直接把task的信息写入legion runtime监听的文件夹即可
def submit_task_to_legion(parsl_executor_id,
    input_files, output_files, resource_specification,
    function_file: str, argument_file: str, result_file: str, map_file: str, 
    path: str) -> (int, LegionReadyTask):
    # 首先需要加上文件锁，理论上应该是一个跨进程的文件读写锁
    with open(path+'.lock', 'w') as lock_file:
        # 获取排他锁
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            # 获取文件总行数
            try:
                with open(path, 'r') as f_read:
                    lines = f_read.readlines()
                    total_lines = len(lines)
            except FileNotFoundError:
                total_lines = -1
            
            legion_task_info = LegionReadyTask(
                total_lines,
                parsl_executor_id,
                ["mamba run -n parsl_py38 --no-capture-output python ./playground/exec_parsl_function.py"],
                input_files,
                output_files,
                map_file,
                function_file,
                argument_file,
                result_file,
                resource_specification,
            )
            # 写入逻辑...
            with open(path, 'a') as f:
                task_meta = {
                    "executor_id": str(legion_task_info.executor_id),
                    "parsl_executor_id": str(parsl_executor_id),
                    "cmd": legion_task_info.cmd,
                    "function_file": legion_task_info.function_file,
                    "argument_file": legion_task_info.argument_file,
                    "result_file": legion_task_info.result_file,
                    "map_file": legion_task_info.map_file,
                    "task_kind": 0,
                    "mem_kind": 16,
                    "request_mem_size": 1024000
                }
                f.write(json.dumps(task_meta) + '\n')
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)
    return total_lines, legion_task_info

def get_task_from_legion(queue: RobustFsQueue, timeout: Optional[float]=None) -> Optional[LegionFinishTask]:
    # 从legion runtime监听的文件夹中读取metadata的信息, 如果task已经完成则返回处理?
    msg = queue.pop(timeout)
    if msg is None:
        return None
    else:
        # 将接收到的msg json解析为数据结构LegionFinishTask
        # 从msg中获取executor_id, parsl_executor_id, result_file
        # 然后返回LegionFinishTask对象
        msg = json.loads(msg)
        return LegionFinishTask(
            executor_id = int(msg['executor_id']),
            parsl_executor_id = int(msg['parsl_executor_id']),
            result_file = msg['result_file']
        )

def serialize_object_to_file(path, obj):
    """Takes any object and serializes it to the file path."""
    serialized_obj = serialize(obj, buffer_threshold=1024 * 1024)
    with open(path, 'wb') as f_out:
        written = 0
        while written < len(serialized_obj):
            written += f_out.write(serialized_obj[written:])

def path_in_task(function_data_dir_name, executor_task_id, *path_components):
    """
    Returns a filename fixed and specific to a task.
    It is used for the following filename's:
        (not given): The subdirectory per task that contains function, result, etc.
        'function': Pickled file that contains the function to be executed.
        'argument': Pickled file that contains the arguments of the function call.
        'result': Pickled file that (will) contain the result of the function.
        'map': Pickled file with a Dict between local parsl names, and remote legion names.
    """
    task_dir = "{:04d}".format(executor_task_id)
    return os.path.join(function_data_dir_name, task_dir, *path_components)

def std_output_to_legion(fdname, stdfspec) -> ParslFileToLegion:
    """Find the name of the file that will contain stdout or stderr and
    return a ParslFileToVine with it. These files are never cached"""
    fname, _ = putils.get_std_fname_mode(fdname, stdfspec)
    return ParslFileToLegion(fname)

def construct_map_file(map_file, input_files, output_files):
    """ Map local filepath of parsl files to the filenames at the execution worker.
    If using a shared filesystem, the filepath is mapped to its absolute filename.
    Otherwise, to its original relative filename. In this later case, legion
    recreates any directory hierarchy needed."""
    file_translation_map = {}
    for spec in itertools.chain(input_files, output_files):
        local_name = spec.parsl_name
        remote_name = os.path.abspath(local_name)
        file_translation_map[local_name] = remote_name
    serialize_object_to_file(map_file, file_translation_map)

def register_file(self, parsl_task) -> ParslFileToLegion:
    """Generates a tuple (parsl_task.filepath, stage, cache) to give to
    legion. cache is always True.
    stage is True if the file has a relative path. (i.e., not
    a URL or an absolute path)"""

    return ParslFileToLegion(parsl_task.filepath)

@wrap_with_logs
def _legion_submit_wait(
        ready_task_queue: 'multiprocessing.Queue[ParslReadyTask]' = None,
        finished_task_queue: 'multiprocessing.Queue[ParslFinishTask]' = None,
        should_stop: Optional[multiprocessing.Event] = None,
        legion_runtime_json_path: Optional[str] = None,
        function_data_dir: Optional[tempfile.TemporaryDirectory] = None,
        legion_queue_dir: Optional[str] = None):
    
    logger.debug("Starting Legion Submit/Wait Process")
    setproctitle("parsl: Legion submit/wait")
    
    legion_queue = RobustFsQueue(legion_queue_dir)
    
    # Get parent pid, useful to shutdown this process when its parent, the legion
    # executor process, exits.
    orig_ppid = os.getppid()
        
    
    # main loop，不断从ready_task_queue中读取任务，提交给Legion Runtime
    while not should_stop.is_set():
        # Check if executor process is still running
        ppid = os.getppid()
        if ppid != orig_ppid:
            logger.debug("Executor process is detected to have exited. Exiting..")
            break
        
        while ready_task_queue.qsize() > 0 and not should_stop.is_set():
            try:
                parsl_ready_task: ParslReadyTask = ready_task_queue.get(timeout=1)
            except:
                logger.error("Queue is empty")
                raise 
            
            try:
                # 开始处理input和output的文件
                input_files = []
                output_files = []

                # Determine whether to stage input files that will exist at the workers
                # Input and output files are always cached
                input_files += [register_file(f) for f in parsl_ready_task.kwargs.get("inputs", []) if isinstance(f, File)]
                output_files += [register_file(f) for f in parsl_ready_task.kwargs.get("outputs", []) if isinstance(f, File)]

                # Also consider any *arg that looks like a file as an input:
                input_files += [register_file(f) for f in parsl_ready_task.args if isinstance(f, File)]
                
                for kwarg, maybe_file in parsl_ready_task.kwargs.items():
                    # Add appropriate input and output files from "stdout" and "stderr" keyword arguments
                    if kwarg == "stdout" or kwarg == "stderr":
                        if maybe_file:
                            output_files.append(std_output_to_legion(kwarg, maybe_file))
                    # For any other keyword that looks like a file, assume it is an input file
                    elif isinstance(maybe_file, File):
                        input_files.append(register_file(maybe_file))

                logger.debug("Process input_files and output_files finished!")
                    
                # Setup files to be used on a worker to execute the function
                function_file = None
                argument_file = None
                result_file = None
                map_file = None

                # Get path to files that will contain the pickled function,
                # arguments, result, and map of input and output files
                function_file = path_in_task(function_data_dir.name, parsl_ready_task.executor_id, "function")
                argument_file = path_in_task(function_data_dir.name, parsl_ready_task.executor_id, "argument")
                result_file = path_in_task(function_data_dir.name, parsl_ready_task.executor_id, "result")
                map_file = path_in_task(function_data_dir.name, parsl_ready_task.executor_id, "map")
                
                logger.debug("Create executor task {} with function at: {}, argument at: {}, \
                        and result to be found at: {} finished!".format(parsl_ready_task.executor_id, function_file, argument_file, result_file))

                # Serialize function object and arguments, separately
                serialize_object_to_file(function_file, deserialize(parsl_ready_task.serialize_func))
                args_dict = {'args': parsl_ready_task.args, 'kwargs': parsl_ready_task.kwargs}
                serialize_object_to_file(argument_file, args_dict)

                # Construct the map file of local filenames at worker
                construct_map_file(map_file, input_files, output_files)
                
                # Create message to put into the message queue
                logger.debug("Placing task {} on message queue".format(parsl_ready_task.executor_id))
                
                logger.debug("Removing executor task from queue")
            except:
                logger.error("Process: input / output / function / argument / result / map failed")
                raise
            
            try:
                legion_executor_id, _ = submit_task_to_legion(parsl_ready_task.executor_id,
                    input_files, output_files, parsl_ready_task.resource_specification,
                    function_file, argument_file, result_file, map_file, legion_runtime_json_path)
                logger.debug(f"Submitted executor task to Legion (in Legion id: {legion_executor_id})")
                
            except Exception as e:
                logger.error("Unable to submit task to legion: {}".format(e))
                finished_task_queue.put_nowait(ParslFinishTask(executor_id=parsl_ready_task.executor_id,
                                                               result_received=False,
                                                               result_file=None,
                                                               reason="task could not be submited to legion",
                                                               status=-1))
                continue
            logger.debug("Executor Parsl task {} submitted as Legion task with id {}".format(parsl_ready_task.executor_id, legion_executor_id))
            
        while not should_stop.is_set():
            task_msg = get_task_from_legion(legion_queue, 0.5)
            # TODO(xlc): 奇怪的bug, legion_task_map经常为空
            if task_msg == None:
                break 
            logger.debug(f"completed Legion executor task: {task_msg.executor_id}")
            
            if os.path.exists(task_msg.result_file):
                logger.debug("Found result in {}".format(task_msg.result_file))
                finished_task_queue.put_nowait(ParslFinishTask(executor_id=int(task_msg.parsl_executor_id),
                                                               result_received=True,
                                                               result_file=task_msg.result_file,
                                                               reason=None,
                                                               status=0))
            else:
                logger.debug("Did not find result in {}".format(task_msg.result_file))
                finished_task_queue.put_nowait(ParslFinishTask(executor_id=int(task_msg.parsl_executor_id),
                                                               result_received=False,
                                                               result_file=None,
                                                               reason="task could not find the result file",
                                                               status=-2))
    logger.debug("_legion_submit_wait Finished")


class LegionExecutor(BlockProviderExecutor, RepresentationMixin):
    """
    A legion-based executor.
    """

    @typeguard.typechecked
    def __init__(self, label: str = 'legion', 
                 legion_runtime_json_path: str = '',
                 legion_queue_dir: str = '',
                 provider: Optional[ExecutionProvider] = None):
        BlockProviderExecutor.__init__(self, provider=provider,
                                       block_error_handler=True)
        self.label: str = label
        
        # 从Legion Executor向Legion Runtime提交任务的队列
        self._ready_task_queue: 'multiprocessing.Queue[ParslFinishTask]' = multiprocessing.Queue()
        # 从Legion Runtime向Legion Executor返回任务结果的队列
        self._finished_task_queue: 'multiprocessing.Queue[ParslFinishTask]' = multiprocessing.Queue()
        # Event to signal whether the manager and factory processes should stop running
        self._should_stop = multiprocessing.Event()

        # Legion Runtime的进程
        self._submit_process: Optional[multiprocessing.Process] = None
        
        # Executor thread to collect results from Legion Runtime and set
        # tasks' futures to done status.
        self._collector_thread: Optional[threading.Thread] = None
        
        # track task id of submitted parsl tasks
        # task ids are incremental and start from 0
        self._executor_task_counter = 0
        
        # 标记tasks正在waiting或running的数量
        self._outstanding_tasks: int = 0
        self._outstanding_tasks_lock: threading.Lock = threading.Lock()
        
        # 存储正在执行的tasks
        self._tasks = dict()
        self._tasks_lock: threading.Lock = threading.Lock()
        
        # Path to directory that holds all tasks' data and results.
        self._function_data_dir: Optional[tempfile.TemporaryDirectory] = None
        
        # 存储Legion Runtime写入的json路径
        self.legion_runtime_json_path = legion_runtime_json_path
        assert(self.legion_runtime_json_path is not None and len(self.legion_runtime_json_path) > 0)
        
        self.legion_queue_dir: str = legion_queue_dir
        assert(self.legion_queue_dir is not None and len(self.legion_queue_dir) > 0)
        
    
    def _get_launch_command(self, block_id):
        # TODO(xlc): 这里应该是需要增加Legion Runtime的json路径
        
        # Implements BlockProviderExecutor's abstract method.
        # This executor uses different terminology for worker/launch
        # commands than in htex.
        return f"PARSL_WORKER_BLOCK_ID={block_id} {self._worker_command}"

    @property
    def outstanding(self) -> int:
        """Count the number of outstanding tasks."""
        logger.debug(f"Counted {self._outstanding_tasks} outstanding tasks")
        return self._outstanding_tasks

    @property
    def workers_per_node(self) -> Union[int, float]:
        return 1

    def __create_data_and_logging_dirs(self):
        # Create neccessary data and logging directories

        # Use the current run directory from Parsl
        run_dir = self.run_dir

        # Create directories for data and results
        log_dir = os.path.join(run_dir, self.label)
        # 如果不存在就进行创建
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        tmp_prefix = f'{self.label}-{getpass.getuser()}-{datetime.now().strftime("%Y%m%d%H%M%S%f")}-'
        self._function_data_dir = tempfile.TemporaryDirectory(prefix=tmp_prefix)
        logger.debug(f"Function data directory: {self._function_data_dir.name}, log directory: {log_dir}")

        
    def start(self) -> None:
        '''
        作用: 创建一个进程来运行Legion Runtime
        '''
        # Create data and logging dirs
        self.__create_data_and_logging_dirs()
        
        logger.debug("Starting LegionExecutor")

        # Create a process to run the Legion manager.
        submit_process_kwargs = {"ready_task_queue": self._ready_task_queue,
                                 "finished_task_queue": self._finished_task_queue,
                                 "should_stop": self._should_stop,
                                 "legion_runtime_json_path": self.legion_runtime_json_path,
                                 "function_data_dir": self._function_data_dir,
                                 "legion_queue_dir": self.legion_queue_dir}
        self._submit_process = multiprocessing.Process(target=_legion_submit_wait, name="Legion-Submit-Process", kwargs=submit_process_kwargs)
        
        # Run thread to collect results and set tasks' futures.
        self._collector_thread = threading.Thread(target=self._collect_legion_results,
                                                  name="Legion-Collector-Thread")
        self._collector_thread.daemon = True
        
        self._submit_process.start()
        self._collector_thread.start()
        
        logger.debug("All components in LegionExecutor started")

    def submit(self, func, resource_specification, *args, **kwargs):
        logger.debug(f'Got func: {func}')
        logger.debug(f'Got resource specification: {resource_specification}')
        logger.debug(f'Got args: {args}')
        logger.debug(f'Got kwargs: {kwargs}')

        # Assign executor task id to app
        parsl_task_id = self._executor_task_counter
        self._executor_task_counter += 1
        
        # Create a per task directory for the function, argument, map, and result files
        os.mkdir(self._path_in_task(parsl_task_id))
        fu = Future()
        with self._tasks_lock:
            self.tasks[int(parsl_task_id)] = fu
            logger.debug(f"push future for task {parsl_task_id}, check self.tasks: {self.tasks}")
            
        parsl_ready_task_info = ParslReadyTask(
            executor_id=parsl_task_id,
            func=func,
            resource_specification=resource_specification,
            args=args,
            kwargs=kwargs)
        
        # Send ready task to manager process
        if not self._submit_process.is_alive():
            raise ExecutorError(self, "Legion Submit Process is not alive")

        self._ready_task_queue.put_nowait(parsl_ready_task_info)
        logger.debug(f"Add parsl task {parsl_ready_task_info} to _ready_task_queue")
        
        # Increment outstanding task counter
        with self._outstanding_tasks_lock:
            self._outstanding_tasks += 1
        
        return fu

    def _path_in_task(self, executor_task_id, *path_components):
        """
        Returns a filename fixed and specific to a task.
        It is used for the following filename's:
            (not given): The subdirectory per task that contains function, result, etc.
            'function': Pickled file that contains the function to be executed.
            'argument': Pickled file that contains the arguments of the function call.
            'result': Pickled file that (will) contain the result of the function.
            'map': Pickled file with a dict between local parsl names, and remote legion names.
        """
        return path_in_task(self._function_data_dir.name, executor_task_id, *path_components)

    def shutdown(self) -> None:
        logger.debug("Legion shutdown started")
        self._should_stop.set()
        
        # Remove the workers that are still going
        kill_ids = [self.blocks_to_job_id[block] for block in self.blocks_to_job_id.keys()]
        if self.provider:
            logger.debug("Cancelling blocks")
            self.provider.cancel(kill_ids)
            
        # Join all processes before exiting
        logger.debug("Joining on submit process")
        self._submit_process.join()
        self._submit_process.close()
        
        # Shutdown multiprocessing queues
        self._ready_task_queue.close()
        self._ready_task_queue.join_thread()
        
        self._collector_thread.join()
        
        self._finished_task_queue.close()
        self._finished_task_queue.join_thread()

        logger.debug("Legion shutdown completed")
        
    @wrap_with_logs
    def _collect_legion_results(self):
        logger.debug("Starting Legion result collector Thread")
        
        try:
            while not self._should_stop.is_set():
                if not self._submit_process.is_alive():
                    raise ExecutorError(self, "Legion Submit Process is not alive")
                
                try:
                    task_report: ParslFinishTask = self._finished_task_queue.get(timeout=1)
                except queue.Empty:
                    continue
                
                logger.debug(f"check task_report.parsl_executor_id: {task_report.parsl_executor_id}, type: {type(task_report.parsl_executor_id)}")
                with self._tasks_lock:
                    logger.debug(f"pop future for task {task_report.parsl_executor_id}, check self.tasks: {self.tasks}")
                    future = self.tasks.pop(int(task_report.parsl_executor_id))
            
                logger.debug(f'Updating Future for Parsl Task: {task_report.parsl_executor_id}. \
                               Task {task_report.parsl_executor_id} has result_received set to {task_report.result_received}')
                if task_report.result_received:
                    try:
                        with open(task_report.result_file, 'rb') as f_in:  
                            result = deserialize(f_in.read())
                    except Exception as e:
                        logger.error(f'Cannot load result from result file {task_report.result_file}. Exception: {e}')
                        ex = LegionTaskFailure('Cannot load result from result file', None)
                        ex.__cause__ = e
                        future.set_exception(ex)
                    else:
                        if isinstance(result, Exception):
                            ex = LegionTaskFailure('Task execution raises an exception', result)
                            ex.__cause__ = result
                            future.set_exception(ex)
                        else:
                            future.set_result(result)
                else:
                    # If there are no results, then the task failed according to one of
                    # legion modes, such as resource exhaustion.
                    ex = LegionTaskFailure(task_report.reason, None)
                    future.set_exception(ex)
            
                # decrement outstanding task counter
                with self._outstanding_tasks_lock:
                    self._outstanding_tasks -= 1
        finally:
            logger.debug(f"Marking all {self.outstanding} outstanding tasks as failed")
            logger.debug("Acquiring tasks_lock")
            with self._tasks_lock:
                logger.debug("Acquired tasks_lock")
                # set exception for tasks waiting for results that legion did not execute
                for fu in self.tasks.values():
                    if not fu.done():
                        fu.set_exception(LegionRuntimeFailure("legion executor failed to execute the task."))
        logger.debug("Exiting Collector Thread")