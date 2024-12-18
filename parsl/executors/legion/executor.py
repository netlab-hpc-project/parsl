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
from datetime import datetime
from typing import Optional, Union

import parsl.utils as putils
from parsl.data_provider.files import File
from parsl.executors.status_handling import BlockProviderExecutor
from parsl.executors.errors import ExecutorError
from parsl.executors.legion.errors import LegionRuntimeFailure
from parsl.process_loggers import wrap_with_logs
from parsl.utils import RepresentationMixin
from parsl.serialize import deserialize, serialize
from parsl.providers import LocalProvider
from parsl.providers.base import ExecutionProvider

from concurrent.futures import Future


logger = logging.getLogger(__name__)

DEFAULT_LAUNCH_CMD = ("process_worker_pool.py")

def submit_task_to_legion(task, path):
    # TODO(xlc): 提交任务给Legion Runtime, 这里应该是写到文件里面
    # 直接把task的信息写入legion runtime监听的文件夹即可
    task.add_to_path(path)

def get_task_from_legion(path):
    # TODO(xlc): 
    # 从legion runtime监听的文件夹中读取metadata的信息, 如果task已经完成则返回处理?
    pass

def mark_legion_task_as_finished(parsl_task, path, finished_task_queue):
    # TODO(xlc): 
    # 把task的最新的状态写入legion runtime监听的文件夹中的metadata字段即可
    pass

def _explain_taskvine_result(task):
    # TODO(xlc): 
    # 从task定义的output中读取结果即可
    pass

class LegionTaskToParsl:
    def __init__(self,
                executor_id: int,              # executor id of task
                result_received: bool,         # whether result is received or not
                result_file: Optional[str],    # path to file that contains the serialized result object
                reason: Optional[str],         # string describing why execution fails
                status: Optional[int]          # exit code of execution of task) -> None:
                ):
        self.executor_id = executor_id
        self.result_received = result_received
        self.result_file = result_file
        self.reason = reason
        self.status = status
    
    
class ParslTaskToLegion:
    def __init__(self, 
                executor_id: int,                 # executor id of Parsl function
                input_files: list,                # list of input files to this function
                output_files: list,               # list of output files to this function
                map_file: Optional[str],          # pickled file containing mapping of local to remote names of files
                function_file: Optional[str],     # pickled file containing the function information
                argument_file: Optional[str],     # pickled file containing the arguments to the function call
                result_file: Optional[str],       # path to the pickled result object of the function execution
                resource_specification: Optional[dict], # 资源类型
                ) -> None:
        self.executor_id = executor_id
        self.input_files = input_files
        self.output_files = output_files
        self.map_file = map_file
        self.function_file = function_file
        self.argument_file = argument_file
        self.result_file = result_file
        self.resource_specification = resource_specification
        
    
    def add_to_path(self, path):
        pass
    
class ParslFileToLegion:
    """
    Support structure to report Parsl filenames to Legion Runtime.
    parsl_name is the local_name or filepath attribute of a Parsl file object.
    """
    def __init__(self, parsl_name: str):
        self.parsl_name = parsl_name

@wrap_with_logs
def _legion_submit_wait(
        ready_task_queue=None,
        finished_task_queue=None,
        should_stop=None,
        legion_runtime_json_path=None):
    
    logger.debug("_legion_submit_wait Started")
    legion_id_to_executor_task_id = {}
    
    task_id_to_result_file = {}
    
    # main loop，不断从ready_task_queue中读取任务，提交给Legion Runtime
    while not should_stop.is_set():
        while ready_task_queue.qsize() > 0 and not should_stop.is_set():
            try:
                task = ready_task_queue.get(timeout=1)
                logger.debug("Removing executor task from queue")
            except:
                logger.debug("Queue is empty")
                continue
            
            try:
                legion_id = submit_task_to_legion(task, legion_runtime_json_path)
                logger.debug("Submitted executor task {} to TaskVine".format(task.executor_id))
                legion_id_to_executor_task_id[str(legion_id)] = str(task.executor_id)
            except Exception as e:
                logger.error("Unable to submit task to taskvine: {}".format(e))
                finished_task_queue.put_nowait(LegionTaskToParsl(executor_id=task.executor_id,
                                                               result_received=False,
                                                               result_file=None,
                                                               reason="task could not be submited to taskvine",
                                                               status=-1))
                continue
            logger.debug("Executor task {} submitted as Legion task with id {}".format(task.executor_id, legion_id))
    
        # 如果此时退出了
        
        while not should_stop.is_set():
            t = get_task_from_legion(legion_runtime_json_path)
            executor_task_id = legion_id_to_executor_task_id[str(t.id)][0]
            legion_id_to_executor_task_id.pop(str(t.id))
            
            # When a task is found
            result_file = task_id_to_result_file.pop(executor_task_id)
            logger.debug(f"completed executor task info: {executor_task_id}, {t.command}, {t.std_output}")
            
            logger.debug("Looking for result in {}".format(result_file))
            
            if os.path.exists(result_file):
                logger.debug("Found result in {}".format(result_file))
                finished_task_queue.put_nowait(LegionTaskToParsl(executor_id=executor_task_id,
                                                               result_received=True,
                                                               result_file=result_file,
                                                               reason=None,
                                                               status=t.exit_code))
            else:
                reason = _explain_taskvine_result(t)
                logger.debug("Did not find result in {}".format(result_file))
                logger.debug("Wrapper Script status: {}\nTaskVine Status: {}".format(t.exit_code, t.result))
                logger.debug("Task with executor id {} / vine id {} failed because:\n{}".format(executor_task_id, t.id, reason))
                finished_task_queue.put_nowait(LegionTaskToParsl(executor_id=executor_task_id,
                                                               result_received=False,
                                                               result_file=None,
                                                               reason=reason,
                                                               status=t.exit_code))
    logger.debug("_legion_submit_wait Finished")


class LegionExecutor(BlockProviderExecutor, RepresentationMixin):
    """
    A legion-based executor.
    """

    @typeguard.typechecked
    def __init__(self, label: str = 'legion', 
                 legion_runtime_json_path: str = '',
                 provider: Optional[ExecutionProvider] = None,
                 launch_cmd: Optional[str] = None,
                 ):
        BlockProviderExecutor.__init__(self, provider=provider,
                                       block_error_handler=True)
        self.label = label
        
        # 从Legion Executor向Legion Runtime提交任务的队列
        self._ready_task_queue: multiprocessing.Queue = multiprocessing.Queue()
        # 从Legion Runtime向Legion Executor返回任务结果的队列
        self._finished_task_queue: multiprocessing.Queue = multiprocessing.Queue()
        # Event to signal whether the manager and factory processes should stop running
        self._should_stop = multiprocessing.Event()

        # Legion Runtime的进程
        self._submit_process = None
        
        # Executor thread to collect results from Legion Runtime and set
        # tasks' futures to done status.
        self._collector_thread = None
        
        # track task id of submitted parsl tasks
        # task ids are incremental and start from 0
        self._executor_task_counter = 0
        
        # 标记tasks正在waiting或running的数量
        self._outstanding_tasks = 0
        self._outstanding_tasks_lock = threading.Lock()
        
        # 存储正在执行的tasks
        self._tasks = dict()
        self._tasks_lock = threading.Lock()
        
        # Path to directory that holds all tasks' data and results.
        self._function_data_dir = ""
        
        # 存储Legion Runtime写入的json路径
        self.legion_runtime_json_path = legion_runtime_json_path
        assert(self.legion_runtime_json_path is not None and len(self.legion_runtime_json_path) > 0)
        
        if not launch_cmd:
            launch_cmd = DEFAULT_LAUNCH_CMD
        self.launch_cmd = launch_cmd

    @property
    def outstanding(self) -> int:
        """Count the number of outstanding tasks."""
        logger.debug(f"Counted {self._outstanding_tasks} outstanding tasks")
        return self._outstanding_tasks
    
    def _get_launch_command(self, block_id):
        # TODO(xlc): 这里应该是需要增加Legion Runtime的json路径
        
        # Implements BlockProviderExecutor's abstract method.
        # This executor uses different terminology for worker/launch
        # commands than in htex.
        return f"PARSL_WORKER_BLOCK_ID={block_id} {self._worker_command}"

    @property
    def workers_per_node(self) -> Union[int, float]:
        return 1

    def __create_data_and_logging_dirs(self):
        # Create neccessary data and logging directories

        # Use the current run directory from Parsl
        run_dir = self.run_dir

        # Create directories for data and results
        log_dir = os.path.join(run_dir, self.label)
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

        # Create a process to run the TaskVine manager.
        submit_process_kwargs = {"ready_task_queue": self._ready_task_queue,
                                 "finished_task_queue": self._finished_task_queue,
                                 "should_stop": self._should_stop,
                                 "legion_runtime_json_path": self.legion_runtime_json_path}
        self._submit_process = multiprocessing.Process(target=_legion_submit_wait, name="Legion-Submit-Process", kwargs=submit_process_kwargs)
        
        # Run thread to collect results and set tasks' futures.
        self._collector_thread = threading.Thread(target=self._collect_legion_results,
                                                  name="TaskVine-Collector-Thread")
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
        executor_task_id = self._executor_task_counter
        self._executor_task_counter += 1
        
        # Create a per task directory for the function, argument, map, and result files
        os.mkdir(self._path_in_task(executor_task_id))
        
        # 开始处理input和output的文件
        input_files = []
        output_files = []

        # Determine whether to stage input files that will exist at the workers
        # Input and output files are always cached
        input_files += [self._register_file(f) for f in kwargs.get("inputs", []) if isinstance(f, File)]
        output_files += [self._register_file(f) for f in kwargs.get("outputs", []) if isinstance(f, File)]

        # Also consider any *arg that looks like a file as an input:
        input_files += [self._register_file(f) for f in args if isinstance(f, File)]
        
        for kwarg, maybe_file in kwargs.items():
            # Add appropriate input and output files from "stdout" and "stderr" keyword arguments
            if kwarg == "stdout" or kwarg == "stderr":
                if maybe_file:
                    output_files.append(self._std_output_to_vine(kwarg, maybe_file))
            # For any other keyword that looks like a file, assume it is an input file
            elif isinstance(maybe_file, File):
                input_files.append(self._register_file(maybe_file))

        
        fu = Future()
        with self._tasks_lock:
            self.tasks[str(executor_task_id)] = fu
            
        # Setup files to be used on a worker to execute the function
        function_file = None
        argument_file = None
        result_file = None
        map_file = None

        # Get path to files that will contain the pickled function,
        # arguments, result, and map of input and output files
        function_file = self._path_in_task(executor_task_id, "function")
        argument_file = self._path_in_task(executor_task_id, "argument")
        result_file = self._path_in_task(executor_task_id, "result")
        map_file = self._path_in_task(executor_task_id, "map")
        
        logger.debug("Creating executor task {} with function at: {}, argument at: {}, \
                and result to be found at: {}".format(executor_task_id, function_file, argument_file, result_file))

        # Serialize function object and arguments, separately
        self._serialize_object_to_file(function_file, func)
        args_dict = {'args': args, 'kwargs': kwargs}
        self._serialize_object_to_file(argument_file, args_dict)

        # Construct the map file of local filenames at worker
        self._construct_map_file(map_file, input_files, output_files)
        
        # Create message to put into the message queue
        logger.debug("Placing task {} on message queue".format(executor_task_id))

        
        task_info = ParslTaskToLegion(executor_id=executor_task_id,
                                    input_files=input_files,
                                    output_files=output_files,
                                    map_file=map_file,
                                    function_file=function_file,
                                    argument_file=argument_file,
                                    result_file=result_file,
                                    resource_specification=resource_specification,)
        
        # Send ready task to manager process
        if not self._submit_process.is_alive():
            raise ExecutorError(self, "Legion Submit Process is not alive")

        self._ready_task_queue.put_nowait(task_info)
        
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
            'map': Pickled file with a dict between local parsl names, and remote taskvine names.
        """
        task_dir = "{:04d}".format(executor_task_id)
        return os.path.join(self._function_data_dir.name, task_dir, *path_components)

    def _construct_map_file(self, map_file, input_files, output_files):
        """ Map local filepath of parsl files to the filenames at the execution worker.
        If using a shared filesystem, the filepath is mapped to its absolute filename.
        Otherwise, to its original relative filename. In this later case, taskvine
        recreates any directory hierarchy needed."""
        file_translation_map = {}
        for spec in itertools.chain(input_files, output_files):
            local_name = spec.parsl_name
            remote_name = os.path.abspath(local_name)
            file_translation_map[local_name] = remote_name
        self._serialize_object_to_file(map_file, file_translation_map)

    def _register_file(self, parsl_file):
        """Generates a tuple (parsl_file.filepath, stage, cache) to give to
        taskvine. cache is always True.
        stage is True if the file has a relative path. (i.e., not
        a URL or an absolute path)"""

        return ParslFileToLegion(parsl_file.filepath)
    
    def _std_output_to_Legion(self, fdname, stdfspec):
        """Find the name of the file that will contain stdout or stderr and
        return a ParslFileToVine with it. These files are never cached"""
        fname, _ = putils.get_std_fname_mode(fdname, stdfspec)
        return ParslFileToLegion(fname)

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
                    task_report = self._finished_task_queue.get(timeout=1)
                except queue.Empty:
                    continue
                
                with self._tasks_lock:
                    future = self.tasks.pop(task_report.executor_id)
            
        finally:
            logger.debug(f"Marking all {self.outstanding} outstanding tasks as failed")
            logger.debug("Acquiring tasks_lock")
            with self._tasks_lock:
                logger.debug("Acquired tasks_lock")
                # set exception for tasks waiting for results that taskvine did not execute
                for fu in self.tasks.values():
                    if not fu.done():
                        fu.set_exception(LegionRuntimeFailure("taskvine executor failed to execute the task."))
        logger.debug("Exiting Collector Thread")
        
    def _serialize_object_to_file(self, path, obj):
        """Takes any object and serializes it to the file path."""
        serialized_obj = serialize(obj, buffer_threshold=1024 * 1024)
        with open(path, 'wb') as f_out:
            written = 0
            while written < len(serialized_obj):
                written += f_out.write(serialized_obj[written:])