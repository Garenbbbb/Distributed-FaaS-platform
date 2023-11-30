from multiprocessing import Pool, Queue
from .model import Task
from util.serialize import deserialize, serialize

def local_worker(task_queue: Queue, result_queue: Queue, num_processes: int = 4):
  cb = lambda task:result_queue.put(task)
  with Pool(num_processes) as pool:
    while True:
      task = task_queue.get()
      pool.apply_async(execute_task, (task,), callback=cb)

def execute_task(task: Task) -> Task:
  status, result = try_execute_task(task.task_info.fn_payload, task.task_info.param_payload)
  task.task_info.status = status
  task.task_info.result = serialize(result)
  return task

def try_execute_task(fn_payload: str, param_payload: str) -> (str, str):
  try:
    fn = deserialize(fn_payload)
  except Exception as e:
    return ("FAILED", f"failed to deserialize function body: {str(e)}")
  try:
    args, kwargs = deserialize(param_payload)
  except Exception as e:
    return ("FAILED", f"failed to deserialize params: {str(e)}")
  try:
    result = fn(*args, **kwargs)
  except Exception as e:
    return ("FAILED", f"failed to execute function: {str(e)}")
  return ("COMPLETED", result)
