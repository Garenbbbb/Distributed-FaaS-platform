from multiprocessing import Pool, Queue
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from model import Task
from util import deserialize, serialize

class WorkerPool():
  def __init__(self, count: int, task_queue: Queue, result_queue: Queue):
    self.t = Thread(target=spawn_workers, args=(count, task_queue, result_queue), daemon=True)
  def spawn(self):
    self.t.start()
  def wait(self):
    self.t.join()

def spawn_workers(count: int, task_queue: Queue, result_queue: Queue):
  with Pool(count) as pool:
    with ThreadPoolExecutor(10) as awaiter:
      while True:
        task = task_queue.get()
        async_result = pool.apply_async(execute_task, (task,))
        awaiter.submit(await_async_result, async_result, result_queue)

def await_async_result(async_result, result_queue: Queue):
  task = async_result.get()
  result_queue.put(task)

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
    params = deserialize(param_payload)
  except Exception as e:
    return ("FAILED", f"failed to deserialize params: {str(e)}")
  try:
    result = fn(*params)
  except Exception as e:
    return ("FAILED", f"failed to execute function: {str(e)}")
  return ("COMPLETED", result)
