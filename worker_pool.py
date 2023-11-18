from multiprocessing import Pool, Queue
import zmq
import random
import json
import time
from threading import Thread
from model import Task
from util import deserialize, serialize



def spawn_workers(count: int, task_queue: Queue, result_queue: Queue):
  cb = lambda task:result_queue.put(task)
  with Pool(count) as pool:
    while True:
      task = task_queue.get()
      pool.apply_async(execute_task, (task,), callback=cb)

def push_worker(count, task_queue, result_queue):
  context = zmq.Context()
  router = context.socket(zmq.ROUTER)
  
  router.identity = b"Router1"
  router.bind("tcp://127.0.0.1:5555")
  available_workers = set()  
  while True:
    try:
      message = router.recv_multipart(flags=zmq.NOBLOCK)
      if message[2] == b"REGISTER":
        worker_id = message[0]
        # print(worker_id)
        available_workers.add(worker_id)
      else:
        print(message)
        result_queue.put(Task(**json.loads(message[2])))
    except Exception:
      pass

    try:
      if available_workers:
        task = task_queue.get(block=False) 
        router.send_multipart([random.choice(list(available_workers)), b"", json.dumps(task.dict()).encode()])
      else: 
        print("Workers are unavaliable.")
    except Exception:
      pass
      # print("Queue is empty. Waiting for tasks...")

WORK_DIR = {"local": spawn_workers, "push": push_worker}


def new_worker_pool(count: int, worker_type: str) -> (Thread, Queue, Queue):
  task_queue = Queue()
  result_queue = Queue()
  t = Thread(target=WORK_DIR[worker_type], args=(count, task_queue, result_queue), daemon=True)
  t.start()
  return t, task_queue, result_queue



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
  return ("COMPLETE", result)
