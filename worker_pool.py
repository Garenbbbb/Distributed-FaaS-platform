from multiprocessing import Pool, Queue
import zmq
import random
import json
import time
from threading import Thread
from model import Task
from util import deserialize, serialize
from config import task_dispatcher_url

class WorkerManger():
  def __init__(self, heartbeat_threshold):
    self.workers = {}
    self.workerLoad = {}
    self.heartbeat_threshold = heartbeat_threshold
    # one thread to check_worker_status

  def receive_heartbeat(self, worker_id, work_load):
    self.workers[worker_id] = time.time()
    self.workerLoad[worker_id] = work_load
  
  def select_worker(self):
    if len(self.workers) > 0:
      return min(self.workerLoad, key=self.workerLoad.get)
    else:
      return None
  
  def worker_avaliable(self):

    return len(self.workers) > 0, self.select_worker()

  def check_worker_status(self):
    current_time = time.time()
    for worker_id, last_heartbeat_time in self.workers.items():
      time_since_last_heartbeat = current_time - last_heartbeat_time
      if time_since_last_heartbeat > self.heartbeat_threshold:   
        del self.workers[worker_id]   
        del self.workerLoad[worker_id]

def spawn_workers(count: int, task_queue: Queue, result_queue: Queue):
  cb = lambda task:result_queue.put(task)
  with Pool(count) as pool:
    while True:
      task = task_queue.get()
      pool.apply_async(execute_task, (task,), callback=cb)

def push_worker(count, task_queue, result_queue):
  context = zmq.Context()
  router = context.socket(zmq.ROUTER)
  router.identity = b"Router"
  router.bind(task_dispatcher_url)
  
  available_workers = WorkerManger(2)  
  while True:
    try:
      message = router.recv_multipart(flags=zmq.NOBLOCK)
      print(message)
      if message[2] == b"REGISTER":
        available_workers.receive_heartbeat(message[0], message[3])
      else:
        result_queue.put(Task(**json.loads(message[2])))
    except Exception:
      pass
    try:
      flag, worker = available_workers.worker_avaliable()
      if flag:
        task = task_queue.get(block=False) 
        router.send_multipart([worker, b"", json.dumps(task.dict()).encode()])
      else: 
        print("Workers are unavaliable.")
        pass
    except Exception:
      # print("Queue is empty. Waiting for tasks...")
      pass

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
