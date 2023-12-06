from random import randint
import logging
import zmq
import json
from .model import Task
from .tasks import *
import time
from collections import defaultdict

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.DEBUG)

class Worker():
  def __init__(self):
    self.tasks = {}
    self.last_heartbeat = 0
  @property
  def since_last_heartbeat(self):
    return time.time() - self.last_heartbeat

class WorkerManger():
  def __init__(self, heartbeat_threshold, task_queue):
    self.workers = defaultdict(Worker)
    self.heartbeat_threshold = heartbeat_threshold
    self.task_queue = task_queue
    # one thread to check_worker_status
  
  def register_task(self, worker_id, task):
    self.workers[worker_id].tasks[task.task_id] = task

  def unregister_task(self, worker_id, task):
    worker = self.workers[worker_id]
    if not worker:
      print(f"Worker {worker_id} does not exist")
      return
    if task.task_id not in worker.tasks:
      print(f"Worker {worker_id} does not own task {task.task_id}")
      return
    del worker.tasks[task.task_id]

  def register_heartbeat(self, worker_id):
    self.workers[worker_id].last_heartbeat = time.time()

  def check_worker_status(self):
    for worker_id, worker in list(self.workers.items()):
      if worker.since_last_heartbeat > self.heartbeat_threshold:
        print(f"Stale worker detected: {worker_id}")
        del self.workers[worker_id]
        for task in worker.tasks.values():
          print(f"Re-adding to task queue: {task.task_id}")
          self.task_queue.put(task)


def pull_worker_handler(task_queue, result_queue):
  context = zmq.Context()
  router = context.socket(zmq.REP)
  router.bind("tcp://*:5555")

  worker_manager = WorkerManger(2, task_queue)
  while True:
    request = router.recv_multipart()
    worker_id, req_type, req_payload = request

    if req_type == REQUEST_TYPE_READY:
      if task_queue.empty():
        # logging.info("Worker ready but no tasks available. Ignoring request since it will be resent.")
        router.send_multipart([REPLY_TYPE_NONE, b""])
        continue

      task = task_queue.get()
      logging.info(f"Sending task {task.task_id} to {worker_id.decode()}")
      worker_manager.register_task(worker_id, task)
      router.send_multipart([REPLY_TYPE_TASK, json.dumps(task.dict()).encode()])
    elif req_type == REQUEST_TYPE_RESULT:
      task = Task(**json.loads(req_payload))
      logging.info(f"Received result for task {task.task_id} from {worker_id.decode()}")
      result_queue.put(task)
      worker_manager.unregister_task(worker_id, task)
      router.send_multipart([REPLY_TYPE_ACK, b""])
    elif req_type == REQUEST_TYPE_HEARTBEAT:
      router.send_multipart([REPLY_TYPE_ACK, b""])
    else:
      logging.error(f"Received unrecognized request type. request: {request}")
      router.send_multipart([REPLY_TYPE_NACK, ""])
    
    worker_manager.register_heartbeat(worker_id)
    worker_manager.check_worker_status()
