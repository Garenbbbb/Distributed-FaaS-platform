from multiprocessing import Queue
import zmq
import json
import time
import copy
import redis
from threading import Thread
from collections import defaultdict
from .model import TaskInfo, Task
from .config import redis_url, redis_port, redis_password, redis_db, task_dispatcher_url

class WorkerManger():
  def __init__(self, heartbeat_threshold, task_queue):
    self.workers = {}
    self.workerLoad = {}
    self.track = defaultdict(set)
    self.heartbeat_threshold = heartbeat_threshold
    self.queue = Queue()
    self.task_q = task_queue
    self.queue_tasks_thread = Thread(target=self.queue_tasks, daemon=True)
    self.queue_tasks_thread.start()
    self.redis_conn = redis.StrictRedis(host=redis_url, port=redis_port, password=redis_password, db=redis_db)
    # one thread to check_worker_status
  
  def track_Ids(self, worker, id):
    self.track[worker].add(id)
  
  def queue_tasks(self):
    while True:
      id = self.queue.get()
      task_info_json = self.redis_conn.get(id)
      task_info = TaskInfo(**json.loads(task_info_json))
      task = Task(task_id=id, task_info=task_info)
      self.task_q.put(task)

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
    tmp = copy.deepcopy(self.workers)
    for worker_id, last_heartbeat_time in tmp.items():
      time_since_last_heartbeat = current_time - last_heartbeat_time
      if time_since_last_heartbeat > self.heartbeat_threshold:   
        del self.workers[worker_id]   
        del self.workerLoad[worker_id]
        for k in self.track[worker_id]:
          if len(k) > 0:
            self.queue.put(k)
        del self.track[worker_id]
        
def push_worker_router(task_queue, result_queue):
  context = zmq.Context()
  router = context.socket(zmq.ROUTER)
  # Error on dropped messages. When no client can be found, socket will signal an EHOSTUNREACH error
  # router.setsockopt(zmq.ROUTER_MANDATORY)
  router.identity = b"Router"
  router.bind(task_dispatcher_url)
  
  available_workers = WorkerManger(2, task_queue)  
  while True:
    try:
      message = router.recv_multipart(flags=zmq.NOBLOCK)
      print(message)
      print(available_workers.workers.keys())
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
        available_workers.track_Ids(worker, task.task_id)
        router.send_multipart([worker, b"", json.dumps(task.dict()).encode()])
      else: 
        # print("Workers are unavaliable.")
        pass
    except Exception:
      # print("Queue is empty. Waiting for tasks...")
      pass
    available_workers.check_worker_status()
