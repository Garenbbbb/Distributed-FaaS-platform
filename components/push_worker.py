# python3 push_worker.py <num_worker_processors> <dispatcher url>
import argparse
import zmq
import json
from lib.model import Task
from lib.tasks import new_task_handler
from lib.local_worker import local_worker
import sys
import uuid

def main(num, url, name):

  t, task_queue, result_queue = new_task_handler(local_worker, num_processes=num)

  context = zmq.Context()
  router = context.socket(zmq.DEALER)
  router.identity = b"WORKER" + name.encode()
  # Should we maybe use this?
  # router.setsockopt(zmq.IDENTITY, b"WORKER" + name.encode())
  print(router.identity)
  router.connect(url)
  poller = zmq.Poller()
  poller.register(router, zmq.POLLIN)
  # executor = ThreadPoolExecutor(max_workers=num)
  taskCnt = 0.0
  while True:
    if poller.poll(1000): 
      response = router.recv_multipart()
      task = Task(**json.loads(response[1]))
      # Process pool
      task_queue.put(task)

      taskCnt += 1
      # Thread Pool
      # future = executor.submit(execute_task, task)
      # # Callback to send the result back to the dispatcher
      # future.add_done_callback(lambda f: router.send_multipart([router.identity, json.dumps(f.result().dict()).encode()]))
    else:
      #Process pool
      try:
        result = result_queue.get(block=False)
        taskCnt -= 1
        router.send_multipart([router.identity, json.dumps(result.dict()).encode()])
      except Exception:
        pass
    router.send_multipart([router.identity, b"REGISTER", str(taskCnt/num).encode()])
   

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  
  num_processes = int(sys.argv[1])
  if len(sys.argv) > 2:
    dispatcher_url = sys.argv[2]
  else:
    dispatcher_url = "tcp://127.0.0.1:5555"
  name = str(uuid.uuid4())

  # parser.add_argument('-n', '--number', type=int, default=2)
  # parser.add_argument('-u', '--url', type=str, default="tcp://127.0.0.1:5555")
  # parser.add_argument('-k', '--name', type=str, default="0")
  # args = parser.parse_args()

  main(num_processes, dispatcher_url, name)