# python3 push_worker.py <num_worker_processors> <dispatcher url>
import argparse
import zmq
import json
from model import Task
from concurrent.futures import ThreadPoolExecutor
from worker_pool import execute_task, new_worker_pool

def main(num, url, name):

  t, task_queue, result_queue = new_worker_pool(num, "local")

  context = zmq.Context()
  router = context.socket(zmq.DEALER)
  router.identity = b"WORKER" + name.encode()
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
      taskCnt+=1
      # Thread Pool
      # future = executor.submit(execute_task, task)
      # # Callback to send the result back to the dispatcher
      # future.add_done_callback(lambda f: router.send_multipart([router.identity, json.dumps(f.result().dict()).encode()]))
    else:
      #Process pool
      try:
        result = result_queue.get(block=False)
        taskCnt-=1
        router.send_multipart([router.identity, json.dumps(result.dict()).encode()])
      except Exception:
        pass
    print(taskCnt/num)
    router.send_multipart([router.identity, b"REGISTER", str(taskCnt/num).encode()])
   

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  
  parser.add_argument('-n', '--number', type=int, default=2)
  parser.add_argument('-u', '--url', type=str, default="tcp://127.0.0.1:5555")
  parser.add_argument('-k', '--name', type=str, default="0")
  args = parser.parse_args()
  main(args.number, args.url, args.name)