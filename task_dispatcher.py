import redis
import json
import argparse
from multiprocessing import Queue
from threading import Thread
from model import Task, TaskInfo
from worker_pool import WorkerPool

redis_conn = redis.StrictRedis(host='localhost', port=6379, password='garen', db=0)

def main():
  args = parse_args()

  if args.mode != "local":
    print(f"TODO: Implement {args.mode} mode")
    exit(1)

  task_queue = Queue()
  queue_tasks_thread = Thread(target=queue_tasks, args=(task_queue,), daemon=True)
  queue_tasks_thread.start()

  result_queue = Queue()
  dequeue_results_thread = Thread(target=dequeue_results, args=(result_queue,), daemon=True)
  dequeue_results_thread.start()
  
  w = WorkerPool(args.workers, task_queue, result_queue)
  w.spawn()
  w.wait()
  

def queue_tasks(task_queue):
  pubsub = redis_conn.pubsub()
  pubsub.subscribe('Tasks')
  for message in pubsub.listen():
    task = get_task(message)
    if task != None:
      task_queue.put(task)


def get_task(message: "dict[str, any]") -> "Task | None":
  if message['type'] != 'message':
    return None
  task_id = message['data']
  try:
    task_info_json = redis_conn.get(task_id)
    task_info = TaskInfo(**json.loads(task_info_json))
    return Task(task_id=task_id, task_info=task_info)
  except Exception as e:
    print("failed to get the task: ", str(e))
    return None


def dequeue_results(result_queue: Queue):
  while True:
    task = result_queue.get()
    redis_conn.set(task.task_id, task.task_info.model_dump_json())


def parse_args():
  parser = argparse.ArgumentParser()
  
  parser.add_argument('-m', '--mode', type=str, default="local")
  parser.add_argument('-p', '--port', type=int, default=20000)
  parser.add_argument('-w', '--workers', type=int, default=1)

  args = parser.parse_args()

  print(args)
  return args


if __name__ == '__main__':
  main()