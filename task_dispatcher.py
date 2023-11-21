import redis
import json
import argparse
from multiprocessing import Queue
from threading import Thread
from model import Task, TaskInfo
from worker_pool import new_worker_pool
from config import redis_url, redis_port, redis_password, redis_db, redis_topic, redis_fail

redis_conn = redis.StrictRedis(host=redis_url, port=redis_port, password=redis_password, db=redis_db)

def main():
  args = parse_args()

  if args.mode != "local" and args.mode != "push":
    print(f"TODO: Implement {args.mode} mode")
    exit(1)

  t, task_queue, result_queue = new_worker_pool(args.workers, args.mode)

  #get failed tasks from redis
  failedTask = recover_tasks()
  for task in failedTask:
    task_queue.put(task)
 
  queue_tasks_thread = Thread(target=queue_tasks, args=(task_queue,), daemon=True)
  queue_tasks_thread.start()
  dequeue_results_thread = Thread(target=dequeue_results, args=(result_queue,), daemon=True)
  dequeue_results_thread.start()

  t.join()

def recover_tasks():
  taskList = []
  set_elements = redis_conn.smembers(redis_fail)
  for element in set_elements:
    task_info_json = redis_conn.get(element)
    task_info = TaskInfo(**json.loads(task_info_json))
    taskList.append(Task(task_id=element, task_info=task_info))
  return taskList
    
  
def queue_tasks(task_queue):
  pubsub = redis_conn.pubsub()
  pubsub.subscribe(redis_topic)
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
    redis_conn.srem(redis_fail, task.task_id)


def parse_args():
  parser = argparse.ArgumentParser()
  
  parser.add_argument('-m', '--mode', type=str, default="push")
  parser.add_argument('-p', '--port', type=int, default=20000)
  parser.add_argument('-w', '--workers', type=int, default=2)

  args = parser.parse_args()

  print(args)
  return args


if __name__ == '__main__':
  main()