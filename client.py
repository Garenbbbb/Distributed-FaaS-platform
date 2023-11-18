import requests
from util import serialize, deserialize
import time
import multiprocessing
from threading import Thread

base_url = "http://127.0.0.1:8000/"


def function(x):
  return x * 2

def sleep_fn(s: int, val: int):
  i = 0
  for _ in range(s*5000000):
    i += 1
  return val

def main():
  fn_id = register_fn(function)
  print(fn_id)
  task_id = execute_fn(fn_id, (2,))
  print(task_id)
  status = get_status(task_id)
  print(status)
  results, _ = await_results([task_id], 0.1)
  print_results(results)

  parallel_feed_tasks(fn_id, (2,), 20)
  # parallel_test(sleep_fn, (10,5), 10)


def parallel_execute(fn_id, args, shared_counter):
  task_id = execute_fn(fn_id, args)
  while True:
    resp = requests.get(f"{base_url}result/{task_id}")
    obj = resp.json()
    if obj["status"] == "COMPLETE" or obj["status"] == "FAILED" :
      with shared_counter.get_lock():
        shared_counter.value += 1
        return
  
# Test to simulate nums of tasks coming in at the same time
def parallel_feed_tasks(fn_id, args, tasks):

  task_id = execute_fn(fn_id, args)
  print("\n\n---> Single Case <---")
  results, duration = await_results([task_id], 0.1)
  print("duration (d_s):", duration)
  print("\n\n---> Parallel Case <---")
  print("function call count (c):", tasks)
  cur = time.time()
  shared_counter = multiprocessing.Value("i", 0)

  for _ in range(tasks):
    thread = Thread(target=parallel_execute, args=(fn_id, args, shared_counter), daemon=True)
    thread.start()
  
  while shared_counter.value != tasks:
    pass

  end = time.time()
  par_duration = end-cur
  print("duration (d_p):", par_duration)
  print("speedup (d_s*c/d_p):", duration*tasks / par_duration)
  print("\n\n")


def parallel_test(fn, args, count: int):
  fn_id = register_fn(fn)
  task_id = execute_fn(fn_id, args)
  print("\n\n---> Single Case <---")
  results, duration = await_results([task_id], 0.1)
  print("duration (d_s):", duration)
  # print_results(results)

  task_ids = []
  for _ in range(count):
    task_id = execute_fn(fn_id, args)
    task_ids.append(task_id)

  print("\n\n---> Parallel Case <---")
  print("function call count (c):", count)
  par_results, par_duration = await_results(task_ids, 0.1)
  print("duration (d_p):", par_duration)
  print("speedup (d_s*c/d_p):", duration*count / par_duration)
  # print_results(par_results)
  print("\n\n")


def register_fn(fn) -> str:
  resp = requests.post(base_url + "register_function",
                      json={"name": "test",
                      "payload": serialize(fn)})
  return resp.json()['function_id']

def execute_fn(fn_id, args) -> str:
  resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_id,
                               "payload": serialize(args)})
  return resp.json()['task_id']

def get_status(task_id: str) -> str:
  resp = requests.get(f"{base_url}status/{task_id}")
  return resp.json()["status"]


def await_results(task_ids: "list[str]", poll_interval: float) -> ("list[dict[str, any]]", int):
  start = time.time()
  results = []
  while len(task_ids) > 0:
    task_id = task_ids[-1]
    resp = requests.get(f"{base_url}result/{task_id}")
    obj = resp.json()
    if obj["status"] != "QUEUED":
      results.append(obj)
      task_ids = task_ids[:-1]
    time.sleep(poll_interval)
  return results, time.time() - start


def print_results(results: "list[dict[str, any]]"):
  for result in results:
    print("task_id:", result["task_id"])
    print("\tstatus:", result["status"])
    print("\tresult:", deserialize(result["result"]))


if __name__ == "__main__":
  main()

