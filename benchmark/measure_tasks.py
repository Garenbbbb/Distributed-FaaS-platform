from util.client import register_fn, execute_fn, get_status
import time
from threading import Thread
import statistics

base_url = "http://127.0.0.1:8000/"

def measure(fn_id, timings):
  start = time.time()

  task_id, err_msg = execute_fn(fn_id,(((), {})))
  if err_msg:
    print("err_msg:", err_msg)
  assert not err_msg

  while True:
    status, err_msg = get_status(task_id)
    if err_msg:
      print("err_msg:", err_msg)
    assert not err_msg
    assert status != "FAILED"

    if status == "COMPLETED":
      timings[task_id] = time.time() - start
      return
    
    time.sleep(0.2)


def run_trial(fn, tasks: int):
  fn_id, err_msg = register_fn(fn)
  assert not err_msg

  timings = {}
  threads = []
  for _ in range(tasks):
    t = Thread(target=measure, args=(fn_id, timings))
    threads.append(t)
  for i, t in enumerate(threads):
    t.start()
  for t in threads:
    t.join()

  mean = statistics.mean(timings.values())
  return mean
