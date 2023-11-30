import requests
from requests.exceptions import HTTPError
from .serialize import serialize, deserialize
import time

def err_result(base_str: str, e: Exception) -> (None, str):
  return None, f"{base_str}. {e.__class__.__name__}: {str(e)}"

def assert_status(resp: requests.Response, code: str):
  resp.raise_for_status()
  if str(resp.status_code) != code:
    raise HTTPError(f"HTTP status code {code} required, got {resp.status_code}")

base_url = "http://127.0.0.1:8000/"

def register_fn(fn) -> ("str | None", str):
  try:
    resp = requests.post(base_url + "register_function",
                        json={"name": fn.__name__,
                        "payload": serialize(fn)})
    assert_status(resp, "201")
    return resp.json()["function_id"], ""
  except Exception as e:
    return err_result(f"Failed to register function named {fn.__name__}", e)

def execute_fn(fn_id, args) -> ("str | None", str):
  try:
    resp = requests.post(base_url + "execute_function",
                        json={"function_id": fn_id,
                              "payload": serialize(args)})
    assert_status(resp, "201")
    return resp.json()["task_id"], ""
  except Exception as e:
    return err_result(f"Failed to execute function with id {fn_id}", e)

def get_status(task_id: str) -> ("str | None", str):
  try:
    resp = requests.get(f"{base_url}status/{task_id}")
    assert_status(resp, "200")
    return resp.json()["status"], ""
  except Exception as e:
    return err_result(f"Failed to get status for task with id {task_id}", e)
  
def get_result(task_id: str) -> ("dict[str, any] | None", str):
  try:
    resp = requests.get(f"{base_url}result/{task_id}")
    assert_status(resp, "200")
    return deserialize(resp.json()["result"]), ""
  except Exception as e:
    return err_result(f"Failed to get result for task with id {task_id}", e)
  
def await_results(task_ids: "list[str]", poll_interval: float) -> (list, int):
  start = time.time()
  results = []
  while len(task_ids) > 0:
    task_id = task_ids[-1]

    status, err_msg = get_status(task_id)
    if err_msg:
      print(err_msg)
      time.sleep(poll_interval)
      continue

    if status == "QUEUED":
      time.sleep(poll_interval)
      continue

    result, err_msg = get_status(task_id)
    if err_msg:
      print(err_msg)
      time.sleep(poll_interval)
      continue

    results.append((task_id, result))
    task_ids.pop()

  return results, time.time() - start

def print_results(results: "list[(str, any)]"):
  for result in results:
    task_id, result_val = result
    print(f"{task_id}: {result_val}")
