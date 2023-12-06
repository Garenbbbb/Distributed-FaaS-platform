import requests
from requests.exceptions import HTTPError
from .serialize import serialize, deserialize

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
  