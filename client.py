import requests
from util import serialize, deserialize
import time

base_url = "http://127.0.0.1:8000/"


def function(x):
  return x * 2

def main():

  resp = requests.post(base_url + "register_function",
                      json={"name": "test",
                      "payload": serialize(function)})
  print(resp.json()['function_id'])


  fn_info = resp.json()
  resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_info['function_id'],
                               "payload": serialize((2,))})
  print(resp)

  task_id = resp.json()["task_id"]
  print(task_id)

  resp = requests.get(f"{base_url}status/{task_id}")
  print(resp.json())

  time.sleep(.5)

  resp = requests.get(f"{base_url}result/{task_id}")
  obj = resp.json()
  print(obj)
  print("deserialized result: ", deserialize(obj["result"]))


if __name__ == "__main__":
  main()

