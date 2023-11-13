import dill
import codecs
import requests

base_url = "http://127.0.0.1:8000/"


def function(x):
  return x * 2


def serialize(obj) -> str:
  return codecs.encode(dill.dumps(obj), "base64").decode()
def deserialize(obj: str):
  return dill.loads(codecs.decode(obj.encode(), "base64"))

def main():

  resp = requests.post(base_url + "register_function",
                      json={"name": "test",
                      "payload": serialize(function)})
  print(resp.json()['function_id'])


  fn_info = resp.json()
  resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_info['function_id'],
                               "payload": serialize(((2,), {}))})
  print(resp)

  task_id = resp.json()["task_id"]
  print(task_id)

  resp = requests.get(f"{base_url}status/{task_id}")
  print(resp.json())

  resp = requests.get(f"{base_url}result/{task_id}")
  print(resp.json())

  
  


if __name__ == "__main__":
  main()

