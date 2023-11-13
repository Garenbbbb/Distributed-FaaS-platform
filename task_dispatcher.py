import redis

redis_conn = redis.StrictRedis(host='localhost', port=6379, db=0)

class Task():
  fn_payload : str
  param_payload: str 
  status: str
  result: str


def main():
  pubsub = redis_conn.pubsub()
  pubsub.subscribe('Tasks')

  for message in pubsub.listen():
    if message['type'] == 'message':
      task_id = message['data']
      print(f"Received task ID: {task_id}")
      try:
        task = redis_conn.get(task_id)
      except Exception as e:
        print("fail to get the task")

             
