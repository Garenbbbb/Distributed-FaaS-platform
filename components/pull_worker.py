import logging
import zmq
import lib.config as config
import argparse
from lib.local_worker import local_worker
from lib.model import Task
from lib.tasks import *
import json
import time
from queue import Empty
import sys
import uuid

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.DEBUG)


# Units in seconds
REPLY_TIMEOUT = 1
REQUEST_THROTTLE = 0.1
HEARTBEAT_INTERVAL = 1


class PushWorker:
  def __init__(self, id, num_processes, dispatcher_url):
    self.id = id
    self.num_processes = num_processes
    self.dispatcher_url = dispatcher_url
    self.task_count = 0
    self.last_request_time = 0

    _, task_queue, result_queue = new_task_handler(local_worker, num_processes=num_processes)
    self.task_queue = task_queue
    self.result_queue = result_queue

    logging.info("Connecting to task dispatcher...")
    self.context = zmq.Context()
    self.client = self.context.socket(zmq.REQ)
    self.client.connect(dispatcher_url)

  def create_request(self):
    if self.task_count < self.num_processes and \
      self.last_request_time + REQUEST_THROTTLE < time.time():
      return [self.id, REQUEST_TYPE_READY, b""]
    
    if self.last_request_time + HEARTBEAT_INTERVAL < time.time():
      return [self.id, REQUEST_TYPE_HEARTBEAT, b""]
    
    try:
      result = self.result_queue.get(block=True, timeout=REQUEST_THROTTLE)
    except Empty as e:
      return None
      
    self.task_count -= 1
    return [self.id, REQUEST_TYPE_RESULT, json.dumps(result.dict()).encode()]
  
  def send_request(self, request):
    # Retry until successful, no max retries
    logging.info(f"Sending ({request})")

    while True:
      self.last_request_time = time.time()
      self.client.send_multipart(request)
      if (self.client.poll(REPLY_TIMEOUT*1000) & zmq.POLLIN) != 0:
        break
      logging.warning("No reply from server")
      # Close connection to allow us to send a new request
      self.client.setsockopt(zmq.LINGER, 0)
      self.client.close()

      logging.info("Reconnecting to server…")
      # Create new connection
      self.client = self.context.socket(zmq.REQ)
      self.client.connect(self.dispatcher_url)
      logging.info(f"Resending ({request})")

    return self.client.recv_multipart()
  
  def handle_reply(self, reply):
    reply_type, reply_payload = reply

    if reply_type == REPLY_TYPE_TASK:
      task = Task(**json.loads(reply_payload))
      self.task_count += 1
      self.task_queue.put(task)
    elif reply_type == REPLY_TYPE_NONE:
      logging.info("No tasks available")
    elif reply_type == REPLY_TYPE_ACK:
      logging.info("Server acknowledgement received")
    else:
      logging.error("Server did not recognize request type")


def main(num_processes: str, dispatcher_url: int, name: str):
  push_worker = PushWorker(b"WORKER" + name.encode(), num_processes, dispatcher_url)

  while True:
    request = push_worker.create_request()
    if request:
      reply = push_worker.send_request(request)
      logging.info(f"Server replied ({reply})")
      push_worker.handle_reply(reply)


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  
  num_processes = int(sys.argv[1])
  if len(sys.argv) > 2:
    dispatcher_url = sys.argv[2]
  else:
    dispatcher_url = config.task_dispatcher_url
  name = str(uuid.uuid4())

  # parser.add_argument('-n', '--num_processes', type=int, default=2)
  # parser.add_argument('-u', '--dispatcher_url', type=str, default=config.task_dispatcher_url)
  # parser.add_argument('-k', '--name', type=str, default="0")
  # args = parser.parse_args()

  main(num_processes, dispatcher_url, name)
