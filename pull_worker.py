import itertools
import logging
import zmq
import config
import argparse
from util import new_task_handler
from local_worker import local_worker
from model import Task
import json
import util
import time

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.DEBUG)

REQUEST_TIMEOUT = 2500 # milliseconds
RETRY_INTERVAL = 1 # seconds

def main(num_processes: str, dispatcher_url: int):
  _, task_queue, result_queue = new_task_handler(local_worker, num_processes=num_processes)

  logging.info("Connecting to task dispatcher...")
  context = zmq.Context()
  client = context.socket(zmq.REQ)
  client.connect(dispatcher_url)

  task_count = 0
  last_none_reply_time = 0

  for sequence in itertools.count():
    if not result_queue.empty():
      result = result_queue.get()
      task_count -= 1
      request_type = b"RESULT"
      request_payload = json.dumps(result.dict()).encode()
    elif last_none_reply_time + RETRY_INTERVAL > time.time():
      continue
    elif task_count < num_processes:
      request_type = b"READY"
      request_payload = b"READY"

    request = [str(sequence).encode(), request_type, request_payload]
    client.send_multipart(request)

    # Retry loop
    while True:
      if (client.poll(REQUEST_TIMEOUT) & zmq.POLLIN) == 0:
        logging.warning("No reply from server")
        # Socket is confused. Close and remove it.
        client.setsockopt(zmq.LINGER, 0)
        client.close()

        logging.info("Reconnecting to server…")
        # Create new connection
        client = context.socket(zmq.REQ)
        client.connect(dispatcher_url)
        logging.info(f"Resending ({request})")
        client.send_multipart(request)
        continue
        
      reply = client.recv_multipart()
      reply_sequence, reply_type, reply_payload = reply

      if int(reply_sequence) != sequence:
        logging.error(f"Mismatched sequence in request vs reply. req_seq: {reply_sequence}, rep_seq: {reply_sequence}")
        continue


      logging.info(f"Server replied OK ({reply})")

      if reply_type == util.REPLY_TYPE_TASK:
        task = Task(**json.loads(reply_payload))
        task_count += 1
        task_queue.put(task)
      elif reply_type == util.REPLY_TYPE_NONE:
        last_none_reply_time = time.time()
        logging.info(f"No tasks available, retrying in ({RETRY_INTERVAL})")
      elif reply_type == util.REPLY_TYPE_ACK:
        logging.info("Server received results")
      else:
        logging.error("Server did not recognize request type")
      
      break


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  
  parser.add_argument('-n', '--num_processes', type=int, default=4)
  parser.add_argument('-u', '--dispatcher_url', type=str, default=config.task_dispatcher_url)

  args = parser.parse_args()
  main(args.num_processes, args.dispatcher_url)
