from random import randint
import itertools
import logging
import time
import zmq
import json
from model import Task
import util

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.DEBUG)

def pull_worker_router(task_queue, result_queue):
  context = zmq.Context()
  router = context.socket(zmq.REP)
  router.bind("tcp://*:5555")

  for iter_count in itertools.count():
    request = router.recv_multipart()
    req_sequence, req_type, req_payload = request

    if req_type == b"READY":
      if task_queue.empty():
        logging.info("Worker ready but no tasks available. Ignoring request since it will be resent.")
        router.send_multipart([req_sequence, util.REPLY_TYPE_NONE, b""])
        continue

      task = task_queue.get()
      router.send_multipart([req_sequence, util.REPLY_TYPE_TASK, json.dumps(task.dict()).encode()])
    elif req_type == b"RESULT":
      result_queue.put(Task(**json.loads(req_payload)))
      router.send_multipart([req_sequence, util.REPLY_TYPE_ACK, b""])
    else:
      logging.error(f"Received unrecognized request type. request: {request}, cycle: {iter_count}")
      router.send_multipart([req_sequence, util.REPLY_TYPE_NACK, ""])
