from threading import Thread
from multiprocessing import Queue

REQUEST_TYPE_READY = b"REQUEST_TYPE_READY"
REQUEST_TYPE_RESULT = b"REQUEST_TYPE_RESULT"
REQUEST_TYPE_HEARTBEAT = b"REQUEST_TYPE_HEARTBEAT"

REPLY_TYPE_TASK = b"REPLY_TYPE_TASK"
REPLY_TYPE_NONE = b"REPLY_TYPE_NONE"
REPLY_TYPE_ACK = b"REPLY_TYPE_ACK"
REPLY_TYPE_NACK = b"REPLY_TYPE_NACK"

def new_task_handler(handler, **handler_kwargs) -> (Thread, Queue, Queue):
  task_queue = Queue()
  result_queue = Queue()
  t = Thread(
    target=handler, 
    args=(task_queue, result_queue),
    kwargs=handler_kwargs,
    daemon=True)
  t.start()
  return t, task_queue, result_queue