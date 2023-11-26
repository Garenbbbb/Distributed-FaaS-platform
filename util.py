import dill
import codecs
from threading import Thread
from multiprocessing import Queue

REPLY_TYPE_TASK = b"REPLY_TYPE_TASK"
REPLY_TYPE_NONE = b"REPLY_TYPE_NONE"
REPLY_TYPE_ACK = b"REPLY_TYPE_ACK"
REPLY_TYPE_NACK = b"REPLY_TYPE_NACK"

def serialize(obj) -> str:
  return codecs.encode(dill.dumps(obj), "base64").decode()
def deserialize(obj: str):
  return dill.loads(codecs.decode(obj.encode(), "base64"))

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