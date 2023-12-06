from util.client import register_fn, execute_fn, get_status, get_result
import time

def mult(delay: int, x: int, y: int):
  import time
  time.sleep(delay)
  return x*y

def concat(delay: int, a: int, *, b: str, c: str):
  import time
  time.sleep(delay)
  return a + b + c

def matmul(a: int):
  import numpy
  dims = (8,8)
  return numpy.matmul(numpy.full(dims, a),numpy.full(dims, a+1))

def execute(fn, get_args, get_kwargs, count):
  test_cases = {}

  fn_id, err_msg = register_fn(fn)
  assert not err_msg
  for i in range(count):
    args = get_args(i)
    kwargs = get_kwargs(i)
    task_id, err_msg = execute_fn(fn_id,((args, kwargs)))
    assert not err_msg
    test_cases[task_id] = fn(*args, **kwargs)
  
  return test_cases

def verify_results(test_cases, comparison_fn=None):
  while len(test_cases) > 0:
    task_ids = list(test_cases.keys())
    for task_id in task_ids:
      status, err_msg = get_status(task_id)
      if err_msg:
        print(err_msg)
      assert not err_msg
      if status == "FAILED":
        result, err_msg = get_result(task_id)
        print(err_msg)
        print(result)
      assert status != "FAILED"

      if status == "COMPLETED":
        result, err_msg = get_result(task_id)
        assert not err_msg
        if comparison_fn:
          assert comparison_fn(result, test_cases[task_id]) 
        else:
          assert result == test_cases[task_id]
        del test_cases[task_id]
        return
      
      time.sleep(0.1)