import argparse
import subprocess
import measure_tasks

def no_op():
  a = 2*2
  pass

def numpy_matmul():
  import numpy
  for _ in range(10):
    numpy.matmul(numpy.random.random((2048,2048)),numpy.random.random((2048,2048)))

def matmul():
  import numpy
  A, B = numpy.random.random((128,128)), numpy.random.random((128,128))
  zip_b = list(zip(*B))
  for _ in range(10):
    [[sum(a*b for a, b in zip(row_a, col_b)) for col_b in zip_b] for row_a in A]

def spin():
  i = 0
  for _ in range(10**8):
    i += 1

def main(no_op_tasks_per_worker: int, load_tasks_per_worker: int):
  with open("results.txt", 'w', encoding="utf-8") as file:
    file.write(f"function mode workers tasks_per_worker avg_task_duration\n")

  for mode in ["local", "push", "pull"]:
    for worker_count in [1, 2, 4, 6, 8]:
      subprocess.run([f"(cd .. && ./start.sh -m {mode} -w {worker_count})"], shell=True)
      print("start ops")
      no_op_task_count, load_task_count = no_op_tasks_per_worker*worker_count, load_tasks_per_worker*worker_count
      no_op_timing = measure_tasks.run_trial(no_op, no_op_task_count)
      numpy_matmul_timing = measure_tasks.run_trial(numpy_matmul, load_task_count)
      matmul_timing = measure_tasks.run_trial(matmul, load_task_count)
      spin_timing = measure_tasks.run_trial(spin, load_task_count)
      print("end ops")

      with open("results.txt", 'a', encoding="utf-8") as file:
        file.writelines([
          f"no_op        {mode} {worker_count} {no_op_tasks_per_worker} {round(no_op_timing, 4)}\n",
          f"numpy_matmul {mode} {worker_count} {load_tasks_per_worker}  {round(numpy_matmul_timing, 4)}\n",
          f"matmul       {mode} {worker_count} {load_tasks_per_worker}  {round(matmul_timing, 4)}\n",
          f"spin         {mode} {worker_count} {load_tasks_per_worker}  {round(spin_timing, 4)}\n",
        ])




if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  
  parser.add_argument('-n', '--no_op_tasks_per_worker', type=int, default=1)
  parser.add_argument('-s', '--load_tasks_per_worker', type=int, default=5)
  args = parser.parse_args()
  main(args.no_op_tasks_per_worker, args.load_tasks_per_worker)
