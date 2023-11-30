import threading
import multiprocessing
import time

thread_count = 4
iteration_count = 10**9

def spin(n: int):
  for _ in range(n):
    pass

def benchmark(par_refs, seq_duration):
  start = time.time()
  for p in par_refs:
    p.start()
  for p in par_refs:
    p.join()
  par_duration = time.time() - start
  print(f"Parallel Duration (t_p, n={len(par_refs)}): {par_duration}")
  print(f"Speedup (t_s*n/t_p): {seq_duration*len(par_refs)/par_duration}")

# Sequential Timing
start = time.time()
spin(iteration_count)
t_s = time.time() - start
print(f"Sequential Duration (t_s): {t_s}")

threads = []
for i in range(thread_count):
  t = threading.Thread(target=spin, args=(iteration_count,))
  threads.append(t)

print("\n---> Benchmark Threads <---")
benchmark(threads, t_s)

processes = []
for i in range(thread_count):
  p = multiprocessing.Process(target=spin, args=(iteration_count,))
  processes.append(p)

print(f"\n---> Benchmark Processes <---")
benchmark(processes, t_s)