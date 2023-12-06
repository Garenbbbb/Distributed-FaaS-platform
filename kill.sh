#!/bin/bash

KILL_DISPATCHER_DELAY=10
KILL_ALL_WORKERS_DELAY=20
DISPATCHER_KILLED=false
WORKERS_KILLED=false

MODE_ARG=$1
echo $MODE_ARG
WORKERS_ARG=$2
echo $WORKERS_ARG
start=$(date +%s)
while true; do
  if [[ $(($(date +%s) - $start)) -gt $KILL_DISPATCHER_DELAY && $DISPATCHER_KILLED == false ]]; then
    DISPATCHER_KILLED=true
    echo "Killing dispatcher"
    pkill -f "task_dispatcher"
    sleep 2
    echo "Starting task dispatcher"
    python3 components/task_dispatcher.py -m $MODE_ARG -w $WORKERS_ARG > "/var/log/faast/task_dispatcher.out" 2>&1 &
  fi

  if [[ $(($(date +%s) - $start)) -gt $KILL_ALL_WORKERS_DELAY && $WORKERS_KILLED == false ]]; then
    WORKERS_KILLED=true
    echo "Killing all workers"
    pkill -f "$MODE_ARG"_worker
    sleep 2
    echo "Starting workers"
    for ((i = 0; i < $((WORKERS_ARG)); i++)); do
      echo "Starting worker $i"
      python3 components/"$MODE_ARG"_worker.py 2 > "/var/log/faast/$MODE_ARG$i" 2>&1 &
    done
  fi

  while true; do
    pids=($(pgrep -f "$MODE_ARG"_worker))
    num_pids=${#pids[@]}
    if (( num_pids < WORKERS_ARG * 3 )); then
      break
    fi
    pid=${pids[$((RANDOM % num_pids))]}

    echo "Killing pid $pid"
    kill $pid
    SLEEP_PERIOD=$((RANDOM % 4 + 1))
    echo "Sleeping for $SLEEP_PERIOD"
    sleep $SLEEP_PERIOD
  done
  
  echo "Spawning new worker"
  python3 components/"$MODE_ARG"_worker.py 2 > /dev/null 2>&1 &
done