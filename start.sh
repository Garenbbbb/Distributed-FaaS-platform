#!/bin/bash

MODE="local"
WORKERS=4
LOG_DIR="/var/log/faast"

usage() {
  echo "Usage: $(basename "$0") [-m <local|push|pull>] [-w <int>]"
  echo "Options:"
  echo "  -m <local|push|pull>  load balancing strategy to be used by task dispatcher and workers"
  echo "  -w <int>              number of workers in the system"
  echo "  -d <string>           directory to use for system logs"
  exit 1
}

while getopts ":m:w:" opt; do
  case $opt in
    m)
      MODE=$OPTARG
      if ! [[ $MODE == "local" || $MODE == "push" || $MODE == "pull" ]]; then
        usage
      fi
      ;;
    w)
      WORKERS=$OPTARG
      if ! [[ $WORKERS =~ ^[0-9]+$ ]]; then
        usage
      fi
      ;;
    d)
      LOG_DIR=$OPTARG
      ;;
    *)
      usage
      ;;
  esac
done



./stop.sh



if [ ! -d "$LOG_DIR" ]; then
  echo "Creating log directory: $LOG_DIR"
  sudo mkdir -p "$LOG_DIR"
  sudo chown $(whoami) "$LOG_DIR"
fi



REDIS_LOG_FILE="$LOG_DIR/redis.out"

# Check if Redis is installed
if ! command -v redis-server &> /dev/null; then
  echo "Redis is not installed. Installing Redis..."
  sudo apt update
  sudo apt install redis-server -y
  echo "Redis installed successfully."
fi

# Check if Redis server is running
if ! pgrep redis-server &> /dev/null; then
  echo "Redis server is not running. Starting Redis..."
  sudo redis-server ./6379.conf > "$REDIS_LOG_FILE" 2>&1 &
  sleep 1
  echo "Redis server started."
else
  echo "Redis server is already running. Avoiding restart to preserve data."
fi
echo "Redis server logs are available at: $REDIS_LOG_FILE"



WEB_SERVICE_LOG_FILE="$LOG_DIR/web_service.out"

echo "Starting web service..."
(cd components && python3 -m uvicorn web_service:app --reload > "$WEB_SERVICE_LOG_FILE" 2>&1 &)
echo "Web service started."
echo "Logs available at: $WEB_SERVICE_LOG_FILE"



TASK_DISPATCHER_LOG_FILE="$LOG_DIR/task_dispatcher.out"

echo "Starting task dispatcher..."
python3 components/task_dispatcher.py -m $MODE -w $WORKERS > "$TASK_DISPATCHER_LOG_FILE" 2>&1 &
sleep 1
echo "Task dispatcher started."
echo "Logs available at: $TASK_DISPATCHER_LOG_FILE"



if [[ $MODE != "local" ]]; then
  echo "Starting $MODE workers..."
  for ((i = 0; i < $((WORKERS)); i++)); do
    WORKER_LOG_FILE="$LOG_DIR/$MODE$i"
    if [[ $MODE == "push" ]]; then
      python3 components/push_worker.py -n 2 -k $i > "$WORKER_LOG_FILE" 2>&1 &
    elif [[ $MODE == "pull" ]]; then
      python3 components/pull_worker.py -n 2 > "$WORKER_LOG_FILE" 2>&1 &
    fi
  done
  echo "Workers started"
fi

