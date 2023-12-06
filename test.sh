#!/bin/bash

MODES=("pull" "push")
WORKERS=4

cleanup() {
  pkill -f kill
}

trap cleanup EXIT

# Run fault tolerance tests on each mode
for MODE in "${MODES[@]}"; do
  ./start.sh -m $MODE -w $WORKERS

  echo "Launching killer with settings $MODE $WORKERS"
  ./kill.sh $MODE $WORKERS > ./kill.log 2>&1 &
  pytest tests
  pkill -f kill
done
