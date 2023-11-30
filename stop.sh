#!/bin/bash

echo "System shutting down (besides redis)..."
pkill -f web_service
pkill -f task_dispatcher
pkill -f pull_worker
pkill -f push_worker
sleep 2
echo "Complete"