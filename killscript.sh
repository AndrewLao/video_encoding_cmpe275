#!/bin/bash

# Find PIDs of processes running node.py and kill them gracefully
echo "Finding and gracefully killing node.py processes..."
pids=$(ps aux | grep node.py | grep -v grep | awk '{print $2}')

if [ -z "$pids" ]; then
  echo "No node.py processes found."
else
  echo "Found PIDs: $pids"
  kill $pids
  echo "Sent graceful termination signal to PIDs: $pids"

  # Give processes a moment to shut down
  sleep 5

  # Check if any processes are still running and force kill if necessary
  remaining_pids=$(ps aux | grep node.py | grep -v grep | awk '{print $2}')
  if [ -n "$remaining_pids" ]; then
    echo "Some processes are still running. Force killing PIDs: $remaining_pids"
    kill -9 $remaining_pids
    echo "Sent forceful termination signal to PIDs: $remaining_pids"
  else
    echo "All node.py processes terminated gracefully."
  fi
fi

# Optional: Clean up temporary directories created by the nodes
echo "Cleaning up temporary directories..."
find /tmp -maxdepth 1 -type d -name "concat_*" -exec rm -rf {} \;
echo "Temporary 'concat_*' directories cleaned up."
rm -rf master_data/
rm -rf *_shards/

# Optional: Clean up video_shards and master_data directories
# Use with caution! This will delete all processed shards and original video files.
# Uncomment the lines below if you want to clear these directories automatically.
# echo "Cleaning up video_shards and master_data directories..."
# rm -rf ./video_shards/*
# rm -rf ./master_data/*
# echo "video_shards and master_data directories cleaned up."

