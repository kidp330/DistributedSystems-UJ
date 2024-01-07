#!/bin/bash

# Set up a function to handle SIGPIPE
handle_pipe() {
  echo in fn is $child_pid
  kill $child_pid
  exit
}

# Ensure the function will be called on SIGPIPE
trap handle_pipe SIGPIPE

# Start your process in the background
./gen.sh 5 | ./main &
child_pid=$!
echo in outer scope is $child_pid

# Wait on your process to finish
wait $child_pid