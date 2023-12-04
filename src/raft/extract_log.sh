#!/bin/bash

# Set the directory where log files are located
log_dir="test_logs"

# List filenames of log files containing the term "ReElection"
found_logs=$(find "$log_dir" -type f -name 'test*.log' -exec grep -l 'ReElection' {} +)

if [ -n "$found_logs" ]; then
    echo "Log files with 'ReElection' found:"
    echo "$found_logs"
else
    echo "No log files found with 'ReElection' in $log_dir."
fi