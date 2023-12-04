#!/bin/bash

# Set the number of iterations
n=$1

# Create the "test" folder if it doesn't exist
test_dir="test_logs"
summary_file="summary.log"
mkdir -p $test_dir

# Remove all log files in the test directory
find "$test_dir" -name 'test*.log' -type f -delete
echo "" > $test_dir/$summary_file

# Array to store background process IDs
declare -a pids

run_test() {
    local i=$i
      # Execute the test command in the background
      go test -race -run 2A > $test_dir/test$i.log
      echo "start test" $i

      # Check if the test failed by searching for "FAIL" in the log
      if grep -q "FAIL" $test_dir/test$i.log; then
          echo "Test $i failed!"
          echo "FAIL: TEST" $i >> $test_dir/$summary_file
      else
          echo "Test $i passed!"
          echo "PASSED: TEST" $i >> $test_dir/$summary_file
      fi
}

# Loop through the iterations
for ((i=1; i<=$n; i++))
do
  run_test $i &
  # Store the PID of the background process
  pids+=($!)
done

# Wait for all background processes to finish
for pid in "${pids[@]}"; do
    wait "$pid"
done

cat $test_dir/$summary_file | grep FAIL

# Remove log files for tests that have passed
for ((i=1; i<=$n; i++))
do
  filename=$test_dir/test$i.log
    if ! grep -q "FAIL" $filename; then
        rm $filename
    fi
done

echo "All tests completed."
