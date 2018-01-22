#!/bin/bash -x
#
# Copyright (C) 2015-2017 Uber Technologies, Inc. (streaming-data@uber.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

export PING_SLEEP=30s
export WORKDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export TEST_OUTPUT=$WORKDIR/test.out

touch $TEST_OUTPUT

dump_output() {
   echo Tailing the last 20000 lines of output:
   tail -20000 $TEST_OUTPUT
}
error_handler() {
  echo ERROR: An error was encountered with the test.
  dump_output
  exit 1
}
# If an error occurs, run our error handler to output a tail of the test
trap 'error_handler' ERR

# Set up a repeating loop to send some output to Travis.

bash -c "while true; do echo \$(date) - testing ...; sleep $PING_SLEEP; done" &
PING_LOOP_PID=$!

mvn test -ff  >> $TEST_OUTPUT 2>&1

# The test finished without returning an error so dump a tail of the output
dump_output

# nicely terminate the ping output loop
kill $PING_LOOP_PID

