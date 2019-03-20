#!/bin/sh
#
# Copyright (C) 2015-2019 Uber Technologies, Inc. (streaming-data@uber.com)
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

TIMEOUT=15
QUIET=0

wait_for() {
  for i in `seq $TIMEOUT` ; do
    nc -z "$HOST" "2181" > /dev/null 2>&1

    result=$?
    if [ $result -eq 0 ] ; then
      if [ $# -gt 0 ] ; then
        echo "Host started"
      fi
      exec /usr/share/zookeeper/bin/zkCli.sh create /ureplicator-example ureplicator-example
      exit 0
    fi
    sleep 1
  done
  echo "Operation timed out" >&2
  exit 0
}
HOST="$1"
TIMEOUT="$2"

wait_for

