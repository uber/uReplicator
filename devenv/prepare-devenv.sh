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

set -x


TIMEOUT=15
QUIET=0

wait_for_zookeeper() {
  echo "wait for zookeeper to startup at $HOST" >&2
  for i in `seq $TIMEOUT` ; do
    nc -z "$HOST" "2181" > /dev/null 2>&1

    result=$?
    if [ $result -eq 0 ] ; then
      if [ $# -gt 0 ] ; then
        echo "Zookeeper started"
      fi
      /usr/share/zookeeper/bin/zkCli.sh create /ureplicator-example ureplicator-example  > /dev/null 2>&1
      return
    fi
    sleep 1
  done
  echo "wait for zookeeper startup timed out" >&2
}

wait_for_cluster_2() {
  echo "wait for kafka cluster2 to startup at $HOST" >&2
  for i in `seq $TIMEOUT` ; do
    nc -z "$HOST" "9094" > /dev/null 2>&1

    result=$?
    if [ $result -eq 0 ] ; then
      if [ $# -gt 0 ] ; then
        echo "Kafka cluster2 started"
      fi
      echo "creating topic dummyTopic in kafka cluster 2" >&2
      $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181/cluster2 --topic dummyTopic --partition 4 --replication-factor 1  > /dev/null 2>&1
      return
    fi
    sleep 1
  done
  echo "wait for kafka cluster2 startup time out" >&2
}

wait_for_cluster_1() {
  echo "wait for kafka cluster1 to startup at $HOST" >&2
   for i in `seq $TIMEOUT` ; do
    nc -z "$HOST" "9093" > /dev/null 2>&1

    result=$?
    if [ $result -eq 0 ] ; then
      if [ $# -gt 0 ] ; then
        echo "Kafka cluster1 started"
      fi
      $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181/cluster1 --topic dummyTopic --partition 4 --replication-factor 1  > /dev/null 2>&1
      ./produce-data-to-kafka-topic-dummyTopic.sh  > /dev/null 2>&1 &
      return
    fi
    sleep 1
  done

  echo "wait for kafka cluster1 startup time out" >&2
}
HOST="$1"
TIMEOUT="$2"

echo "start to prepare devenv"
wait_for_zookeeper
wait_for_cluster_1
wait_for_cluster_2
exit 0
