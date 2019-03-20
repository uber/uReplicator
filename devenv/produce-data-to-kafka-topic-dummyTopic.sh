#!/bin/bash -e
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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=$(dirname $DIR)
ZOOKEEPER=localhost:2181/cluster1
KAFKA_BROKER=localhost:9093

# overwritten options
while getopts "z:b:" option
do
  case ${option} in
    z) ZOOKEEPER="${OPTARG}";;
    b) KAFKA_BROKER="${OPTARG}";;
  esac
done
echo "Using ${ZOOKEEPER} as the zookeeper. You can overwrite it with '-z yourlocation'"
echo "Using ${KAFKA_BROKER} as the kafka broker. You can overwrite it with '-b yourlocation'"

# check if the topic exists. if not, create the topic
EXIST=$($KAFKA_HOME/bin/kafka-topics.sh --describe --topic dummyTopic --zookeeper $ZOOKEEPER)
if [ -z "$EXIST" ]
  then
    $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER --topic dummyTopic --partition 4 --replication-factor 1
fi

# produce raw data
while sleep 1
do
  $KAFKA_HOME/bin/kafka-console-producer.sh < /usr/kafka/dummyTopicData.log --topic dummyTopic --broker $KAFKA_BROKER
done
