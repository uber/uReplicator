#!/bin/bash -e
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
EXIST=$($BASE_DIR/deploy/kafka/bin/kafka-topics.sh --describe --topic dummyTopic --zookeeper $ZOOKEEPER)
if [ -z "$EXIST" ]
  then
    $BASE_DIR/deploy/kafka/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER --topic dummyTopic --partition 1 --replication-factor 1
fi

# produce raw data
while sleep 1
do
  $BASE_DIR/deploy/kafka/bin/kafka-console-producer.sh < $BASE_DIR/bin/dummyTopicData.log --topic dummyTopic --broker $KAFKA_BROKER
done
