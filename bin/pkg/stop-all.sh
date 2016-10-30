#!/bin/bash -e

echo "EXECUTING: stop controller"
PID=`pgrep -f "Dapp_name=uReplicator-Controller"`
kill -9 $PID

echo "EXECUTING: stop worker"
PID=`pgrep -f "Dapp_name=uReplicator-Worker"`
kill -9 $PID

echo "EXECUTING: stop producing"
PID=`pgrep -f "./bin/produce-data-to-kafka-topic-dummyTopic.sh"`
kill -9 $PID

echo "EXECUTING: stop consuming"
PID=`pgrep -f "kafka.tools.ConsoleConsumer"`
for i in "${PID[@]}"
do
    kill -9 $i
done

bin/grid stop all