#!/bin/bash -e

echo "EXECUTING: stop controller"
PID1=`pgrep -f "Dapp_name=uReplicator-Controller"` || true
if [ -n "$PID1" ]; then
    kill -9 ${PID1}
fi

echo "EXECUTING: stop worker"
PID2=`pgrep -f "Dapp_name=uReplicator-Worker"` || true
if [ -n "$PID2" ]; then
    kill -9 ${PID2}
fi

echo "EXECUTING: stop producing"
PID3=`pgrep -f "./bin/produce-data-to-kafka-topic-dummyTopic.sh"` || true
if [ -n "$PID3" ]; then
    kill -9 ${PID3}
fi

echo "EXECUTING: stop consuming"
PID4=$(ps ax | grep -i 'kafka.tools.ConsoleConsumer' | grep java | grep -v grep | awk '{print $1}')
if [ -n "$PID4" ]; then
  for i in "${PID4[@]}"
  do
      kill -9 ${i}
  done
fi

bin/grid stop all
