#!/bin/bash
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

set -e
if [ ! -z "$WAIT_FOR_HOST" ]
then
echo "wait for host $WAIT_FOR_HOST to be ready"
if [ -z "$WAIT_FOR_TIME_OUT" ]
then
export WAIT_FOR_TIME_OUT=60
fi
echo "./wait-for-it.sh $WAIT_FOR_HOST  $WAIT_FOR_TIME_OUT"
./wait-for-it.sh $WAIT_FOR_HOST  $WAIT_FOR_TIME_OUT

fi

export UREP_VERSION="2.0.1-SNAPSHOT"

case "$1" in
  manager)
    exec java -Dlog4j.configuration=file:config/tools-log4j.properties -server -cp uReplicator-Manager/target/uReplicator-Manager-$UREP_VERSION-jar-with-dependencies.jar com.uber.stream.kafka.mirrormaker.manager.ManagerStarter ${@:2}
  ;;
  controller)
    exec java -Dlog4j.configuration=file:config/tools-log4j.properties -server -cp uReplicator-Controller/target/uReplicator-Controller-$UREP_VERSION-jar-with-dependencies.jar com.uber.stream.kafka.mirrormaker.controller.ControllerStarter ${@:2}
  ;;
  worker)
    exec java -Dlog4j.configuration=file:config/tools-log4j.properties -server -cp uReplicator-Worker-3.0/target/uReplicator-Worker-3.0-$UREP_VERSION-jar-with-dependencies.jar  com.uber.stream.ureplicator.worker.WorkerStarter ${@:2}
  ;;
  *)
    exec $@
  ;;
esac
