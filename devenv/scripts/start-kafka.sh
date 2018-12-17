#!/bin/bash

set -e


exec $KAFKA_HOME/bin/kafka-server-start.sh ${@:1}

