#!/bin/bash
set -e

case "$1" in
  controller)
    exec ./uReplicator-Distribution/target/uReplicator-Distribution-pkg/bin/start-controller.sh ${@:2}
  ;;
  worker)
    exec ./uReplicator-Distribution/target/uReplicator-Distribution-pkg/bin/start-worker.sh ${@:2}
  ;;
  *)
    exec $@
  ;;
esac
