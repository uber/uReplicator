#!/usr/bin/env bash

ROOT=$(dirname $0)
CONSUL_ADDR=${CONSUL_ADDR:-"localhost:8500"}
envsubst <${ROOT}/docker_env.sh.ctmpl.envtemplate > ${ROOT}/docker_env.sh.ctmpl
echo "" >> docker_env.sh.ctmpl
consul-template -once -template ${ROOT}/docker_env.sh.ctmpl:${ROOT}/docker_env.sh -consul-addr=${CONSUL_ADDR}
echo "" >> docker_env.sh
chmod +x docker_env.sh