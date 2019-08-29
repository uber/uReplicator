uReplicator 
============
[![Build Status](https://travis-ci.com/uber/uReplicator.svg?branch=master)](https://travis-ci.com/uber/uReplicator)

## Update

From 11/20/2018, old master branch (no uReplcator-Manager module) is moved to branch-0.1. New master is backward-compatible and supports both non-federation and federation mode.

## Highlight

uReplicator provides a Kafka replication solution with high performance, scalability and stability.

uReplicator is good at:

*   High throughput
    *   uReplicator has a controller to assign partitions to workers based on throughput in source cluster so each worker can achieve max throughput. (Currently it depends on [Chaperone](https://github.com/uber/chaperone); We will make it get workload from JMX shortly)
    *   uReplicator checks lags on each worker and removes heavy traffic from lagging workers.
*   High availability
    *   uReplicator uses smart rebalance instead of high level consumer rebalance which can guarantee smooth replication.
*   High scalability
    *   When the scale of Kafka infrastructure increases, simply add more hosts to uReplicator and it will scale up automatically
*   Smart operation (Federated uReplicator)
    *   Federated uReplicator can set up replication route automatically.
    *   When a route has higher traffic or lag, Federated uReplicator can add workers automatically and release afterwards.
    *   uReplicator can detect new partitions in source cluster and start replication automatically

## Documentation

You can find [quick start](https://github.com/uber/uReplicator/wiki/uReplicator-User-Guide#2-quick-start) and [configurations](https://github.com/uber/uReplicator/wiki/uReplicator-User-Guide#3-configurations) in [uReplicator User Guide](https://github.com/uber/uReplicator/wiki/uReplicator-User-Guide).
