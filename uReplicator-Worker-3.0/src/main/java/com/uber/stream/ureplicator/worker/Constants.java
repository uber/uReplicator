/*
 * Copyright (C) 2015-2019 Uber Technologies, Inc. (streaming-data@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.uber.stream.ureplicator.worker;

public final class Constants {

  private Constants() {
  }

  public static final String MANAGER_WORKER_HELIX_PREFIX = "manager-worker-";
  public static final String CONTROLLER_WORKER_HELIX_PREFIX = "controller-worker-";

  public static final String PRODUCER_TYPE_CONFIG = "producer.type";
  public static final String DEFAULT_PRODUCER_TYPE = "async";

  public static final String HELIX_CLUSTER_NAME = "helixClusterName";
  public static final String DEFAULT_HELIX_CLUSTER_NAME = "testMirrorMaker";

  public static final String HELIX_INSTANCE_ID = "instanceId";

  public static final String HELIX_ZK_SERVER = "zkServer";
  public static final String DEFAULT_HELIX_ZK_SERVER = "localhost:2181/uReplicator";

  public static final String ZK_SERVER = "zookeeper.connect";

  public static final String PRODUCER_ZK_OBSERVER = "zkPath";
  public static final String DEFAULT_PRODUCER_ZK_OBSERVER = "/brokers/topics";

  public static final String PRODUCER_NUMBER_OF_PRODUCERS = "num.of.producers";
  public static final String DEFAULT_NUMBER_OF_PRODUCERS = "2";


  public static final String PRODUCER_OFFSET_COMMIT_INTERVAL_MS = "offset.commit.interval.ms";
  public static final String DEFAULT_PRODUCER_OFFSET_COMMIT_INTERVAL_MS = "60000";


  public static final String FEDERATED_DEPLOYMENT_NAME = "federated.deployment.name";

  public static final String ROUTE_SEPARATOR = "@";

  public static final String FEDERATED_CLUSTER_SERVER_CONFIG_PREFIX = "kafka.cluster.servers.";

  public static final String FEDERATED_SECURE_CLUSTER_SERVER_CONFIG_PREFIX = "secure.kafka.cluster.servers.";

  public static final String FEDERATED_CLUSTER_ZK_CONFIG_PREFIX = "kafka.cluster.zkStr.";

  public static final String COMMIT_ZOOKEEPER_SERVER_CONFIG = "commit.zookeeper.connect";

  public static final String PRODUCER_THREAD_PREFIX = "producer-thread-";
}
