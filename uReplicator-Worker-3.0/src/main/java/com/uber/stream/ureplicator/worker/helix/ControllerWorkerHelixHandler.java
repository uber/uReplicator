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
package com.uber.stream.ureplicator.worker.helix;

import com.codahale.metrics.Meter;
import com.uber.stream.kafka.mirrormaker.common.core.HelixHandler;
import com.uber.stream.kafka.mirrormaker.common.core.OnlineOfflineStateFactory;
import com.uber.stream.ureplicator.common.KafkaUReplicatorMetricsReporter;
import com.uber.stream.ureplicator.worker.Constants;
import com.uber.stream.ureplicator.worker.WorkerInstance;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControllerWorkerHelixHandler implements HelixHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerWorkerHelixHandler.class);

  public static final Meter addTopicPartitionFailureMeter = new Meter();
  public static final String METRIC_ADD_TOPIC_PARTITION_FAILURE = "addTopicPartitionFailure";
  public static final Meter deleteTopicPartitionFailureMeter = new Meter();
  public static final String METRIC_DELETE_TOPIC_PARTITION_FAILURE = "deleteTopicPartitionFailure";

  private final String workerInstanceId;
  private final String helixZkURL;
  private final String helixClusterName;
  private final String srcCluster;
  private final String dstCluster;
  private final String routeId;
  private final String federatedDeploymentName;
  private final WorkerInstance workerInstance;
  private HelixManager helixManager;

  public ControllerWorkerHelixHandler(Properties helixProps,
      String helixClusterName,
      WorkerInstance workerInstance) {
    this(helixProps, helixClusterName, null, null, null, null, null, workerInstance);
  }

  public ControllerWorkerHelixHandler(Properties helixProps,
      String helixClusterName, String instanceId, String srcCluster, String dstCluster, String routeId,
      String federatedDeploymentName,
      WorkerInstance workerInstance) {
    this.srcCluster = srcCluster;
    this.dstCluster = dstCluster;
    this.routeId = routeId;
    this.helixClusterName = helixClusterName;
    this.federatedDeploymentName = federatedDeploymentName;
    this.helixZkURL = helixProps
        .getProperty(Constants.HELIX_ZK_SERVER, Constants.DEFAULT_HELIX_ZK_SERVER);
    this.workerInstanceId = StringUtils.isEmpty(instanceId) ? helixProps
        .getProperty(Constants.HELIX_INSTANCE_ID, "uReplicator-" + System.currentTimeMillis()) : instanceId;
    this.workerInstance = workerInstance;
  }

  public void start() throws Exception {
    try {
      workerInstance.start(srcCluster, dstCluster, routeId, federatedDeploymentName, workerInstanceId);
      KafkaUReplicatorMetricsReporter.get()
          .registerMetric(METRIC_ADD_TOPIC_PARTITION_FAILURE, addTopicPartitionFailureMeter);
      KafkaUReplicatorMetricsReporter.get()
          .registerMetric(METRIC_DELETE_TOPIC_PARTITION_FAILURE, deleteTopicPartitionFailureMeter);

      if (helixManager != null && helixManager.isConnected()) {
        LOGGER.warn("ControllerWorkerHelixManager already connected");
        return;
      }
      LOGGER.info("Starting ControllerWorkerHelixHandler");
      helixManager = HelixManagerFactory.getZKHelixManager(helixClusterName,
          workerInstanceId,
          InstanceType.PARTICIPANT,
          helixZkURL);
      helixManager.connect();
      helixManager.getStateMachineEngine()
          .registerStateModelFactory(OnlineOfflineStateFactory.STATE_MODE_DEF,
              new OnlineOfflineStateFactory(this));
    } catch (Exception e) {
      LOGGER.error("Add instance to helix cluster failed. helixCluster: {}",
          helixClusterName, e);
      throw e;
    }
    LOGGER.info("Register ControllerWorkerHelixHandler finished");
  }

  @Override
  public void onBecomeOnlineFromOffline(Message message) {
    addTopicPartition(message.getResourceName(), message.getPartitionName());
  }

  @Override
  public void onBecomeOfflineFromOnline(Message message) {
    deleteTopicPartition(message.getResourceName(), message.getPartitionName());
  }

  @Override
  public void onBecomeDroppedFromOffline(Message message) {
    deleteTopicPartition(message.getResourceName(), message.getPartitionName());
  }

  private void addTopicPartition(String topic, String partition) {
    try {
      workerInstance.addTopicPartition(topic,
          Integer.parseInt(partition));
    } catch (Throwable t) {
      addTopicPartitionFailureMeter.mark();
      LOGGER.error("addTopicPartition Failed. topic: {}, partition: {}", topic, partition, t);
      // add topic partition failure can be detected by controller offset monitor and helix will mark this as ERROR
      throw t;
    }
  }

  private void deleteTopicPartition(String topic, String partition) {
    try {
      workerInstance.deleteTopicPartition(topic,
          Integer.parseInt(partition));
    } catch (Throwable t) {
      deleteTopicPartitionFailureMeter.mark();
      LOGGER.error("deleteTopicPartition Failed. topic: {}, partition: {}", topic, partition, t);
      // exception should be rare, helix will mark this as ERROR.
      throw t;
    }
  }

  @Override
  public void onBecomeOfflineFromError(Message message) {
    LOGGER.warn("Unsupported state change onBecomeOfflineFromError. topic: {}, partition: {}",
        message.getResourceName(), message.getPartitionName());
  }

  @Override
  public void onBecomeDroppedFromError(Message message) {
    LOGGER.warn("Unsupported state change onBecomeDroppedFromError. topic: {}, partition: {}",
        message.getResourceName(), message.getPartitionName());
  }

  public void shutdown() {
    LOGGER.info("Shutting down ControllerWorkerHelixHandler");
    if (workerInstance != null) {
      try {
        workerInstance.cleanShutdown();
      } catch (Exception e) {
        LOGGER.error("uReplicator Worker shutdown failed", e);
      }
    }
    if (helixManager != null && helixManager.isConnected()) {
      helixManager.disconnect();
    }
    LOGGER.info("Shutdown ControllerWorkerHelixHandler finished");

  }
}
