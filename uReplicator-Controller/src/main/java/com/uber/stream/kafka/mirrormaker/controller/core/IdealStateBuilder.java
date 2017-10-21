/*
 * Copyright (C) 2015-2017 Uber Technologies, Inc. (streaming-data@uber.com)
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
package com.uber.stream.kafka.mirrormaker.controller.core;

import com.uber.stream.kafka.mirrormaker.common.core.OnlineOfflineStateModel;
import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle idealStates changes for new topic added and expanded.
 */
public class IdealStateBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(IdealStateBuilder.class);

  public static IdealState buildCustomIdealStateFor(String topicName,
      int numTopicPartitions,
      PriorityQueue<InstanceTopicPartitionHolder> instanceToNumServingTopicPartitionMap) {

    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(numTopicPartitions).setNumReplica(1)
        .setMaxPartitionsPerNode(numTopicPartitions);

    for (int i = 0; i < numTopicPartitions; ++i) {
      InstanceTopicPartitionHolder liveInstance = instanceToNumServingTopicPartitionMap.poll();
      if (liveInstance != null) {
        customModeIdealStateBuilder.assignInstanceAndState(Integer.toString(i),
            liveInstance.getInstanceName(), "ONLINE");
        liveInstance.addTopicPartition(new TopicPartition(topicName, i));
        instanceToNumServingTopicPartitionMap.add(liveInstance);
      }
    }
    return customModeIdealStateBuilder.build();
  }

  public static IdealState expandCustomRebalanceModeIdealStateFor(IdealState oldIdealState,
      String topicName, int newNumTopicPartitions, ControllerConf controllerConf,
      PriorityQueue<InstanceTopicPartitionHolder> currentServingInstances) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(newNumTopicPartitions).setNumReplica(1)
        .setMaxPartitionsPerNode(newNumTopicPartitions);

    int numOldPartitions = oldIdealState.getNumPartitions();
    for (int i = 0; i < numOldPartitions; ++i) {
      String partitionName = Integer.toString(i);
      try {
        String instanceName =
            oldIdealState.getInstanceStateMap(partitionName).keySet().iterator().next();
        customModeIdealStateBuilder.assignInstanceAndState(partitionName, instanceName, "ONLINE");
      } catch (Exception e) {
        // No worker added into the cluster.
      }
    }

    ZkClient zkClient = null;
    String topicPath = "/consumers/" + controllerConf.getGroupId() + "/offsets/" + topicName;
    String consumerOffsetPath = topicPath + "/";
    String zkString = controllerConf.getConsumerCommitZkPath().isEmpty() ?
        controllerConf.getSrcKafkaZkPath() : controllerConf.getConsumerCommitZkPath();
    boolean pathExisted = false;
    if (!StringUtils.isEmpty(zkString)) {
      zkClient = new ZkClient(zkString, 30000, 30000, ZKStringSerializer$.MODULE$);
      try {
        if (!zkClient.exists(topicPath)) {
          zkClient.createPersistent(topicPath);
        }
        pathExisted = true;
      } catch (Exception e) {
        LOGGER.warn("Fails to create path {}", topicPath, e);
      }
    }
    // Assign new partitions to as many workers as possible
    List<InstanceTopicPartitionHolder> instancesForNewPartitions = new ArrayList<>();
    while (instancesForNewPartitions.size() < newNumTopicPartitions - numOldPartitions
        && !currentServingInstances.isEmpty()) {
      instancesForNewPartitions.add(currentServingInstances.poll());
    }
    if (!instancesForNewPartitions.isEmpty()) {
      for (int i = numOldPartitions; i < newNumTopicPartitions; ++i) {
        if (pathExisted) {
          Object obj = zkClient.readData(consumerOffsetPath + i, true);
          if (obj == null) {
            zkClient.createPersistent(consumerOffsetPath + i, "0");
            LOGGER.info("Create new zk node " + zkString + consumerOffsetPath + i);
          }
        }

        InstanceTopicPartitionHolder liveInstance =
            instancesForNewPartitions.get(i % instancesForNewPartitions.size());
        customModeIdealStateBuilder.assignInstanceAndState(Integer.toString(i),
            liveInstance.getInstanceName(),
            "ONLINE");
        liveInstance.addTopicPartition(new TopicPartition(topicName, i));
        LOGGER.info("Assign new partition " + topicName + ":" + i + " to instance " + liveInstance.getInstanceName());
      }
      currentServingInstances.addAll(instancesForNewPartitions);
    }
    if (zkClient != null) {
      zkClient.close();
    }
    return customModeIdealStateBuilder.build();
  }

}
