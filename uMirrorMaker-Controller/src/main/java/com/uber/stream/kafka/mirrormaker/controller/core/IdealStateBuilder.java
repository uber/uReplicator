/**
 * Copyright (C) 2015-2016 Uber Technology Inc. (streaming-core@uber.com)
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

import java.util.PriorityQueue;

import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;

/**
 * Handle idealStates changes for new topic added and expanded.
 */
public class IdealStateBuilder {

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
      String topicName, int newNumTopicPartitions,
      PriorityQueue<InstanceTopicPartitionHolder> instanceToNumServingTopicPartitionMap) {
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
    for (int i = numOldPartitions; i < newNumTopicPartitions; ++i) {
      InstanceTopicPartitionHolder liveInstance = instanceToNumServingTopicPartitionMap.poll();
      customModeIdealStateBuilder.assignInstanceAndState(Integer.toString(i),
          liveInstance.getInstanceName(), "ONLINE");
      liveInstance.addTopicPartition(new TopicPartition(topicName, i));
      instanceToNumServingTopicPartitionMap.add(liveInstance);
    }
    return customModeIdealStateBuilder.build();
  }

}
