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
package com.uber.stream.kafka.mirrormaker.manager.core;

import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.common.core.OnlineOfflineStateModel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.common.utils.PartitionAllocator;
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
      String partition,
      InstanceTopicPartitionHolder instance) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(1).setNumReplica(1)
        .setMaxPartitionsPerNode(1);

    if (instance != null) {
      customModeIdealStateBuilder.assignInstanceAndState(partition, instance.getInstanceName(), "ONLINE");
    }

    return customModeIdealStateBuilder.build();
  }

  public static IdealState buildCustomIdealStateFor(String topicName,
                                                    int numTopicPartitions, String pipeline,
                                                    PriorityQueue<InstanceTopicPartitionHolder> instanceToNumServingTopicPartitionMap) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

    customModeIdealStateBuilder
            .setStateModel(OnlineOfflineStateModel.name)
            .setNumPartitions(instanceToNumServingTopicPartitionMap.size()).setNumReplica(1)
            .setMaxPartitionsPerNode(instanceToNumServingTopicPartitionMap.size());

    if(instanceToNumServingTopicPartitionMap.size() == 1){
      InstanceTopicPartitionHolder instance = instanceToNumServingTopicPartitionMap.peek();
      String partition = instance.getRouteString();
      customModeIdealStateBuilder.assignInstanceAndState(partition, instance.getInstanceName(), "ONLINE");
      instance.addTopicPartition(new TopicPartition(topicName, numTopicPartitions, pipeline));
      return customModeIdealStateBuilder.build();
    }

    List<Integer> partitionAll = new ArrayList<>();
    for(int i = 0; i < numTopicPartitions; i ++){
      partitionAll.add(i);
    }
    List<InstanceTopicPartitionHolder> instanceAll = Arrays.asList(instanceToNumServingTopicPartitionMap.toArray(new InstanceTopicPartitionHolder[]{}));
    for(InstanceTopicPartitionHolder instance : instanceAll){
      List<Integer> partitions = PartitionAllocator.allocate(instance, partitionAll, instanceAll);
      customModeIdealStateBuilder.assignInstanceAndState(instance.getRouteString() + "@" + PartitionAllocator.partitionToString(partitions),
              instance.getInstanceName(), "ONLINE");
      LOGGER.info("assign route : {} parition : {} to instancename : {}", instance.getRouteString(), partitions, instance.getInstanceName());
      for(Integer partition : partitions){
        instance.addOneTopicPartition(new TopicPartition(topicName, partition, pipeline));
      }
    }
    return customModeIdealStateBuilder.build();
  }

  public static IdealState buildCustomIdealStateFor(String topicName,
      String partition,
      List<String> instances) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(1).setNumReplica(instances.size())
        .setMaxPartitionsPerNode(1);

    for (String instance : instances) {
      customModeIdealStateBuilder.assignInstanceAndState(partition, instance, "ONLINE");
    }
    return customModeIdealStateBuilder.build();
  }

  public static IdealState resetCustomIdealStateFor(IdealState oldIdealState,
      String topicName, String partitionToReplace, String newInstanceName) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

    int oldNumPartitions = oldIdealState.getNumPartitions();

    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(oldNumPartitions).setNumReplica(1)
        .setMaxPartitionsPerNode(oldNumPartitions);

    for (String partitionName : oldIdealState.getPartitionSet()) {
      String instanceName = oldIdealState.getInstanceStateMap(partitionName).keySet().iterator().next();
      String instanceToUse = partitionName.equals(partitionToReplace) ? newInstanceName : instanceName;
      customModeIdealStateBuilder.assignInstanceAndState(partitionName, instanceToUse, "ONLINE");
    }

    return customModeIdealStateBuilder.build();
  }

  /*public static IdealState rebalanceCustomIdealStateFor(IdealState oldIdealState,
      String topicName, String oldPartition, String newPartition, String newInstanceName) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

    int oldNumPartitions = oldIdealState.getNumPartitions();

    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(oldNumPartitions+1).setNumReplica(1)
        .setMaxPartitionsPerNode(oldNumPartitions+1);

    for (String partitionName : oldIdealState.getPartitionSet()) {
      String instanceName = oldIdealState.getInstanceStateMap(partitionName).keySet().iterator().next();
      String instanceToUse = partitionName.equals(oldPartition) ? newInstanceName : instanceName;
      String partitionToUse = partitionName.equals(oldPartition) ? newPartition : oldPartition;
      customModeIdealStateBuilder.assignInstanceAndState(partitionToUse, instanceToUse, "ONLINE");
    }

    return customModeIdealStateBuilder.build();
  }*/

  public static IdealState resetCustomIdealStateFor(IdealState oldIdealState,
      String topicName, String oldPartition, String newPartition, String newInstanceName) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

    int oldNumPartitions = oldIdealState.getNumPartitions();

    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(oldNumPartitions).setNumReplica(1)
        .setMaxPartitionsPerNode(oldNumPartitions);

    for (String partitionName : oldIdealState.getPartitionSet()) {
      String instanceName = oldIdealState.getInstanceStateMap(partitionName).keySet().iterator().next();
      String instanceToUse = partitionName.equals(oldPartition) ? newInstanceName : instanceName;
      String partitionToUse = partitionName.equals(oldPartition) ? newPartition : oldPartition;
      customModeIdealStateBuilder.assignInstanceAndState(partitionToUse, instanceToUse, "ONLINE");
    }

    return customModeIdealStateBuilder.build();
  }

  public static IdealState resetCustomIdealStateFor(IdealState oldIdealState,
      String topicName, List<String> instanceToReplace, String availableInstance, int maxNumReplica) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

    int oldNumPartitions = oldIdealState.getNumPartitions();

    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(oldNumPartitions).setNumReplica(maxNumReplica)
        .setMaxPartitionsPerNode(oldNumPartitions);
    for (String partitionName : oldIdealState.getPartitionSet()) {
      for (String instanceName : oldIdealState.getInstanceStateMap(partitionName).keySet()) {
        String instanceToUse = instanceToReplace.contains(instanceName) ? availableInstance : instanceName;
        customModeIdealStateBuilder.assignInstanceAndState(partitionName, instanceToUse, "ONLINE");
        if (instanceToReplace.contains(instanceName)) {
          LOGGER.info("replaceing: route: {}@{}, old {}, new {}",
              topicName, partitionName, instanceName, instanceToUse);
        }
      }
    }

    return customModeIdealStateBuilder.build();
  }

  public static IdealState expandCustomIdealStateFor(IdealState oldIdealState,
      String topicName, String newPartition, InstanceTopicPartitionHolder instance) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

    int oldNumPartitions = oldIdealState.getNumPartitions();

    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(oldNumPartitions + 1).setNumReplica(1)
        .setMaxPartitionsPerNode(oldNumPartitions + 1);

    for (String partitionName : oldIdealState.getPartitionSet()) {
      String instanceName = oldIdealState.getInstanceStateMap(partitionName).keySet().iterator().next();
      customModeIdealStateBuilder.assignInstanceAndState(partitionName, instanceName, "ONLINE");
    }

    customModeIdealStateBuilder.assignInstanceAndState(newPartition, instance.getInstanceName(), "ONLINE");

    return customModeIdealStateBuilder.build();
  }

  public static IdealState expandCustomIdealStateFor(IdealState oldIdealState,
      String topicName, String newPartition, List<String> instances, int maxNumReplica) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

    int oldNumPartitions = oldIdealState.getNumPartitions();

    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(oldNumPartitions + 1).setNumReplica(maxNumReplica)
        .setMaxPartitionsPerNode(oldNumPartitions + 1);

    for (String partitionName : oldIdealState.getPartitionSet()) {
      for (String instanceName : oldIdealState.getInstanceStateMap(partitionName).keySet()) {
        customModeIdealStateBuilder.assignInstanceAndState(partitionName, instanceName, "ONLINE");
      }
    }

    for (String instance : instances) {
      customModeIdealStateBuilder.assignInstanceAndState(newPartition, instance, "ONLINE");
    }

    return customModeIdealStateBuilder.build();
  }

  public static IdealState expandInstanceCustomIdealStateFor(IdealState oldIdealState,
      String topicName, String newPartition, List<String> instances, int maxNumReplica) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

    int oldNumPartitions = oldIdealState.getNumPartitions();

    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(oldNumPartitions).setNumReplica(maxNumReplica)
        .setMaxPartitionsPerNode(oldNumPartitions);

    for (String partitionName : oldIdealState.getPartitionSet()) {
      for (String instanceName : oldIdealState.getInstanceStateMap(partitionName).keySet()) {
        customModeIdealStateBuilder.assignInstanceAndState(partitionName, instanceName, "ONLINE");
      }
      if (partitionName.equals(newPartition)) {
        for (String newInstanceName : instances) {
          customModeIdealStateBuilder.assignInstanceAndState(partitionName, newInstanceName, "ONLINE");
        }
      }
    }

    return customModeIdealStateBuilder.build();
  }

  public static IdealState shrinkInstanceCustomIdealStateFor(IdealState oldIdealState,
      String topicName, String partition, List<String> instancesToRemove, int maxNumReplica) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

    int oldNumPartitions = oldIdealState.getNumPartitions();

    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(oldNumPartitions).setNumReplica(maxNumReplica)
        .setMaxPartitionsPerNode(oldNumPartitions);

    for (String partitionName : oldIdealState.getPartitionSet()) {
      if (partitionName.equals(partition)) {
        for (String instanceName : oldIdealState.getInstanceStateMap(partitionName).keySet()) {
          if (!instancesToRemove.contains(instanceName)) {
            customModeIdealStateBuilder.assignInstanceAndState(partitionName, instanceName, "ONLINE");
          }
        }
      } else {
        for (String instanceName : oldIdealState.getInstanceStateMap(partitionName).keySet()) {
          customModeIdealStateBuilder.assignInstanceAndState(partitionName, instanceName, "ONLINE");
        }
      }
    }

    return customModeIdealStateBuilder.build();
  }

  public static IdealState shrinkCustomIdealStateFor(IdealState oldIdealState,
      String topicName, String partitionToDelete) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

    int oldNumPartitions = oldIdealState.getNumPartitions();

    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(oldNumPartitions - 1).setNumReplica(1)
        .setMaxPartitionsPerNode(oldNumPartitions - 1);

    for (String partitionName : oldIdealState.getPartitionSet()) {
      if (!partitionName.equals(partitionToDelete)) {
        String instanceName = oldIdealState.getInstanceStateMap(partitionName).keySet().iterator().next();
        customModeIdealStateBuilder.assignInstanceAndState(partitionName, instanceName, "ONLINE");
      }
    }

    return customModeIdealStateBuilder.build();
  }

}
