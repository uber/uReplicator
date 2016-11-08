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
package com.uber.stream.kafka.mirrormaker.controller.utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.store.zk.ZkHelixPropertyStore;

import com.google.common.collect.ImmutableList;
import com.uber.stream.kafka.mirrormaker.controller.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.controller.core.OnlineOfflineStateModel;
import com.uber.stream.kafka.mirrormaker.controller.core.TopicPartition;

public class HelixUtils {
  public static String getAbsoluteZkPathForHelix(String zkBaseUrl) {
    zkBaseUrl = StringUtils.chomp(zkBaseUrl, "/");
    return zkBaseUrl;
  }

  public static ZkHelixPropertyStore<ZNRecord> getZkPropertyStore(HelixManager helixManager,
      String clusterName) {
    ZkBaseDataAccessor<ZNRecord> baseAccessor =
        (ZkBaseDataAccessor<ZNRecord>) helixManager.getHelixDataAccessor().getBaseDataAccessor();
    String propertyStorePath = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName);

    ZkHelixPropertyStore<ZNRecord> propertyStore = new ZkHelixPropertyStore<ZNRecord>(baseAccessor,
        propertyStorePath, Arrays.asList(propertyStorePath));

    return propertyStore;
  }

  public static List<String> liveInstances(HelixManager helixManager) {
    HelixDataAccessor helixDataAccessor = helixManager.getHelixDataAccessor();
    PropertyKey liveInstancesKey = helixDataAccessor.keyBuilder().liveInstances();
    return ImmutableList.copyOf(helixDataAccessor.getChildNames(liveInstancesKey));
  }

  /**
   * From IdealStates.
   * @param helixManager
   * @return InstanceToNumTopicPartitionMap
   */
  public static Map<String, Set<TopicPartition>> getInstanceToTopicPartitionsMap(
      HelixManager helixManager) {
    Map<String, Set<TopicPartition>> instanceToNumTopicPartitionMap =
        new HashMap<String, Set<TopicPartition>>();
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    String helixClusterName = helixManager.getClusterName();
    for (String topic : helixAdmin.getResourcesInCluster(helixClusterName)) {
      IdealState is = helixAdmin.getResourceIdealState(helixClusterName, topic);
      for (String partition : is.getPartitionSet()) {
        TopicPartition tpi = new TopicPartition(topic, Integer.parseInt(partition));
        for (String instance : is.getInstanceSet(partition)) {
          if (!instanceToNumTopicPartitionMap.containsKey(instance)) {
            instanceToNumTopicPartitionMap.put(instance, new HashSet<TopicPartition>());
          }
          instanceToNumTopicPartitionMap.get(instance).add(tpi);
        }
      }
    }
    return instanceToNumTopicPartitionMap;
  }

  public static IdealState buildCustomIdealStateFor(String topicName,
      int numTopicPartitions,
      PriorityQueue<InstanceTopicPartitionHolder> instanceToNumServingTopicPartitionMap) {

    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(numTopicPartitions).setNumReplica(1)
        .setMaxPartitionsPerNode(numTopicPartitions);

    for (int i = 0; i < numTopicPartitions; ++i) {
      synchronized (instanceToNumServingTopicPartitionMap) {
        InstanceTopicPartitionHolder liveInstance = instanceToNumServingTopicPartitionMap.poll();
        customModeIdealStateBuilder.assignInstanceAndState(Integer.toString(i),
            liveInstance.getInstanceName(), "ONLINE");
        liveInstance.addTopicPartition(new TopicPartition(topicName, i));
        instanceToNumServingTopicPartitionMap.add(liveInstance);
      }
    }
    return customModeIdealStateBuilder.build();
  }

  public static Map<String, IdealState> getIdealStatesFromAssignment(
      Set<InstanceTopicPartitionHolder> newAssignment) {
    Map<String, CustomModeISBuilder> idealStatesBuilderMap =
        new HashMap<String, CustomModeISBuilder>();
    for (InstanceTopicPartitionHolder instance : newAssignment) {
      for (TopicPartition tpi : instance.getServingTopicPartitionSet()) {
        String topicName = tpi.getTopic();
        String partition = Integer.toString(tpi.getPartition());
        if (!idealStatesBuilderMap.containsKey(topicName)) {
          final CustomModeISBuilder customModeIdealStateBuilder =
              new CustomModeISBuilder(topicName);
          customModeIdealStateBuilder
              .setStateModel(OnlineOfflineStateModel.name)
              .setNumReplica(1);

          idealStatesBuilderMap.put(topicName, customModeIdealStateBuilder);
        }
        idealStatesBuilderMap.get(topicName).assignInstanceAndState(partition,
            instance.getInstanceName(),
            "ONLINE");
      }
    }
    Map<String, IdealState> idealStatesMap = new HashMap<String, IdealState>();
    for (String topic : idealStatesBuilderMap.keySet()) {
      IdealState idealState = idealStatesBuilderMap.get(topic).build();
      idealState.setMaxPartitionsPerInstance(idealState.getPartitionSet().size());
      idealState.setNumPartitions(idealState.getPartitionSet().size());
      idealStatesMap.put(topic, idealState);
    }
    return idealStatesMap;
  }

  public static Set<TopicPartition> getUnassignedPartitions(HelixManager helixManager) {
    Set<TopicPartition> unassignedPartitions = new HashSet<TopicPartition>();
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    String helixClusterName = helixManager.getClusterName();
    for (String topic : helixAdmin.getResourcesInCluster(helixClusterName)) {
      IdealState is = helixAdmin.getResourceIdealState(helixClusterName, topic);
      int numPartitions = is.getNumPartitions();
      for (int partition = 0; partition < numPartitions; ++partition) {
        if (is.getInstanceSet(Integer.toString(partition)).isEmpty()) {
          TopicPartition tpi = new TopicPartition(topic, partition);
          unassignedPartitions.add(tpi);
        }
      }
    }
    return unassignedPartitions;
  }

}
