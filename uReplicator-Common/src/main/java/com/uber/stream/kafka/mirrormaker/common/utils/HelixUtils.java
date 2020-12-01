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
package com.uber.stream.kafka.mirrormaker.common.utils;

import static com.uber.stream.kafka.mirrormaker.common.Constants.DISABLE;
import static com.uber.stream.kafka.mirrormaker.common.Constants.ENABLE;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.uber.stream.kafka.mirrormaker.common.Constants;
import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.common.core.KafkaBrokerTopicObserver;
import com.uber.stream.kafka.mirrormaker.common.core.OnlineOfflineStateModel;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
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
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.store.zk.ZkHelixPropertyStore;

public class HelixUtils {

  private static final String BLACKLIST_TAG = "blacklisted";

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

  public static Map<String, String> getInstanceToHostnameMap(HelixManager helixManager) {
    List<String> instances = liveInstances(helixManager);
    HashMap<String, String> retVal = new HashMap<>(instances.size());
    for (String instance : instances) {
      InstanceConfig config = helixManager.getConfigAccessor().getInstanceConfig(
          helixManager.getClusterName(), instance);
      if (config != null) {
        retVal.put(instance, config.getHostName());
      }
    }
    return retVal;
  }

  public static Map<String, HostAndPort> getInstanceToHostInfoMap(HelixManager helixManager) {
    List<String> instances = liveInstances(helixManager);
    HashMap<String, HostAndPort> retVal = new HashMap<>(instances.size());
    for (String instance : instances) {
      InstanceConfig config = helixManager.getConfigAccessor().getInstanceConfig(
          helixManager.getClusterName(), instance);
      if (config != null) {
        retVal.put(instance,
            HostAndPort.fromParts(config.getHostName(), Integer.valueOf(config.getPort())));
      }
    }
    return retVal;
  }

  public static List<String> liveInstances(HelixManager helixManager) {
    HelixDataAccessor helixDataAccessor = helixManager.getHelixDataAccessor();
    PropertyKey liveInstancesKey = helixDataAccessor.keyBuilder().liveInstances();
    return ImmutableList.copyOf(helixDataAccessor.getChildNames(liveInstancesKey));
  }

  public static List<String> blacklistedInstances(HelixManager helixManager) {
    return helixManager.getClusterManagmentTool()
        .getInstancesInClusterWithTag(helixManager.getClusterName(), BLACKLIST_TAG);
  }

  private static String getSrcFromRoute(String route) {
    return route.substring(1, route.indexOf("@", 1));
  }

  private static String getPipelineFromRoute(String route) {
    return route.substring(0, route.lastIndexOf("@"));
  }

  public static Map<String, Set<TopicPartition>> getInstanceToTopicPartitionsMap(
      HelixManager helixManager) {
    return getInstanceToTopicPartitionsMap(helixManager, null);
  }

  /**
   * From IdealStates.
   *
   * @return InstanceToNumTopicPartitionMap
   */
  public static Map<String, Set<TopicPartition>> getInstanceToTopicPartitionsMap(
      HelixManager helixManager,
      Map<String, KafkaBrokerTopicObserver> clusterToObserverMap) {
    Map<String, Set<TopicPartition>> instanceToNumTopicPartitionMap = new HashMap<>();
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    String helixClusterName = helixManager.getClusterName();
    for (String topic : helixAdmin.getResourcesInCluster(helixClusterName)) {
      IdealState is = helixAdmin.getResourceIdealState(helixClusterName, topic);
      for (String partition : is.getPartitionSet()) {
        TopicPartition tpi;
        if (partition.startsWith("@")) {
          if (clusterToObserverMap != null) {
            if (clusterToObserverMap.containsKey(getSrcFromRoute(partition))) {
              TopicPartition topicPartition = clusterToObserverMap.get(getSrcFromRoute(partition)).getTopicPartitionWithRefresh(topic);
              int trueNumPartition = topicPartition != null ? topicPartition.getPartition() : -1;
              tpi = new TopicPartition(topic, trueNumPartition, partition);
            } else {
              // this only happens when source cluster is not in managerConf.getSourceClusters().
              // TODO: add metrics and alert on this
              LOGGER.warn("Failed to find cluster {} from clusterToObserverMap", getSrcFromRoute(partition));
              tpi = new TopicPartition(topic, -1, partition);
            }
          } else {
            tpi = new TopicPartition(topic, -1, partition);
          }
        } else {
          // route
          tpi = new TopicPartition(topic, Integer.parseInt(partition));
        }
        for (String instance : is.getInstanceSet(partition)) {
          instanceToNumTopicPartitionMap.putIfAbsent(instance, new HashSet<>());
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
      Set<InstanceTopicPartitionHolder> newAssignment,
      Set<TopicPartition> blacklistedTopicPartitions) {
    Map<String, CustomModeISBuilder> idealStatesBuilderMap = new HashMap<>();
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
        String state = Constants.HELIX_ONLINE_STATE;
        if (blacklistedTopicPartitions.contains(tpi)) {
          state = Constants.HELIX_OFFLINE_STATE;
        }
        idealStatesBuilderMap.get(topicName).assignInstanceAndState(partition,
            instance.getInstanceName(),
            state);
      }
    }
    Map<String, IdealState> idealStatesMap = new HashMap<>();
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

  public static void updateClusterConfig(HelixManager helixManager, String key, String value) {
    HelixConfigScope scope = newClusterConfigScope(helixManager);
    Map<String, String> properties = new HashMap<>();
    properties.put(key, value);
    helixManager.getClusterManagmentTool().setConfig(scope, properties);
  }

  public static boolean isClusterConfigEnabled(HelixManager helixManager, String key) {
    HelixConfigScope scope = newClusterConfigScope(helixManager);
    Map<String, String> configs = helixManager.getClusterManagmentTool()
        .getConfig(scope, Arrays.asList(key));
    if (configs.containsKey(key) && configs.get(key).equalsIgnoreCase(DISABLE)) {
      return false;
    }
    return true;
  }

  public static boolean isClusterConfigEnabled(HelixManager helixManager, String key,
      boolean defaultValue) {
    HelixConfigScope scope = newClusterConfigScope(helixManager);
    Map<String, String> configs = helixManager.getClusterManagmentTool()
        .getConfig(scope, Arrays.asList(key));
    if (configs != null && configs.containsKey(key)) {
      return configs.get(key).equalsIgnoreCase(ENABLE);
    }
    return defaultValue;
  }

  public static HelixConfigScope newClusterConfigScope(HelixManager helixManager) {
    return new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER)
        .forCluster(helixManager.getClusterName())
        .build();
  }

}
