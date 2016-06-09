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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.uber.stream.kafka.mirrormaker.controller.reporter.HelixKafkaMirrorMakerMetricsReporter;
import com.uber.stream.kafka.mirrormaker.controller.utils.HelixUtils;

/**
 * We only considering add or remove box(es), not considering the replacing.
 * For replacing, we just need to bring up a new box and give the old instanceId no auto-balancing
 * needed.
 */
public class AutoRebalanceLiveInstanceChangeListener implements LiveInstanceChangeListener {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AutoRebalanceLiveInstanceChangeListener.class);

  private final ScheduledExecutorService _delayedScheuler =
      Executors.newSingleThreadScheduledExecutor();
  private final HelixMirrorMakerManager _helixMirrorMakerManager;
  private final HelixManager _helixManager;

  private final Counter _numLiveInstances = new Counter();
  private final Meter _rebalanceRate = new Meter();
  private final Timer _rebalanceTimer = new Timer();

  private final int _delayedAutoReblanceTimeInSeconds;

  public AutoRebalanceLiveInstanceChangeListener(HelixMirrorMakerManager helixMirrorMakerManager,
      HelixManager helixManager, int delayedAutoReblanceTimeInSeconds) {
    _helixMirrorMakerManager = helixMirrorMakerManager;
    _helixManager = helixManager;
    _delayedAutoReblanceTimeInSeconds = delayedAutoReblanceTimeInSeconds;
    LOGGER.info("Delayed Auto Reblance Time In Seconds: {}", _delayedAutoReblanceTimeInSeconds);
    registerMetrics();
  }

  private void registerMetrics() {
    try {
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("worker.liveInstances",
          _numLiveInstances);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("worker.rebalance.rate",
          _rebalanceRate);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("worker.rebalance.timer",
          _rebalanceTimer);
    } catch (Exception e) {
      LOGGER.error("Error registering metrics!", e);
    }
  }

  @Override
  public synchronized void onLiveInstanceChange(final List<LiveInstance> liveInstances,
      NotificationContext changeContext) {
    LOGGER.info("AutoRebalanceLiveInstanceChangeListener.onLiveInstanceChange() wakes up!");
    _delayedScheuler.schedule(new Runnable() {
      @Override
      public void run() {
        try {
          rebalanceCurrentCluster(_helixMirrorMakerManager.getCurrentLiveInstances()); 
        } catch (Exception e) {
          LOGGER.error("Got exception during rebalance the whole cluster! ", e);
        }
      }
    }, _delayedAutoReblanceTimeInSeconds, TimeUnit.SECONDS);
  }

  public synchronized void rebalanceCurrentCluster(List<LiveInstance> liveInstances) {
    Context context = _rebalanceTimer.time();
    LOGGER.info("AutoRebalanceLiveInstanceChangeListener.onLiveInstanceChange() wakes up!");
    try {
      _numLiveInstances.inc(liveInstances.size() - _numLiveInstances.getCount());
      if (!_helixManager.isLeader()) {
        LOGGER.info("Not leader, do nothing!");
        return;
      }
      if (!_helixMirrorMakerManager.isAutoBalancingEnabled()) {
        LOGGER.info("Is leader, but auto-balancing is disabled, do nothing!");
        return;
      }
      if (liveInstances.isEmpty()) {
        LOGGER.info("No live instances, do nothing!");
        return;
      }
      final Map<String, Set<TopicPartition>> instanceToTopicPartitionMap =
          HelixUtils.getInstanceToTopicPartitionsMap(_helixManager);
      Set<TopicPartition> unassignedTopicPartitions =
          HelixUtils.getUnassignedPartitions(_helixManager);
      if (instanceToTopicPartitionMap.isEmpty() && unassignedTopicPartitions.isEmpty()) {
        LOGGER.info("No topic got assigned yet, do nothing!");
        return;
      }
      Set<InstanceTopicPartitionHolder> newAssignment =
          rescaleInstanceToTopicPartitionMap(liveInstances, instanceToTopicPartitionMap,
              unassignedTopicPartitions);
      if (newAssignment == null) {
        LOGGER.info("No Live Instances got changed, do nothing!");
        return;
      }
      LOGGER.info("Trying to fetch IdealStatesMap from current assignment!");
      Map<String, IdealState> idealStatesFromAssignment =
          HelixUtils.getIdealStatesFromAssignment(newAssignment);
      LOGGER.info("Trying to assign new IdealStatesMap!");
      assignIdealStates(_helixManager, idealStatesFromAssignment);

      _helixMirrorMakerManager.updateCurrentServingInstance();
      _rebalanceRate.mark();
    } finally {
      context.close();
    }
  }

  @SuppressWarnings("unused")
  private static boolean isAnyWorkerDown(List<LiveInstance> liveInstances,
      Map<String, Set<TopicPartition>> instanceToTopicPartitionMap) {
    Set<String> removedInstances =
        getRemovedInstanceSet(getLiveInstanceName(liveInstances),
            instanceToTopicPartitionMap.keySet());
    return !removedInstances.isEmpty();
  }

  private static Set<InstanceTopicPartitionHolder> rescaleInstanceToTopicPartitionMap(
      List<LiveInstance> liveInstances,
      Map<String, Set<TopicPartition>> instanceToTopicPartitionMap,
      Set<TopicPartition> unassignedTopicPartitions) {
    Set<String> newInstances =
        getAddedInstanceSet(getLiveInstanceName(liveInstances),
            instanceToTopicPartitionMap.keySet());
    Set<String> removedInstances =
        getRemovedInstanceSet(getLiveInstanceName(liveInstances),
            instanceToTopicPartitionMap.keySet());
    if (newInstances.isEmpty() && removedInstances.isEmpty()) {
      return null;
    }
    LOGGER.info("Trying to rescale cluster with new instances - " + Arrays.toString(
        newInstances.toArray(new String[0])) + " and removed instances - " + Arrays.toString(
            removedInstances.toArray(new String[0])));
    TreeSet<InstanceTopicPartitionHolder> orderedSet =
        new TreeSet<>(InstanceTopicPartitionHolder.getComparator());
    Set<TopicPartition> tpiNeedsToBeAssigned = new HashSet<TopicPartition>();
    tpiNeedsToBeAssigned.addAll(unassignedTopicPartitions);
    for (String instanceName : instanceToTopicPartitionMap.keySet()) {
      if (!removedInstances.contains(instanceName)) {
        InstanceTopicPartitionHolder instance = new InstanceTopicPartitionHolder(instanceName);
        instance.addTopicPartitions(instanceToTopicPartitionMap.get(instanceName));
        orderedSet.add(instance);
      } else {
        tpiNeedsToBeAssigned.addAll(instanceToTopicPartitionMap.get(instanceName));
      }
    }
    for (String instanceName : newInstances) {
      orderedSet.add(new InstanceTopicPartitionHolder(instanceName));
    }
    orderedSet.last().addTopicPartitions(tpiNeedsToBeAssigned);
    Set<InstanceTopicPartitionHolder> balanceAssignment = balanceAssignment(orderedSet);
    return balanceAssignment;
  }

  private static void assignIdealStates(HelixManager helixManager,
      Map<String, IdealState> idealStatesFromAssignment) {
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    String helixClusterName = helixManager.getClusterName();
    for (String topic : idealStatesFromAssignment.keySet()) {
      IdealState idealState = idealStatesFromAssignment.get(topic);
      helixAdmin.setResourceIdealState(helixClusterName, topic, idealState);
    }
  }

  private static Set<InstanceTopicPartitionHolder> balanceAssignment(
      TreeSet<InstanceTopicPartitionHolder> orderedSet) {
    while (!isAssignmentBalanced(orderedSet)) {
      InstanceTopicPartitionHolder lowestInstance = orderedSet.pollFirst();
      InstanceTopicPartitionHolder highestInstance = orderedSet.pollLast();
      TopicPartition tpi = highestInstance.getServingTopicPartitionSet().iterator().next();
      highestInstance.removeTopicPartition(tpi);
      lowestInstance.addTopicPartition(tpi);
      orderedSet.add(lowestInstance);
      orderedSet.add(highestInstance);
    }
    return orderedSet;
  }

  private static boolean isAssignmentBalanced(TreeSet<InstanceTopicPartitionHolder> set) {
    return (set.last().getNumServingTopicPartitions()
        - set.first().getNumServingTopicPartitions() <= 1);
  }

  private static Set<String> getLiveInstanceName(List<LiveInstance> liveInstances) {
    Set<String> liveInstanceNames = new HashSet<String>();
    for (LiveInstance liveInstance : liveInstances) {
      liveInstanceNames.add(liveInstance.getInstanceName());
    }
    return liveInstanceNames;
  }

  private static Set<String> getAddedInstanceSet(Set<String> liveInstances,
      Set<String> currentInstances) {

    Set<String> addedInstances = new HashSet<String>();
    addedInstances.addAll(liveInstances);
    addedInstances.removeAll(currentInstances);
    return addedInstances;
  }

  private static Set<String> getRemovedInstanceSet(Set<String> liveInstances,
      Set<String> currentInstances) {
    Set<String> removedInstances = new HashSet<String>();
    removedInstances.addAll(currentInstances);
    removedInstances.removeAll(liveInstances);
    return removedInstances;
  }

}
