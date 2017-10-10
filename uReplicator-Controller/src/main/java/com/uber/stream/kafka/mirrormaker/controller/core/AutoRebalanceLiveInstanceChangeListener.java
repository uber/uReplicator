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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.uber.stream.kafka.mirrormaker.controller.reporter.HelixKafkaMirrorMakerMetricsReporter;
import com.uber.stream.kafka.mirrormaker.controller.utils.HelixUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

  // If the workload of a worker compared to average workload is more than the
  // threshold, it is considered as overloaded. For example with 1.2, if average
  // workload is 5MB/s, if a worker has workload higher than 5 * 1.2 = 6MB/s
  // then it is considered to be overloaded. The larger the threshold, the more
  // unbalancing is allowed but it will trigger less partition re-assignments.
  private final double _overloadedRatioThreshold;

  public AutoRebalanceLiveInstanceChangeListener(HelixMirrorMakerManager helixMirrorMakerManager,
      HelixManager helixManager, int delayedAutoReblanceTimeInSeconds, int autoRebalancePeriodInSeconds,
      double overloadedRatioThreshold) {
    _helixMirrorMakerManager = helixMirrorMakerManager;
    _helixManager = helixManager;
    _delayedAutoReblanceTimeInSeconds = delayedAutoReblanceTimeInSeconds;
    _overloadedRatioThreshold = overloadedRatioThreshold;
    LOGGER.info("Delayed Auto Reblance Time In Seconds: {}", _delayedAutoReblanceTimeInSeconds);
    registerMetrics();

    if (autoRebalancePeriodInSeconds > 0) {
      LOGGER.info("Trying to schedule auto rebalancing at rate " + autoRebalancePeriodInSeconds + " seconds");
      _delayedScheuler.scheduleWithFixedDelay(
          new Runnable() {
            @Override
            public void run() {
              try {
                rebalanceCurrentCluster(_helixMirrorMakerManager.getCurrentLiveInstances(), false);
              } catch (Exception e) {
                LOGGER.error("Got exception during periodically rebalancing the whole cluster! ", e);
              }
            }
          }, Math.max(_delayedAutoReblanceTimeInSeconds, autoRebalancePeriodInSeconds),
          autoRebalancePeriodInSeconds, TimeUnit.SECONDS);
    }
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
  public void onLiveInstanceChange(final List<LiveInstance> liveInstances,
      NotificationContext changeContext) {
    LOGGER.info("AutoRebalanceLiveInstanceChangeListener.onLiveInstanceChange() wakes up!");
    _delayedScheuler.schedule(new Runnable() {
      @Override
      public void run() {
        try {
          rebalanceCurrentCluster(_helixMirrorMakerManager.getCurrentLiveInstances(), true);
        } catch (Exception e) {
          LOGGER.error("Got exception during rebalance the whole cluster! ", e);
        }
      }
    }, _delayedAutoReblanceTimeInSeconds, TimeUnit.SECONDS);
  }

  public synchronized void rebalanceCurrentCluster(List<LiveInstance> liveInstances, boolean checkInstanceChange) {
    Context context = _rebalanceTimer.time();
    LOGGER.info("AutoRebalanceLiveInstanceChangeListener.rebalanceCurrentCluster() wakes up!");
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
      Set<InstanceTopicPartitionHolder> newAssignment = rescaleInstanceToTopicPartitionMap(liveInstances,
          instanceToTopicPartitionMap, unassignedTopicPartitions, checkInstanceChange);
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
  private boolean isAnyWorkerDown(List<LiveInstance> liveInstances,
      Map<String, Set<TopicPartition>> instanceToTopicPartitionMap) {
    Set<String> removedInstances =
        getRemovedInstanceSet(getLiveInstanceName(liveInstances),
            instanceToTopicPartitionMap.keySet());
    return !removedInstances.isEmpty();
  }

  private Set<InstanceTopicPartitionHolder> rescaleInstanceToTopicPartitionMap(List<LiveInstance> liveInstances,
      Map<String, Set<TopicPartition>> instanceToTopicPartitionMap, Set<TopicPartition> unassignedTopicPartitions,
      boolean checkInstanceChange) {
    Set<String> newInstances =
        getAddedInstanceSet(getLiveInstanceName(liveInstances),
            instanceToTopicPartitionMap.keySet());
    Set<String> removedInstances =
        getRemovedInstanceSet(getLiveInstanceName(liveInstances),
            instanceToTopicPartitionMap.keySet());
    if (checkInstanceChange && newInstances.isEmpty() && removedInstances.isEmpty()) {
      return null;
    }
    LOGGER.info("Trying to rescale cluster with new instances - " + Arrays.toString(
        newInstances.toArray(new String[0])) + " and removed instances - " + Arrays.toString(
        removedInstances.toArray(new String[0])) + " for unassigned partitions: " + unassignedTopicPartitions);
    TreeSet<InstanceTopicPartitionHolder> orderedInstances =
        new TreeSet<>(InstanceTopicPartitionHolder
            .getTotalWorkloadComparator(_helixMirrorMakerManager.getWorkloadInfoRetriever()));
    List<TopicPartition> tpiNeedsToBeAssigned = new ArrayList<>();
    tpiNeedsToBeAssigned.addAll(unassignedTopicPartitions);
    for (String instanceName : instanceToTopicPartitionMap.keySet()) {
      if (!removedInstances.contains(instanceName)) {
        InstanceTopicPartitionHolder instance = new InstanceTopicPartitionHolder(instanceName);
        instance.addTopicPartitions(instanceToTopicPartitionMap.get(instanceName));
        orderedInstances.add(instance);
      } else {
        tpiNeedsToBeAssigned.addAll(instanceToTopicPartitionMap.get(instanceName));
      }
    }
    for (String instanceName : newInstances) {
      orderedInstances.add(new InstanceTopicPartitionHolder(instanceName));
    }
    Set<InstanceTopicPartitionHolder> balanceAssignment = assignPartitions(orderedInstances, tpiNeedsToBeAssigned,
        !newInstances.isEmpty());
    return balanceAssignment;
  }

  private void assignIdealStates(HelixManager helixManager,
      Map<String, IdealState> idealStatesFromAssignment) {
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    String helixClusterName = helixManager.getClusterName();
    for (String topic : idealStatesFromAssignment.keySet()) {
      IdealState idealState = idealStatesFromAssignment.get(topic);
      helixAdmin.setResourceIdealState(helixClusterName, topic, idealState);
    }
  }

  private Set<InstanceTopicPartitionHolder> assignPartitions(TreeSet<InstanceTopicPartitionHolder> orderedInstances,
      List<TopicPartition> partitionsToBeAssigned, boolean forced) {
    if (orderedInstances.isEmpty()) {
      LOGGER.error("No workers to take " + partitionsToBeAssigned.size() + " partitions");
      return orderedInstances;
    }
    // if the current assignment is overloaded on some workers, re-assign some partitions of these workers
    List<TopicPartition> reassignedPartitions = removeOverloadedParitions(orderedInstances, partitionsToBeAssigned,
        forced);

    partitionsToBeAssigned.addAll(reassignedPartitions);
    if (partitionsToBeAssigned.isEmpty()) {
      return orderedInstances;
    }
    // sort partitions based on workload in reverse order (high -> low)
    Collections.sort(partitionsToBeAssigned,
        Collections
            .reverseOrder(TopicPartition.getWorkloadComparator(_helixMirrorMakerManager.getWorkloadInfoRetriever())));
    // assign partitions of the same topic to different workers if possible
    // TODO: check whether the other partitions of a new assigned topic is already assigned to the worker
    List<TopicPartition> sameTopic = new ArrayList<>();
    List<InstanceTopicPartitionHolder> lowestInstances = new ArrayList<>();
    for (int i = 0; i < partitionsToBeAssigned.size(); ) {
      sameTopic.clear();
      lowestInstances.clear();

      TopicPartition tp = partitionsToBeAssigned.get(i);
      sameTopic.add(tp);
      i++;
      while (i < partitionsToBeAssigned.size() && partitionsToBeAssigned.get(i).getTopic().equals(tp.getTopic())) {
        sameTopic.add(partitionsToBeAssigned.get(i));
        i++;
      }

      while (!orderedInstances.isEmpty() && lowestInstances.size() < sameTopic.size()) {
        lowestInstances.add(orderedInstances.pollFirst());
      }
      for (int j = 0; j < sameTopic.size(); j++) {
        lowestInstances.get(j % lowestInstances.size()).addTopicPartition(sameTopic.get(j));
      }
      orderedInstances.addAll(lowestInstances);
    }
    return orderedInstances;
  }

  /**
   * Take the assigned partitions from the instances if the worker is
   * overloaded.
   *
   * @param orderedInstances the instances ordered by total workload from low to high
   * @param partitionsToBeAssigned the unsigned partitions
   * @param forced force to take the assigned partitions from the workers that exceeds average workload if true;
   * otherwise, take the assigned partitions from the overloaded workers (considering _overloadedRatioThreshold)
   * @return the partitions taken from the instances. orderedInstances will be modified accordingly.
   */
  private List<TopicPartition> removeOverloadedParitions(TreeSet<InstanceTopicPartitionHolder> orderedInstances,
      List<TopicPartition> partitionsToBeAssigned, boolean forced) {
    WorkloadInfoRetriever retriever = _helixMirrorMakerManager.getWorkloadInfoRetriever();
    Set<String> topics = new HashSet<>();
    // calculate the total workload including the assigned and unassigned topics
    TopicWorkload totalWorkload = new TopicWorkload(0, 0, 0);
    for (TopicPartition tp : partitionsToBeAssigned) {
      String topic = tp.getTopic();
      if (!topics.contains(topic)) {
        totalWorkload.add(retriever.topicWorkload(topic));
        topics.add(topic);
      }
    }
    for (InstanceTopicPartitionHolder instance : orderedInstances) {
      for (TopicPartition tp : instance.getServingTopicPartitionSet()) {
        String topic = tp.getTopic();
        if (!topics.contains(topic)) {
          totalWorkload.add(retriever.topicWorkload(topic));
          topics.add(topic);
        }
      }
    }

    TopicWorkload averageWorkload = new TopicWorkload(totalWorkload.getBytesPerSecond() / orderedInstances.size(),
        totalWorkload.getMsgsPerSecond() / orderedInstances.size());
    // adjust average by excluding the instances that have a single partition but exceed the average workload
    // because the workload cannot be further divided to multiple workers
    int excludeInstances = 0;
    for (InstanceTopicPartitionHolder instance : orderedInstances) {
      Set<TopicPartition> partitions = instance.getServingTopicPartitionSet();
      if (partitions.size() != 1) {
        continue;
      }
      TopicWorkload workerWorkload = instance.totalWorkload(retriever);
      if (workerWorkload.compareTotal(averageWorkload) > 0) {
        excludeInstances++;
        totalWorkload.setMsgsPerSecond(totalWorkload.getMsgsPerSecond() - workerWorkload.getMsgsPerSecond());
        totalWorkload.setBytesPerSecond(totalWorkload.getBytesPerSecond() - workerWorkload.getBytesPerSecond());
      }
    }
    int numInstances = orderedInstances.size() - excludeInstances;
    if (numInstances > 0) {
      averageWorkload = new TopicWorkload(totalWorkload.getBytesPerSecond() / numInstances,
          totalWorkload.getMsgsPerSecond() / numInstances);
    }

    TopicWorkload maxWorkload = (forced && _overloadedRatioThreshold > 1.0) ? averageWorkload
        : new TopicWorkload(averageWorkload.getBytesPerSecond() * _overloadedRatioThreshold,
            averageWorkload.getMsgsPerSecond() * _overloadedRatioThreshold);

    List<TopicPartition> overloaded = new ArrayList<>();
    List<InstanceTopicPartitionHolder> processedInstances = new ArrayList<>();
    while (!orderedInstances.isEmpty()) {
      InstanceTopicPartitionHolder highest = orderedInstances.pollLast();
      processedInstances.add(highest);
      if (highest.getNumServingTopicPartitions() <= 1) {
        // no need to rebalance to other worker because it is a single partition
        continue;
      }
      TopicWorkload workerWorkload = highest.totalWorkload(retriever);
      if (workerWorkload.compareTotal(maxWorkload) <= 0) {
        break;
      }
      TopicWorkload diff = new TopicWorkload(workerWorkload.getBytesPerSecond() - averageWorkload.getBytesPerSecond(),
          workerWorkload.getMsgsPerSecond() - averageWorkload.getMsgsPerSecond());
      TopicWorkload workloadToRemove = new TopicWorkload(0, 0, 0);
      List<TopicPartition> partitions = new ArrayList<>(highest.getServingTopicPartitionSet());
      Collections.shuffle(partitions); // choose random partitions
      for (TopicPartition tp : partitions.subList(0, partitions.size() - 1)) {
        TopicWorkload tpWorkload = retriever.topicWorkload(tp.getTopic());
        workloadToRemove.add(tpWorkload.getBytesPerSecondPerPartition(), tpWorkload.getMsgsPerSecondPerPartition());
        highest.removeTopicPartition(tp);
        overloaded.add(tp);
        if (workloadToRemove.compareTotal(diff) >= 0) {
          break;
        }
      }
    }
    // re-sort the list after removing partitions
    orderedInstances.addAll(processedInstances);

    return overloaded;
  }

  private Set<String> getLiveInstanceName(List<LiveInstance> liveInstances) {
    Set<String> liveInstanceNames = new HashSet<String>();
    for (LiveInstance liveInstance : liveInstances) {
      liveInstanceNames.add(liveInstance.getInstanceName());
    }
    return liveInstanceNames;
  }

  private Set<String> getAddedInstanceSet(Set<String> liveInstances,
      Set<String> currentInstances) {

    Set<String> addedInstances = new HashSet<String>();
    addedInstances.addAll(liveInstances);
    addedInstances.removeAll(currentInstances);
    return addedInstances;
  }

  private Set<String> getRemovedInstanceSet(Set<String> liveInstances,
      Set<String> currentInstances) {
    Set<String> removedInstances = new HashSet<String>();
    removedInstances.addAll(currentInstances);
    removedInstances.removeAll(liveInstances);
    return removedInstances;
  }

}
