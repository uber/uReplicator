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
import java.util.HashMap;
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

  // if the difference between latest offset and commit offset is less than this
  // threshold, it is not considered as lag
  private static final long DEFAULT_MIN_LAG_OFFSET = 100000;
  private long _minLagOffset = DEFAULT_MIN_LAG_OFFSET;

  // if the difference between latest offset and commit offset is less than this
  // threshold in terms of ingestion time, it is not considered as lag
  private static final long DEFAULT_MIN_LAG_TIME_SEC = 300;
  private long _minLagTimeSec = DEFAULT_MIN_LAG_TIME_SEC;

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
        LOGGER.info("No assignment got changed, do nothing!");
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
    Set<InstanceTopicPartitionHolder> instances = new HashSet<>();
    List<TopicPartition> tpiNeedsToBeAssigned = new ArrayList<>();
    tpiNeedsToBeAssigned.addAll(unassignedTopicPartitions);
    for (String instanceName : instanceToTopicPartitionMap.keySet()) {
      if (!removedInstances.contains(instanceName)) {
        InstanceTopicPartitionHolder instance = new InstanceTopicPartitionHolder(instanceName);
        instance.addTopicPartitions(instanceToTopicPartitionMap.get(instanceName));
        instances.add(instance);
      } else {
        tpiNeedsToBeAssigned.addAll(instanceToTopicPartitionMap.get(instanceName));
      }
    }
    for (String instanceName : newInstances) {
      instances.add(new InstanceTopicPartitionHolder(instanceName));
    }
    Set<InstanceTopicPartitionHolder> balanceAssignment = balancePartitions(instances, tpiNeedsToBeAssigned,
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

  private Set<InstanceTopicPartitionHolder> balancePartitions(Set<InstanceTopicPartitionHolder> instances,
      List<TopicPartition> partitionsToBeAssigned, boolean forced) {
    if (instances.isEmpty()) {
      LOGGER.error("No workers to take " + partitionsToBeAssigned.size() + " partitions");
      return null;
    }
    List<TopicPartition> laggingPartitions = new ArrayList<>();
    List<TopicPartition> nonLaggingPartitions = new ArrayList<>();
    for (TopicPartition tp : partitionsToBeAssigned) {
      if (getLagTime(tp) > 0) {
        laggingPartitions.add(tp);
      } else {
        nonLaggingPartitions.add(tp);
      }
    }

    boolean assignmentChanged = false;

    // re-distribute the lagging partitions first
    ITopicWorkloadWeighter laggingPartitionWeighter = new ITopicWorkloadWeighter() {
      @Override
      public double partitionWeight(TopicPartition tp) {
        return getLagTime(tp) > 0 ? 1.0 : 0.0;
      }
    };
    TreeSet<InstanceTopicPartitionHolder> instancesSortedByLag = new TreeSet<>(InstanceTopicPartitionHolder
        .getTotalWorkloadComparator(_helixMirrorMakerManager.getWorkloadInfoRetriever(), laggingPartitionWeighter));
    instancesSortedByLag.addAll(instances);
    List<TopicPartition> reassignedLaggingPartitions = removeOverloadedParitions(instancesSortedByLag,
        laggingPartitions, null, true, laggingPartitionWeighter);
    laggingPartitions.addAll(reassignedLaggingPartitions);
    if (assignPartitions(instancesSortedByLag, laggingPartitions)) {
      assignmentChanged = true;
    }

    // if the current assignment is overloaded on some workers, re-assign some partitions of these workers
    ITopicWorkloadWeighter adjustedWeighter = new ITopicWorkloadWeighter() {
      @Override
      public double partitionWeight(TopicPartition tp) {
        // give 1.0 more weight for each minute lag up to 2 hour
        return 1.0 + Math.min(120, getLagTime(tp) / 60);
      }
    };
    // the lagging partitions should not be re-assigned again
    Set<TopicPartition> pinnedPartitions = new HashSet<>();
    for (InstanceTopicPartitionHolder instance : instances) {
      for (TopicPartition tp : instance.getServingTopicPartitionSet()) {
        if (getLagTime(tp) > 0) {
          pinnedPartitions.add(tp);
        }
      }
    }
    LOGGER.info("balancePartitions: Current lagging partitions: " + pinnedPartitions);
    TreeSet<InstanceTopicPartitionHolder> orderedInstances =
        new TreeSet<>(InstanceTopicPartitionHolder.getTotalWorkloadComparator(_helixMirrorMakerManager.getWorkloadInfoRetriever(), null));
    orderedInstances.addAll(instances);
    List<TopicPartition> reassignedPartitions = removeOverloadedParitions(orderedInstances,
        nonLaggingPartitions, pinnedPartitions, forced, adjustedWeighter);
    nonLaggingPartitions.addAll(reassignedPartitions);
    if (assignPartitions(orderedInstances, nonLaggingPartitions)) {
      assignmentChanged = true;
    }

    return assignmentChanged ? orderedInstances : null;
  }

  private boolean assignPartitions(TreeSet<InstanceTopicPartitionHolder> orderedInstances,
      List<TopicPartition> partitionsToBeAssigned) {
    if (orderedInstances.isEmpty() || partitionsToBeAssigned.isEmpty()) {
      return false;
    }
    // sort partitions based on workload in reverse order (high -> low)
    Collections.sort(partitionsToBeAssigned,
        Collections.reverseOrder(TopicPartition.getWorkloadComparator(_helixMirrorMakerManager.getWorkloadInfoRetriever())));
    // assign partitions of the same topic to different workers if possible

    List<TopicPartition> sameTopic = new ArrayList<>();
    List<InstanceTopicPartitionHolder> lowestInstances = new ArrayList<>();
    for (int i = 0; i < partitionsToBeAssigned.size();) {
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
    return true;
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
      List<TopicPartition> partitionsToBeAssigned, Set<TopicPartition> pinnedPartitions, boolean forced,
      ITopicWorkloadWeighter weighter) {
    List<TopicPartition> overloaded = new ArrayList<>();
    WorkloadInfoRetriever retriever = _helixMirrorMakerManager.getWorkloadInfoRetriever();
    TopicWorkload totalWorkload = new TopicWorkload(0, 0, 0);
    boolean noPartitions = true;
    for (TopicPartition tp : partitionsToBeAssigned) {
      double weight = weighter.partitionWeight(tp);
      // weight == 0 means the partition is not considered for workload
      if (weight > 0) {
        TopicWorkload tw = retriever.topicWorkload(tp.getTopic());
        totalWorkload.add(weight * tw.getBytesPerSecondPerPartition(), weight * tw.getMsgsPerSecondPerPartition());
        noPartitions = false;
      }
    }

    // instancesPartitionsCount count how many partitions are considered for workload for each instance
    Map<String, Integer> instancesPartitionsCount = new HashMap<>();
    for (InstanceTopicPartitionHolder instance : orderedInstances) {
      int partitionCount = 0;
      for (TopicPartition tp : instance.getServingTopicPartitionSet()) {
        double weight = weighter.partitionWeight(tp);
        if (weight > 0) {
          partitionCount++;
          TopicWorkload tw = retriever.topicWorkload(tp.getTopic());
          totalWorkload.add(weight * tw.getBytesPerSecondPerPartition(), weight * tw.getMsgsPerSecondPerPartition());
          noPartitions = false;
        }
      }
      instancesPartitionsCount.put(instance.getInstanceName(), partitionCount);
    }
    if (noPartitions) {
      return overloaded;
    }

    TopicWorkload averageWorkload = new TopicWorkload(totalWorkload.getBytesPerSecond() / orderedInstances.size(),
        totalWorkload.getMsgsPerSecond() / orderedInstances.size());
    // adjust average by excluding the instances that have a single partition but exceeds the average workload
    // because the workload cannot be further divided to multiple workers
    int excludeInstances = 0;
    for (InstanceTopicPartitionHolder instance : orderedInstances) {
      if (instancesPartitionsCount.get(instance.getInstanceName()) == 1) {
        for (TopicPartition tp : instance.getServingTopicPartitionSet()) {
          if (weighter.partitionWeight(tp) > 0) {
            double weight = weighter.partitionWeight(tp);
            TopicWorkload tw = retriever.topicWorkload(tp.getTopic());
            if (tw.compareTotal(averageWorkload) > 0) {
              excludeInstances++;
              totalWorkload.setMsgsPerSecond(totalWorkload.getMsgsPerSecond() - weight * tw.getMsgsPerSecondPerPartition());
              totalWorkload.setBytesPerSecond(totalWorkload.getBytesPerSecond() - weight * tw.getBytesPerSecondPerPartition());
            }
            break;
          }
        }
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

    List<InstanceTopicPartitionHolder> processedInstances = new ArrayList<>();
    while (!orderedInstances.isEmpty()) {
      InstanceTopicPartitionHolder highest = orderedInstances.pollLast();
      processedInstances.add(highest);
      if (instancesPartitionsCount.get(highest.getInstanceName()) <= 1) {
        // no need to rebalance to other worker because it is a single or no partition
        continue;
      }
      TopicWorkload workerWorkload = highest.totalWorkload(retriever, weighter);
      if (workerWorkload.compareTotal(maxWorkload) <= 0) {
        break;
      }
      TopicWorkload diff = new TopicWorkload(workerWorkload.getBytesPerSecond() - averageWorkload.getBytesPerSecond(),
          workerWorkload.getMsgsPerSecond() - averageWorkload.getMsgsPerSecond());
      TopicWorkload workloadToRemove = new TopicWorkload(0, 0, 0);
      List<TopicPartition> partitions = new ArrayList<>(highest.getServingTopicPartitionSet());
      Collections.shuffle(partitions); // choose random partitions
      for (TopicPartition tp : partitions) {
        double weight = weighter.partitionWeight(tp);
        if (weight == 0 || pinnedPartitions != null && pinnedPartitions.contains(tp)) {
          continue;
        }
        TopicWorkload tpWorkload = retriever.topicWorkload(tp.getTopic());
        workloadToRemove.add(weight * tpWorkload.getBytesPerSecondPerPartition(),
            weight * tpWorkload.getMsgsPerSecondPerPartition());
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

  /**
   * Return the lagging time if the given partition has lag.
   *
   * @param tp topic partition
   * @return the lagging time in seconds if the given partition has lag; otherwise return 0.
   */
  private long getLagTime(TopicPartition tp) {
    TopicPartitionLag tpl = _helixMirrorMakerManager.getOffsetMonitor().getTopicPartitionOffset(tp);
    if (tpl == null || tpl.getLatestOffset() < 0 || tpl.getCommitOffset() < 0) {
      return 0;
    }
    long lag = tpl.getLatestOffset() - tpl.getCommitOffset();
    if (lag <= _minLagOffset) {
      return 0;
    }
    double msgRate = _helixMirrorMakerManager.getWorkloadInfoRetriever().topicWorkload(tp.getTopic())
        .getMsgsPerSecondPerPartition();
    if (msgRate < 1) {
      msgRate = 1;
    }
    double lagTime = lag / msgRate;
    if (lagTime > _minLagTimeSec) {
      return Math.round(lagTime);
    }
    return 0;
  }

}
