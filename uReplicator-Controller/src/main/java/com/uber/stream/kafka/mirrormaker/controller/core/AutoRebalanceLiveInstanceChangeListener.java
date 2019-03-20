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
package com.uber.stream.kafka.mirrormaker.controller.core;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.uber.stream.kafka.mirrormaker.common.core.ITopicWorkloadWeighter;
import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.common.core.TopicWorkload;
import com.uber.stream.kafka.mirrormaker.common.core.WorkloadInfoRetriever;
import com.uber.stream.kafka.mirrormaker.common.utils.HelixUtils;
import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import com.uber.stream.kafka.mirrormaker.common.modules.TopicPartitionLag;
import com.uber.stream.kafka.mirrormaker.controller.reporter.HelixKafkaMirrorMakerMetricsReporter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import kafka.common.TopicAndPartition;
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
  private final Counter _numIdleInstances = new Counter();
  private final Counter _numBlacklistedInstances = new Counter();
  private final Meter _rebalanceRate = new Meter();
  private final Timer _rebalanceTimer = new Timer();

  private final int _maxWorkingInstances;

  private final int _delayedAutoReblanceTimeInSeconds;

  // If the workload of a worker compared to average workload is more than the
  // threshold, it is considered as overloaded. For example with 1.2, if average
  // workload is 5MB/s, if a worker has workload higher than 5 * 1.2 = 6MB/s
  // then it is considered to be overloaded. The larger the threshold, the more
  // unbalancing is allowed but it will trigger less partition re-assignments.
  private final double _overloadedRatioThreshold;


  // the maximum ratio of instances can be used as dedicated for lagging partitions
  private final double _maxDedicatedInstancesRatio;


  private long _lastRebalanceTimeMillis = 0;

  private final int _maxStuckPartitionMovements;
  private final long _movePartitionAfterStuckMillis;

  private Map<TopicPartition, List<Long>> _movePartitionHistoryMap = new HashMap<>();

  public AutoRebalanceLiveInstanceChangeListener(HelixMirrorMakerManager helixMirrorMakerManager,
      HelixManager helixManager, ControllerConf controllerConf) {
    _helixMirrorMakerManager = helixMirrorMakerManager;
    _helixManager = helixManager;
    _maxWorkingInstances = controllerConf.getMaxWorkingInstances();
    _delayedAutoReblanceTimeInSeconds = controllerConf.getAutoRebalanceDelayInSeconds();
    _overloadedRatioThreshold = controllerConf.getAutoRebalanceWorkloadRatioThreshold();
    _maxDedicatedInstancesRatio = controllerConf.getMaxDedicatedLaggingInstancesRatio();
    _maxStuckPartitionMovements = controllerConf.getMaxStuckPartitionMovements();
    _movePartitionAfterStuckMillis = TimeUnit.MINUTES.toMillis(controllerConf.getMoveStuckPartitionAfterMinutes());
    LOGGER.info("Delayed Auto Reblance Time In Seconds: {}", _delayedAutoReblanceTimeInSeconds);
    registerMetrics();

    int autoRebalancePeriodInSeconds = controllerConf.getAutoRebalancePeriodInSeconds();
    final int minIntervalInSeconds = controllerConf.getAutoRebalanceMinIntervalInSeconds();
    if (autoRebalancePeriodInSeconds > 0) {
      LOGGER.info("Trying to schedule auto rebalancing at rate " + autoRebalancePeriodInSeconds + " seconds");
      _delayedScheuler.scheduleWithFixedDelay(
          new Runnable() {
            @Override
            public void run() {
              try {
                if (_helixMirrorMakerManager.getWorkloadInfoRetriever().isInitialized()
                    && System.currentTimeMillis() - _lastRebalanceTimeMillis > 1000L * minIntervalInSeconds) {
                  rebalanceCurrentCluster(_helixMirrorMakerManager.getCurrentLiveInstances(),
                      _helixMirrorMakerManager.getBlacklistedInstances(), false, false);
                }
              } catch (Exception e) {
                LOGGER.error("Got exception during periodically rebalancing the whole cluster! ", e);
              }
            }
          }, Math.max(_delayedAutoReblanceTimeInSeconds, autoRebalancePeriodInSeconds), autoRebalancePeriodInSeconds,
          TimeUnit.SECONDS);
    }
  }

  public void stop() {
    LOGGER.info("Stopping AutoRebalanceLiveInstanceChangeListener");
    _delayedScheuler.shutdown();
  }

  private void registerMetrics() {
    try {
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("worker.liveInstances",
          _numLiveInstances);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("worker.idleInstances",
          _numIdleInstances);
      HelixKafkaMirrorMakerMetricsReporter.get().registerMetric("worker.blacklistedInstances",
          _numBlacklistedInstances);
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
          rebalanceCurrentCluster(_helixMirrorMakerManager.getCurrentLiveInstances(),
              _helixMirrorMakerManager.getBlacklistedInstances(), true, false);
        } catch (Exception e) {
          LOGGER.error("Got exception during rebalance the whole cluster! ", e);
        }
      }
    }, _delayedAutoReblanceTimeInSeconds, TimeUnit.SECONDS);
  }

  public boolean triggerRebalanceCluster() {
    try {
      if (!_helixMirrorMakerManager.getWorkloadInfoRetriever().isInitialized()) {
        return false;
      }
      rebalanceCurrentCluster(_helixMirrorMakerManager.getCurrentLiveInstances(),
          _helixMirrorMakerManager.getBlacklistedInstances(), false, true);
      return true;
    } catch (Exception e) {
      LOGGER.error("Got exception during manually triggering rebalance the whole cluster! ", e);
      return false;
    }
  }

  private synchronized void rebalanceCurrentCluster(List<LiveInstance> liveInstances,
      List<String> excludedInstances, boolean checkInstanceChange, boolean forced) {
    Context context = _rebalanceTimer.time();
    LOGGER.info("AutoRebalanceLiveInstanceChangeListener.rebalanceCurrentCluster() wakes up!");
    try {
      _numLiveInstances.inc(liveInstances.size() - _numLiveInstances.getCount());
      _numBlacklistedInstances.inc(excludedInstances.size() - _numBlacklistedInstances.getCount());
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
      Set<TopicPartition> unassignedTopicPartitions = HelixUtils.getUnassignedPartitions(_helixManager);
      if (instanceToTopicPartitionMap.isEmpty() && unassignedTopicPartitions.isEmpty()) {
        LOGGER.info("No topic got assigned yet, do nothing!");
        return;
      }
      Set<TopicPartition> blacklistedTopicPartitions = _helixMirrorMakerManager.getTopicPartitionBlacklist();
      Set<InstanceTopicPartitionHolder> newAssignment = rescaleInstanceToTopicPartitionMap(liveInstances,
          excludedInstances, instanceToTopicPartitionMap, unassignedTopicPartitions, blacklistedTopicPartitions, checkInstanceChange, forced);
      if (newAssignment == null) {
        LOGGER.info("No assignment got changed, do nothing!");
        return;
      }
      LOGGER.info("New assignment: " + newAssignment);
      _lastRebalanceTimeMillis = System.currentTimeMillis();
      Map<String, IdealState> idealStatesFromAssignment =
          HelixUtils.getIdealStatesFromAssignment(newAssignment, blacklistedTopicPartitions);
      LOGGER.info("Trying to assign new IdealStatesMap!");
      assignIdealStates(_helixManager, idealStatesFromAssignment);

      _helixMirrorMakerManager.updateCurrentServingInstance();
      _rebalanceRate.mark();
    } finally {
      context.close();
    }
  }

  private Set<InstanceTopicPartitionHolder> rescaleInstanceToTopicPartitionMap(
      List<LiveInstance> liveInstances,
      List<String> excludedInstances,
      Map<String, Set<TopicPartition>> instanceToTopicPartitionMap,
      Set<TopicPartition> unassignedTopicPartitions,
      Set<TopicPartition> blacklistedTopicPartitions,
      boolean checkInstanceChange,
      boolean forced) {
    Set<String> liveInstanceNames = getLiveInstanceName(liveInstances, excludedInstances);
    // removedInstances contains the excludedInstances that currently have workload too
    Set<String> removedInstances = getRemovedInstanceSet(liveInstanceNames, instanceToTopicPartitionMap.keySet());
    Set<String> idleInstances = getIdleInstanceSet(liveInstanceNames, instanceToTopicPartitionMap);

    int numIdleInstancesToAssign;
    if (_maxWorkingInstances == 0) {
      numIdleInstancesToAssign = idleInstances.size();
    } else {
      numIdleInstancesToAssign = _maxWorkingInstances - (instanceToTopicPartitionMap.size() - removedInstances.size());
      if (numIdleInstancesToAssign < 0) {
        numIdleInstancesToAssign = 0;
      } else if (numIdleInstancesToAssign > idleInstances.size()) {
        numIdleInstancesToAssign = idleInstances.size();
      }
    }

    if (numIdleInstancesToAssign > 0) {
      _numIdleInstances.inc(idleInstances.size() - numIdleInstancesToAssign - _numIdleInstances.getCount());
    } else {
      _numIdleInstances.inc(idleInstances.size() - _numIdleInstances.getCount());
    }

    if (!forced && numIdleInstancesToAssign <= 0 && checkInstanceChange && removedInstances.isEmpty()
        && (_maxWorkingInstances <= 0 || _maxWorkingInstances >= instanceToTopicPartitionMap.size())) {
      // no instance change
      return null;
    }

    LOGGER.info("Trying to rescale cluster with " + liveInstanceNames.size() + " live instances ("
        + excludedInstances.size() + " excluded - " + excludedInstances + "), using " + numIdleInstancesToAssign
        + " out of " + idleInstances.size() + " idle instances, and removed " + removedInstances.size()
        + " instances - " + Arrays.toString(removedInstances.toArray(new String[0])) + " for unassigned partitions: "
        + unassignedTopicPartitions);
    Set<InstanceTopicPartitionHolder> instances = new HashSet<>();
    List<TopicPartition> tpiNeedsToBeAssigned = new ArrayList<>();
    tpiNeedsToBeAssigned.addAll(unassignedTopicPartitions);

    boolean assignmentChanged = false;
    for (Map.Entry<String, Set<TopicPartition>> entry : instanceToTopicPartitionMap.entrySet()) {
      String instanceName = entry.getKey();
      if (entry.getValue().isEmpty()) {
        // it is an idle instance, do nothing
      } else if (!removedInstances.contains(instanceName)) {
        // keep the instance assignment as it
        InstanceTopicPartitionHolder instance = new InstanceTopicPartitionHolder(instanceName);
        instance.addTopicPartitions(entry.getValue());
        instances.add(instance);
      } else if (!idleInstances.isEmpty() && numIdleInstancesToAssign > 0) {
        // assign all the workload to another idle instance
        String idleInstanceToAssign = idleInstances.iterator().next();
        idleInstances.remove(idleInstanceToAssign);
        numIdleInstancesToAssign--;
        InstanceTopicPartitionHolder instance = new InstanceTopicPartitionHolder(idleInstanceToAssign);
        instance.addTopicPartitions(entry.getValue());
        instances.add(instance);
        assignmentChanged = true;
        LOGGER.info("Move workload from instance " + instanceName + " to " + idleInstanceToAssign + ": "
            + entry.getValue());
      } else {
        // assign the workload to all instances
        tpiNeedsToBeAssigned.addAll(entry.getValue());
      }
    }

    if (_maxWorkingInstances > 0 && instances.size() > _maxWorkingInstances) {
      // there are working instances more than the expected number, move them to idle pool
      Iterator<InstanceTopicPartitionHolder> iter = instances.iterator();
      while (instances.size() > _maxWorkingInstances && iter.hasNext()) {
        InstanceTopicPartitionHolder itph = iter.next();
        LOGGER.info("Move workload from instance " + itph.getInstanceName() + " to become idle: "
            + itph.getServingTopicPartitionSet());
        tpiNeedsToBeAssigned.addAll(itph.getServingTopicPartitionSet());
        iter.remove();
      }
    } else if (numIdleInstancesToAssign > 0) {
      // put more idle instances to the list for rebalancing
      for (String instanceName : idleInstances) {
        if (numIdleInstancesToAssign <= 0) {
          break;
        }
        instances.add(new InstanceTopicPartitionHolder(instanceName));
        forced = true; // force to rebalance so as to assign partitions to the idle instances
        numIdleInstancesToAssign--;
      }
    }

    if (balancePartitions(instances, tpiNeedsToBeAssigned, blacklistedTopicPartitions, forced)) {
      assignmentChanged = true;
    }

    // add empty task for the removed/excluded instances
    for (String ri : removedInstances) {
      instances.add(new InstanceTopicPartitionHolder(ri));
      assignmentChanged = true;
    }
    return assignmentChanged ? instances : null;
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

  /**
   * Move stuck partitions to other instances.
   *
   * @param instances non-empty set of current instances
   * @return the partitions that have been moved. Null if no movement happens.
   */
  private Set<TopicPartition> moveStuckPartitions(Set<InstanceTopicPartitionHolder> instances) {
    if (_maxStuckPartitionMovements <= 0 || _movePartitionAfterStuckMillis <= 0) {
      return null;
    }

    Set<TopicPartition> allStuckPartitions = getStuckTopicPartitions();
    if (allStuckPartitions.isEmpty()) {
      _movePartitionHistoryMap.clear();
      return null;
    }

    // clean up move history if the partition has progress
    Iterator<Entry<TopicPartition, List<Long>>> iter = _movePartitionHistoryMap.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<TopicPartition, List<Long>> entry = iter.next();
      if (!allStuckPartitions.contains(entry.getKey())) {
        iter.remove();
      }
    }

    // find the corresponding stuck instances
    Set<TopicPartition> stuckPartitionsToMove = new HashSet<>();
    TreeSet<InstanceTopicPartitionHolder> nonStuckInstances = new TreeSet<>(InstanceTopicPartitionHolder
        .perPartitionWorkloadComparator(_helixMirrorMakerManager.getWorkloadInfoRetriever(), null));
    long now = System.currentTimeMillis();
    for (InstanceTopicPartitionHolder itph : instances) {
      boolean isStuckInstance = false;
      for (TopicPartition tp : allStuckPartitions) {
        if (itph.getServingTopicPartitionSet().contains(tp)) {
          isStuckInstance = true;
          List<Long> moveHistory = _movePartitionHistoryMap.get(tp);
          if (moveHistory == null) {
            moveHistory = new ArrayList<>();
            _movePartitionHistoryMap.put(tp, moveHistory);
          } else if (moveHistory.size() >= _maxStuckPartitionMovements) {
            LOGGER.info("moveStuckPartitions: Skip moving stuck partition " + tp + " from "
                + itph.getInstanceName() + " because moving reaches upper bound of " + _maxStuckPartitionMovements);
            continue;
          } else if (!moveHistory.isEmpty()
              && moveHistory.get(moveHistory.size() - 1) + _movePartitionAfterStuckMillis > now) {
            LOGGER.info("moveStuckPartitions: Skip moving stuck partition " + tp + " from "
                + itph.getInstanceName() + " because it was moved recently");
            continue;
          }
          LOGGER.info("moveStuckPartitions: Trying to move stuck partition " + tp + " from " + itph.getInstanceName());
          moveHistory.add(now);
          itph.removeTopicPartition(tp);
          stuckPartitionsToMove.add(tp);
        }
      }
      if (!isStuckInstance) {
        nonStuckInstances.add(itph);
      }
    }

    if (stuckPartitionsToMove.isEmpty()) {
      LOGGER.info("moveStuckPartitions: No stuck partitions can be moved");
      return null;
    }

    // try to move the partitions to non-stuck instances
    if (nonStuckInstances.isEmpty()) {
      // No non-stuck instance. Shuffle the stuck partitions across all
      // instances but some partitions may still stay at the same instance
      LOGGER.info("moveStuckPartitions: All instances are stuck. Shuffle the stuck partitions instead");
      nonStuckInstances.addAll(instances);
    }
    assignPartitions(nonStuckInstances, new ArrayList<>(stuckPartitionsToMove));

    return stuckPartitionsToMove;
  }

  private boolean balancePartitions(
      Set<InstanceTopicPartitionHolder> instances,
      List<TopicPartition> partitionsToBeAssigned,
      Set<TopicPartition> blacklistedTopicPartitions,
      boolean forced) {
    if (instances.isEmpty()) {
      LOGGER.error("No workers to take " + partitionsToBeAssigned.size() + " partitions");
      return false;
    }

    boolean assignmentChanged = false;

    Set<TopicPartition> stuckPartitionsMoved = moveStuckPartitions(instances);
    if (stuckPartitionsMoved != null && !stuckPartitionsMoved.isEmpty()) {
      assignmentChanged = true;
    }

    // collect lag information for all topic partitions
    final Map<TopicPartition, Long> lagTimeMap = new HashMap<>();
    List<TopicPartition> laggingPartitions = new ArrayList<>();
    List<TopicPartition> nonLaggingPartitions = new ArrayList<>();
    for (TopicPartition tp : partitionsToBeAssigned) {
      if (blacklistedTopicPartitions.contains(tp)) {
        LOGGER.info("%s in blacklist, skip assignment", tp.toString());
        continue;
      }

      TopicPartitionLag lagInfo = _helixMirrorMakerManager.calculateLagTime(tp);
      if (lagInfo != null && lagInfo.getLag() > 0) {
        lagTimeMap.put(tp, lagInfo.getLag());
        laggingPartitions.add(tp);
      } else {
        nonLaggingPartitions.add(tp);
      }
    }
    for (InstanceTopicPartitionHolder instance : instances) {
      for (TopicPartition tp : instance.getServingTopicPartitionSet()) {
        TopicPartitionLag lagInfo = _helixMirrorMakerManager.calculateLagTime(tp);
        if (lagInfo != null && lagInfo.getLag() > 0) {
          lagTimeMap.put(tp, lagInfo.getLag());
        }
      }
    }
    LOGGER.info("balancePartitions: Current lagging partitions: " + lagTimeMap);

    // re-distribute the lagging partitions first
    ITopicWorkloadWeighter laggingPartitionWeighter = (TopicPartition tp) -> lagTimeMap.containsKey(tp) ? 1.0 : 0.0;

    TreeSet<InstanceTopicPartitionHolder> instancesSortedByLag = new TreeSet<>(InstanceTopicPartitionHolder
        .perPartitionWorkloadComparator(_helixMirrorMakerManager.getWorkloadInfoRetriever(), laggingPartitionWeighter));
    instancesSortedByLag.addAll(instances);
    List<TopicPartition> reassignedLaggingPartitions = removeOverloadedParitions(instancesSortedByLag,
        laggingPartitions, null, true, laggingPartitionWeighter);
    laggingPartitions.addAll(reassignedLaggingPartitions);

    if (assignPartitions(instancesSortedByLag, laggingPartitions)) {
      assignmentChanged = true;
    }

    // dedicated instances serve only lagging partitions
    int maxDedicated = (int) (instances.size() * _maxDedicatedInstancesRatio);
    TreeSet<InstanceTopicPartitionHolder> orderedInstances = new TreeSet<>(InstanceTopicPartitionHolder
        .perPartitionWorkloadComparator(_helixMirrorMakerManager.getWorkloadInfoRetriever(), null));
    // instancesSortedByLag are sorted by lags, so the instances with lags appear after the instances with non-lags only
    List<InstanceTopicPartitionHolder> dedicatedInstances = new ArrayList<>();
    for (InstanceTopicPartitionHolder instance : instancesSortedByLag) {
      if (dedicatedInstances.size() > maxDedicated) {
        orderedInstances.add(instance);
      } else {
        boolean hasLag = false;
        for (TopicPartition tp : instance.getServingTopicPartitionSet()) {
          if (lagTimeMap.containsKey(tp)) {
            hasLag = true;
            break;
          }
        }
        if (hasLag) {
          // this instance is a dedicated one for lagging partitions only
          dedicatedInstances.add(instance);
          for (TopicPartition tp : instance.getServingTopicPartitionSet()) {
            if (!lagTimeMap.containsKey(tp)) {
              instance.removeTopicPartition(tp);
              nonLaggingPartitions.add(tp);
            }
          }
        } else {
          orderedInstances.add(instance);
        }
      }
    }
    if (!dedicatedInstances.isEmpty()) {
      LOGGER.info("balancePartitions: dedicated instances: " + dedicatedInstances);
    }

    // if the current assignment is overloaded on some workers, re-assign some partitions of these workers
    ITopicWorkloadWeighter adjustedWeighter = new ITopicWorkloadWeighter() {
      @Override
      public double partitionWeight(TopicPartition tp) {
        // give 1.0 more weight for each minute lag up to 2 hour
        Long lag = lagTimeMap.get(tp);
        if (lag == null) {
          return 1.0;
        }
        return 1.0 + Math.min(120, lag / 60);
      }
    };

    Set<TopicPartition> pinnedPartitions = new HashSet<>();
    pinnedPartitions.addAll(lagTimeMap.keySet());
    if (stuckPartitionsMoved != null) {
      pinnedPartitions.addAll(stuckPartitionsMoved);
    }

    List<TopicPartition> reassignedPartitions = removeOverloadedParitions(orderedInstances,
        nonLaggingPartitions, pinnedPartitions, forced, adjustedWeighter);
    nonLaggingPartitions.addAll(reassignedPartitions);
    if (assignPartitions(orderedInstances, nonLaggingPartitions)) {
      assignmentChanged = true;
    }

    return assignmentChanged;
  }

  private boolean assignPartitions(TreeSet<InstanceTopicPartitionHolder> orderedInstances,
      List<TopicPartition> partitionsToBeAssigned) {
    if (orderedInstances.isEmpty() || partitionsToBeAssigned.isEmpty()) {
      return false;
    }
    // sort partitions based on workload in reverse order (high -> low)
    Collections.sort(partitionsToBeAssigned,
        Collections.reverseOrder(
            TopicPartition.getWorkloadComparator(_helixMirrorMakerManager.getWorkloadInfoRetriever())));
    // assign partitions of the same topic to different workers if possible

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
              totalWorkload.setMsgsPerSecond(
                  totalWorkload.getMsgsPerSecond() - weight * tw.getMsgsPerSecondPerPartition());
              totalWorkload.setBytesPerSecond(
                  totalWorkload.getBytesPerSecond() - weight * tw.getBytesPerSecondPerPartition());
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
      TopicWorkload diff = new TopicWorkload(workerWorkload.getBytesPerSecond() - maxWorkload.getBytesPerSecond(),
          workerWorkload.getMsgsPerSecond() - maxWorkload.getMsgsPerSecond());
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

  private Set<String> getLiveInstanceName(List<LiveInstance> liveInstances, List<String> excludedInstances) {
    Set<String> liveInstanceNames = new HashSet<>();
    for (LiveInstance liveInstance : liveInstances) {
      if (!excludedInstances.contains(liveInstance.getInstanceName())) {
        liveInstanceNames.add(liveInstance.getInstanceName());
      }
    }
    return liveInstanceNames;
  }

  private Set<String> getIdleInstanceSet(Set<String> liveInstances,
      Map<String, Set<TopicPartition>> instanceToTopicPartitionMap) {
    Set<String> idleInstances = new HashSet<>();
    for (String instance : liveInstances) {
      Set<TopicPartition> tps = instanceToTopicPartitionMap.get(instance);
      if (tps == null || tps.isEmpty()) {
        idleInstances.add(instance);
      }
    }
    return idleInstances;
  }

  private Set<String> getRemovedInstanceSet(Set<String> liveInstances, Set<String> currentInstances) {
    Set<String> removedInstances = new HashSet<>();
    removedInstances.addAll(currentInstances);
    removedInstances.removeAll(liveInstances);
    return removedInstances;
  }


  /**
   * Get stuck topic partitions via offset manager.
   *
   * @return the topic partitions that have been stuck for at least _movePartitionAfterStuckMillis.
   */
  private Set<TopicPartition> getStuckTopicPartitions() {
    Set<TopicPartition> partitions = new HashSet<>();
    if (_movePartitionAfterStuckMillis <= 0) {
      return partitions;
    }
    Map<TopicAndPartition, TopicPartitionLag> noProgressMap = _helixMirrorMakerManager.getOffsetMonitor()
        .getNoProgressTopicToOffsetMap();
    long now = System.currentTimeMillis();
    for (Map.Entry<TopicAndPartition, TopicPartitionLag> entry : noProgressMap.entrySet()) {
      TopicPartitionLag lastLag = entry.getValue();
      if (now - lastLag.getTimeStamp() > _movePartitionAfterStuckMillis) {
        partitions.add(new TopicPartition(entry.getKey().topic(), entry.getKey().partition()));
      }
    }
    return partitions;
  }

}
