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
package com.uber.stream.kafka.mirrormaker.common.core;

import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * InstanceTopicPartitionHolder is a wrapper for instance and the topicPartitionSet it's holding.
 */
public class InstanceTopicPartitionHolder {

  private static final String SEPARATOR = "@";

  private final String _instanceName;
  private final Set<TopicPartition> _topicPartitionSet = new HashSet<>();
  private final TopicPartition _route;
  private final Set<String> _workerSet = new HashSet<>();
  private int _totalNumPartitions = 0;

  public InstanceTopicPartitionHolder(String instance) {
    this(instance, null);
  }

  public InstanceTopicPartitionHolder(String instance, TopicPartition route) {
    _instanceName = instance;
    _route = route;
  }

  public String getInstanceName() {
    return _instanceName;
  }

  public TopicPartition getRoute() {
    return _route;
  }

  public String getRouteString() {
    return _route.getTopic() + "@" + _route.getPartition();
  }

  public Set<String> getWorkerSet() {
    return _workerSet;
  }

  public Set<TopicPartition> getServingTopicPartitionSet() {
    return ImmutableSet.copyOf(_topicPartitionSet);
  }

  public int getNumServingTopicPartitions() {
    return _topicPartitionSet.size();
  }

  public int getTotalNumPartitions() {
    return _totalNumPartitions;
  }

  public void addTopicPartition(TopicPartition topicPartitionInfo) {
    _topicPartitionSet.add(topicPartitionInfo);
    _totalNumPartitions += topicPartitionInfo.getPartition();
  }

  public void removeTopicPartition(TopicPartition topicPartitionInfo) {
    if (_topicPartitionSet.contains(topicPartitionInfo)) {
      _topicPartitionSet.remove(topicPartitionInfo);
      _totalNumPartitions -= topicPartitionInfo.getPartition();
    }
  }

  public void clearTopicPartitions() {
    _topicPartitionSet.clear();
    _totalNumPartitions = 0;
  }

  public void addWorker(String worker) {
    _workerSet.add(worker);
  }

  public void addWorkers(Collection<String> workers) {
    _workerSet.addAll(workers);
  }

  public void removeWorkers(Collection<String> workers) {
    _workerSet.removeAll(workers);
  }

  /*public TopicWorkload totalWorkload(WorkloadInfoRetriever infoRetriever, ITopicWorkloadWeighter weighter) {
    TopicWorkload total = new TopicWorkload(0, 0, 0);
    for (TopicPartition part : _topicPartitionSet) {
      TopicWorkload tw = infoRetriever.topicWorkload(part.getTopic());
      double weight = (weighter == null) ? 1.0 : weighter.partitionWeight(part);
      total.add(tw.getBytesPerSecondPerPartition() * weight, tw.getMsgsPerSecondPerPartition() * weight);
    }
    return total;
  }*/

  public TopicWorkload totalWorkload(WorkloadInfoRetriever infoRetriever, ITopicWorkloadWeighter weighter) {
    return totalWorkload(infoRetriever, weighter, true);
  }

  public TopicWorkload totalWorkload(WorkloadInfoRetriever infoRetriever, ITopicWorkloadWeighter weighter, boolean perPartition) {
    TopicWorkload total = new TopicWorkload(0, 0, 0);
    for (TopicPartition part : _topicPartitionSet) {
      TopicWorkload tw = infoRetriever.topicWorkload(part.getTopic());
      double weight = (weighter == null) ? 1.0 : weighter.partitionWeight(part);
      if (perPartition) {
        total.add(tw.getBytesPerSecondPerPartition() * weight, tw.getMsgsPerSecondPerPartition() * weight);
      } else {
        total.add(tw.getBytesPerSecond() * weight, tw.getMsgsPerSecond() * weight);
      }
    }
    return total;
  }

  public static Comparator<InstanceTopicPartitionHolder> perPartitionWorkloadComparator(
      final WorkloadInfoRetriever infoRetriever, final ITopicWorkloadWeighter weighter) {
    return (InstanceTopicPartitionHolder o1, InstanceTopicPartitionHolder o2) -> {
      if (infoRetriever != null) {
        TopicWorkload workload1 = (o1 == null) ? new TopicWorkload(0, 0) : o1.totalWorkload(infoRetriever, weighter, true);
        TopicWorkload workload2 = (o2 == null) ? new TopicWorkload(0, 0) : o2.totalWorkload(infoRetriever, weighter, true);
        int cmp = workload1.compareTotal(workload2);
        if (cmp != 0) {
          return cmp;
        }
      }
      return compareNumServingTopicPartitions(o1, o2);
    };
  }

  public static Comparator<InstanceTopicPartitionHolder> totalWorkloadComparator(
      final Map<String, TopicWorkload> topicWorkloadMap) {
    return (InstanceTopicPartitionHolder o1, InstanceTopicPartitionHolder o2) -> {
      if (topicWorkloadMap != null) {
        TopicWorkload workload1 = topicWorkloadMap.containsKey(o1.getRouteString()) ? topicWorkloadMap.get(o1.getRouteString()) : new TopicWorkload(0, 0);
        TopicWorkload workload2 = topicWorkloadMap.containsKey(o2.getRouteString()) ? topicWorkloadMap.get(o2.getRouteString()) : new TopicWorkload(0, 0);
        int cmp = workload1.compareTotal(workload2);
        if (cmp != 0) {
          return cmp;
        }
      }
      return compareNumServingTopicPartitions(o1, o2);
    };
  }

  private static int compareNumServingTopicPartitions(InstanceTopicPartitionHolder o1, InstanceTopicPartitionHolder o2) {
    // if workload is the same, compare them based on the number of partitions
    int size1 = (o1 == null) ? -1 : o1.getNumServingTopicPartitions();
    int size2 = (o2 == null) ? -1 : o2.getNumServingTopicPartitions();
    if (size1 != size2) {
      return size1 - size2;
    } else {
      return o1.getInstanceName().compareTo(o2.getInstanceName());
    }
  }

  public void addTopicPartitions(Collection<TopicPartition> topicPartitionInfos) {
    for (TopicPartition tp : topicPartitionInfos) {
      addTopicPartition(tp);
    }
  }

  @Override
  public String toString() {
    if (_route == null) {
      return String.format("{%s=%s}", _instanceName, _topicPartitionSet);
    } else {
      return String.format("{%s,%s,topics:%s,workers:%s}",
          _instanceName, getRouteString(), _topicPartitionSet, _workerSet);
    }
  }

  @Override
  public int hashCode() {
    return _instanceName.hashCode() + (_route == null ? 0 : _route.hashCode());
  }

  public boolean isSameDc() {
    String[] srcDst = _route.getTopic().split(SEPARATOR);
    String src = srcDst[1];
    String dst = srcDst[2];
    return src.substring(0, 3).equals(dst.substring(0, 3));
  }

  public String getSrc() {
    String[] srcDst = _route.getTopic().split(SEPARATOR);
    return srcDst[1];
  }
}
