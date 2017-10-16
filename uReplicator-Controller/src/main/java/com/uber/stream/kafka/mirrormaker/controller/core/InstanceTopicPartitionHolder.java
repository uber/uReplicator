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

import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

/**
 * InstanceTopicPartitionHolder is a wrapper for instance and the topicPartitionSet it's holding.
 */
public class InstanceTopicPartitionHolder {

  private final String _instanceName;
  private final Set<TopicPartition> _topicPartitionSet = new HashSet<>();

  public InstanceTopicPartitionHolder(String instance) {
    _instanceName = instance;
  }

  public String getInstanceName() {
    return _instanceName;
  }

  public Set<TopicPartition> getServingTopicPartitionSet() {
    return ImmutableSet.copyOf(_topicPartitionSet);
  }

  public int getNumServingTopicPartitions() {
    return _topicPartitionSet.size();
  }

  public void addTopicPartition(TopicPartition topicPartitionInfo) {
    _topicPartitionSet.add(topicPartitionInfo);
  }

  public void removeTopicPartition(TopicPartition topicPartitionInfo) {
    _topicPartitionSet.remove(topicPartitionInfo);
  }

  public void clearTopicPartitions() {
    _topicPartitionSet.clear();
  }

  public TopicWorkload totalWorkload(WorkloadInfoRetriever infoRetriever, ITopicWorkloadWeighter weighter) {
    TopicWorkload total = new TopicWorkload(0, 0, 0);
    for (TopicPartition part : _topicPartitionSet) {
      TopicWorkload tw = infoRetriever.topicWorkload(part.getTopic());
      double weight = (weighter == null) ? 1.0 : weighter.partitionWeight(part);
      total.add(tw.getBytesPerSecondPerPartition() * weight, tw.getMsgsPerSecondPerPartition() * weight);
    }
    return total;
  }

  public static Comparator<InstanceTopicPartitionHolder> getTotalWorkloadComparator(
      final WorkloadInfoRetriever infoRetriever, final ITopicWorkloadWeighter weighter) {
    return new Comparator<InstanceTopicPartitionHolder>() {
      @Override
      public int compare(InstanceTopicPartitionHolder o1, InstanceTopicPartitionHolder o2) {
        TopicWorkload workload1 = (o1 == null) ? new TopicWorkload(0, 0) : o1.totalWorkload(infoRetriever, weighter);
        TopicWorkload workload2 = (o2 == null) ? new TopicWorkload(0, 0) : o2.totalWorkload(infoRetriever, weighter);
        int cmp = workload1.compareTotal(workload2);
        if (cmp != 0) {
          return cmp;
        }
        // if workload is the same, compare them based on the number of partitions
        int size1 = (o1 == null) ? -1 : o1.getNumServingTopicPartitions();
        int size2 = (o2 == null) ? -1 : o2.getNumServingTopicPartitions();
        if (size1 != size2) {
          return size1 - size2;
        } else {
          return o1.getInstanceName().compareTo(o2.getInstanceName());
        }

      }
    };
  }

  public void addTopicPartitions(Collection<TopicPartition> topicPartitionInfos) {
    _topicPartitionSet.addAll(topicPartitionInfos);
  }

  @Override
  public String toString() {
    return String.format("{%s=%s}", _instanceName, _topicPartitionSet);
  }

  @Override
  public int hashCode() {
    return _instanceName.hashCode();
  }

}
