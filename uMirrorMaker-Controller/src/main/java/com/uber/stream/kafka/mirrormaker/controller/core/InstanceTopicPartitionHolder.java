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

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

/**
 * InstanceTopicPartitionHolder is a wrapper for instance and the topicPartitionSet it's holding.
 */
public class InstanceTopicPartitionHolder {

  private final String _instanceName;
  private final Set<TopicPartition> _topicPartitionSet = new HashSet<TopicPartition>();

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

  public static Comparator<InstanceTopicPartitionHolder> getComparator() {
    return new Comparator<InstanceTopicPartitionHolder>() {
      @Override
      public int compare(InstanceTopicPartitionHolder o1, InstanceTopicPartitionHolder o2) {
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
  public int hashCode() {
    return _instanceName.hashCode();
  }

}
