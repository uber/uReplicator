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
package com.uber.streaming.worker;

import javax.annotation.Nullable;
import org.apache.commons.lang.StringUtils;

/**
 * unit of work assign by controller
 */
public final class Task {

  private final String topic;

  private final int partition;

  @Nullable
  private final String cluster;

  @Nullable
  private final String consumerGroup;

  @Nullable
  private final Workload workload;

  @Nullable
  private final Long startOffset;

  @Nullable
  private final Long endOffset;

  public Task(String topic, int partition) {
    this(topic, partition, null, null, null, null, null);
  }

  public Task(String topic, int partition, String cluster,
      String consumerGroup, Workload workload,
      Long startOffset, Long endOffset) {
    if (StringUtils.isBlank(topic)) {
      throw new IllegalArgumentException("topic can't be null");
    }
    this.topic = topic;
    this.partition = partition;
    this.cluster = cluster;
    this.consumerGroup = consumerGroup;
    this.workload = workload;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return partition;
  }

  public String getCluster() {
    return cluster;
  }

  public String getConsumerGroup() {
    return consumerGroup;
  }

  public Workload getWorkload() {
    return workload;
  }

  public Long getStartOffset() {
    return startOffset;
  }

  public Long getEndOffset() {
    return endOffset;
  }
}
