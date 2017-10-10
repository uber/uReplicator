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

public class TopicWorkload implements Comparable<TopicWorkload> {

  public static final double DEFAULT_BYTES_PER_SECOND = 1000.0;
  public static final double DEFAULT_MSGS_PER_SECOND = 1;
  public static final int DEFAULT_PARTITIONS = 1;

  private double bytesPerSecond;
  private double msgsPerSecond;
  private int partitions;
  private long lastUpdate = 0;

  public TopicWorkload(double bytesPerSecond, double msgsPerSecond) {
    this(bytesPerSecond, msgsPerSecond, DEFAULT_PARTITIONS);
  }

  public TopicWorkload(double bytesPerSecond, double msgsPerSecond, int partitions) {
    this.bytesPerSecond = bytesPerSecond;
    this.msgsPerSecond = msgsPerSecond;
    this.partitions = partitions;
  }

  public double getBytesPerSecond() {
    return this.bytesPerSecond;
  }

  public double getBytesPerSecondPerPartition() {
    return this.partitions > 0 ? this.bytesPerSecond / this.partitions : DEFAULT_BYTES_PER_SECOND;
  }

  public void setBytesPerSecond(double bytesPerSecond) {
    this.bytesPerSecond = bytesPerSecond;
  }

  public double getMsgsPerSecond() {
    return this.msgsPerSecond;
  }

  public double getMsgsPerSecondPerPartition() {
    return this.partitions > 0 ? this.msgsPerSecond / this.partitions : DEFAULT_MSGS_PER_SECOND;
  }

  public void setMsgsPerSecond(double msgsPerSecond) {
    this.msgsPerSecond = msgsPerSecond;
  }

  public int getPartitions() {
    return this.partitions;
  }

  public void setParitions(int partitions) {
    this.partitions = partitions;
  }

  public long getLastUpdate() {
    return this.lastUpdate;
  }

  public void setLastUpdate(long updateAt) {
    this.lastUpdate = updateAt;
  }

  public void add(TopicWorkload another) {
    this.bytesPerSecond += another.getBytesPerSecond();
    this.msgsPerSecond += another.getMsgsPerSecond();
    this.partitions += another.partitions;
  }

  public void add(double bytesPerSecond, double msgsPerSecond) {
    this.bytesPerSecond += bytesPerSecond;
    this.msgsPerSecond += msgsPerSecond;
  }

  // based on workload per partition
  @Override
  public int compareTo(TopicWorkload that) {
    if (this == that) {
      return 0;
    }
    if (that == null) {
      return 1;
    }
    // TODO: decide which metric to compare
    int cmp = Double.compare(this.getBytesPerSecondPerPartition(), that.getBytesPerSecondPerPartition());
    if (cmp != 0) {
      return cmp;
    }
    cmp = Double.compare(this.getMsgsPerSecondPerPartition(), that.getMsgsPerSecondPerPartition());
    return cmp;
  }

  /**
   * Compare workload regardless of partitions.
   */
  public int compareTotal(TopicWorkload that) {
    if (this == that) {
      return 0;
    }
    if (that == null) {
      return 1;
    }
    // TODO: decide which metric to compare
    int cmp = Double.compare(this.getBytesPerSecond(), that.getBytesPerSecond());
    if (cmp != 0) {
      return cmp;
    }
    cmp = Double.compare(this.getMsgsPerSecond(), that.getMsgsPerSecond());
    return cmp;
  }

  @Override
  public String toString() {
    return String.format("{%.1f bytes/s, %.1f msgs/s, %d partitions}", bytesPerSecond, msgsPerSecond, partitions);
  }
}
