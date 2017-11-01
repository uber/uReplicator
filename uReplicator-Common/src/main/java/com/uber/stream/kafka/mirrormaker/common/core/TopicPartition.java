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
package com.uber.stream.kafka.mirrormaker.common.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.util.Comparator;

/**
 * Simple data structure for holding topic and partitions.
 * This is kind of abused as the numPartitions could also indicate the partitionId.
 *
 * @author xiangfu
 */
public class TopicPartition {

  private final String _topic;
  private final int _partition;
  private final String _pipeline;

  public TopicPartition(String topic, int numPartitions) {
    this(topic, numPartitions, "");
  }

  public TopicPartition(String topic, int numPartitions, String pipeline) {
    _topic = topic;
    _partition = numPartitions;
    _pipeline = pipeline;
  }

  /**
   * This is used only for POST and PUT call to create the pojo.
   */
  public static TopicPartition init(String jsonRequest) {
    JSONObject jsonObject = JSON.parseObject(jsonRequest);
    if (!jsonObject.containsKey("topic")) {
      throw new RuntimeException("Cannot initialize TopicPartitionInfo, missing field: topic");
    }
    if (!jsonObject.containsKey("numPartitions")) {
      throw new RuntimeException(
          "Cannot initialize TopicPartitionInfo, missing field: numPartitions");
    }
    return new TopicPartition(jsonObject.getString("topic"),
        jsonObject.getIntValue("numPartitions"));
  }

  public int getPartition() {
    return _partition;
  }

  public String getTopic() {
    return _topic;
  }

  public String getPipeline() {
    return _pipeline;
  }

  public String toString() {
    return String.format("{topic: %s, partition: %s}", _topic, _partition);
  }

  public JSONObject toJSON() {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("topic", _topic);
    jsonObject.put("numPartitions", _partition);
    return jsonObject;
  }

  public static Comparator<TopicPartition> getWorkloadComparator(
      final WorkloadInfoRetriever infoRetriever) {
    return new Comparator<TopicPartition>() {
      @Override
      public int compare(TopicPartition o1, TopicPartition o2) {
        TopicWorkload workload1 = infoRetriever.topicWorkload(o1.getTopic());
        TopicWorkload workload2 = infoRetriever.topicWorkload(o2.getTopic());
        if (workload1 == workload2) {
          return 0;
        }
        int cmp = workload1.compareTo(workload2);
        if (cmp != 0) {
          return cmp;
        }
        // if workload is the same, compare them based on the name and partition
        cmp = o1.getTopic().compareTo(o2.getTopic());
        if (cmp != 0) {
          return cmp;
        }
        return Integer.compare(o1.getPartition(), o2.getPartition());
      }
    };
  }

  @Override
  public boolean equals(Object that) {
    if (that == null || !(that instanceof TopicPartition)) {
      return false;
    }
    TopicPartition tp2 = (TopicPartition) that;
    return this._partition == tp2._partition && this._topic.equals(tp2._topic);
  }

  @Override
  public int hashCode() {
    return _topic.hashCode() + _partition;
  }

}
