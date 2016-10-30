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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * Simple data structure for holding topic and partitions.
 * This is kind of abused as the numPartitions could also indicate the partitionId.
 *
 * @author xiangfu
 */
public class TopicPartition {

  private final String _topic;
  private final int _partition;

  public TopicPartition(String topic, int numPartitions) {
    _topic = topic;
    _partition = numPartitions;
  }

  /**
   * This is used only for POST and PUT call to create the pojo.
   *
   * @param jsonRequest
   * @return
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

  public String toString() {
    return String.format("{topic: %s, partition: %s}", _topic, _partition);
  }

  public JSONObject toJSON() {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("topic", _topic);
    jsonObject.put("numPartitions", _partition);
    return jsonObject;
  }
}
