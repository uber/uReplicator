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
package com.uber.stream.kafka.mirrormaker.common.modules;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class TopicPartitionLag {
  private String topicName;
  private int partitionId;
  private long latestOffset;
  private long commitOffset;
  private long timeStamp;
  private long lag;
  private long lagTime = 0;

  public TopicPartitionLag(String topicName, int partitionId, long latestOffset, long commitOffset) {
    this.topicName = topicName;
    this.partitionId = partitionId;
    this.latestOffset = latestOffset;
    this.commitOffset = commitOffset;
    this.lag = latestOffset - commitOffset;
    this.timeStamp = System.currentTimeMillis();
  }

  @Override
  public String toString() {
    return String.format("%s:%d-%d/%d", topicName, latestOffset, commitOffset);
  }

}
