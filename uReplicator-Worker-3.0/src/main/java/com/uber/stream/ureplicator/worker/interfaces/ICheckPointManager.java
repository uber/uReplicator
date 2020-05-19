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
package com.uber.stream.ureplicator.worker.interfaces;

import java.util.Map;
import org.apache.kafka.common.TopicPartition;

public interface ICheckPointManager {

  /**
   * Commits topic partition offsets to offset store
   * @param topicPartitionOffsets topic partition offsets map
   * @return the status of offset commit
   */
  boolean commitOffset(Map<TopicPartition, Long> topicPartitionOffsets);

  Long fetchOffset(TopicPartition topicPartition);

  void shutdown();
}
