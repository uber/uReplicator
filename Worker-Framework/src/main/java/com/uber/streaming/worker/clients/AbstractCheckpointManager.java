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
package com.uber.streaming.worker.clients;

import com.uber.streaming.worker.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
  AbstractCheckpointManager implements get/update for checkpoint.
  The implement of commitOffset and fetchOffsetInfo requires to be thread safe.
 */
public abstract class AbstractCheckpointManager implements CheckpointManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCheckpointManager.class);

  @Override
  public void setFetchOffset(Task task, long fetchedOffset) {
    CheckpointInfo info = getCheckpointInfo(task);
    if (info != null) {
      info.setFetchOffset(fetchedOffset);
    } else {
      LOGGER.warn("Get offset info for task failed, topic: {}, partition: {}, consumerGroup: {}",
          task.getTopic(), task.getPartition(), task.getConsumerGroup());
    }
  }

  @Override
  public void setCommitOffset(Task task, long commitOffset) {
    CheckpointInfo info = getCheckpointInfo(task);
    if (info != null) {
      info.setCommitOffset(commitOffset);
    } else {
      LOGGER.warn("Get offset info for task failed, topic: {}, partition: {}, consumerGroup: {}",
          task.getTopic(), task.getPartition(), task.getConsumerGroup());
    }
  }
}
