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
import java.io.Closeable;
import java.util.List;

/**
 * Interacts with remote data store (ie. zookeeper, kafka __consumer_offsets topic) for fetch,
 * commit offset.
 */
public interface CheckpointManager extends Closeable {

  /**
   * Commits offset to data store for all tasks
   */
  void commitOffset();

  /**
   * Commits offset to data store for specified tasks
   */
  void commitOffset(List<Task> task);

  /**
   * Updates massage offset on task to mark messages before this offset has been successfully
   * processed and enqueue to next stage by fetcher
   */
  void setFetchOffset(Task task, long fetchedOffset);

  /**
   * Updates offset to be committed.(implementation will decide update either in-memory value or
   * data store)
   */
  void setCommitOffset(Task task, long commitOffset);

  /**
   * Gets checkpoint info
   */
  CheckpointInfo getCheckpointInfo(Task task);

  /**
   * Gets checkpoint info
   */
  CheckpointInfo getCheckpointInfo(Task task, boolean refresh);
}
