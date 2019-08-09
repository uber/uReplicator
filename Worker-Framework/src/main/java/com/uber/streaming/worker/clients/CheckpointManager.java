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

/**
 * Interacts with remote data store for fetch, commit offset. Keeps in-memory cache for latest
 * offset for each components
 */
public interface CheckpointManager {

  /**
   * Commits in memory produced offset to data store(ie. zk, kafka __consumer_offsets topic)
   */
  void commitOffset();

  /**
   * Updates in-memory fetched offset
   */
  void updateFetchedOffset(Task task, long fetchedOffset);

  /**
   * Updates in-memory produced offset
   */
  void updateProducedOffset(Task task, long producedOffset);


  /**
   * Updates in-memory processed offset
   */
  void updateProcessedOffset(Task task, long processedOffset);

  /**
   * Gets in memory offset cache
   */
  CheckpointInfo fetchOffsetInfo(Task task);

  /**
   * Updates actual consumer start offset
   */
  void updateActualStartOffset(Task task, long beginOffset);
}
