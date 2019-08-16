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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Interface to store in-memory topic partition offset info for kafka
 */
public final class CheckpointInfo {

  private final Task task;

  // message fetch offset
  private final AtomicLong fetchOffset;
  // message offset that is ready to commit
  private final AtomicLong commitOffset;

  // starting offset for consumer fetcher thread to startwith, -1 means honor commit offset at kafka consumer.
  private long startingOffset;

  // end offset for task
  private final Long endOffset;

  public CheckpointInfo(
      Task task,
      long startingOffset,
      Long endOffset) {
    this.task = task;
    this.startingOffset = startingOffset;
    this.endOffset = endOffset;
    this.fetchOffset = new AtomicLong(startingOffset);
    this.commitOffset = new AtomicLong(startingOffset);
  }


  public void setFetchOffset(Long fetchedOffset) {
    this.fetchOffset.set(fetchedOffset);
  }

  public void setCommitOffset(Long commitOffset) {
    this.commitOffset.set(commitOffset);
  }

  public boolean bounded(Long currentOffset) {
    return this.endOffset != null && this.endOffset <= currentOffset;
  }

  public long getStartingOffset() {
    return startingOffset;
  }

  public long getFetchOffset() {
    return fetchOffset.get();
  }

  public long getCommitOffset() {
    return commitOffset.get();
  }
}
