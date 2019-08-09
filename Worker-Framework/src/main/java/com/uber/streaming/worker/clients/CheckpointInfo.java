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
  private final AtomicLong fetchedOffset;
  private final AtomicLong producedOffset;
  private final AtomicLong processedOffset;
  private final Long endOffset;
  // actual starting offset can be different from original starting offset on offset "out of range" scenario.
  private long actualStartingOffset;

  public CheckpointInfo(
      Task task,
      long startingOffset,
      Long endOffset) {
    this.task = task;
    this.actualStartingOffset = startingOffset;
    this.endOffset = endOffset;
    this.fetchedOffset = new AtomicLong(startingOffset);
    this.producedOffset = new AtomicLong(startingOffset);
    this.processedOffset = new AtomicLong(startingOffset);
  }


  public void setFetchedOffset(Long fetchedOffset) {
    this.fetchedOffset.set(fetchedOffset);
  }

  public void setProducedOffset(Long producedOffset) {
    this.producedOffset.set(producedOffset);
  }

  public void setProcessedOffset(Long processedOffset) {
    this.processedOffset.set(processedOffset);
  }

  public void updateStartingOffset(Long startingOffset) {
    this.actualStartingOffset = startingOffset;
  }

  public boolean bounded(Long currentOffset) {
    return this.endOffset != null && this.endOffset <= currentOffset;
  }

  public long getActualStartingOffset() {
    return actualStartingOffset;
  }

  public long getFetchedOffset() {
    return fetchedOffset.get();
  }

  public long getProcessedOffsetOffset() {
    return processedOffset.get();
  }

  public long getProducedOffset() {
    return producedOffset.get();
  }
}
