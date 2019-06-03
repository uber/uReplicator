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
package com.uber.stream.ureplicator.worker;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public class PartitionOffsetInfo {

  private final AtomicLong fetchOffset;
  private final AtomicLong consumeOffset;
  private final Long startingOffset;
  private final Long endingOffset;
  private final TopicPartition topicPartition;
  private final BlockingQueue<FetchedDataChunk> chunkQueue;

  public PartitionOffsetInfo(TopicPartition topicPartition, Long startingOffset, Long endingOffset,
      BlockingQueue<FetchedDataChunk> chunkQueue) {
    this.fetchOffset = startingOffset != null ? new AtomicLong(startingOffset) : new AtomicLong(0);
    this.consumeOffset =
        startingOffset != null ? new AtomicLong(startingOffset) : new AtomicLong(0);
    this.startingOffset = startingOffset;
    this.endingOffset = endingOffset;
    this.topicPartition = topicPartition;
    this.chunkQueue = chunkQueue;
  }

  public Long consumeOffset() {
    return consumeOffset.get();
  }

  public Long startingOffset() {
    return startingOffset;
  }

  public Long fetchOffset() {
    return fetchOffset.get();
  }

  public void setConsumeOffset(long consumeOffset) {
    if (this.endingOffset != null && this.endingOffset < consumeOffset) {
      throw new RuntimeException(String
          .format("setConsumeOffset out of range, consumeOffset %d, consumeOffset %d",
              endingOffset, consumeOffset));
    }
    this.consumeOffset.set(consumeOffset);
  }

  public void enqueue(List<ConsumerRecord> consumerRecords) throws InterruptedException {
    if (consumerRecords == null || consumerRecords.size() == 0) {
      return;
    }
    int size = consumerRecords.size();
    long offset = consumerRecords.get(size - 1).offset();
    FetchedDataChunk dataChunk = new FetchedDataChunk(this, consumerRecords);
    chunkQueue.put(dataChunk);
    this.fetchOffset.set(offset + 1);
  }

  public TopicPartition topicPartition() {
    return topicPartition;
  }

  public boolean consumedEndBounded() {
    return bounded(consumeOffset.get());
  }

  private boolean bounded(Long currentOffset) {
    return this.endingOffset != null && this.endingOffset <= currentOffset;

  }

  public boolean fetchedEndBounded() {
    return bounded(fetchOffset.get());
  }
}
