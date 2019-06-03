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


import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An iterator that blocks until a value can be read from the supplied queue.
 * Copied from https://github.com/apache/kafka/blob/1.1/core/src/main/scala/kafka/consumer/ConsumerIterator.scala
 */
public class ConsumerIterator extends IteratorTemplate<ConsumerRecord> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerIterator.class);

  private final AtomicReference<Iterator<ConsumerRecord>> current = new AtomicReference<>(null);
  private final int consumerTimeoutMs;
  private long nextOffset = -1L;
  private final BlockingQueue<FetchedDataChunk> channel;
  private PartitionOffsetInfo currentPartitionInfo = null;

  public ConsumerIterator(BlockingQueue<FetchedDataChunk> channel, int consumerTimeoutMs) {
    this.channel = channel;
    this.consumerTimeoutMs = consumerTimeoutMs;
  }

  @Override
  public ConsumerRecord next() {
    ConsumerRecord records = super.next();
    if (currentPartitionInfo != null) {
      currentPartitionInfo.setConsumeOffset(nextOffset);
    }
    return records;
  }

  @Override
  public ConsumerRecord makeNext() {
    Iterator<ConsumerRecord> localCurrent = current.get();
    // if we don't have an iterator, get one
    if (localCurrent == null || !localCurrent.hasNext() || consumedEndBounded()) {
      FetchedDataChunk currentChuck;
      try {
        if (consumerTimeoutMs < 0) {
          currentChuck = channel.take();
        } else {
          currentChuck = channel.poll(consumerTimeoutMs, TimeUnit.MILLISECONDS);
          if (currentChuck == null) {
            resetState();
            throw new ConsumerTimeoutException();
          }
        }
      } catch (InterruptedException e) {
        LOGGER.error("Error poll from channel.", e);
        resetState();
        throw new RuntimeException(e.getMessage(), e);
      }
      localCurrent = currentChuck.consumerRecords().iterator();
      currentPartitionInfo = currentChuck.partitionOffsetInfo();
      current.set(localCurrent);
    }
    ConsumerRecord item = localCurrent.next();
    while (item.offset() < currentPartitionInfo.consumeOffset() && localCurrent.hasNext()) {
      item = localCurrent.next();
    }
    nextOffset = item.offset() + 1;
    return item;
  }

  public void cleanCurrentChunk() {
    current.set(null);
  }

  public class ConsumerTimeoutException extends RuntimeException {

  }

  private boolean consumedEndBounded() {
    return currentPartitionInfo != null && currentPartitionInfo.consumedEndBounded();
  }
}


