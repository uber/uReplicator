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

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultProducer.class);

  private final boolean syncProducer;
  private final KafkaProducer producer;

  private final long offsetCommitIntervalMs;
  private final AtomicInteger recordCount = new AtomicInteger(0);
  private final boolean abortOnSendFailure;
  private final WorkerInstance workerInstance;
  private final ReentrantLock flushCommitLock = new ReentrantLock();

  private boolean producerAbort = false;
  private long lastOffsetCommitMs = System.currentTimeMillis();
  private String threadName;

  public DefaultProducer(String threadId, Properties producerProps,
      Boolean abortOnSendFailure,
      WorkerInstance instance) {
    threadName = Constants.PRODUCER_THREAD_PREFIX + threadId;
    this.abortOnSendFailure = abortOnSendFailure;
    this.syncProducer = producerProps
        .getProperty(Constants.PRODUCER_TYPE_CONFIG, Constants.DEFAULT_PRODUCER_TYPE)
        .equals("sync");
    String offsetCommitIntervalMsStr = producerProps
        .getProperty(Constants.PRODUCER_OFFSET_COMMIT_INTERVAL_MS,
            Constants.DEFAULT_PRODUCER_OFFSET_COMMIT_INTERVAL_MS);
    this.offsetCommitIntervalMs = Integer.parseInt(offsetCommitIntervalMsStr);
    this.workerInstance = instance;
    String producerGroup = producerProps
        .getProperty(ProducerConfig.CLIENT_ID_CONFIG, "uReplicator");
    producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, producerGroup + "-" + threadId);
    producer = new KafkaProducer(producerProps);
  }

  public void send(ProducerRecord record, int srcPartition, long srcOffset)
      throws ExecutionException, InterruptedException {
    recordCount.getAndIncrement();
    if (syncProducer) {
      this.producer.send(record).get();
    } else {
      this.producer.send(record,
          new UReplicatorProducerCallback(record.topic(), srcPartition, srcOffset));
    }
  }

  public boolean maybeFlush(boolean forceCommit) throws InterruptedException {
    synchronized (flushCommitLock) {
      if (forceCommit || System.currentTimeMillis() - lastOffsetCommitMs > offsetCommitIntervalMs) {
        LOGGER.info("[{}] Flushing producer. forceCommit: {}", threadName, forceCommit);
        producer.flush();
        while (!producerAbort && recordCount.get() != 0) {
          flushCommitLock.wait(100);
        }
        LOGGER.info("[{}] Flushing producer finished. producerAbort: {}", threadName, producerAbort);
        if (producerAbort) {
          LOGGER.warn("[{}] Exiting on send failure, skip committing offsets.", threadName);
          return false;
        }
        lastOffsetCommitMs = System.currentTimeMillis();
        return true;
      } else {
        return false;
      }
    }
  }

  public boolean producerAbort() {
    return producerAbort;
  }

  public class UReplicatorProducerCallback implements Callback {

    private final int srcPartition;
    private final long srcOffset;
    private final String topic;

    public UReplicatorProducerCallback(String topic, int srcPartition, long srcOffset) {
      this.topic = topic;
      this.srcPartition = srcPartition;
      this.srcOffset = srcOffset;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception e) {
      try {
        if (e != null) {
          LOGGER.error("[{}] Closing producer due to send failure. topic: {}", threadName, topic, e);
          if (abortOnSendFailure) {
            producerAbort = true;
            producer.close();
          }
        } else {
          onCompletionWithoutException(metadata, srcPartition, srcOffset);
        }
      } finally {
        recordCount.decrementAndGet();
      }
    }

    public void onCompletionWithoutException(RecordMetadata metadata, int srcPartition,
        long srcOffset) {
      workerInstance.onProducerCompletionWithoutException(metadata, srcPartition, srcOffset);
    }
  }

  public void shutdown() {
    producer.close();
    recordCount.set(0);
  }
}
