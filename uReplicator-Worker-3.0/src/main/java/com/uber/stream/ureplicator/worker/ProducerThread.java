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

import com.uber.stream.ureplicator.worker.ConsumerIterator.ConsumerTimeoutException;
import com.uber.stream.ureplicator.worker.interfaces.ICheckPointManager;
import com.uber.stream.ureplicator.worker.interfaces.IMessageTransformer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerThread extends Thread {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProducerThread.class);

  protected final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final AtomicInteger numDroppedMessage = new AtomicInteger(0);

  private final DefaultProducer producer;
  private final IMessageTransformer messageTransformer;

  private final ICheckPointManager checkpointManager;
  private final ConsumerIterator incomeData;
  private final Map<TopicPartition, Long> consumedOffsets = new HashMap<>();
  private final WorkerInstance workerInstance;
  protected AtomicBoolean isShuttingDown = new AtomicBoolean(false);

  /**
   * Constructor
   *
   * @param threadId thread id
   * @param producerProps producer configuration properties
   * @param abortOnSendFailure whether abort when send failure
   * @param incomeData consumed message stream
   * @param messageTransformer message transformer
   * @param checkpointManager check point manager
   * @param workerInstance worker instance
   */
  public ProducerThread(String threadId,
      Properties producerProps,
      Boolean abortOnSendFailure,
      ConsumerIterator incomeData,
      IMessageTransformer messageTransformer,
      ICheckPointManager checkpointManager,
      WorkerInstance workerInstance) {
    this.messageTransformer = messageTransformer;
    this.checkpointManager = checkpointManager;
    this.incomeData = incomeData;
    this.workerInstance = workerInstance;
    maybeSetDefaultProperty(producerProps, ProducerConfig.RETRIES_CONFIG, "2147483647");
    maybeSetDefaultProperty(producerProps, ProducerConfig.MAX_BLOCK_MS_CONFIG, "600000");
    maybeSetDefaultProperty(producerProps, ProducerConfig.ACKS_CONFIG, "all");
    maybeSetDefaultProperty(producerProps, ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
    this.producer = new DefaultProducer(threadId, producerProps, abortOnSendFailure,
        workerInstance);
    String threadName = Constants.PRODUCER_THREAD_PREFIX + threadId;
    setName(threadName);
  }

  @Override
  public void run() {
    try {
      while (!producer.producerAbort() && !isShuttingDown.get()) {
        try {
          // poll consume record
          if (incomeData.hasNext()) {
            ConsumerRecord record = incomeData.next();
            ProducerRecord resp = messageTransformer.process(record);
            if (resp == null) {
              numDroppedMessage.getAndIncrement();
            } else {
              producer.send(resp, record.partition(), record.offset());
            }
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            consumedOffsets.put(tp, record.offset() + 1);
          }
        } catch (ConsumerTimeoutException e) {
          LOGGER.trace("[{}]Caught ConsumerTimeoutException, continue iteration.", getName());
          // TODO: add backoff ms for ConsumerTimeoutException
        } catch (Exception e) {
          LOGGER.error("[{}]Caught Exception, continue iteration.", getName(), e);
          break;
        }
        flushAndCommitOffset(false);
      }
    } finally {
      LOGGER.info("[{}]Thread exited.", getName());
      if (!isShuttingDown.get()) {
        LOGGER.error(
            "[{}]Thread exited abnormally, stopping the whole uReplicator.", getName());
        // start clean shutdown worker

      }
      shutdownLatch.countDown();
      workerInstance.cleanShutdown();
    }
  }

  private synchronized void flushAndCommitOffset(boolean forceCommit) {
    try {
      if (consumedOffsets.size() != 0 && producer.maybeFlush(forceCommit)) {
        checkpointManager.commitOffset(consumedOffsets);
        consumedOffsets.clear();
      }
    } catch (InterruptedException e) {
      LOGGER.error("[{}]Caught InterruptedException on flush.", getName(), e);
    }
  }

  public void shutdown() {
    if (!isShuttingDown.compareAndSet(false, true) || shutdownLatch.getCount() == 0) {
      return;
    }
    LOGGER.info("[{}]shutting down", getName());
    try {
      shutdownLatch.await();
      LOGGER.info("[{}]shutdown complete", getName());
    } catch (InterruptedException e) {
      LOGGER.error("[{}]Shutdown interrupted", getName(), e);
    }

    LOGGER.info("[{}]Flushing last batches and commit offsets", getName());
    flushAndCommitOffset(true);
    producer.shutdown();
  }

  private void maybeSetDefaultProperty(Properties properties, String propertyName, String defaultValue) {
    String propertyValue = properties.getProperty(propertyName, defaultValue);
    properties.setProperty(propertyName, propertyValue);
    if (properties.getProperty(propertyName) != defaultValue) {
      LOGGER.info("Property {} is overridden to {} - data loss or message reordering is possible.", propertyName, propertyValue);
    }
  }
}
