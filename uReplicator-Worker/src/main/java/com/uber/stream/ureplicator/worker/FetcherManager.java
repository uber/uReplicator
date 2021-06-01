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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import com.uber.stream.ureplicator.worker.interfaces.IConsumerFetcherManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.GuardedBy;
import kafka.utils.ShutdownableThread;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FetcherManager manages ConsumerFetcherThread. Topic partitions will distributed into multiple
 * fetcher threads. The distribution method uses topic partition hashcode mod by num of fetcher
 * thread(properties: num.consumer.fetchers). Once FetcherManager is created, start() and
 * stopAllConnections() can be called repeatedly until shutdown() is called.
 */
public class FetcherManager extends ShutdownableThread implements
    IConsumerFetcherManager {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(FetcherManager.class);

  protected final String FETCHER_THREAD_PREFIX = "ConsumerFetcherThread";

  // fetcherMapLock guards addFetcherForPartitions and removeFetcherForPartitions
  protected final Object fetcherMapLock;
  // updateMapLock guards partitionAddMap and partitionDeleteMap
  protected final Object updateMapLock;
  @GuardedBy("fetcherMapLock")
  protected final Map<TopicPartition, String> partitionThreadMap;
  @GuardedBy("updateMapLock")
  protected final Map<TopicPartition, PartitionOffsetInfo> partitionMap;

  // partitionAddMap stores the topic partition pending add
  // partitionDeleteMap stores the topic partition pending delete
  // the purpose of using these two fields is for non-blocking add/delete topic partition
  @GuardedBy("updateMapLock")
  protected final Map<TopicPartition, PartitionOffsetInfo> partitionAddMap;
  @GuardedBy("updateMapLock")
  protected final Map<TopicPartition, Boolean> partitionDeleteMap;

  protected final Map<String, ConsumerFetcherThread> fetcherThreadMap;
  protected final CustomizedConsumerConfig consumerProperties;
  protected final int refreshBackoff;
  protected final int numberOfConsumerFetcher;
  protected final RateLimiter messageLimiter;
  protected final List<BlockingQueue<FetchedDataChunk>> messageQueue;
  // TODO:(yayang) recycle fetcherId from shutdown fetcher thread
  protected final AtomicInteger fetcherId;

  public FetcherManager(String threadName,
      CustomizedConsumerConfig consumerProperties,
      List<BlockingQueue<FetchedDataChunk>> messageQueue) {
    this(threadName, consumerProperties, new ConcurrentHashMap<>(), messageQueue);
  }

  @VisibleForTesting
  protected FetcherManager(String threadName,
      CustomizedConsumerConfig consumerProperties,
      Map<String, ConsumerFetcherThread> fetcherThreadMap,
      List<BlockingQueue<FetchedDataChunk>> messageQueue) {
    super(threadName, true);
    this.consumerProperties = consumerProperties;
    this.messageQueue = messageQueue;
    // uReplicator manages commit offset itself
    consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    this.numberOfConsumerFetcher = consumerProperties.getNumberOfConsumerFetcher();
    LOGGER.info("Property {} : {} ", CustomizedConsumerConfig.NUMBER_OF_CONSUMER_FETCHERS,
        numberOfConsumerFetcher);
    this.refreshBackoff = consumerProperties.getFetcherManagerRefreshMs();
    this.fetcherThreadMap = fetcherThreadMap;
    if (consumerProperties.getConsumerNumOfMessageRate() > 0) {
      this.messageLimiter = RateLimiter.create(consumerProperties.getConsumerNumOfMessageRate());
    } else {
      this.messageLimiter = null;
    }
    this.fetcherMapLock = new Object();
    this.updateMapLock = new Object();
    this.partitionThreadMap = new ConcurrentHashMap<>();
    this.partitionMap = new ConcurrentHashMap<>();
    this.partitionAddMap = new ConcurrentHashMap<>();
    this.partitionDeleteMap = new ConcurrentHashMap<>();
    this.fetcherId = new AtomicInteger(0);
  }

  public void setMessageRate(Double rate) {
    if (messageLimiter == null) {
      throw new RuntimeException(String
          .format("Ratelimiter not defined because of consumer properties %s not configured",
              CustomizedConsumerConfig.CONSUMER_NUM_OF_MESSAGES_RATE));
    } else {
      messageLimiter.setRate(rate);
    }
  }

  public void addTopicPartition(TopicPartition topicPartition, PartitionOffsetInfo partitionInfo) {
    synchronized (updateMapLock) {
      if (!partitionMap.containsKey(topicPartition)) {
        partitionAddMap.put(topicPartition, partitionInfo);
      }
      if (partitionDeleteMap.containsKey(topicPartition)) {
        partitionDeleteMap.remove(topicPartition);
      }
    }
  }

  public void removeTopicPartition(TopicPartition topicPartition) {
    synchronized (updateMapLock) {
      if (partitionMap.containsKey(topicPartition)) {
        partitionDeleteMap.put(topicPartition, true);
      }
      if (partitionAddMap.containsKey(topicPartition)) {
        partitionAddMap.remove(topicPartition);
      }
    }
  }

  @GuardedBy("fetcherMapLock")
  private void removeFetcherForPartitions(Set<TopicPartition> topicPartition) {
    LOGGER.info("Enter remove fetcher thread for partitions {}", topicPartition);
    synchronized (fetcherMapLock) {
      List<String> fetcherThreadList = ImmutableList.copyOf(fetcherThreadMap.keySet());
      for (String fetcherThreadName : fetcherThreadList) {
        ConsumerFetcherThread fetcherThread = fetcherThreadMap.get(fetcherThreadName);
        fetcherThread.removePartitions(topicPartition);
      }
    }
    LOGGER.info("Remove fetcher thread for partitions {} finished", topicPartition);
  }

  @GuardedBy("fetcherMapLock")
  protected void addFetcherForTopicPartition(
      TopicPartition tp, PartitionOffsetInfo offsetInfo, String fetcherThreadName) {
    LOGGER.info("Enter add fetcher thread for partitions {}, fetcherThread {}", tp,
        fetcherThreadName);
    synchronized (fetcherMapLock) {
      if (StringUtils.isBlank(fetcherThreadName)) {
        LOGGER.warn("Unexpected behavior, can't find threadName for topic partition {}", tp);
        return;
      }

      ConsumerFetcherThread fetcherThread = fetcherThreadMap
          .getOrDefault(fetcherThreadName, null);
      if (fetcherThread == null) {
        try {
          int queueIndex = Math.abs(fetcherThreadName.hashCode() % messageQueue.size());

          LOGGER.info("Creating fetcher thread {}", fetcherThreadName);
          CustomizedConsumerConfig cloned = (CustomizedConsumerConfig) consumerProperties.clone();
          String clientIdPrefix = consumerProperties
              .getProperty(ConsumerConfig.CLIENT_ID_CONFIG, "ureplicator");
          int fetcherThreadId = fetcherId.incrementAndGet();
          // Kafka Consumer doesn't support having two kafka consumer using the same client id,
          // It throws error: WARN Error registering AppInfo mbean
          cloned.setProperty(ConsumerConfig.CLIENT_ID_CONFIG,
              String.format("%s-%d", clientIdPrefix, fetcherThreadId));
          fetcherThread = createConsumerFetcherThread(fetcherThreadName, cloned,
              messageLimiter, messageQueue.get(queueIndex));
          fetcherThread.start();
          fetcherThreadMap.put(fetcherThreadName, fetcherThread);
          LOGGER.info("Fetcher fetcher thread {} created", fetcherThreadName);
        } catch (Exception e) {
          LOGGER.error("Failed to create new fetcher thread {}", getName(), e);
          return;
        }
      }
      fetcherThread.addPartitions(ImmutableMap.of(tp, offsetInfo));
      partitionThreadMap.putIfAbsent(tp, fetcherThreadName);
    }
    LOGGER.info("Add fetcher thread for partitions {} finished", tp);
  }

  @VisibleForTesting
  protected ConsumerFetcherThread createConsumerFetcherThread(String threadName,
      CustomizedConsumerConfig properties,
      RateLimiter rateLimiter, BlockingQueue<FetchedDataChunk> chunkQueue) {
    return new ConsumerFetcherThread(threadName, properties,
        rateLimiter, chunkQueue);
  }

  /**
   * shutdown all CompactConsumerFetcherThreads
   */
  public void shutdown() {
    LOGGER.info("CompactConsumerFetcherManager shutdown");
    super.initiateShutdown();

    synchronized (fetcherMapLock) {
      for (ConsumerFetcherThread fetcherThread : fetcherThreadMap.values()) {
        fetcherThread.initiateShutdown();
      }
      for (ConsumerFetcherThread fetcherThread : fetcherThreadMap.values()) {
        fetcherThread.awaitShutdown();
      }
    }

    synchronized (updateMapLock) {
      super.awaitShutdown();
      partitionMap.clear();
      partitionAddMap.clear();
      partitionDeleteMap.clear();
      partitionThreadMap.clear();
      LOGGER.info("CompactConsumerFetcherManager stopConnections finished");
    }
    fetcherThreadMap.clear();
  }

  /**
   * Gets current serving topic partition
   *
   * @return serving topic partition and offset info
   */
  public Set<TopicPartition> getTopicPartitions() {
    return ImmutableSet.copyOf(partitionMap.keySet());
  }

  protected String getFetcherThreadName(TopicPartition tp) {
    int fetcherId = Math.abs(tp.hashCode() % numberOfConsumerFetcher);
    return String.format("%s-%d", FETCHER_THREAD_PREFIX, fetcherId);
  }


  /**
   * Adds topic partitions to fetcher thread
   *
   * @param topicPartitions topic partitions need to be added into fetcher thread
   * @return topic partitions added to fetcher thread
   */
  protected Map<TopicPartition, PartitionOffsetInfo> addTopicPartitionsToFetcherThread(
      Map<TopicPartition, PartitionOffsetInfo> topicPartitions) {
    Map<TopicPartition, PartitionOffsetInfo> result = new HashMap<>();
    if (topicPartitions == null || topicPartitions.size() == 0) {
      return result;
    }
    for (Map.Entry<TopicPartition, PartitionOffsetInfo> entry : topicPartitions.entrySet()) {
      String fetcherThreadName = getFetcherThreadName(entry.getKey());
      addFetcherForTopicPartition(entry.getKey(), entry.getValue(), fetcherThreadName);
      result.putIfAbsent(entry.getKey(), entry.getValue());
    }
    return result;
  }

  @Override
  public void doWork() {
    synchronized (updateMapLock) {
      try {
        if (partitionAddMap.size() != 0) {
          Map<TopicPartition, PartitionOffsetInfo> newPartitionMap = addTopicPartitionsToFetcherThread(
              partitionAddMap);

          for (Map.Entry<TopicPartition, PartitionOffsetInfo> entry : newPartitionMap.entrySet()) {
            partitionMap.put(entry.getKey(), entry.getValue());
            partitionAddMap.remove(entry.getKey());
          }
        }

        if (partitionAddMap.size() != 0) {
          LOGGER.info("topic partitions not successfully add to fetcher thread: {}",
              partitionAddMap.keySet());
        }

        // FIXME: propagate topic partition change to controller
        // Remove topic partition already finished
        for (PartitionOffsetInfo partition : partitionMap.values()) {
          if (partition.consumedEndBounded()) {
            partitionDeleteMap.put(partition.topicPartition(), true);
          }
        }

        for (TopicPartition partition : partitionDeleteMap.keySet()) {
          if (partitionMap.containsKey(partition)) {
            partitionMap.remove(partition);
            partitionThreadMap.remove(partition);
          }
        }

        if (partitionDeleteMap.size() != 0) {
          removeFetcherForPartitions(partitionDeleteMap.keySet());
        }
        partitionDeleteMap.clear();

      } catch (Throwable t) {
        LOGGER.error("[{}]: Catch Throwable exception: {}", getName(), t.getMessage(), t);
      }
    }
    try {
      Thread.sleep(refreshBackoff);
    } catch (InterruptedException e) {
      LOGGER.error("[{}]InterruptedException on sleep", getName());
    }
  }
}
