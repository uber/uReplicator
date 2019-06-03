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
import com.uber.stream.ureplicator.worker.interfaces.IConsumerFetcherManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import java.util.concurrent.ConcurrentHashMap;
import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FetcherManagerGroupByLeaderId creates the CompactConsumerFetcherThreads. It groups partitions
 * into small groups using topic partition leader id  and create fetcher thread for each group. Once
 * CompactConsumerFetcherManager is created, start() and stopAllConnections() can be called repeatedly
 * until shutdown() is called.
 */
public class FetcherManagerGroupByLeaderId extends ShutdownableThread implements
    IConsumerFetcherManager {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(FetcherManagerGroupByLeaderId.class);

  private final String fetcherThreadPrefix = "CompactConsumerFetcherThread";

  // fetcherMapLock guards addFetcherForPartitions and removeFetcherForPartitions
  private final Object fetcherMapLock = new Object();
  // updateMapLock guards partitionAddMap and partitionDeleteMap
  private final Object updateMapLock = new Object();
  private final Map<TopicPartition, Integer> partitionLeaderMap = new ConcurrentHashMap<>();
  private final Map<TopicPartition, PartitionOffsetInfo> partitionMap = new ConcurrentHashMap<>();

  // partitionAddMap stores the topic partition pending add
  // partitionDeleteMap stores the topic partition pending delete
  // the purpose of using these two fields is for non-blocking add/delete topic partition
  private final Map<TopicPartition, PartitionOffsetInfo> partitionAddMap = new ConcurrentHashMap<>();
  private final Map<TopicPartition, Boolean> partitionDeleteMap = new ConcurrentHashMap<>();

  private final Map<String, ConsumerFetcherThread> fetcherThreadMap;
  private final CustomizedConsumerConfig consumerProperties;
  private final int refreshLeaderBackoff;
  private final int numberOfConsumerFetcher;

  @VisibleForTesting
  protected Consumer kafkaConsumer;

  public FetcherManagerGroupByLeaderId(String threadName,
      CustomizedConsumerConfig consumerProperties) {
    this(threadName, consumerProperties, new ConcurrentHashMap<>(),
        new KafkaConsumer(consumerProperties));
  }

  @VisibleForTesting
  protected FetcherManagerGroupByLeaderId(String threadName,
      CustomizedConsumerConfig consumerProperties,
      Map<String, ConsumerFetcherThread> fetcherThreadMap, Consumer kafkaConsumer) {
    super(threadName, true);
    this.consumerProperties = consumerProperties;
    this.numberOfConsumerFetcher = consumerProperties.getNumberOfConsumerFetcher();
    this.refreshLeaderBackoff = consumerProperties.getLeaderRefreshMs();
    this.fetcherThreadMap = fetcherThreadMap;
    this.kafkaConsumer = kafkaConsumer;
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

  private void removeFetcherForPartitions(Set<TopicPartition> topicPartition) {
    LOGGER.info("Enter remove fetcher thread for partitions {}", topicPartition);
    synchronized (fetcherMapLock) {
      List<String> fetcherThreadList = ImmutableList.copyOf(fetcherThreadMap.keySet());
      for (String fetcherThreadName : fetcherThreadList) {
        ConsumerFetcherThread fetcherThread = fetcherThreadMap.get(fetcherThreadName);
        fetcherThread.removePartitions(topicPartition);
        if (fetcherThread.getTopicPartitions().isEmpty()) {
          fetcherThread.shutdown();
          fetcherThreadMap.remove(fetcherThreadName);
          LOGGER.info("Shutting down fetcher thread {}", fetcherThreadName);
        }
      }
    }
    LOGGER.info("Remove fetcher thread for partitions {} finished", topicPartition);
  }

  private void addFetcherForPartitions(
      Map<TopicPartition, PartitionOffsetInfo> partitionAndOffsets) {
    LOGGER.info("Enter add fetcher thread for partitions {}", partitionAndOffsets.keySet());
    synchronized (fetcherMapLock) {
      for (TopicPartition tp : partitionAndOffsets.keySet()) {
        PartitionOffsetInfo offsetInfo = partitionAndOffsets.get(tp);
        Integer leaderId = partitionLeaderMap.getOrDefault(tp, null);
        if (leaderId == null) {
          continue;
        }

        String fetchThreadName = getFetcherThreadName(leaderId, getFetcherId(tp));
        ConsumerFetcherThread fetcherThread = fetcherThreadMap
            .getOrDefault(fetchThreadName, null);
        if (fetcherThread == null) {
          try {
            LOGGER.info("Creating fetcher thread {}", fetchThreadName);
            fetcherThread = new ConsumerFetcherThread(fetchThreadName, consumerProperties);
            fetcherThread.start();
            fetcherThreadMap.put(fetchThreadName, fetcherThread);
            LOGGER.info("Fetcher fetcher thread {} created", fetchThreadName);
          } catch (Exception e) {
            LOGGER.error("Failed to create new fetcher thread {}", getName(), e);
            continue;
          }
        }
        fetcherThread.addPartitions(ImmutableMap.of(tp, offsetInfo));
      }
    }
    LOGGER.info("Add fetcher thread for partitions {} finished", partitionAndOffsets.keySet());
  }


  private String getFetcherThreadName(int leaderBroker, int fetcherId) {
    return String.format("%s-%d-%d", fetcherThreadPrefix, leaderBroker, fetcherId);
  }

  private int getFetcherId(TopicPartition tp) {
    return Math.abs(tp.hashCode()) % numberOfConsumerFetcher;
  }

  /**
   * shutdown all CompactConsumerFetcherThreads
   */
  public void shutdown() {
    LOGGER.info("CompactConsumerFetcherManager stopConnections");
    super.initiateShutdown();
    synchronized (fetcherMapLock) {
      for (ConsumerFetcherThread fetcherThread : fetcherThreadMap.values()) {
        fetcherThread.shutdown();
      }
      kafkaConsumer.close();
      super.awaitShutdown();
      partitionMap.clear();
      partitionAddMap.clear();
      partitionDeleteMap.clear();
      partitionLeaderMap.clear();
      LOGGER.info("CompactConsumerFetcherManager stopConnections finished");
    }
  }

  /**
   * Gets current serving topic partition
   *
   * @return serving topic partition and offset info
   */
  public Set<PartitionOffsetInfo> getTopicPartitions() {
    return ImmutableSet.copyOf(partitionMap.values());
  }

  @Override
  public void doWork() {
    synchronized (updateMapLock) {
      Map<TopicPartition, PartitionOffsetInfo> partitionNewLeaderMap = new ConcurrentHashMap<>();
      List<TopicPartition> partitionsWithLeader = findLeaderForNoLeaderPartitions(
          ImmutableList.copyOf(partitionAddMap.keySet()));
      for (TopicPartition tp : partitionsWithLeader) {
        partitionNewLeaderMap.put(tp, partitionAddMap.get(tp));
        partitionMap.put(tp, partitionNewLeaderMap.get(tp));
        partitionAddMap.remove(tp);
      }

      if (partitionNewLeaderMap.size() != 0) {
        addFetcherForPartitions(partitionNewLeaderMap);
        partitionNewLeaderMap.clear();
      }

      if (partitionAddMap.size() != 0) {
        LOGGER.info("topic partitions can't find leader: {}", partitionAddMap.keySet());
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
          partitionLeaderMap.remove(partition);
        }
      }

      if (partitionDeleteMap.size() != 0) {
        removeFetcherForPartitions(partitionDeleteMap.keySet());
      }
      partitionDeleteMap.clear();
    }
    try {
      Thread.sleep(refreshLeaderBackoff);
    } catch (InterruptedException e) {
      LOGGER.error("[{}]InterruptedException on sleep", getName());
    }
  }

  private List<TopicPartition> findLeaderForNoLeaderPartitions(
      List<TopicPartition> topicPartitions) {
    List<TopicPartition> partitionsWithLeader = new ArrayList<>();
    if (topicPartitions.size() == 0) {
      return partitionsWithLeader;
    }
    Map<String, List<PartitionInfo>> topicInfo = kafkaConsumer.listTopics();

    for (TopicPartition tp : topicPartitions) {
      Integer leader = findLeaderId(tp, topicInfo.get(tp.topic()));
      if (leader != null) {
        partitionsWithLeader.add(tp);
        partitionLeaderMap.put(tp, leader);
      }
    }
    return partitionsWithLeader;
  }

  private Integer findLeaderId(TopicPartition topicPartition,
      List<PartitionInfo> partitionInfoList) {
    if (partitionInfoList == null) {
      return null;
    }
    Optional<PartitionInfo> optInfo = partitionInfoList.stream()
        .filter(x -> x.partition() == topicPartition.partition()).findFirst();

    if (optInfo.isPresent() && optInfo.get().leader() != null) {
      return optInfo.get().leader().id();
    }
    return null;
  }
}
