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

import com.uber.stream.ureplicator.common.observer.TopicPartitionLeaderObserver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FetcherManagerGroupByLeaderId extends FetcherManager and changed fetcher thread distribution
 * logic by overriding addTopicPartitionsToFetcherThread. Topic partitions distribution
 * based on destination cluster leaderId. This has more tolerance on single broker slow down.
 */
public class FetcherManagerGroupByLeaderId extends FetcherManager {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(FetcherManagerGroupByLeaderId.class);

  @VisibleForTesting
  protected TopicPartitionLeaderObserver clusterObserver;

  public FetcherManagerGroupByLeaderId(String threadName,
      CustomizedConsumerConfig consumerProperties,
      List<BlockingQueue<FetchedDataChunk>> messageQueue,
      TopicPartitionLeaderObserver clusterObserver) {
    this(threadName, consumerProperties, new ConcurrentHashMap<>(), messageQueue, clusterObserver);
  }

  @VisibleForTesting
  protected FetcherManagerGroupByLeaderId(String threadName,
      CustomizedConsumerConfig consumerProperties,
      Map<String, ConsumerFetcherThread> fetcherThreadMap,
      List<BlockingQueue<FetchedDataChunk>> messageQueue,
      TopicPartitionLeaderObserver clusterObserver) {
    super(threadName, consumerProperties, fetcherThreadMap, messageQueue);
    this.clusterObserver = clusterObserver;
  }

  private String getFetcherThreadName(int leaderBroker) {
    return String.format("%s-%d", FETCHER_THREAD_PREFIX, leaderBroker % numberOfConsumerFetcher);
  }

  @Override
  public Map<TopicPartition, PartitionOffsetInfo> addTopicPartitionsToFetcherThread(
      Map<TopicPartition, PartitionOffsetInfo> topicPartitions) {

    Map<TopicPartition, PartitionOffsetInfo> result = new HashMap<>();
    if (topicPartitions == null || topicPartitions.size() == 0) {
      return result;
    }

    Map<TopicPartition, Integer> topicsWithLeader = clusterObserver
        .findLeaderForPartitions(new ArrayList<>(topicPartitions.keySet()));

    for (Map.Entry<TopicPartition, PartitionOffsetInfo> entry : topicPartitions.entrySet()) {
      Integer leaderId = topicsWithLeader.get(entry.getKey());
      if (leaderId == null) {
        continue;
      }
      String fetcherThreadName = getFetcherThreadName(leaderId);
      addFetcherForTopicPartition(entry.getKey(), entry.getValue(), fetcherThreadName);
      result.putIfAbsent(entry.getKey(), entry.getValue());
    }
    return result;
  }
}
