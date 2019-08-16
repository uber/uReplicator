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
package com.uber.streaming.worker.fetcher;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.uber.streaming.worker.Fetcher;
import com.uber.streaming.worker.Sink;
import com.uber.streaming.worker.Task;
import com.uber.streaming.worker.clients.CheckpointManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.GuardedBy;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FetcherManager manages ConsumerFetcherThread. Task will distributed into multiple fetcher threads
 * by using two keys: 1. topic partition hashcode mod by num of fetcher thread(properties:
 * num.consumer.fetchers) 2. consumer group. Implementation with other grouping algorithm shall
 * override getOrCreateFetcher
 */
public class KafkaConsumerFetcherManager implements Fetcher<List<ConsumerRecord>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerFetcherManager.class);

  private final KafkaConsumerFetcherConfig consumerProps;

  protected final int numberOfConsumerFetcher;

  private final CheckpointManager checkpointManager;
  protected Sink<List<ConsumerRecord>> processorSink;

  @GuardedBy("fetcherThreadMap")
  protected final Map<String, KafkaConsumerFetcherThread> fetcherThreadMap;

  // fetcherThreadLock guards fetcherThreadMap
  protected final Object fetcherThreadLock = new Object();
  private final String defaultConsumerGroup;


  protected KafkaConsumerFetcherManager(
      KafkaConsumerFetcherConfig conf, CheckpointManager checkpointManager) {
    this(conf, checkpointManager, new ConcurrentHashMap<>());
  }

  // TODO:(yayang) add a constructor uses code configuration instead of properties
  @VisibleForTesting
  protected KafkaConsumerFetcherManager(KafkaConsumerFetcherConfig conf,
      CheckpointManager checkpointManager,
      Map<String, KafkaConsumerFetcherThread> fetcherThreadMap) {
    Preconditions.checkNotNull(checkpointManager, "checkpointManager can't be null");
    Preconditions.checkNotNull(conf, "KafkaConsumerFetcherConfig can't be null");
    if (StringUtils.isBlank(
        conf.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))) {
      throw new IllegalArgumentException(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG + " required in FetcherProperties");
    }
    if (StringUtils.isBlank(conf.getProperty(ConsumerConfig.GROUP_ID_CONFIG))) {
      throw new IllegalArgumentException(
          ConsumerConfig.GROUP_ID_CONFIG + " required in FetcherProperties");
    }

    this.consumerProps = conf;
    this.checkpointManager = checkpointManager;
    this.defaultConsumerGroup = consumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);

    // disable offset auto commit
    this.consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    KafkaConsumerFetcherConfig consumerFetcherConfig = new KafkaConsumerFetcherConfig(
        consumerProps);
    this.numberOfConsumerFetcher = consumerFetcherConfig.getNumberOfConsumerFetcher();
    this.fetcherThreadMap = fetcherThreadMap;
  }


  private Fetcher getOrCreateFetcher(Task task) {
    String fetcherName = getKafkaConsumerFetcherThreadName(task);
    KafkaConsumerFetcherThread fetcherThread = fetcherThreadMap.get(fetcherName);
    if (fetcherThread != null && fetcherThread.isRunning()) {
      return fetcherThread;
    } else {
      return createFetcher(task, fetcherName);
    }
  }

  private Fetcher createFetcher(Task task, String fetcherName) {
    synchronized (fetcherThreadLock) {
      // create new thread
      KafkaConsumerFetcherThread fetcherThread = fetcherThreadMap.get(fetcherName);
      if (fetcherThread != null && fetcherThread.isRunning()) {
        return fetcherThread;
      }
      fetcherThreadMap.remove(fetcherName);
      String consumerGroup = task.getConsumerGroup(defaultConsumerGroup);
      // clone properties to a new KafkaConsumerFetcherConfig
      KafkaConsumerFetcherConfig consumerFetcherConfig = new KafkaConsumerFetcherConfig(
          consumerProps);

      LOGGER.info("Creating new fetcher thread: {}", fetcherName);
      consumerFetcherConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
      KafkaConsumerFetcherThread thread = new KafkaConsumerFetcherThread(fetcherName,
          consumerFetcherConfig, checkpointManager);
      thread.setNextStage(processorSink);
      thread.start();
      fetcherThreadMap.putIfAbsent(fetcherName, thread);
      return thread;
    }
  }

  private String getKafkaConsumerFetcherThreadName(Task task) {
    int fetcherId = Objects.hash(task.getTopic(), task.getPartition()) % numberOfConsumerFetcher;
    String consumerGroup =
        StringUtils.isBlank(task.getConsumerGroup()) ? defaultConsumerGroup
            : task.getConsumerGroup();
    return String.format("KafkaConsumerFetcherThread-%s-%d", consumerGroup, fetcherId);
  }

  @Override
  public void start() {
    if (processorSink == null) {
      LOGGER.error("data sink required");
      throw new IllegalArgumentException("data sink required");
    }
    if (checkpointManager == null) {
      LOGGER.error("servicesManager.checkpointManager required");
      throw new IllegalArgumentException("servicesManager.checkpointManager required");
    }
  }


  @Override
  public void addTask(Task task) {
    getOrCreateFetcher(task).addTask(task);
  }

  @Override
  public void removeTask(Task task) {
    // TODO:(yayang) 1. make sure KafkaConsumerFetcherManager is running
    // TODO:(yayang) 2. shutdown fetcher thread when it's empty.
    getOrCreateFetcher(task).removeTask(task);
  }

  @Override
  public void pauseTask(Task task) {
    // TODO: to be implemented
  }

  @Override
  public void resumeTask(Task task) {
    // TODO: to be implemented
  }

  @Override
  public List<Task> getTasks() {
    List<Task> tasks = new ArrayList<>();
    synchronized (fetcherThreadLock) {
      for (KafkaConsumerFetcherThread fetcherThread : fetcherThreadMap.values()) {
        tasks.addAll(fetcherThread.getTasks());
      }
    }
    return tasks;
  }

  @Override
  public void setNextStage(Sink sink) {
    this.processorSink = sink;
  }

  @Override
  public void close() {
    // TOD: use completable future here
    synchronized (fetcherThreadLock) {
      for (KafkaConsumerFetcherThread thread : fetcherThreadMap.values()) {
        thread.initiateShutdown();
      }
      for (KafkaConsumerFetcherThread thread : fetcherThreadMap.values()) {
        thread.awaitShutdown();
      }
    }
  }

  public static class Builder {
    private CheckpointManager checkpointManager;
    private KafkaConsumerFetcherConfig conf;

    public Builder() {

    }

    public Builder setCheckpointManager(CheckpointManager checkpointManager) {
      this.checkpointManager = checkpointManager;
      return this;
    }

    public Builder setConf(KafkaConsumerFetcherConfig conf) {
      this.conf = conf;
      return this;
    }

    public KafkaConsumerFetcherManager build() {
      return new KafkaConsumerFetcherManager(conf, checkpointManager);
    }
  }
}
