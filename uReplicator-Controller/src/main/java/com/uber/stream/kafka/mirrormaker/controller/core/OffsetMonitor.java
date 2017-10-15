/*
 * Copyright (C) 2015-2017 Uber Technologies, Inc. (streaming-data@uber.com)
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
package com.uber.stream.kafka.mirrormaker.controller.core;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;
import com.uber.stream.kafka.mirrormaker.controller.reporter.HelixKafkaMirrorMakerMetricsReporter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffsetMonitor {

  private static final Logger logger = LoggerFactory.getLogger(OffsetMonitor.class);

  private final HelixMirrorMakerManager helixMirrorMakerManager;
  private final LinkedBlockingQueue<ZkClient> zkClientQueue;
  private final long refreshIntervalInSec;
  private final String consumerOffsetPath;

  private final ScheduledExecutorService refreshExecutor;
  private final ExecutorService cronExecutor;

  private List<String> srcBrokerList;
  private List<String> topicList;
  private final Map<String, SimpleConsumer> brokerConsumer;
  private final Map<TopicAndPartition, BrokerEndPoint> partitionLeader;
  private final Map<TopicAndPartition, TopicPartitionLag> topicPartitionToOffsetMap;

  private static final String NO_PROGRESS_METRIC_NAME = "NumNoProgressPartitions";
  private static final long MIN_NO_PROGRESS_TIME_MS = TimeUnit.MINUTES.toMillis(10);
  private final Map<TopicAndPartition, TopicPartitionLag> noProgressMap;
  private final AtomicInteger numNoProgressTopicPartitions = new AtomicInteger();

  public OffsetMonitor(final HelixMirrorMakerManager helixMirrorMakerManager,
      ControllerConf controllerConf) {
    int numOffsetThread = controllerConf.getNumOffsetThread();
    this.helixMirrorMakerManager = helixMirrorMakerManager;
    String zkString = controllerConf.getConsumerCommitZkPath().isEmpty() ?
        controllerConf.getSrcKafkaZkPath() : controllerConf.getConsumerCommitZkPath();
    this.zkClientQueue = new LinkedBlockingQueue<>(numOffsetThread);
    for (int i = 0; i < numOffsetThread; i++) {
      ZkClient zkClient = new ZkClient(zkString, 30000, 30000, ZKStringSerializer$.MODULE$);
      zkClientQueue.add(zkClient);
    }

    ZkClient zkClient = new ZkClient(controllerConf.getSrcKafkaZkPath(), 30000, 30000, ZKStringSerializer$.MODULE$);
    List<String> brokerIdList = zkClient.getChildren("/brokers/ids");
    JSONParser parser = new JSONParser();
    this.srcBrokerList = new ArrayList<>();
    for (String id : brokerIdList) {
      try {
        JSONObject json = (JSONObject) parser.parse(zkClient.readData("/brokers/ids/" + id).toString());
        srcBrokerList.add(String.valueOf(json.get("host")) + ":" + String.valueOf(json.get("port")));
      } catch (ParseException e) {
        logger.warn("Failed to get broker", e);
      }
    }

    this.consumerOffsetPath = "/consumers/" + controllerConf.getGroupId() + "/offsets/";
    // disable monitor if GROUP_ID is not set
    if (controllerConf.getGroupId().isEmpty()) {
      logger.warn("Consumer GROUP_ID is not set. Offset manager is disabled");
      this.refreshIntervalInSec = 0;
    } else {
      this.refreshIntervalInSec = controllerConf.getOffsetRefreshIntervalInSec();
    }

    this.refreshExecutor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("topic-list-cron-%d").setDaemon(true).build());
    this.cronExecutor = new ThreadPoolExecutor(numOffsetThread, numOffsetThread, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(controllerConf.getBlockingQueueSize()),
        new ThreadFactoryBuilder().setNameFormat("topic-offset-cron-%d").setDaemon(true).build());

    this.topicList = new ArrayList<>();
    this.brokerConsumer = new ConcurrentHashMap<>();
    this.partitionLeader = new ConcurrentHashMap<>();
    this.topicPartitionToOffsetMap = new ConcurrentHashMap<>();
    this.noProgressMap = new ConcurrentHashMap<>();
  }

  public void start() {
    if (refreshIntervalInSec > 0) {
      // delay for 1-5 minutes
      int delaySec = 60 + new Random().nextInt(240);
      logger.info("OffsetMonitor starts updating offsets every {} seconds with delay {} seconds", refreshIntervalInSec,
          delaySec);
      logger.info("OffsetMonitor starts with brokerList=" + srcBrokerList);

      refreshExecutor.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          logger.info("TopicList starts updating");
          updateTopicList();
          updateOffset();
          updateOffsetMetrics();
        }
      }, delaySec, refreshIntervalInSec, TimeUnit.SECONDS);
      registerNoProgressMetric();
    } else {
      logger.info("OffsetMonitor is disabled");
    }
  }

  public void stop() throws InterruptedException {
    refreshExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
    cronExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
    for (SimpleConsumer consumer : brokerConsumer.values()) {
      consumer.close();
    }
    logger.info("OffsetMonitor closed");
  }

  private void updateTopicList() {
    logger.info("Update topicList");
    topicList.clear();
    partitionLeader.clear();

    // update topicList
    topicList = helixMirrorMakerManager.getTopicLists();
    logger.debug("TopicList: {}", topicList);

    // update partitionLeader
    for (String broker : srcBrokerList) {
      try {
        SimpleConsumer consumer = getSimpleConsumer(broker);
        TopicMetadataRequest req = new TopicMetadataRequest(topicList);
        kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
        List<TopicMetadata> metaData = resp.topicsMetadata();

        for (TopicMetadata tmd : metaData) {
          for (PartitionMetadata pmd : tmd.partitionsMetadata()) {
            TopicAndPartition topicAndPartition = new TopicAndPartition(tmd.topic(), pmd.partitionId());
            partitionLeader.put(topicAndPartition, pmd.leader());
          }
        }
        break;
      } catch (Exception e) {
        logger.warn("Got exception to get metadata from broker=" + broker, e);
      }
    }
  }

  protected void updateOffset() {
    logger.debug("OffsetMonitor updates offset with leaders=" + partitionLeader);

    for (Map.Entry<TopicAndPartition, BrokerEndPoint> entry : partitionLeader.entrySet()) {
      String leaderBroker = getHostPort(entry.getValue());
      TopicAndPartition tp = entry.getKey();
      if (StringUtils.isEmpty(leaderBroker)) {
        logger.warn("{} does not have leader partition", tp);
      } else {
        try {
          cronExecutor.submit(updateOffsetTask(leaderBroker, tp));
        } catch (RejectedExecutionException re) {
          logger.warn(String.format("cronExecutor is full! Drop task for topic: %s, partition: %d",
              tp.topic(), tp.partition()), re);
        } catch (Throwable t) {
          logger.error(String.format("cronExecutor got throwable! Drop task for topic: %s, partition: %d",
              tp.topic(), tp.partition()), t);
          throw t;
        }
      }
    }
  }

  private Runnable updateOffsetTask(final String leaderBroker, final TopicAndPartition tp) {
    return new Runnable() {
      @Override
      public void run() {
        if (StringUtils.isEmpty(consumerOffsetPath)) {
          logger.warn("No consumer group id, skip updateOffsetTask");
          return;
        }
        ZkClient zk = zkClientQueue.poll();
        try {
          Object obj = zk.readData(consumerOffsetPath + tp.topic() + "/" + tp.partition(), true);
          long commitOffset = obj == null ? -1 : Long.valueOf(String.valueOf(obj));

          SimpleConsumer consumer = getSimpleConsumer(leaderBroker);
          long latestOffset = getLatestOffset(consumer, tp);
          if (latestOffset < 0) {
            latestOffset = -1;
          }

          TopicPartitionLag previousOffset = topicPartitionToOffsetMap
              .put(tp, new TopicPartitionLag(latestOffset, commitOffset));
          logger.debug("Get latest offset={} committed offset={} for {}", latestOffset, commitOffset, tp);
          if (latestOffset > 0 && commitOffset > 0) {
            if (latestOffset - commitOffset > 0 && previousOffset != null
                && previousOffset.getCommitOffset() == commitOffset) {
              TopicPartitionLag oldLag = noProgressMap.get(tp);
              // keep the oldest record (the time began to have no progress) in
              // order to measure whether the time larger than the threshold,
              // therefore we do not overwrite the old record if the commit
              // offset is the same as current
              if (oldLag == null || oldLag.getCommitOffset() != commitOffset) {
                noProgressMap.put(tp, previousOffset);
              }
            } else {
              noProgressMap.remove(tp);
            }
          }
        } catch (Exception e) {
          logger.warn("Got exception to get offset for TopicPartition=" + tp, e);
        } finally {
          zkClientQueue.add(zk);
        }
      }
    };
  }

  private static String getHostPort(BrokerEndPoint leader) {
    if (leader != null) {
      return leader.host() + ":" + leader.port();
    }
    return null;
  }

  private SimpleConsumer getSimpleConsumer(String broker) {
    SimpleConsumer consumer = brokerConsumer.get(broker);
    if (consumer == null) {
      int idx = broker.indexOf(":");
      if (idx >= 0) {
        String brokerHost = broker.substring(0, idx);
        String port = broker.substring(idx + 1);
        consumer = new SimpleConsumer(brokerHost, Integer.parseInt(port), 60000, 64 * 1024,
            "metadataFetcher-" + brokerHost);
        brokerConsumer.put(broker, consumer);
      }
    }
    return consumer;
  }

  private long getLatestOffset(SimpleConsumer consumer, TopicAndPartition topicAndPartition) {
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1));
    kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo,
        kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
    OffsetResponse response = consumer.getOffsetsBefore(request);

    if (response.hasError()) {
      logger.warn("Failed to fetch offset for {} due to {}", topicAndPartition,
          response.errorCode(topicAndPartition.topic(), topicAndPartition.partition()));
      return -1;
    }

    long[] offsets = response.offsets(topicAndPartition.topic(), topicAndPartition.partition());
    return offsets[0];
  }

  private TopicAndPartition toTopicAndPartition(TopicPartition topicPartition) {
    return new TopicAndPartition(topicPartition.getTopic(), topicPartition.getPartition());
  }

  public TopicPartitionLag getTopicPartitionOffset(TopicPartition topicPartition) {
    return topicPartitionToOffsetMap.get(toTopicAndPartition(topicPartition));
  }

  /**
   * Expose the internal offset map. Use for REST GET only.
   *
   * @return the internal offset map
   */
  public Map<TopicAndPartition, TopicPartitionLag> getTopicToOffsetMap() {
    return topicPartitionToOffsetMap;
  }

  private static String getOffsetLagName(TopicAndPartition tp) {
    return "OffsetMonitorLag." + tp.topic().replace('.', '_') + "." + tp.partition();
  }

  Map<TopicAndPartition, TopicPartitionLag> getNoProgressTopicToOffsetMap() {
    return noProgressMap;
  }

  private synchronized void updateOffsetMetrics() {
    MetricRegistry metricRegistry = HelixKafkaMirrorMakerMetricsReporter.get().getRegistry();
    @SuppressWarnings("rawtypes")
    Map<String, Gauge> gauges = metricRegistry.getGauges();
    for (final TopicAndPartition topicPartition : topicPartitionToOffsetMap.keySet()) {
      String metricName = getOffsetLagName(topicPartition);
      if (!gauges.containsKey(metricName)) {
        Gauge<Long> gauge = new Gauge<Long>() {
          @Override
          public Long getValue() {
            TopicPartitionLag lag = topicPartitionToOffsetMap.get(topicPartition);
            if (lag == null || lag.getLatestOffset() <= 0 || lag.getCommitOffset() <= 0
                || lag.getLatestOffset() <= lag.getCommitOffset()) {
              return 0L;
            }
            return lag.getLatestOffset() - lag.getCommitOffset();
          }
        };
        try {
          metricRegistry.register(metricName, gauge);
        } catch (Exception e) {
          logger.error("Error while registering lag metric " + metricName, e);
        }
      }
    }

    List<TopicAndPartition> noProgressPartitions = getNoProgessTopicPartitions();
    numNoProgressTopicPartitions.set(noProgressPartitions.size());
    if (!noProgressPartitions.isEmpty()) {
      logger.info("Topic partitions with no progress: " + noProgressPartitions);
    }
  }

  private void registerNoProgressMetric() {
    MetricRegistry metricRegistry = HelixKafkaMirrorMakerMetricsReporter.get().getRegistry();
    Gauge<Integer> gauge = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return numNoProgressTopicPartitions.get();
      }
    };
    try {
      metricRegistry.register(NO_PROGRESS_METRIC_NAME, gauge);
    } catch (Exception e) {
      logger.error("Error while registering no progress metric " + NO_PROGRESS_METRIC_NAME, e);
    }
  }

  private List<TopicAndPartition> getNoProgessTopicPartitions() {
    List<TopicAndPartition> tps = new ArrayList<>();
    for (Map.Entry<TopicAndPartition, TopicPartitionLag> entry : noProgressMap.entrySet()) {
      TopicPartitionLag currentLag = topicPartitionToOffsetMap.get(entry.getKey());
      if (currentLag == null || currentLag.getCommitOffset() <= 0 || currentLag.getLatestOffset() <= 0
          || currentLag.getLatestOffset() <= currentLag.getCommitOffset()) {
        continue;
      }
      TopicPartitionLag lastLag = entry.getValue();
      if (currentLag.getTimeStamp() - lastLag.getTimeStamp() > MIN_NO_PROGRESS_TIME_MS
          && currentLag.getCommitOffset() == lastLag.getCommitOffset()) {
        tps.add(entry.getKey());
      }
    }
    return tps;
  }

}
