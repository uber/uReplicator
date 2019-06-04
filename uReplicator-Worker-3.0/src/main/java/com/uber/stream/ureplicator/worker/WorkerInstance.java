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

import com.uber.stream.kafka.mirrormaker.common.core.TopicPartitionCountObserver;
import com.uber.stream.ureplicator.common.KafkaUReplicatorMetricsReporter;
import com.uber.stream.ureplicator.common.MetricsReporterConf;
import com.uber.stream.ureplicator.worker.interfaces.ICheckPointManager;
import com.uber.stream.ureplicator.worker.interfaces.IConsumerFetcherManager;
import com.uber.stream.ureplicator.worker.interfaces.IMessageTransformer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerInstance {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerInstance.class);

  protected AtomicBoolean isShuttingDown = new AtomicBoolean(false);

  protected final WorkerConf workerConf;
  protected final Map<String, String> topicMapping;
  private final Properties producerProps;
  private final CustomizedConsumerConfig consumerProps;
  private final Properties clusterProps;
  private final List<BlockingQueue<FetchedDataChunk>> messageQueue = new ArrayList<>();
  private final List<ConsumerIterator> consumerStream = new ArrayList<>();
  private final int numOfProducer;
  private final int maxQueueSize;

  private String topicObserverZk;
  private IConsumerFetcherManager fetcherManager;
  private IMessageTransformer messageTransformer;
  private ProducerManager producerManager;
  private ICheckPointManager checkpointManager;
  // use to observe destination topic partition
  protected TopicPartitionCountObserver observer;

  /**
   * Main constructor
   *
   * @param workerConf worker configuration
   */
  public WorkerInstance(WorkerConf workerConf) {
    this.workerConf = workerConf;
    producerProps = WorkerUtils.loadProperties(workerConf.getProducerConfigFile());
    consumerProps = new CustomizedConsumerConfig(
        WorkerUtils.loadProperties(workerConf.getConsumerConfigFile()));
    clusterProps = WorkerUtils.loadProperties(workerConf.getClusterConfigFile());
    if (workerConf.getFederatedEnabled() && clusterProps == null) {
      LOGGER.error("cluster config file required for federated mode");
      throw new IllegalArgumentException("cluster config file required for federated mode");
    }
    Properties topicMappingProps = WorkerUtils.loadProperties(workerConf.getTopicMappingFile());
    topicMapping = initializeTopicMapping(topicMappingProps);
    String numOfProducerStr = producerProps.getProperty(Constants.PRODUCER_NUMBER_OF_PRODUCERS,
        Constants.DEFAULT_NUMBER_OF_PRODUCERS);
    maxQueueSize = consumerProps.getConsumerMaxQueueSize();
    numOfProducer = Math.max(1, Integer.valueOf(numOfProducerStr));
  }

  /**
   * Sets message transformer
   *
   * @param messageTransformer customized message transformer
   */
  public void setMessageTransformer(IMessageTransformer messageTransformer) {
    this.messageTransformer = messageTransformer;
  }

  /**
   * Sets checkpoint manager
   *
   * @param checkpointManager checkpoint manager
   */
  public void setCheckpointManager(ICheckPointManager checkpointManager) {
    this.checkpointManager = checkpointManager;
  }

  /**
   * Starts worker instance, srcCluster and dstCluster for non federated mode is empty
   *
   * @param srcCluster source cluster name
   * @param dstCluster destination cluster name
   * @param routeId routeId for federated uReplicator
   * @param federatedDeploymentName deployment name for federated uReplicator
   */
  public void start(String srcCluster, String dstCluster, String routeId,
      String federatedDeploymentName) {
    if (isShuttingDown.get()) {
      LOGGER.error("WorkerInstance start failure, can't restart a shutdown instance");
    }

    initializeProperties(srcCluster, dstCluster);
    // Init blocking queue
    initializeConsumerStream();
    initializeTopicObserver();
    initializeMetricsReporter(srcCluster, dstCluster, routeId, federatedDeploymentName);
    additionalConfigs(srcCluster, dstCluster);

    if (messageTransformer == null) {
      messageTransformer = new DefaultMessageTransformer(observer, topicMapping);
    }

    if (checkpointManager == null) {
      String groupId = consumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG, "uReplicator");
      if (workerConf.getFederatedEnabled()) {
        groupId = "ureplicator-" + srcCluster + "-" + dstCluster;
      }
      checkpointManager = new ZookeeperCheckpointManager(consumerProps, groupId);
    }

    fetcherManager = new FetcherManagerGroupByLeaderId("fetcher-manager-thread",
        consumerProps);
    fetcherManager.start();

    producerManager = new ProducerManager(consumerStream, producerProps,
        workerConf.getAbortOnSendFailure(), messageTransformer, checkpointManager, this);
    producerManager.start();
  }

  /**
   * Adds topic partition to worker instance
   *
   * @param topic topic name
   * @param partition partition id
   */
  public void addTopicPartition(String topic, int partition) {
    addTopicPartition(topic, partition, null, null, null);
  }

  /**
   * Adds topic partition to worker instance
   *
   * @param topic topic name
   * @param partition partition id
   * @param startingOffset starting offset for topic partition
   * @param endingOffset ending offset for topic partition
   * @param dstTopic topic name in destination cluster
   */
  public void addTopicPartition(String topic, int partition, Long startingOffset,
      Long endingOffset, String dstTopic) {
    if (observer != null) {
      observer.addTopic(topic);
    }
    if (StringUtils.isNotBlank(dstTopic)) {
      topicMapping.put(topic, dstTopic);
    }
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    long offset =
        startingOffset != null ? startingOffset
            : checkpointManager.fetchOffset(topicPartition);

    int queueId = calculateQueueId(topicPartition, numOfProducer);
    LOGGER.info("Adding topic: {}, partition {}, starting offset {}, checkQueueId {}",
        topic, partition, offset, queueId);
    PartitionOffsetInfo offsetInfo = new PartitionOffsetInfo(topicPartition, offset, endingOffset,
        messageQueue.get(queueId));
    fetcherManager.addTopicPartition(topicPartition, offsetInfo);
  }

  /**
   * Deletes topic partition
   *
   * @param topic topic name
   * @param partition partition id
   */
  public void deleteTopicPartition(String topic, int partition) {
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    fetcherManager.removeTopicPartition(topicPartition);
  }

  public synchronized void cleanShutdown() {
    if (!isShuttingDown.compareAndSet(false, true)) {
      return;
    }
    LOGGER.info("Start clean shutdown");

    if (observer != null) {
      try {
        LOGGER.info("Shutdown observer");

        observer.shutdown();
      } catch (Exception e) {
        LOGGER.error("Failed to shut down observer", e);
      } finally {
        observer = null;
      }
    }

    if (fetcherManager != null) {
      try {
        LOGGER.info("Shutdown Consumer");
        fetcherManager.shutdown();
      } catch (Exception e) {
        LOGGER.error("Failed to shut down consumer", e);
      }
    }

    if (producerManager != null) {
      LOGGER.info("Shutdown producer manager");
      producerManager.cleanShutdown();
    }

    for (ConsumerIterator iterator : consumerStream) {
      iterator.cleanCurrentChunk();
    }
    KafkaUReplicatorMetricsReporter.stop();

    topicMapping.clear();
    LOGGER.info("Kafka uReplicator worker shutdown successfully");
  }

  private int calculateQueueId(TopicPartition topicPartition, int numOfProducer) {
    return Math.abs(topicPartition.hashCode()) % numOfProducer;
  }

  private void initializeConsumerStream() {
    int consumerTimeout = consumerProps.getConsumerTimeoutMs();
    for (int i = 0; i < numOfProducer; i++) {
      messageQueue.add(new LinkedBlockingQueue<>(maxQueueSize));
      ConsumerIterator iterator = new ConsumerIterator(messageQueue.get(i), consumerTimeout);
      consumerStream.add(iterator);
    }
  }

  private void initializeProperties(String srcCluster, String dstCluster) {
    String commitZk = consumerProps.getProperty(Constants.ZK_SERVER, "");
    topicObserverZk = producerProps.getProperty(Constants.ZK_SERVER, "");
    // override properties for federated
    if (workerConf.getFederatedEnabled()) {
      if (StringUtils.isEmpty(srcCluster) || StringUtils.isEmpty(dstCluster)) {
        throw new RuntimeException(
            String.format(
                "srcCluster and dstCluster are required for federated mode. current value: {} - {}",
                srcCluster, dstCluster)
        );
      }
      String srcServers = clusterProps
          .getProperty(Constants.FEDERATED_CLUSTER_SERVER_CONFIG_PREFIX + srcCluster, "");
      String dstServers = clusterProps
          .getProperty(Constants.FEDERATED_CLUSTER_SERVER_CONFIG_PREFIX + dstCluster, "");
      producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, dstServers);
      consumerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, srcServers);

      commitZk = clusterProps
          .getProperty(Constants.FEDERATED_CLUSTER_ZK_CONFIG_PREFIX + srcCluster, "");
      topicObserverZk = clusterProps
          .getProperty(Constants.FEDERATED_CLUSTER_ZK_CONFIG_PREFIX + dstCluster, "");
      if (StringUtils.isBlank(commitZk)) {
        throw new IllegalArgumentException(
            String.format("Failed to find %s in  config file %s:",
                Constants.FEDERATED_CLUSTER_ZK_CONFIG_PREFIX + srcCluster,
                workerConf.getClusterConfigFile()));
      }
    } else {
      commitZk = consumerProps.getProperty(Constants.COMMIT_ZOOKEEPER_SERVER_CONFIG, commitZk);
      if (StringUtils.isBlank(commitZk)) {
        throw new IllegalArgumentException(
            "Failed to find commitZk server, property zkServer or commit.zookeeper.connect is required in Consumer config file :"
                + workerConf.getConsumerConfigFile());
      }
    }
    consumerProps.setProperty(Constants.COMMIT_ZOOKEEPER_SERVER_CONFIG, commitZk);
  }

  private void initializeMetricsReporter(String srcCluster, String dstCluster, String routeId, String federatedDeploymentName) {
    List<String> additionalInfo = new ArrayList<>();
    additionalInfo.add(workerConf.getMetricsPrefix());
    if (workerConf.getFederatedEnabled()) {
      additionalInfo.add(federatedDeploymentName);
      additionalInfo.add(String.format("%s-%s-%s", srcCluster, dstCluster, routeId));
    }
    MetricsReporterConf metricsReporterConf = new MetricsReporterConf(workerConf.getRegion(),
        additionalInfo, workerConf.getHostname(), workerConf.getGraphiteHost(),
        workerConf.getGraphitePort());
    KafkaUReplicatorMetricsReporter.init(metricsReporterConf);
  }

  private void initializeTopicObserver() {
    if (StringUtils.isNotBlank(topicObserverZk) && workerConf
        .enableDestinationPartitionCountObserver()) {
      String zkPath = producerProps
          .getProperty(Constants.PRODUCER_ZK_OBSERVER, Constants.DEFAULT_PRODUCER_ZK_OBSERVER);
      observer = new TopicPartitionCountObserver(topicObserverZk,
          zkPath,
          Integer.parseInt(producerProps.getProperty("connection.timeout.ms", "120000")),
          Integer.parseInt(producerProps.getProperty("session.timeout.ms", "600000")),
          Integer.parseInt(producerProps.getProperty("refresh.interval.ms", "3600000")));
      observer.start();
      for (String dstTopic : topicMapping.values()) {
        observer.addTopic(dstTopic);
      }
    } else {
      LOGGER.info("Disable TopicPartitionCountObserver to use round robin to produce msg.");
    }
  }

  private Map<String, String> initializeTopicMapping(Properties topicMappingProps) {
    Map<String, String> mapping = new HashMap<>();
    if (topicMappingProps == null) {
      return mapping;
    }
    for (String consumerTopic : topicMappingProps.stringPropertyNames()) {
      String producerTopic = topicMappingProps.getProperty(consumerTopic);
      if (StringUtils.isNotBlank(producerTopic)) {
        mapping.put(consumerTopic, producerTopic);
      }
    }
    return mapping;
  }

  public void additionalConfigs(String srcCluster, String dstCluster) {
  }

  /**
   * Additional callback handler after message produce completed
   *
   * @param metadata record metadata
   * @param srcPartition source cluster partition id
   * @param srcOffset source cluster offset
   */
  protected void onProducerCompletionWithoutException(RecordMetadata metadata, int srcPartition,
      long srcOffset) {
  }
}
