/**
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
package kafka.mirrormaker

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, TimeUnit}

import kafka.common.{AppInfo, OffsetAndMetadata, OffsetMetadataAndError, TopicAndPartition}
import kafka.consumer._
import kafka.metrics.{KafkaMetricsGroup, KafkaMetricsReporter}
import kafka.offsetmanager.OffsetManager
import kafka.serializer.DefaultDecoder
import kafka.utils.{Pool, ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient

import scala.collection.JavaConverters._

/**
 * This class handles the consumers interaction with zookeeper
 * Stores the consumer offsets to Zookeeper.
 *
 * @param consumerIdString
 * @param consumerConfig
 */
class KafkaConnector(private val consumerIdString: String,
                     private val consumerConfig: ConsumerConfig) extends KafkaMetricsGroup {

  private val zkClient: ZkClient = ZkUtils.createZkClient(consumerConfig.zkConnect, consumerConfig.zkSessionTimeoutMs, consumerConfig.zkConnectionTimeoutMs)
  private val queue: LinkedBlockingQueue[FetchedDataChunk] = new LinkedBlockingQueue[FetchedDataChunk](consumerConfig.queuedMaxMessages)
  private val decoder: DefaultDecoder = new DefaultDecoder()
  private val fetcherManager: CompactConsumerFetcherManager = new CompactConsumerFetcherManager(consumerIdString, consumerConfig, zkClient)
  private val zkUtils = ZkUtils.apply(zkClient, isZkSecurityEnabled = false)
  private val cluster = zkUtils.getCluster()

  // Using a concurrent hash map for efficiency. Without this we will need a lock
  val topicRegistry = new ConcurrentHashMap[TopicAndPartition, PartitionTopicInfo]()
  private val isShuttingDown = new AtomicBoolean(false)

  // Create Zookeeper/Kafka offsetManager based on the consumer's configuration
  private val offsetManager = OffsetManager(consumerConfig, zkUtils)
  private val checkpointedOffsets = new Pool[TopicAndPartition, Long]

  // Initialize the fetcher manager
  fetcherManager.startConnections(Nil, cluster)

  KafkaMetricsReporter.startReporters(consumerConfig.props)
  AppInfo.registerInfo()

  def shutdown(): Unit = {
    val canShutdown = isShuttingDown.compareAndSet(false, true)
    if (canShutdown) {
      info("Connector is now shutting down !")
      KafkaMetricsGroup.removeAllConsumerMetrics(consumerConfig.clientId)
      fetcherManager.stopConnections()
      if (zkClient != null) {
        zkClient.close()
      }
    }
  }

  def getStream(): KafkaStream[Array[Byte], Array[Byte]] = {
    val stream = new KafkaStream(queue, consumerConfig.consumerTimeoutMs, decoder, decoder, consumerConfig.clientId)
    stream
  }

  def addTopicPartition(topic: String, partition: Int): Unit = {
    info("Adding topic: %s , partition %d".format(topic, partition))

    val topicAndPartition = TopicAndPartition(topic, partition)
    if (topicRegistry.keySet().contains(topicAndPartition)) {
      info("Topic %s and partition %d already exist. Ignoring operation".format(topic, partition))
      return
    }

    // Fetch the current offset of the added topic, partition and consumerGroup.
    val offset = offsetManager.fetchOffset(topicAndPartition)._2.offset
    info("Fetched offset : %d, for topic: %s , partition %d".format(offset, topic, partition))
    val consumedOffset = new AtomicLong(offset)
    val fetchedOffset = new AtomicLong(offset)
    val partTopicInfo: PartitionTopicInfo = new PartitionTopicInfo(topic,
      partition,
      queue,
      consumedOffset,
      fetchedOffset,
      new AtomicInteger(consumerConfig.fetchMessageMaxBytes),
      consumerIdString)

    fetcherManager.addTopicPartition(partTopicInfo)
    topicRegistry.put(TopicAndPartition(topic, partition), partTopicInfo)
  }

  def deleteTopicPartition(topic: String, partition: Int, deleteOnly: Boolean): Unit = {
    info("Removing topic: %s , partition %d".format(topic, partition))
    val topicAndPartition = TopicAndPartition(topic, partition)
    if (!topicRegistry.keySet().contains(topicAndPartition)) {
      info("Topic %s and partition %d don't exist. Ignoring operation".format(topic, partition))
      return
    }

    val pti = topicRegistry.get(topicAndPartition)
    fetcherManager.removeTopicPartition(pti)

    // commit offset for this topic partition before deletion
    if (!deleteOnly) {
      offsetManager.commitOffsets(Map(topicAndPartition -> OffsetAndMetadata(pti.getConsumeOffset())), checkpointedOffsets)
    }

    topicRegistry.remove(topicAndPartition)
    info("Finish deleteTopicPartition in KafkaConnector for topic: %s , partition %d".format(topic, partition))
  }

  def commitOffsets(): Unit = {
    // Convert the Java concurrent hashmap into a map of offsets to commit
    val offsetsToCommit = topicRegistry.asScala.map { case (topic, info) =>
      topic -> OffsetAndMetadata(info.getConsumeOffset())}.toMap

    // Commit all the changed offsets.
    offsetManager.commitOffsets(offsetsToCommit, checkpointedOffsets)
  }
}
