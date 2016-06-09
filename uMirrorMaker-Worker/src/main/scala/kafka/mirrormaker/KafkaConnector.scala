/**
 * Copyright (C) 2015-2016 Uber Technology Inc. (streaming-core@uber.com)
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
import kafka.serializer.DefaultDecoder
import kafka.utils.{ZkUtils, Pool, ZKGroupTopicDirs}
import org.I0Itec.zkclient.ZkClient

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * This class handles the consumers interaction with zookeeper
 * Stores the consumer offsets to Zookeeper.
 * @param consumerIdString
 * @param config
 */
class KafkaConnector(private val consumerIdString: String,
                     private val config: ConsumerConfig) extends KafkaMetricsGroup {
  private val zkClient: ZkClient = ZkUtils.createZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs)
  private val queue: LinkedBlockingQueue[FetchedDataChunk] = new LinkedBlockingQueue[FetchedDataChunk](config.queuedMaxMessages)
  private val decoder : DefaultDecoder = new DefaultDecoder()
  private val fetcherManager: CompactConsumerFetcherManager = new CompactConsumerFetcherManager(consumerIdString, config, zkClient)
  private val zkUtils = ZkUtils.apply(zkClient, false)
  private val cluster = zkUtils.getCluster()
  private var allPartitionInfos = new mutable.MutableList[PartitionTopicInfo]()

  // Using a concurrent hash map for efficiency. Without this we will need a lock
  val topicRegistry = new ConcurrentHashMap[TopicAndPartition, PartitionTopicInfo]()
  private val checkpointedZkOffsets = new Pool[TopicAndPartition, Long]
  private val isShuttingDown = new AtomicBoolean(false)

  // useful for tracking migration of consumers to store offsets in kafka
  private val kafkaCommitMeter = newMeter("KafkaCommitsPerSec", "commits", TimeUnit.SECONDS, Map("clientId" -> config.clientId))
  private val zkCommitMeter = newMeter("ZooKeeperCommitsPerSec", "commits", TimeUnit.SECONDS, Map("clientId" -> config.clientId))

  // Initialize the fetcher manager
  fetcherManager.startConnections(Nil, cluster)

  KafkaMetricsReporter.startReporters(config.props)
  AppInfo.registerInfo()

  def shutdown(): Unit = {
    val canShutdown = isShuttingDown.compareAndSet(false, true)
    if (canShutdown) {
      info("Connector is now shutting down !")
      KafkaMetricsGroup.removeAllConsumerMetrics(config.clientId)
      fetcherManager.stopConnections()
      commitOffsets
      if (zkClient != null) {
        zkClient.close()
      }
    }
  }

  def getStream(): KafkaStream[Array[Byte], Array[Byte]] = {
    val stream = new KafkaStream(queue, config.consumerTimeoutMs, decoder, decoder, config.clientId)
    stream
  }

  def addTopicPartition(topic: String, partition: Int): Unit = {
    info("Adding topic: %s , partition %d".format(topic, partition))

    val topicAndPartition = TopicAndPartition(topic, partition)
    if (topicRegistry.keySet().contains(topicAndPartition)) {
      info("Topic %s and partition %d already exist. Ignoring operation".format(topic, partition))
      return
    }

    val offsets = fetchOffsetFromZooKeeper(TopicAndPartition(topic, partition))
    var offset = offsets._2.offset
    info("Fetched offset : %d, for topic: %s , partition %d".format(offset, topic, partition))
    val consumedOffset = new AtomicLong(offset)
    val fetchedOffset = new AtomicLong(offset)
    val partTopicInfo: PartitionTopicInfo = new PartitionTopicInfo(topic,
      partition,
      queue,
      consumedOffset,
      fetchedOffset,
      new AtomicInteger(config.fetchMessageMaxBytes),
      consumerIdString)

    fetcherManager.addTopicPartition(partTopicInfo)
    topicRegistry.put(TopicAndPartition(topic, partition), partTopicInfo)
  }

  def deleteTopicPartition(topic: String, partition: Int): Unit = {
    info("Removing topic: %s , partition %d".format(topic, partition))
    val topicAndPartition = TopicAndPartition(topic, partition)
    if (!topicRegistry.keySet().contains(topicAndPartition)) {
      info("Topic %s and partition %d don't exist. Ignoring operation".format(topic, partition))
      return
    }

    val pti = topicRegistry.get(topicAndPartition)
    fetcherManager.removeTopicPartition(pti)

    // commit offset for this topic partition before deletion
    commitOffsetToZooKeeper(topicAndPartition, OffsetAndMetadata(pti.getConsumeOffset()).offset)

    topicRegistry.remove(topicAndPartition)
    info("Finish deleteTopicPartition in KafkaConnector for topic: %s , partition %d".format(topic, partition))
  }

  def commitOffsets: Unit = {
    // Convert the Java concurrent hashmap into a map of offsets to commit
    val offsetsToCommit = topicRegistry.asScala.map { case (topic, info) => {
        topic -> OffsetAndMetadata(info.getConsumeOffset())
      }
    }
    kafkaCommitMeter.mark(offsetsToCommit.size)
    // Commit all offsets to Zookeeper
    offsetsToCommit.foreach { case (topicAndPartition, offsetAndMetadata) =>
      commitOffsetToZooKeeper(topicAndPartition, offsetAndMetadata.offset)
    }
  }

  def commitOffsetToZooKeeper(topicPartition: TopicAndPartition, offset: Long) {
    // Check if the offsets need to be updated
    if (checkpointedZkOffsets.get(topicPartition) != offset) {
      val topicDirs = new ZKGroupTopicDirs(config.groupId, topicPartition.topic)
      zkUtils.updatePersistentPath(topicDirs.consumerOffsetDir + "/" + topicPartition.partition, offset.toString)
      checkpointedZkOffsets.put(topicPartition, offset)
      zkCommitMeter.mark()
    }
  }

  private def fetchOffsetFromZooKeeper(topicPartition: TopicAndPartition) = {
    val dirs = new ZKGroupTopicDirs(config.groupId, topicPartition.topic)
    val offsetString = zkUtils.readDataMaybeNull(dirs.consumerOffsetDir + "/" + topicPartition.partition)._1
    offsetString match {
      case Some(offsetStr) => (topicPartition, OffsetMetadataAndError(offsetStr.toLong))
      case None => (topicPartition, OffsetMetadataAndError.NoOffset)
    }
  }

}
