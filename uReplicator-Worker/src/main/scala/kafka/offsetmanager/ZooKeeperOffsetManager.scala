package kafka.offsetmanager

import java.util.concurrent.TimeUnit

import kafka.common.{OffsetAndMetadata, OffsetMetadataAndError, TopicAndPartition}
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.{Pool, ZKGroupTopicDirs, ZkUtils}

class ZooKeeperOffsetManager(zkUtils: ZkUtils, groupId: String, clientId: String) extends OffsetManager {

  private val zkCommitMeter = KafkaMetricsGroup.newMeter("ZooKeeperCommitsPerSec", "commits", TimeUnit.SECONDS, Map("clientId" -> clientId))

  /**
    * Added foreach on the map to execute the original code which copied from KafkaConnector class
    *
    * @param offsetsAtr - Map which contain topic, partition and the offset to commit.
    * @param checkpointedOffsets - Pool of the committed offsets.
    */
  override def commitOffsets(offsetsAtr: Map[TopicAndPartition, OffsetAndMetadata],
                             checkpointedOffsets: Pool[TopicAndPartition, Long]): Unit = {
    offsetsAtr.foreach{case (topicAndPartition: TopicAndPartition, offsetAndMetadata: OffsetAndMetadata) =>
      if (checkpointedOffsets.get(topicAndPartition) != offsetAndMetadata.offset) {
        val topicDirs = new ZKGroupTopicDirs(groupId, topicAndPartition.topic)
        zkUtils.updatePersistentPath(topicDirs.consumerOffsetDir + "/" + topicAndPartition.partition, offsetAndMetadata.offset.toString)
        checkpointedOffsets.put(topicAndPartition, offsetAndMetadata.offset)
        zkCommitMeter.mark()
      }
    }
  }

  /**
    * This code copied from KafkaConnector class
    *
    * @param topicAndPartition - The requested topic and partition for fetching.
    * @return - The requested topic, partition and the current offset of the consumerGroup.
    */
  override def fetchOffset(topicAndPartition: TopicAndPartition): (TopicAndPartition, OffsetMetadataAndError) = {
    val dirs = new ZKGroupTopicDirs(groupId, topicAndPartition.topic)
    val offsetString = zkUtils.readDataMaybeNull(dirs.consumerOffsetDir + "/" + topicAndPartition.partition)._1
    offsetString match {
      case Some(offsetStr) => (topicAndPartition, OffsetMetadataAndError(offsetStr.toLong))
      case None => (topicAndPartition, OffsetMetadataAndError.NoOffset)
    }
  }
}

