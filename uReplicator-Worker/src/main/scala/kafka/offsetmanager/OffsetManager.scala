package kafka.offsetmanager

import kafka.common.{OffsetAndMetadata, OffsetMetadataAndError, TopicAndPartition}
import kafka.consumer.ConsumerConfig
import kafka.utils.{Pool, ZkUtils}

trait OffsetManager {

  def commitOffsets(offsetsAtr: Map[TopicAndPartition, OffsetAndMetadata],
                    checkpointedOffsets: Pool[TopicAndPartition, Long]) : Unit

  def fetchOffset(topicAndPartition: TopicAndPartition) : (TopicAndPartition, OffsetMetadataAndError)

}

object OffsetManager {

  private var offsetManager: Option[OffsetManager] = _

  def apply(config: ConsumerConfig, zkUtils: ZkUtils): OffsetManager ={
    if (offsetManager.isEmpty)
      offsetManager = config.offsetsStorage match {
        case "kafka" => Some(new KafkaOffsetManager(zkUtils.getAllBrokersInCluster(),
          config.groupId, config.clientId, config.offsetsChannelSocketTimeoutMs,
          config.offsetsChannelBackoffMs, config.offsetsCommitMaxRetries))
        case _ => Some(new ZooKeeperOffsetManager(zkUtils, config.groupId, config.clientId))
      }

    offsetManager.get
  }
}