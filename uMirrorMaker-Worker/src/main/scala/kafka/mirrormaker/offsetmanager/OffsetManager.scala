package kafka.mirrormaker.offsetmanager

import kafka.common.{OffsetAndMetadata, OffsetMetadataAndError, TopicAndPartition}
import kafka.consumer.ConsumerConfig
import kafka.utils.{Pool, ZkUtils}
import org.apache.kafka.common.protocol.SecurityProtocol

/**
  * Generic trait for offsetManager which have the ability to commit and fetch offsets.
  */
trait OffsetManager {

  def commitOffsets(offsetsAtr: Map[TopicAndPartition, OffsetAndMetadata],
                    checkpointedOffsets: Pool[TopicAndPartition, Long]): Unit

  def fetchOffset(topicAndPartition: TopicAndPartition): (TopicAndPartition, OffsetMetadataAndError)
}

/**
  * OffsetManager Factory, create KafkaOffsetManger of ZookeeperOffsetManager based on the consumer's configuration.
  */
object OffsetManager{

  private var offsetManager: Option[OffsetManager] = None

  def apply(config: ConsumerConfig, zkUtils: ZkUtils): OffsetManager ={
    if (offsetManager.isEmpty)
      offsetManager = config.offsetsStorage match {
        case "kafka" => Some(new KafkaOffsetManager(zkUtils.getAllBrokerEndPointsForChannel(SecurityProtocol.PLAINTEXT),
          config.groupId, config.clientId, config.offsetsChannelSocketTimeoutMs,
          config.offsetsChannelBackoffMs, config.offsetsCommitMaxRetries))
        case _ => Some(new ZooKeeperOffsetManager(zkUtils, config.groupId, config.clientId))
      }

    offsetManager.get
  }
}
