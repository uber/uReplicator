package kafka.offsetmanager

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import kafka.api._
import kafka.cluster.{Broker, BrokerEndPoint}
import kafka.common._
import kafka.network.BlockingChannel
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

class KafkaOffsetManagerConnector(brokers : Seq[Broker], socketTimeoutMs: Int, backoffMs: Int, maxRetries: Int) {

  private final val log = Logger.getLogger(this.getClass)
  private val correlationId = new AtomicInteger(1)

  /**
    * Create blocking channel for the broker's connection
    *
    * @param brokerEndPoint - Broker end point that will be used for the channel creation
    * @return - Blocking channel for the broker's connection
    */
  private def getDefaultChannel(brokerEndPoint: BrokerEndPoint): BlockingChannel =
    new BlockingChannel(brokerEndPoint.host, brokerEndPoint.port, BlockingChannel.UseDefaultBufferSize,
      BlockingChannel.UseDefaultBufferSize, socketTimeoutMs)

  /**
    * Sends input request to input channel. Trying to reopen channel in case it closed
    *
    * @param channel - Input channel to send the request upon
    * @param request - The request that we want to send
    * @return - Response ByteBuffer, None in case error occurred
    */
  private def sendToKafka(channel : BlockingChannel, request : RequestOrResponse) : Option[ByteBuffer] = {

    if(!channel.isConnected)
      channel.connect()

    if(channel.isConnected) {
      channel.send(request)
      val byteBuffer = channel.receive().payload()
      channel.disconnect()
      return Some(byteBuffer)
    }

    None
  }

  /**
    * Find consumer group coordinator in object brokers. If coordinator found return coordinator otherwise return None
    *
    * @param consumerGroup - The consumer group of the coordinator the method looking for
    * @return Option(BrokerEndPoint) if coordinator found otherwise None
    */
  private def getGroupCoordinator(consumerGroup: String): Option[BrokerEndPoint] = {

    log.info(s"Looking for coordinator of consumerGroup $consumerGroup in brokers: ${brokers.map(_.toString())}")

    brokers.foreach(broker => {

      log.info(s"Asking broker: $broker for coordinator. " +
        s"consumerGroup: $consumerGroup, correlationID: ${correlationId.get}.")

      val channel = getDefaultChannel(BrokerEndPoint(broker.id, broker.endPoints.head.host, broker.endPoints.head.port))

      val request = new GroupCoordinatorRequest(consumerGroup,
        correlationId = correlationId.getAndIncrement)

      val byteBufferResponse = sendToKafka(channel, request)

      if(byteBufferResponse.isDefined) {

        val response = GroupCoordinatorResponse.readFrom(byteBufferResponse.get)

        if(response.errorCode == ErrorMapping.NoError && response.coordinatorOpt.isDefined) {

          log.info(s"Answer for correlationID ${response.correlationId}: $consumerGroup's " +
            s"coordinator is: ${response.coordinatorOpt.get.connectionString()}")
          return response.coordinatorOpt

        } else{
          log.warn(s"Answer for correlationID ${response.correlationId}: couldn't find $consumerGroup's coordinator.")
          ErrorMapping.maybeThrowException(response.errorCode)
        }
      }
      else{
        log.warn(s"Failed to get response from broker: $broker " +
          s"during coordinator seeking, correlationID: ${correlationId.get}")
      }
    })

    log.error(s"Wasn't able to find $consumerGroup's coordinator.")

    None
  }

  /**
    * Getting the coordinator channel for the consumer group from the brokers
    *
    * @param consumerGroup - Consumer group that we are looking for
    * @return Coordinator channel if found. ChannelNotAvailableException otherwise
    */
  private def getCoordinatorChannel(consumerGroup : String) : BlockingChannel = {

    val coordinator = getGroupCoordinator(consumerGroup)

    if(coordinator.isDefined){

      val channel = getDefaultChannel(coordinator.get)
      channel.connect()

      if (channel.isConnected)
        channel
      else
        throw new ConsumerCoordinatorNotAvailableException(s"Wasn't able to connect to ${channel.host}:${channel.port} channel")
    }
    else
      throw new NotCoordinatorForConsumerException(s"Wasn't able to locate coordinator channel for group: $consumerGroup")
  }

  /**
    * Performs a single commit attempt to Kafka offset manager
    *
    * @param consumerGroup - Consumer group of the commit
    * @param offsets - Committed offsets
    * @return - True in case commit successful, exception otherwise
    */
  private def commitAttempt(consumerGroup : String,
                            offsets : Map[TopicAndPartition, OffsetAndMetadata]): Unit = {

    val channel = getCoordinatorChannel(consumerGroup)

    log.info(s"Commit offset attempt to broker: ${channel.host}:${channel.port}. " +
      s"consumerGroup: $consumerGroup, correlationID: ${correlationId.get}.")

    val request = new OffsetCommitRequest(consumerGroup, offsets,
      correlationId = correlationId.getAndIncrement)

    val responseBuffer = sendToKafka(channel, request)

    if(responseBuffer.isDefined) {
      val response = OffsetCommitResponse.readFrom(responseBuffer.get)

      if(response.hasError){

        log.error(s"Commit attempt returned error. consumerGroup: $consumerGroup," +
          s" broker: ${channel.host}:${channel.port}, correlationID: ${response.correlationId}")
        val errorCode = response.commitStatus.find(elem => elem._2 != ErrorMapping.NoError).get._2
        ErrorMapping.maybeThrowException(errorCode)
      }
    }
    else
      throw new BrokerNotAvailableException(s"Commit attempt couldn't receive buffer. consumerGroup: $consumerGroup," +
        s" broker: ${channel.host}:${channel.port}, correlationID: ${correlationId.get}.")
  }

  /**
    * Trying to perform commit to kafka offset manager several times
    *
    * @param consumerGroup - Consumer group to commit
    * @param offsets - Offsets to commit
    * @return True in case commit successful false otherwise
    */
  def commit(consumerGroup : String,
             offsets : Map[TopicAndPartition, OffsetAndMetadata]) : Unit =
    retry(maxRetries, backoffMs)(commitAttempt(consumerGroup, offsets))

  /**
    * Perform a single attempt to fetch last offsets of each partition from Kafka offset manager
    *
    * @param consumerGroup - Consumer group
    * @param topicAndPartitionArr - Request topic and partitions
    * @return Map of partition and offset pairs or None if problem occured
    */
  private def fetchAttempt(consumerGroup : String,
                           topicAndPartitionArr : Seq[TopicAndPartition]) :
  Map[TopicAndPartition, OffsetMetadataAndError] = {

    val channel = getCoordinatorChannel(consumerGroup)

    log.info(s"Fetch offset attempt from broker: ${channel.host}:${channel.port}. " +
      s"Consumer group: $consumerGroup, Correlation id: ${correlationId.get}.")

    val request = new OffsetFetchRequest(consumerGroup, topicAndPartitionArr,
      correlationId = correlationId.getAndIncrement())

    val responseBuffer = sendToKafka(channel, request)

    if(responseBuffer.isDefined) {

      val response = OffsetFetchResponse.readFrom(responseBuffer.get).requestInfo
      val hasError = response.find(elem => elem._2.error != ErrorMapping.NoError)

      if(hasError.isDefined && hasError.get._2.error != ErrorMapping.NoError) {
        log.error(s"Fetch attempt returned error. Consumer group $consumerGroup," +
          s" broker: ${channel.host}:${channel.port}, correlationID: ${correlationId.get}")
        ErrorMapping.maybeThrowException(hasError.get._2.error)
        throw new UnknownException
      }
      else
        response
    }
    else {
      throw new BrokerNotAvailableException(s"Commit attempt couldn't receive buffer. Consumer group $consumerGroup," +
        s" broker: ${channel.host}:${channel.port}, correlationID: ${correlationId.get}.")
    }
  }

  /**
    * Attempting fetch from Kafka offset manager several times
    *
    * @param consumerGroup - Consumer group to query
    * @return Partition-Offset pairs on success, None on error
    */
  def fetch(consumerGroup : String,
            topicAndPartition: TopicAndPartition) :
  Option[(TopicAndPartition, OffsetMetadataAndError)] = {

    val fetchResult = retry(maxRetries, backoffMs)(fetchAttempt(consumerGroup, Seq(topicAndPartition)))

    if(fetchResult.isDefined) Some(fetchResult.get.head) else None
  }

  @annotation.tailrec
  private def retry[T](maxRetries: Int, backoffMs: Int)(fn: => T): Option[T] = {

    val x = Try { fn }

    x match {

      case x : Success[T] =>
        Some(x.get)

      case x : Failure[T] if maxRetries > 1 =>
        log.warn("Retryable exception:" + x.exception.getMessage, x.exception)
        Thread.sleep(backoffMs)
        retry(maxRetries - 1, backoffMs)(fn)

      case x : Failure[T] =>
        log.warn("Retryable exception:" + x.exception.getMessage, x.exception)
        None
    }
  }





}
