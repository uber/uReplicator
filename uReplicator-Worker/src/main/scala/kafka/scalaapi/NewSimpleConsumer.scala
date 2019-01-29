package kafka.scalaapi

import com.uber.kafka.consumer.NewSimpleConsumerConfig
import kafka.common.TopicAndPartition
import org.apache.kafka.common.TopicPartition
import java.util.Properties
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import org.slf4j.{Logger, LoggerFactory}

import com.uber.kafka.consumer.NewSimpleConsumerConfig
import kafka.common.{ClientIdAndBroker, ErrorMapping, KafkaException, TopicAndPartition}
import kafka.utils.CoreUtils._

class NewSimpleConsumer(val host: String, val port: Int, val config: NewSimpleConsumerConfig) {
  val logger1: Logger = LoggerFactory.getLogger(this.getClass)
  private val underlying = new com.uber.kafka.consumer.NewSimpleConsumer(host, port, config)

  def connect(): Unit = {
    underlying.connect()
  }

  def fetch(requestBuilder: org.apache.kafka.common.requests.FetchRequest.Builder): FetchResponse = {
    //     TODO: change this to implicts
    val a = underlying.fetch(requestBuilder)
    val b = a.responseData().keySet().size()
    logger1.info(s"here12345678 $a $b $requestBuilder")
    new FetchResponse(a)
  }

  def earliestOrLatestOffset(topicAndPartition: TopicAndPartition, earliestOrLatest: Long, consumerId: Int): Long = {
    val topicPartition = new TopicPartition(topicAndPartition.topic, topicAndPartition.partition)
    println("here123 $topicPartition $earliestOrLatest")
    underlying.earliestOrLatestOffset(topicPartition, earliestOrLatest)
  }

  def close(): Unit = {
    underlying.disconnect()
  }
}
