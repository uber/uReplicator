package kafka.scalaapi

import org.apache.kafka.common.TopicPartition
import org.slf4j.{Logger, LoggerFactory}

import com.uber.kafka.consumer.NewSimpleConsumerConfig
import kafka.common.TopicAndPartition

class NewSimpleConsumer(val host: String, val port: Int, val config: NewSimpleConsumerConfig) {
  val logger1: Logger = LoggerFactory.getLogger(this.getClass)
  private val underlying = new com.uber.kafka.consumer.NewSimpleConsumer(host, port, config)

  def connect(): Unit = {
    underlying.connect()
  }

  def fetch(requestBuilder: org.apache.kafka.common.requests.FetchRequest.Builder): FetchResponse = {
    new FetchResponse(underlying.fetch(requestBuilder))
  }

  def earliestOrLatestOffset(topicAndPartition: TopicAndPartition, earliestOrLatest: Long, consumerId: Int): Long = {
    val topicPartition = new TopicPartition(topicAndPartition.topic, topicAndPartition.partition)
    underlying.earliestOrLatestOffset(topicPartition, earliestOrLatest)
  }

  def close(): Unit = {
    underlying.disconnect()
  }
}
