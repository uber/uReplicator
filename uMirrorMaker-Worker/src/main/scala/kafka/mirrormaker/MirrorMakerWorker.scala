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

import java.net.InetAddress
import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.{Collections, Properties, UUID}

import com.yammer.metrics.core.Gauge
import joptsimple.OptionParser
import kafka.consumer._
import kafka.message.MessageAndMetadata
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.{CommandLineUtils, Logging}
import org.apache.helix.participant.StateMachineEngine
import org.apache.helix.{HelixManager, HelixManagerFactory, InstanceType}
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.utils.Utils

import scala.io.Source

/**
 * The mirror maker has the following architecture:
 * - There is one mirror maker thread uses one KafkaConnector and owns a Kafka stream.
 * - All the mirror maker threads share one producer.
 * - Mirror maker thread periodically flushes the producer and then commits all offsets.
 *
 * @note For mirror maker, the following settings are set by default to make sure there is no data loss:
 *       1. use new producer with following settings
 *            acks=all
 *            retries=max integer
 *            block.on.buffer.full=true
 *            max.in.flight.requests.per.connection=1
 *       2. Consumer Settings
 *            auto.commit.enable=false
 *       3. Mirror Maker Setting:
 *            abort.on.send.failure=true
 */
object MirrorMakerWorker extends Logging with KafkaMetricsGroup {

  private var helixClusterName: String = null
  private var instanceId: String = null
  private var zkServer: String = null
  private var helixZkManager: HelixManager = null

  private var connector: KafkaConnector = null
  private var producer: MirrorMakerProducer = null
  private var mirrorMakerThread: MirrorMakerThread = null
  private val isShuttingdown: AtomicBoolean = new AtomicBoolean(false)
  // Track the messages not successfully sent by mirror maker.
  private var numDroppedMessages: AtomicInteger = new AtomicInteger(0)
  private val messageHandler: MirrorMakerMessageHandler = defaultMirrorMakerMessageHandler
  private var offsetCommitIntervalMs = 0
  private var abortOnSendFailure: Boolean = true
  @volatile private var exitingOnSendFailure: Boolean = false
  private var topicMappings = Map.empty[String, String]



  def main(args: Array[String]) {
    info("Starting mirror maker")
    val parser = new OptionParser

    val consumerConfigOpt = parser.accepts("consumer.config",
      "Embedded consumer config for consuming from the source cluster.")
      .withRequiredArg()
      .describedAs("config file")
      .ofType(classOf[String])

    val producerConfigOpt = parser.accepts("producer.config",
      "Embedded producer config.")
      .withRequiredArg()
      .describedAs("config file")
      .ofType(classOf[String])

    val helixConfigOpt = parser.accepts("helix.config",
      "Embedded helix config.")
      .withRequiredArg()
      .describedAs("config file")
      .ofType(classOf[String])

    val offsetCommitIntervalMsOpt = parser.accepts("offset.commit.interval.ms",
      "Offset commit interval in ms")
      .withRequiredArg()
      .describedAs("offset commit interval in millisecond")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(60000)

    val abortOnSendFailureOpt = parser.accepts("abort.on.send.failure",
      "Configure the mirror maker to exit on a failed send.")
      .withRequiredArg()
      .describedAs("Stop the entire mirror maker when a send failure occurs")
      .ofType(classOf[String])
      .defaultsTo("true")

    val topicMappingsOpt = parser.accepts("topic.mappings",
      "Path to file containing line deliminated mappings of topics to consume from and produce to.")
      .withRequiredArg()
      .describedAs("Path to mappings file")
      .ofType(classOf[String])

    val helpOpt = parser.accepts("help", "Print this message.")

    if (args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "Continuously copy data between two Kafka clusters.")


    val options = parser.parse(args: _*)

    if (options.has(helpOpt)) {
      parser.printHelpOn(System.out)
      System.exit(0)
    }

    CommandLineUtils.checkRequiredArgs(parser, options, consumerConfigOpt, producerConfigOpt, helixConfigOpt)

    abortOnSendFailure = options.valueOf(abortOnSendFailureOpt).toBoolean
    offsetCommitIntervalMs = options.valueOf(offsetCommitIntervalMsOpt).intValue()

    Runtime.getRuntime.addShutdownHook(new Thread("MirrorMakerShutdownHook") {
      override def run() {
        cleanShutdown()
      }
    })

    // create producer
    val producerProps = Utils.loadProps(options.valueOf(producerConfigOpt))
    // Defaults to no data loss settings.
    maybeSetDefaultProperty(producerProps, ProducerConfig.RETRIES_CONFIG, Int.MaxValue.toString)
    maybeSetDefaultProperty(producerProps, ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "true")
    maybeSetDefaultProperty(producerProps, ProducerConfig.ACKS_CONFIG, "all")
    maybeSetDefaultProperty(producerProps, ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    producer = new MirrorMakerProducer(producerProps)

    // create helix manager
    val helixProps = Utils.loadProps(options.valueOf(helixConfigOpt))
    zkServer = helixProps.getProperty("zkServer", "localhost:2181")
    instanceId = helixProps.getProperty("instanceId", "HelixMirrorMaker-" + System.currentTimeMillis)
    helixClusterName = helixProps.getProperty("helixClusterName", "testMirrorMaker")


    // Create consumer connector
    val consumerConfigProps = Utils.loadProps(options.valueOf(consumerConfigOpt))
    // Disable consumer auto offsets commit to prevent data loss.
    maybeSetDefaultProperty(consumerConfigProps, "auto.commit.enable", "false")
    // Set the consumer timeout so we will not block for low volume pipeline. The timeout is necessary to make sure
    // Offsets are still committed for those low volume pipelines.
    maybeSetDefaultProperty(consumerConfigProps, "consumer.timeout.ms", "10000")
    val consumerConfig = new ConsumerConfig(consumerConfigProps)
    val consumerIdString = {
      var consumerUuid : String = null
      consumerConfig.consumerId match {
        case Some(consumerId) // for testing only
        => consumerUuid = consumerId
        case None // generate unique consumerId automatically
        => val uuid = UUID.randomUUID()
          consumerUuid = "%s-%d-%s".format(
            InetAddress.getLocalHost.getHostName, System.currentTimeMillis,
            uuid.getMostSignificantBits().toHexString.substring(0,8))
      }
      consumerConfig.groupId + "_" + consumerUuid
    }
    connector = new KafkaConnector(consumerIdString, consumerConfig)
    addToHelixController()

    // If a message send failed after retries are exhausted. The offset of the messages will also be removed from
    // the unacked offset list to avoid offset commit being stuck on that offset. In this case, the offset of that
    // message was not really acked, but was skipped. This metric records the number of skipped offsets.
    newGauge("MirrorMaker-numDroppedMessages",
      new Gauge[Int] {
        def value = numDroppedMessages.get()
      },
      Map("clientId" -> consumerConfig.clientId)
    )

    // initialize topic mappings for rewriting topic names between consuming side and producing side
    topicMappings = if (options.has(topicMappingsOpt)) {
      val topicMappingsFile = options.valueOf(topicMappingsOpt)
      val topicMappingPattern = """\s*(\S+)\s+(\S+)\s*""".r
      Source.fromFile(topicMappingsFile).getLines().flatMap(_ match {
        case topicMappingPattern(consumerTopic, producerTopic) => {
          info("Topic mapping: '" + consumerTopic + "' -> '" + producerTopic + "'")
          Some(consumerTopic -> producerTopic)
        }
        case line => {
          error("Invalid mapping '" + line + "'")
          None
        }
      }).toMap
    } else Map.empty[String, String]

    // Create mirror maker threads
    mirrorMakerThread = new MirrorMakerThread(connector, instanceId)
    mirrorMakerThread.start()
    mirrorMakerThread.awaitShutdown()
  }

  def commitOffsets(connector: KafkaConnector) {
    if (!exitingOnSendFailure) {
      trace("Committing offsets.")
      connector.commitOffsets
    } else {
      info("Exiting on send failure, skip committing offsets.")
    }
  }

  def addToHelixController(): Unit = {
    helixZkManager = HelixManagerFactory.getZKHelixManager(helixClusterName, instanceId, InstanceType.PARTICIPANT, zkServer)
    val stateMachineEngine: StateMachineEngine  = helixZkManager.getStateMachineEngine()
    // register the MirrorMaker worker
    val stateModelFactory = new HelixWorkerOnlineOfflineStateModelFactory(instanceId, connector)
    stateMachineEngine.registerStateModelFactory("OnlineOffline", stateModelFactory)
    helixZkManager.connect()
  }

  def cleanShutdown() {
    if (isShuttingdown.compareAndSet(false, true)) {
      info("Start clean shutdown.")
      // Shutdown consumer threads.
      info("Shutting down consumer thread.")
      if (mirrorMakerThread != null) {
        mirrorMakerThread.shutdown()
        mirrorMakerThread.awaitShutdown()
      }
      // commit offsets
      commitOffsets(connector)

      // shutdown producer
      info("Closing producer.")
      producer.close()
      info("Kafka mirror maker shutdown successfully")

      // disconnect with helixZkManager
      helixZkManager.disconnect()
      info("helix connection shutdown successfully")
    }
  }

  private def maybeSetDefaultProperty(properties: Properties, propertyName: String, defaultValue: String) {
    val propertyValue = properties.getProperty(propertyName)
    properties.setProperty(propertyName, Option(propertyValue).getOrElse(defaultValue))
    if (properties.getProperty(propertyName) != defaultValue)
      info("Property %s is overridden to %s - data loss or message reordering is possible.".format(propertyName, propertyValue))
  }

  class MirrorMakerThread(connector: KafkaConnector,
                          val threadId: String) extends Thread with Logging with KafkaMetricsGroup {
    private val threadName = "mirrormaker-thread-" + threadId
    private val shutdownLatch: CountDownLatch = new CountDownLatch(1)
    private var lastOffsetCommitMs = System.currentTimeMillis()
    @volatile private var shuttingDown: Boolean = false
    this.logIdent = "[%s] ".format(threadName)

    setName(threadName)

    override def run() {
      info("Starting mirror maker thread " + threadName)
      try {
        // Creating one stream per each connector instance
        val stream = connector.getStream()
        val iter = stream.iterator()

        while (!exitingOnSendFailure && !shuttingDown) {
          try {
            while (!exitingOnSendFailure && !shuttingDown && iter.hasNext()) {
              val data = iter.next()
              trace("Sending message with value size %d".format(data.message().size))
              val records = messageHandler.handle(data)
              val iterRecords = records.iterator()
              while (iterRecords.hasNext) {
                producer.send(iterRecords.next())
              }
              maybeFlushAndCommitOffsets()
            }
          } catch {
            case e: ConsumerTimeoutException =>
              e.printStackTrace()
              trace("Caught ConsumerTimeoutException, continue iteration.")
          }
          maybeFlushAndCommitOffsets()
        }
      } catch {
        case t: Throwable =>
          t.printStackTrace()
          fatal("Mirror maker thread failure due to ", t)
      } finally {
        info("Flushing producer.")
        producer.flush()
        info("Committing consumer offsets.")
        commitOffsets(connector)
        info("Shutting down consumer connectors.")
        connector.shutdown()
        shutdownLatch.countDown()
        info("Mirror maker thread stopped")
        // if it exits accidentally, stop the entire mirror maker
        if (!isShuttingdown.get()) {
          fatal("Mirror maker thread exited abnormally, stopping the whole mirror maker.")
          System.exit(-1)
        }
      }
    }

    def maybeFlushAndCommitOffsets() {
      if (System.currentTimeMillis() - lastOffsetCommitMs > offsetCommitIntervalMs) {
        producer.flush()
        commitOffsets(connector)
        lastOffsetCommitMs = System.currentTimeMillis()
      }
    }

    def shutdown() {
      try {
        info(threadName + " shutting down")
        shuttingDown = true
      }
      catch {
        case ie: InterruptedException =>
          warn("Interrupt during shutdown of the mirror maker thread")
      }
    }

    def awaitShutdown() {
      try {
        shutdownLatch.await()
        info("Mirror maker thread shutdown complete")
      } catch {
        case ie: InterruptedException =>
          warn("Shutdown of the mirror maker thread interrupted")
      }
    }
  }

  private class MirrorMakerProducer(val producerProps: Properties) {

    val sync = producerProps.getProperty("producer.type", "async").equals("sync")

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)

    def send(record: ProducerRecord[Array[Byte], Array[Byte]]) {
      if (sync) {
        this.producer.send(record).get()
      } else {
        this.producer.send(record,
          new MirrorMakerProducerCallback(record.topic(), record.key(), record.value()))
      }
    }

    def flush() {
      this.producer.flush()
    }

    def close() {
      this.producer.close()
    }

    def close(timeout: Long) {
      this.producer.close(timeout, TimeUnit.MILLISECONDS)
    }
  }

  private class MirrorMakerProducerCallback (topic: String, key: Array[Byte], value: Array[Byte])
    extends ErrorLoggingCallback(topic, key, value, false) {

    override def onCompletion(metadata: RecordMetadata, exception: Exception) {
      if (exception != null) {
        // Use default call back to log error. This means the max retries of producer has reached and message
        // still could not be sent.
        super.onCompletion(metadata, exception)
        // If abort.on.send.failure is set, stop the mirror maker. Otherwise log skipped message and move on.
        if (abortOnSendFailure) {
          info("Closing producer due to send failure.")
          exitingOnSendFailure = true
          producer.close(0)
        }
        numDroppedMessages.incrementAndGet()
      }
    }
  }


  /**
   * If message.handler.args is specified. A constructor that takes in a String as argument must exist.
   */
  trait MirrorMakerMessageHandler {
    def handle(record: MessageAndMetadata[Array[Byte], Array[Byte]]): util.List[ProducerRecord[Array[Byte], Array[Byte]]]
  }

  private object defaultMirrorMakerMessageHandler extends MirrorMakerMessageHandler {
    override def handle(record: MessageAndMetadata[Array[Byte], Array[Byte]]): util.List[ProducerRecord[Array[Byte], Array[Byte]]] = {
      // rewrite topic between consuming side and producing side
      val topic = topicMappings.get(record.topic).getOrElse(record.topic)
      Collections.singletonList(new ProducerRecord[Array[Byte], Array[Byte]](topic, record.key(), record.message()))
    }
  }

}
