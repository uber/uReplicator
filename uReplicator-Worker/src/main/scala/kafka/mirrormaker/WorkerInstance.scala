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

import java.net.InetAddress
import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.{Collections, Properties, UUID}

import com.yammer.metrics.core.{Gauge, Meter}
import joptsimple.OptionSet
import kafka.consumer._
import kafka.message.MessageAndMetadata
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.Logging
import org.apache.helix.manager.zk.ZKHelixManager
import org.apache.helix.participant.StateMachineEngine
import org.apache.helix.{HelixManager, InstanceType}
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
 * @note      For mirror maker, the following settings are set by default to make sure there is no data loss:
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
class WorkerInstance(private val workerConfig: MirrorMakerWorkerConf,
                     private val options: OptionSet,
                     private val srcCluster: Option[String],
                     private val dstCluster: Option[String],
                     private val helixClusterName: String,
                     private val route: String) extends Logging with KafkaMetricsGroup {
  private var instanceId: String = null
  private var clientId: String = null
  private var zkServer: String = null
  private var helixZkManager: HelixManager = null

  private var connector: KafkaConnector = null
  private var producer: MirrorMakerProducer = null
  private var mirrorMakerThread: MirrorMakerThread = null
  private val isShuttingDown: AtomicBoolean = new AtomicBoolean(false)
  // Track the messages not successfully sent by mirror maker.
  private val numDroppedMessages: AtomicInteger = new AtomicInteger(0)
  private val messageHandler: MirrorMakerMessageHandler = defaultMirrorMakerMessageHandler
  private var offsetCommitIntervalMs = 0
  private var abortOnSendFailure: Boolean = true
  @volatile private var exitingOnSendFailure: Boolean = false
  private var topicMappings = Map.empty[String, String]

  private var filterEnabled: Boolean = false

  private var lastOffsetCommitMs = System.currentTimeMillis()
  private val recordCount: AtomicInteger = new AtomicInteger(0)
  private val flushCommitLock: ReentrantLock = new ReentrantLock

  var flushLatency: KafkaTimer = null
  var commitLatency: KafkaTimer = null
  var callbackLatency: KafkaTimer = null
  var startMeter: Meter = null
  var mapFailureMeter: Meter = null
  var topicPartitionCountObserver: TopicPartitionCountObserver = null

  def start() {
    info("Starting uReplicator worker instance")

    abortOnSendFailure = options.valueOf(workerConfig.getAbortOnSendFailureOpt).toBoolean
    offsetCommitIntervalMs = options.valueOf(workerConfig.getOffsetCommitIntervalMsOpt).toInt

    // get source and destination cluster info
    val clusterProps = {
      try {
        Utils.loadProps(options.valueOf(workerConfig.getClusterConfigOpt))
      } catch {
        case e: Exception
        => null
      }
    }

    val dstZkProps = {
      try {
        Utils.loadProps(options.valueOf(workerConfig.getDstZkConfigOpt))
      } catch {
        case e: Exception
        => null
      }
    }

    if (dstZkProps != null && dstZkProps.getProperty("enable", "false").toBoolean) {
      dstCluster match {
        case Some(dstCluster)
        =>
          if (clusterProps == null) {
            throw new Exception("No cluster configuration provided")
          }
          info("TopicPartitionCountObserver is enabled")
          topicPartitionCountObserver = new TopicPartitionCountObserver(
            clusterProps.getProperty("kafka.cluster.zkStr." + dstCluster, ""),
            dstZkProps.getProperty("zkPath", "/brokers/topics"),
            dstZkProps.getProperty("connection.timeout.ms", "120000").toInt,
            dstZkProps.getProperty("session.timeout.ms", "600000").toInt,
            dstZkProps.getProperty("refresh.interval.ms", "3600000").toInt)
        case None // non-federated mode
        =>
          info("TopicPartitionCountObserver is enabled")
          topicPartitionCountObserver = new TopicPartitionCountObserver(
            dstZkProps.getProperty("zkServer", "localhost:2181"),
            dstZkProps.getProperty("zkPath", "/brokers/topics"),
            dstZkProps.getProperty("connection.timeout.ms", "120000").toInt,
            dstZkProps.getProperty("session.timeout.ms", "600000").toInt,
            dstZkProps.getProperty("refresh.interval.ms", "3600000").toInt)
      }
      topicPartitionCountObserver.start()
    } else {
      info("Disable TopicPartitionCountObserver to use round robin to produce msg")
    }

    val filterProps = {
      try {
        Utils.loadProps(options.valueOf(workerConfig.getFilterConfigOpt))
      } catch {
        case e: Exception
        => null
      }
    }

    if (filterProps != null) {
      filterEnabled = filterProps.getProperty("enable", "false").toBoolean
      if (filterEnabled) {
        info("Message filter is enabled")
      } else {
        info("Message filter is disabled, send all the msg to dst cluster")
      }
    }

    // create producer
    val producerProps = Utils.loadProps(options.valueOf(workerConfig.getProducerConfigOpt))
    dstCluster match {
      case Some(dstCluster)
      =>
        if (clusterProps == null) {
          throw new Exception("No cluster configuration provided")
        }
        val dstServers = clusterProps.getProperty("kafka.cluster.servers." + dstCluster, "")
        if (dstServers.isEmpty()) {
          error("Cannot find bootstratp servers for destination cluster: " + dstCluster)
          throw new Exception("Cannot find bootstrap servers for destination cluster: " + dstCluster)
        } else {
          producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, dstServers)
          producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, route)
        }
      case None // non-federated mode
      =>
    }
    // Defaults to no data loss settings.
    maybeSetDefaultProperty(producerProps, ProducerConfig.RETRIES_CONFIG, Int.MaxValue.toString)
    // BLOCK_ON_BUFFER_FULL_CONFIG will cause producer stuck
    maybeSetDefaultProperty(producerProps, ProducerConfig.MAX_BLOCK_MS_CONFIG, "600000")
    maybeSetDefaultProperty(producerProps, ProducerConfig.ACKS_CONFIG, "all")
    maybeSetDefaultProperty(producerProps, ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    producer = new MirrorMakerProducer(producerProps)

    // create helix manager
    val helixProps = Utils.loadProps(options.valueOf(workerConfig.getHelixConfigOpt))
    zkServer = helixProps.getProperty("zkServer", "localhost:2181")
    instanceId = helixProps.getProperty("instanceId", "HelixMirrorMaker-" + System.currentTimeMillis)

    var srcBrokerList = ""
    // Create consumer connector
    val consumerConfigProps = Utils.loadProps(options.valueOf(workerConfig.getConsumerConfigOpt))
    // Disable consumer auto offsets commit to prevent data loss.
    maybeSetDefaultProperty(consumerConfigProps, "auto.commit.enable", "false")
    // Set the consumer timeout so we will not block for low volume pipeline. The timeout is necessary to make sure
    // Offsets are still committed for those low volume pipelines.
    maybeSetDefaultProperty(consumerConfigProps, "consumer.timeout.ms", "10000")
    srcCluster match {
      case Some(srcCluster)
      =>
        if (clusterProps == null) {
          throw new Exception("No cluster configuration provided")
        }
        val srcClusterZk = clusterProps.getProperty("kafka.cluster.zkStr." + srcCluster, "")
        srcBrokerList = clusterProps.getProperty("kafka.cluster.servers." + srcCluster, "")
        val commitZkConnection = clusterProps.getProperty("commit.zookeeper.connect", srcClusterZk)
        if (srcClusterZk.isEmpty()) {
          error("Cannot find zkStr for source cluster: " + srcCluster)
          throw new Exception("Cannot find zkStr for source cluster: " + srcCluster)
        } else {
          consumerConfigProps.setProperty("zookeeper.connect", srcClusterZk)
          consumerConfigProps.setProperty("broker.list", srcBrokerList)

          consumerConfigProps.setProperty("commit.zookeeper.connect", commitZkConnection)
          consumerConfigProps.setProperty("group.id", "ureplicator-" + srcCluster + "-" + dstCluster.getOrElse("none"))
          consumerConfigProps.setProperty("client.id", route)
        }
      case None // non-federated mode
      =>
    }

    val consumerConfig = new ConsumerConfig(consumerConfigProps)
    val consumerIdString = {
      var consumerUuid: String = null
      consumerConfig.consumerId match {
        case Some(consumerId) // for testing only
        => consumerUuid = consumerId
        case None // generate unique consumerId automatically
        => val uuid = UUID.randomUUID()
          consumerUuid = "%s-%d-%s".format(
            InetAddress.getLocalHost.getHostName, System.currentTimeMillis,
            uuid.getMostSignificantBits().toHexString.substring(0, 8))
      }
      consumerConfig.groupId + "_" + consumerUuid
    }

    connector = new KafkaConnector(consumerIdString, consumerConfig, srcBrokerList)

    additionalConfigs(srcCluster, dstCluster)

    addToHelixController()

    // If a message send failed after retries are exhausted. The offset of the messages will also be removed from
    // the unacked offset list to avoid offset commit being stuck on that offset. In this case, the offset of that
    // message was not really acked, but was skipped. This metric records the number of skipped offsets.
    clientId = consumerConfig.clientId
    newGauge("MirrorMaker-numDroppedMessages",
      new Gauge[Int] {
        def value = numDroppedMessages.get()
      },
      Map("clientId" -> clientId)
    )

    flushLatency = new KafkaTimer(newTimer("MirrorMaker-flushLatencyMs",
      TimeUnit.MILLISECONDS, TimeUnit.SECONDS, Map("clientId" -> consumerConfig.clientId)))
    commitLatency = new KafkaTimer(newTimer("MirrorMaker-commitLatencyMs",
      TimeUnit.MILLISECONDS, TimeUnit.SECONDS, Map("clientId" -> consumerConfig.clientId)))
    callbackLatency = new KafkaTimer(newTimer("MirrorMaker-callbackLatency",
      TimeUnit.MILLISECONDS, TimeUnit.SECONDS, Map("clientId" -> consumerConfig.clientId)))

    startMeter = newMeter("MirrorMaker-startPerSec", "start", TimeUnit.SECONDS,
      Map("clientId" -> consumerConfig.clientId))
    startMeter.mark()

    mapFailureMeter = newMeter("MirrorMaker-mapFailurePerSec", "start", TimeUnit.SECONDS,
      Map("clientId" -> consumerConfig.clientId))

    // initialize topic mappings for rewriting topic names between consuming side and producing side
    // TODO: add checking in uReplicator-Controller/Manager
    topicMappings = if (options.has(workerConfig.getTopicMappingsOpt)) {
      val topicMappingsFile = options.valueOf(workerConfig.getTopicMappingsOpt)
      val topicMappingPattern = """\s*(\S+)\s+(\S+)\s*""".r
      Source.fromFile(topicMappingsFile).getLines().flatMap(_ match {
        case topicMappingPattern(consumerTopic, producerTopic) => {
          info("Topic mapping: '" + consumerTopic + "' -> '" + producerTopic + "'")
          if (topicPartitionCountObserver != null) {
            topicPartitionCountObserver.addTopic(producerTopic)
          }
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
  }

  def removeCustomizedMetrics() {
    removeMetric("MirrorMaker-numDroppedMessages", Map("clientId" -> clientId))
    removeMetric("MirrorMaker-flushLatencyMs", Map("clientId" -> clientId))
    removeMetric("MirrorMaker-commitLatencyMs", Map("clientId" -> clientId))
    removeMetric("MirrorMaker-callbackLatency", Map("clientId" -> clientId))
    removeMetric("MirrorMaker-startPerSec", Map("clientId" -> clientId))
    removeMetric("MirrorMaker-mapFailurePerSec", Map("clientId" -> clientId))
    trace("Removed metrics: MirrorMaker-numDroppedMessages for clientId=" + clientId)
    trace("Removed metrics: MirrorMaker-flushLatencyMs for clientId=" + clientId)
    trace("Removed metrics: MirrorMaker-commitLatencyMs for clientId=" + clientId)
    trace("Removed metrics: MirrorMaker-callbackLatency for clientId=" + clientId)
    trace("Removed metrics: MirrorMaker-startPerSec for clientId=" + clientId)
    trace("Removed metrics: MirrorMaker-mapFailurePerSec for clientId=" + clientId)
  }

  class WorkerZKHelixManager(clusterName: String,
                             instanceName: String,
                             `type`: InstanceType,
                             zkAddr: String)
    extends ZKHelixManager(clusterName, instanceName, `type`, zkAddr) {
    override def disconnect(): Unit = {
      if (isShuttingDown.get()) {
        info("Is shutting down; call super.disconnect()")
        super.disconnect()
      } else {
        info("Is not shutting down; call cleanShutdown()")
        cleanShutdown()
      }
    }
  }


  def addToHelixController(): Unit = {
    helixZkManager = new WorkerZKHelixManager(helixClusterName, instanceId, InstanceType.PARTICIPANT, zkServer)
    val stateMachineEngine: StateMachineEngine = helixZkManager.getStateMachineEngine()
    // register the MirrorMaker worker
    val stateModelFactory = new HelixWorkerOnlineOfflineStateModelFactory(instanceId, connector, topicPartitionCountObserver)
    stateMachineEngine.registerStateModelFactory("OnlineOffline", stateModelFactory)
    helixZkManager.connect()
  }

  def maybeFlushAndCommitOffsets(forceCommit: Boolean) {
    val a = System.currentTimeMillis() - lastOffsetCommitMs
    info(s"testing12345 $a $lastOffsetCommitMs $offsetCommitIntervalMs")
    if (forceCommit || System.currentTimeMillis() - lastOffsetCommitMs > offsetCommitIntervalMs) {
      info("Flushing producer.")
      flushLatency.time {
        producer.flush()
      }

      callbackLatency.time {
        flushCommitLock.synchronized {
          while (!exitingOnSendFailure && recordCount.get() != 0) {
            flushCommitLock.wait(100)
          }
        }
      }

      if (!exitingOnSendFailure) {
        info("Committing offsets.")
        commitLatency.time {
          connector.commitOffsets
        }
        lastOffsetCommitMs = System.currentTimeMillis()
      } else {
        info("Exiting on send failure, skip committing offsets.")
      }
    }
  }

  def cleanShutdown() {
    if (isShuttingDown.compareAndSet(false, true)) {
      info("Start clean shutdown.")
      if (mirrorMakerThread != null) {
        // Shutdown consumer threads.
        info("Shutting down consumer thread.")
        mirrorMakerThread.shutdown()
        mirrorMakerThread.awaitShutdown()
      }

      if (producer != null) {
        info("Flushing last batches and commit offsets.")
        // flush the last batches of msg and commit the offsets
        // since MM threads are stopped, it's consistent to flush/commit here
        // commit offsets
        maybeFlushAndCommitOffsets(true)
      }

      if (connector != null) {
        info("Shutting down consumer connectors.")
        connector.shutdown()
      }

      if (producer != null) {
        // shutdown producer
        info("Closing producer.")
        producer.close()
      }

      // shutdown topic observer for destination kafka cluster
      if (topicPartitionCountObserver != null) {
        topicPartitionCountObserver.shutdown()
      }

      if (helixZkManager != null) {
        // disconnect with helixZkManager
        helixZkManager.disconnect()
        info("helix connection shutdown successfully")
      }

      removeCustomizedMetrics()

      info("Kafka uReplicator worker shutdown successfully")
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
                val record = iterRecords.next()
                if (!filterEnabled || needToSend(record, srcCluster, dstCluster, data.offset)) {
                  producer.send(record, data.partition, data.offset)
                }
              }
              maybeFlushAndCommitOffsets(true)
            }
          } catch {
            case e: ConsumerTimeoutException =>
              trace("Caught ConsumerTimeoutException, continue iteration.")
          }
          maybeFlushAndCommitOffsets(true)
        }
      } catch {
        case t: Throwable =>
          exitingOnSendFailure = true
          t.printStackTrace()
          fatal("Mirror maker thread failure due to ", t)
      } finally {
        shutdownLatch.countDown()
        info("Mirror maker thread stopped")
        // if it exits accidentally, like only one thread dies, stop the entire mirror maker
        if (!isShuttingDown.get()) {
          fatal("Mirror maker thread exited abnormally, stopping the whole mirror maker.")
          System.exit(-1)
        }
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

  // Override needToSend to implement your own producer filter
  def needToSend(record: ProducerRecord[Array[Byte], Array[Byte]],
                 srcCluster: Option[String],
                 dstCluster: Option[String],
                 srcOffset: Long): Boolean = {
    true
  }

  class MirrorMakerProducer(val producerProps: Properties) {

    val sync = producerProps.getProperty("producer.type", "async").equals("sync")

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)

    def send(record: ProducerRecord[Array[Byte], Array[Byte]], srcPartition: Int, srcOffset: Long) {
      recordCount.getAndIncrement()
      if (sync) {
        this.producer.send(record).get()
      } else {
        this.producer.send(record,
          new MirrorMakerProducerCallback(record.topic(), record.key(), record.value(), srcPartition, srcOffset))
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

  def onCompletionWithoutException(metadata: RecordMetadata, srcPartition: Int, srcOffset: Long) {}

  def additionalConfigs(srcCluster: Option[String], dstCluster: Option[String]) {}

  class MirrorMakerProducerCallback(topic: String, key: Array[Byte], value: Array[Byte],
                                    srcPartition: Int, srcOffset: Long)
    extends ErrorLoggingCallback(topic, key, value, false) {

    override def onCompletion(metadata: RecordMetadata, exception: Exception) {
      try {
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
        } else {
          onCompletionWithoutException(metadata, srcPartition, srcOffset)
        }
      } finally {
        if (recordCount.decrementAndGet() == 0 || exitingOnSendFailure) {
          flushCommitLock.synchronized {
            flushCommitLock.notifyAll()
          }
        }
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
      val topic = topicMappings.getOrElse(record.topic, record.topic)
      var partitionCount = 0
      if (topicPartitionCountObserver != null) {
        partitionCount = topicPartitionCountObserver.getPartitionCount(topic)
      }
      if (partitionCount > 0 && record.partition >= 0) {
        Collections.singletonList(new ProducerRecord[Array[Byte], Array[Byte]](topic,
          record.partition % partitionCount, record.key(), record.message()))
      } else {
        if (topicPartitionCountObserver != null) {
          // this is failure if topicPartitionCountObserver is enabled
          mapFailureMeter.mark()
        }
        Collections.singletonList(new ProducerRecord[Array[Byte], Array[Byte]](topic,
          record.key(), record.message()))
      }
    }
  }

}
