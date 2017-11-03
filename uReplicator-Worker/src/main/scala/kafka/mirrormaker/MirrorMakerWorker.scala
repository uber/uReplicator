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

import com.yammer.metrics.core.Gauge
import kafka.consumer._
import kafka.message.MessageAndMetadata
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.{CommandLineUtils, Logging}
import org.apache.helix.manager.zk.ZKHelixManager
import org.apache.helix.participant.StateMachineEngine
import org.apache.helix.{HelixManager, HelixManagerFactory, InstanceType}
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.utils.Utils

import scala.io.Source
import joptsimple.OptionSet

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
class MirrorMakerWorker extends Logging with KafkaMetricsGroup {
  private val ManagerWorkerHelixPrefix = "manager-worker-"

  def main(args: Array[String]) {
    info("Starting mirror maker")

    val mirrorMakerWorkerConf = getMirrorMakerWorkerConf
    val parser = mirrorMakerWorkerConf.getParser

    if (args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "Continuously copy data between two Kafka clusters.")

    val options = parser.parse(args: _*)

    if (options.has(mirrorMakerWorkerConf.getHelpOpt)) {
      parser.printHelpOn(System.out)
      System.exit(0)
    }

    CommandLineUtils.checkRequiredArgs(parser, options,
      mirrorMakerWorkerConf.getConsumerConfigOpt,
      mirrorMakerWorkerConf.getProducerConfigOpt,
      mirrorMakerWorkerConf.getHelixConfigOpt)

    val helixProps = Utils.loadProps(options.valueOf(mirrorMakerWorkerConf.getHelixConfigOpt))

    if (helixProps.getProperty("federated.enabled", "false").equals("true")) {
      // start the worker in federated mode
      val zkServer = helixProps.getProperty("zkServer", "localhost:2181")
      val instanceId = helixProps.getProperty("instanceId", "HelixMirrorMaker-" + System.currentTimeMillis)
      val deploymentName = helixProps.getProperty("federated.deployment.name", "")
      if (deploymentName.isEmpty()) {
        error("Deployment name is missing for federated mode")
        System.exit(1)
      }
      val managerWorkerHelixName = ManagerWorkerHelixPrefix + deploymentName

      val managerWorkerHelix = new ZKHelixManager(managerWorkerHelixName, instanceId, InstanceType.PARTICIPANT, zkServer)
      val stateMachineEngine: StateMachineEngine = managerWorkerHelix.getStateMachineEngine()
      val managerWorkerHandler = new ManagerWorkerHelixHandler(mirrorMakerWorkerConf, options)
      // register the MirrorMaker worker to Manager-Worker cluster
      val stateModelFactory = new ManagerWorkerOnlineOfflineStateModelFactory(managerWorkerHandler)
      stateMachineEngine.registerStateModelFactory("OnlineOffline", stateModelFactory)

      val mainThread = new FederatedMainThread()
      Runtime.getRuntime.addShutdownHook(new Thread("MirrorMakerShutdownHook") {
        override def run() {
          info("Shutting down federated worker")
          mainThread.shutdown()
          managerWorkerHandler.stop()
          managerWorkerHelix.disconnect()
        }
      })

      managerWorkerHelix.connect()
      mainThread.start()
    } else {
      val helixClusterName = helixProps.getProperty("helixClusterName", "testMirrorMaker")
      val worker = new WorkerInstance(mirrorMakerWorkerConf, options, null, null, helixClusterName)
      Runtime.getRuntime.addShutdownHook(new Thread("MirrorMakerShutdownHook") {
        override def run() {
          worker.cleanShutdown()
        }
      })
      worker.start()
    }
  }

  def getWorkerInstance(workerConfig: MirrorMakerWorkerConf, options: OptionSet,
    srcCluster: String, dstCluster: String, helixClusterName: String): WorkerInstance = {
    new WorkerInstance(workerConfig, options, srcCluster, dstCluster, helixClusterName);
  }

  def getMirrorMakerWorkerConf: MirrorMakerWorkerConf = {
    new MirrorMakerWorkerConf()
  }

  class FederatedMainThread() extends Thread {
    private val latch = new CountDownLatch(1)
    setName("federated-thread-main-thread")

    def shutdown() {
      latch.countDown()
    }

    override def run() {
      info("Starting federated main thread")
      latch.await()
    }
  }

}
