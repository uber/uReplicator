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


import java.util.Properties

import kafka.api.IntegrationTestHarness
import kafka.consumer.SimpleConsumer
import kafka.integration.KafkaServerTestHarness
import kafka.producer.Producer
import kafka.security.auth.SimpleAclAuthorizer
import kafka.serializer.StringEncoder
import kafka.server.KafkaConfig
import kafka.utils.{StaticPartitioner, TestUtils, Logging}
import org.junit.{After, Before}
import org.scalatest.junit.JUnitSuite

/**
 * TODO, file in progress
 */
class MirrorMakerWorkerTest extends JUnitSuite with IntegrationTestHarness with Logging {

  // broker properties init
  val brokerId1: Integer = 1
  val brokerId2: Integer = 2
  val port1: Integer = 9093
  val port2: Integer = 9094
  val zkConnect1 = "localhost:2181/cluster1"
  val zkConnect2 = "localhost:2181/cluster2"
  val logDirs1 = "/tmp/kafka-logs/kafka1"
  val logDirs2 = "/tmp/kafka-logs/kafka2"
  val overridingProps1 = new Properties()
  overridingProps1.put(KafkaConfig.BrokerIdProp, brokerId1.toString)
  overridingProps1.put(KafkaConfig.PortProp, port1.toString)
  overridingProps1.put(KafkaConfig.LogDirsProp, logDirs1)
  overridingProps1.put(KafkaConfig.ZkConnectProp, zkConnect1)
  val overridingProps2 = new Properties()
  overridingProps2.put(KafkaConfig.BrokerIdProp, brokerId2.toString)
  overridingProps2.put(KafkaConfig.PortProp, port2.toString)
  overridingProps2.put(KafkaConfig.LogDirsProp, logDirs2)
  overridingProps2.put(KafkaConfig.ZkConnectProp, zkConnect2)
  val brokerConfig1 = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, zkConnect, enableControlledShutdown = false), overridingProps1)
  val brokerConfig2 = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1, zkConnect, enableControlledShutdown = false), overridingProps2)

  val host = "localhost"
  var producer: Producer[String, String] = null
  var consumer: SimpleConsumer = null

  @Before
  override def setUp() {
    super.setUp
    producer = TestUtils.createProducer[String, String](TestUtils.getBrokerListStrFromServers(servers),
      encoder = classOf[StringEncoder].getName,
      keyEncoder = classOf[StringEncoder].getName,
      partitioner = classOf[StaticPartitioner].getName)
    consumer = new SimpleConsumer(host, servers(0).boundPort(), 1000000, 64 * 1024, "")
  }

  @After
  override def tearDown() {
    producer.close()
    consumer.close()
    super.tearDown
  }

  // configure the servers and clients
  override def generateConfigs() = Seq(brokerConfig1, brokerConfig2)

}



