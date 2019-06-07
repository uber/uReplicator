/*
 * Copyright (C) 2015-2019 Uber Technologies, Inc. (streaming-data@uber.com)
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
package com.uber.stream.ureplicator.worker;

import com.uber.stream.kafka.mirrormaker.common.core.OnlineOfflineStateModel;
import com.uber.stream.kafka.mirrormaker.common.utils.HelixSetupUtils;
import com.uber.stream.kafka.mirrormaker.common.utils.ZkStarter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class TestUtils {

  public static final String SRC_CLUSTER = "cluster1";
  public static final String DST_CLUSTER = "cluster2";
  public static final String CONTROLLER_WORKER_HELIX_CLUSTER = String
      .format("controller-worker-%s-%s-0", SRC_CLUSTER, DST_CLUSTER);
  public static final String ROUTE_NAME = String
      .format("@%s@%s", TestUtils.SRC_CLUSTER, TestUtils.DST_CLUSTER);
  public static final String MANAGER_WORKER_HELIX_CLUSTER = "manager-worker-integration-test";
  public static final String HELIX_ROOT_NODE = ZkStarter.DEFAULT_ZK_STR + "/ureplicator";

  public static final String INVALID_ROUTE_NAME1 = "@cluster1@cluster2@1";
  public static final String INVALID_ROUTE_NAME2 = "cluster1";
  public static final String HELIX_CLUSTER_PATH = "/ureplicator";

  public static final String[] WORKER_ARGS = new String[]{
      "--consumer.config", "src/test/resources/consumer.properties",
      "--producer.config", "src/test/resources/producer.properties",
      "--helix.config", "src/test/resources/helix-1.properties",
      "--cluster.config", "src/test/resources/clusters.properties",
      "--dstzk.config", "src/test/resources/dstzk.properties",
      "--offset.commit.interval.ms", "6000"
  };

  private static KafkaProducer createProducer(String bootstrapServer) {
    Properties producerProps = new Properties();
    producerProps
        .setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "test");
    producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class.getName());
    producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class.getName());
    KafkaProducer producer = new KafkaProducer(producerProps);
    return producer;
  }

  private static Consumer<Byte[], Byte[]> createConsumer(String bootstrapServer) {
    final Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,
        "KafkaExampleConsumer");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Create the consumer using props.
    final Consumer<Byte[], Byte[]> consumer =
        new KafkaConsumer<>(consumerProps);
    // Subscribe to the topic.
    return consumer;
  }


  public static void produceMessages(String bootstrapServer, String topicname, int messageCount) {
    KafkaProducer producer = createProducer(bootstrapServer);
    for (int i = 0; i < messageCount; i++) {
      ProducerRecord<Byte[], Byte[]> record = new ProducerRecord(topicname, null,
          String.format("Test Value - %d", i).getBytes());
      producer.send(record);
    }
    producer.flush();
    producer.close();
  }

  public static void produceMessages(String bootstrapServer, String topicname, int messageCount,
      int numOfPartitions) {
    KafkaProducer producer = createProducer(bootstrapServer);
    for (int i = 0; i < messageCount; i++) {
      ProducerRecord<Byte[], Byte[]> record = new ProducerRecord(topicname, i % numOfPartitions,
          null,
          String.format("Test Value - %d", i).getBytes());
      producer.send(record);
    }
    producer.flush();
    producer.close();
  }

  public static List<ConsumerRecord<Byte[], Byte[]>> consumeMessage(String bootstrapServer,
      String topicName,
      int timeoutMs
  ) throws InterruptedException {

    long time = new Date().getTime();
    Consumer<Byte[], Byte[]> consumer = createConsumer(bootstrapServer);
    consumer.subscribe(Collections.singletonList(topicName));

    List<ConsumerRecord<Byte[], Byte[]>> result = new ArrayList<>();
    while ((new Date().getTime()) - time < timeoutMs) {
      ConsumerRecords<Byte[], Byte[]> records = consumer.poll(1000);
      Iterator<ConsumerRecord<Byte[], Byte[]>> iterator = records.iterator();
      while (iterator.hasNext()) {
        result.add(iterator.next());
      }
      Thread.sleep(500);
    }
    consumer.close();
    return result;
  }

  public static IdealState buildControllerWorkerCustomIdealState(String topicName,
      Map<String, String> partitionInstanceMap,
      String state) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(partitionInstanceMap.size()).setNumReplica(1)
        .setMaxPartitionsPerNode(1);

    for (String key : partitionInstanceMap.keySet()) {
      customModeIdealStateBuilder.assignInstanceAndState(key, partitionInstanceMap.get(key), state);
    }

    return customModeIdealStateBuilder.build();
  }


  public static IdealState buildManagerWorkerCustomIdealState(String routeName,
      List<String> instanceIds,
      String state) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(routeName);

    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(1).setNumReplica(1)
        .setMaxPartitionsPerNode(1);

    for (String instanceId : instanceIds) {
      customModeIdealStateBuilder.assignInstanceAndState("0", instanceId, state);
    }

    return customModeIdealStateBuilder.build();
  }

  public static List<ConsumerRecord> consumerRecords(ConsumerIterator iterator, long timeout)
      throws InterruptedException {
    Long startTime = System.currentTimeMillis();

    List<ConsumerRecord> res = new ArrayList<>();
    while (System.currentTimeMillis() - startTime < timeout) {
      try {
        iterator.hasNext();
      } catch (Exception e) {
        Thread.sleep(200);
        continue;
      }
      res.add(iterator.next());
    }
    return res;
  }


  public static WorkerConf initWorkerConf() {
    WorkerConf conf = new WorkerConf();
    conf.setAbortOnSendFailure(true);
    conf.setConsumerConfigFile("src/test/resources/consumer.properties");
    conf.setProducerConfigFile("src/test/resources/producer.properties");
    conf.setHelixConfigFile("src/test/resources/helix.properties");
    conf.setClusterConfigFile("src/test/resources/clusters.properties");
    conf.setFederatedEnabled(true);
    return conf;
  }

  public static ZKHelixAdmin initHelixClustersForWorkerTest(Properties properties, String route) throws InterruptedException {
    String zkRoot = properties.getProperty("zkServer");
    Thread.sleep(500);
    ZkClient zkClient = ZkUtils.createZkClient(ZkStarter.DEFAULT_ZK_STR, 1000, 1000);
    zkClient.createPersistent("/ureplicator");
    zkClient.close();
    ZKHelixAdmin helixAdmin = new ZKHelixAdmin(zkRoot);
    String deployment = properties.getProperty("federated.deployment.name");
    String managerHelixClusterName = WorkerUtils.getManagerWorkerHelixClusterName(deployment);
    String controllerHelixClusterName = WorkerUtils.getControllerWorkerHelixClusterName(route);
    HelixSetupUtils.setup(managerHelixClusterName, zkRoot, "0");
    HelixSetupUtils.setup(controllerHelixClusterName, zkRoot, "0");
    return helixAdmin;
  }
}
