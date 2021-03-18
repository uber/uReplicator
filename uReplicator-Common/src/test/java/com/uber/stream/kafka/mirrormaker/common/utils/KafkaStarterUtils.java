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
package com.uber.stream.kafka.mirrormaker.common.utils;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.zk.KafkaZkClient;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utilities to start Kafka during unit tests.
 */
public class KafkaStarterUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStarterUtils.class);
  public static final int DEFAULT_KAFKA_PORT = 19092;
  public static final int DEFAULT_BROKER_ID = 0;
  public static final String DEFAULT_ZK_STR = ZkStarter.DEFAULT_ZK_STR + "/kafka";
  public static final String DEFAULT_KAFKA_BROKER = "localhost:" + DEFAULT_KAFKA_PORT;
  public static String kafkaDataDir = "";
  public static Properties getDefaultKafkaConfiguration() {
    Properties properties = new Properties();
    properties.setProperty("controlled.shutdown.enable", "false");
    // set replication factor to 1 to fix offsets topic creation failure.
    // default offsets topic replication factor is 3 but most of the unittest only start one kafka process
    properties.setProperty("offsets.topic.replication.factor", "1");
    return properties;
  }

  public static KafkaServerStartable startServer(final int port, final int brokerId,
      final String zkStr, final Properties configuration) {
    // Create the ZK nodes for Kafka, if needed
    int indexOfFirstSlash = zkStr.indexOf('/');
    if (indexOfFirstSlash != -1) {
      String bareZkUrl = zkStr.substring(0, indexOfFirstSlash);
      String zkNodePath = zkStr.substring(indexOfFirstSlash);
      ZkClient client = new ZkClient(bareZkUrl);
      client.createPersistent(zkNodePath, true);
      client.close();
    }

    File logDir = new File("/tmp/kafka-" + Double.toHexString(Math.random()));
    logDir.mkdirs();
    logDir.deleteOnExit();
    kafkaDataDir = logDir.toString();

    configureKafkaPort(configuration, port);
    configureZkConnectionString(configuration, zkStr);
    configureBrokerId(configuration, brokerId);
    configureKafkaLogDirectory(configuration, logDir);
    KafkaConfig config = new KafkaConfig(configuration);
    KafkaServerStartable serverStartable = new KafkaServerStartable(config);
    serverStartable.startup();
    return serverStartable;
  }

  public static void configureSegmentSizeBytes(Properties properties, int segmentSize) {
    properties.put("log.segment.bytes", Integer.toString(segmentSize));
  }

  public static void configureLogRetentionSizeBytes(Properties properties,
      int logRetentionSizeBytes) {
    properties.put("log.retention.bytes", Integer.toString(logRetentionSizeBytes));
  }

  public static void configureKafkaLogDirectory(Properties configuration, File logDir) {
    configuration.put("log.dirs", logDir.getAbsolutePath());
  }

  public static void configureBrokerId(Properties configuration, int brokerId) {
    configuration.put("broker.id", Integer.toString(brokerId));
  }

  public static void configureZkConnectionString(Properties configuration, String zkStr) {
    configuration.put("zookeeper.connect", zkStr);
  }

  public static void configureKafkaPort(Properties configuration, int port) {
    if (configuration.containsKey("ssl.keystore.location")) {
      configuration.put("listeners", "SSL://:" + port);
    } else {
      configuration.put("port", Integer.toString(port));
    }
  }

  public static void stopServer(KafkaServerStartable serverStartable) {
    serverStartable.shutdown();
    if (StringUtils.isNotEmpty(kafkaDataDir)) {
      try {
        FileUtils.deleteDirectory(new File(kafkaDataDir));
      } catch (IOException e) {
        LOGGER.warn("Caught exception while stopping kafka", e);
      }
    }
  }

  public static void createTopic(String kafkaTopic, String zkStr) {
    createTopic(kafkaTopic, 1, zkStr, "1");
  }

  public static void createTopic(String kafkaTopic, int numOfPartitions, String zkStr, String replicatorFactor) {
    // TopicCommand.main() will call System.exit() finally, which will break maven-surefire-plugin
    try {
      String[] args = new String[]{"--create", "--zookeeper", zkStr, "--replication-factor", replicatorFactor,
          "--partitions", String.valueOf(numOfPartitions), "--topic", kafkaTopic};
      KafkaZkClient zkClient = KafkaZkClient
          .apply(zkStr, false, 3000, 3000, Integer.MAX_VALUE, Time.SYSTEM, "kafka.server",
              "SessionExpireListener");
      TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(args);
      TopicCommand.createTopic(zkClient, opts);
    } catch (TopicExistsException e) {
      // Catch TopicExistsException otherwise it will break maven-surefire-plugin
      System.out.println("Topic already existed");
    }
  }

  public static void expandTopic(String kafkaTopic, int numOfPartitions, String zkStr) {
    try {
      String[] args = new String[]{"--alter", "--zookeeper", zkStr,
          "--partitions", String.valueOf(numOfPartitions), "--topic", kafkaTopic};
      KafkaZkClient zkClient = KafkaZkClient
          .apply(zkStr, false, 3000, 3000, Integer.MAX_VALUE, Time.SYSTEM, "kafka.server",
              "SessionExpireListener");
      TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(args);
      TopicCommand.alterTopic(zkClient, opts);
    } catch (TopicExistsException e) {
      // Catch TopicExistsException otherwise it will break maven-surefire-plugin
      System.out.println("Expand topic failed");
    }
  }
}
