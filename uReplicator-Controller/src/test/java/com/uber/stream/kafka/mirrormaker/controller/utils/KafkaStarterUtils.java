/*
 * Copyright (C) 2015-2017 Uber Technology Inc. (streaming-data@uber.com)
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
package com.uber.stream.kafka.mirrormaker.controller.utils;

import java.io.File;
import java.util.Properties;
import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.errors.TopicExistsException;


/**
 * Utilities to start Kafka during unit tests.
 */
public class KafkaStarterUtils {

  public static final int DEFAULT_KAFKA_PORT = 19092;
  public static final int DEFAULT_BROKER_ID = 0;
  public static final String DEFAULT_ZK_STR = ZkStarter.DEFAULT_ZK_STR + "/kafka";
  public static final String DEFAULT_KAFKA_BROKER = "localhost:" + DEFAULT_KAFKA_PORT;

  public static Properties getDefaultKafkaConfiguration() {
    return new Properties();
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

  public static void configureLogRetentionSizeBytes(Properties properties, int logRetentionSizeBytes) {
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
    configuration.put("port", Integer.toString(port));
  }

  public static void stopServer(KafkaServerStartable serverStartable) {
    serverStartable.shutdown();
    FileUtils.deleteQuietly(new File(serverStartable.serverConfig().logDirs().apply(0)));
  }

  public static void createTopic(String kafkaTopic, String zkStr) {
    // TopicCommand.main() will call System.exit() finally, which will break maven-surefire-plugin
    try {
      String[] args = new String[]{"--create", "--zookeeper", zkStr, "--replication-factor", "1",
          "--partitions", "1", "--topic", kafkaTopic};
      ZkUtils zkUtils = ZkUtils.apply(zkStr, 30000, 30000, false);
      TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(args);
      TopicCommand.createTopic(zkUtils, opts);
    } catch (TopicExistsException e) {
      // Catch TopicExistsException otherwise it will break maven-surefire-plugin
      System.out.println("Topic already existed");
    }
  }
}
