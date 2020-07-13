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

import com.uber.stream.kafka.mirrormaker.common.utils.ZkStarter;
import com.uber.stream.ureplicator.common.KafkaUReplicatorMetricsReporter;
import com.uber.stream.ureplicator.common.MetricsReporterConf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ZookeeperCheckpointManagerIntegrationTests {

  private final String SRC_CLUSTER_ZK = ZkStarter.DEFAULT_ZK_STR + "/" + TestUtils.SRC_CLUSTER;
  private final String TEST_TOPIC_PREFIX = "testZookeeperCheckpointManager";
  private Map<TopicPartition, Long> offsetCommitMap = new HashMap<>();

  @BeforeTest
  public void setup() {
    ZkStarter.startLocalZkServer();
    ZkClient zkClient = ZkUtils.createZkClient(ZkStarter.DEFAULT_ZK_STR, 1000, 1000);
    zkClient.createPersistent("/" + TestUtils.SRC_CLUSTER);
    zkClient.close();
    KafkaUReplicatorMetricsReporter
        .init(new MetricsReporterConf("dca1", new ArrayList<>(), "localhost", null, null));

    for (int i = 0; i < 50; i++) {
      offsetCommitMap
          .put(new TopicPartition(TEST_TOPIC_PREFIX + String.valueOf(i / 10), i % 10), i * 10l);
    }
  }

  @Test
  public void testZookeeperCheckpointManager() {
    CustomizedConsumerConfig consumerConfig = new CustomizedConsumerConfig(new Properties());
    consumerConfig.setProperty("commit.zookeeper.connect", SRC_CLUSTER_ZK);
    String groupId = "ureplicator-cluster1-cluster2";
    ZookeeperCheckpointManager checkpointManager = new ZookeeperCheckpointManager(consumerConfig,
        groupId);


    for (TopicPartition topicPartition : offsetCommitMap.keySet()) {
      Long offset = checkpointManager.fetchOffset(topicPartition);
      Assert.assertEquals(offset, Long.valueOf(-1));
    }
    checkpointManager.commitOffset(offsetCommitMap);

    for (Map.Entry<TopicPartition, Long> entry : offsetCommitMap.entrySet()) {
      Long offset = checkpointManager.fetchOffset(entry.getKey());
      Assert.assertEquals(offset, entry.getValue());
    }
  }

  @AfterTest
  public void shutdown() {
    ZkStarter.stopLocalZkServer();
  }

}

