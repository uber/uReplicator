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

import com.uber.stream.ureplicator.common.KafkaUReplicatorMetricsReporter;
import com.uber.stream.ureplicator.common.MetricsReporterConf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMock;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ZookeeperCheckpointManagerTests {

  private static final String TEST_TOPIC_PREFIX = "testZookeeperCheckpointManager";
  private ZkClient zkClient = EasyMock.createMock(ZkClient.class);
  private Map<TopicPartition, Long> offsetCommitMap = new HashMap<>();
  private ZookeeperCheckpointManager zookeeperCheckpointManager;


  @BeforeTest
  public void setup() {
    KafkaUReplicatorMetricsReporter
        .init(new MetricsReporterConf("dca1", new ArrayList<>(), "localhost", null, null));
    zookeeperCheckpointManager = new ZookeeperCheckpointManager(zkClient,"ureplicator-cluster1-cluster2");
    for (int i = 0; i < 50; i++) {
      offsetCommitMap
          .put(new TopicPartition(TEST_TOPIC_PREFIX + String.valueOf(i / 10), i % 10), i * 10l);
    }
  }

  @Test
  public void testZookeeperCheckpointManagerFailure() {
    zkClient.writeData(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().andThrow(new RuntimeException("exception"));
    boolean succeed = zookeeperCheckpointManager.commitOffset(offsetCommitMap);
    Assert.assertFalse(succeed);
  }
}
