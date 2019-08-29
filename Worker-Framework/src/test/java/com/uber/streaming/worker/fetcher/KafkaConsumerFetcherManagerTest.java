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
package com.uber.streaming.worker.fetcher;

import com.uber.streaming.worker.Task;
import com.uber.streaming.worker.TestUtils;
import com.uber.streaming.worker.TestWorkerBuilder;
import com.uber.streaming.worker.WorkerInstance;
import com.uber.streaming.worker.clients.CheckpointManager;
import com.uber.streaming.worker.clients.KafkaCheckpointManager;
import com.uber.streaming.worker.utils.ZkStarter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import kafka.server.KafkaServerStartable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class KafkaConsumerFetcherManagerTest {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(KafkaConsumerFetcherThreadTest.class);

  private KafkaServerStartable kafka;
  private final int clusterPort = 19092;
  private final String bootstrapServer = "localhost:" + clusterPort;
  private final String clusterZk = ZkStarter.DEFAULT_ZK_STR + "/" + clusterPort;
  private final String testTopic1 = "KafkaConsumerFetcherManagerTest1";
  private final String testTopic2 = "KafkaConsumerFetcherManagerTest2";

  @BeforeTest
  public void setup() {
    kafka = TestUtils.startupKafka(clusterZk, clusterPort, Arrays.asList(testTopic1, testTopic2));
  }

  @Test
  public void testConsumerFetcherManagerStartup() {
    Properties consumerProps = TestUtils.createKafkaConsumerProperties(bootstrapServer);
    CheckpointManager checkpointManager = new KafkaCheckpointManager(consumerProps);

    try {
      new KafkaConsumerFetcherManager.Builder()
          .setConf(new KafkaConsumerFetcherConfig(consumerProps)).build();
      Assert.fail("consumerFetcherThread should failed because of checkpoint not provided");
    } catch (Exception e) {
      Assert.assertEquals("checkpointManager can't be null", e.getMessage());
    }
    KafkaConsumerFetcherManager fetcherManager = new KafkaConsumerFetcherManager.Builder().setConf(new KafkaConsumerFetcherConfig(consumerProps))
        .setCheckpointManager(checkpointManager).build();
    try {
      fetcherManager.start();
      Assert.fail("consumerFetcherThread should failed because of data sink not provided");

    } catch (Exception e) {
      Assert.assertEquals("data sink required", e.getMessage());
    }
  }

  @Test
  public void testKafkaConsumerFetcherManager() throws InterruptedException, IOException {
    TestWorkerBuilder builder = new TestWorkerBuilder(bootstrapServer);

    WorkerInstance worker = builder.startWorkerWithKafkaConsumerFetcherManager();

    Task task1 = new Task(testTopic1, 0, "test", "testKafkaConsumerFetcherManager", null, null,
        10L);
    Task task2 = new Task(testTopic1, 0);

    worker.addTask(task1);
    worker.addTask(task2);

    Thread.sleep(500);
    Assert.assertEquals(2, builder.fetcher.getTasks().size());

    TestUtils.produceMessages(bootstrapServer, testTopic1, 15);
    // highwater mark will be 15
    Thread.sleep(500);
    Assert.assertEquals(30, (builder.dispatcher).messages.size());

    TestUtils.produceMessages(bootstrapServer, testTopic1, 20);
    // highwater mark will be 35
    Thread.sleep(500);
    Assert.assertEquals(50, (builder.dispatcher).messages.size());

    builder.checkpointManager.setCommitOffset(task1, 20);
    builder.checkpointManager.commitOffset(Arrays.asList(task1));
    // shutdown before commit offset
    worker.shutdown();
    builder.checkpointManager.close();
    LOGGER.info("Shutdown worker without commit offset");

    builder = new TestWorkerBuilder(bootstrapServer);

    worker = builder.startWorkerWithKafkaConsumerFetcherManager();
    worker.addTask(task1);
    Thread.sleep(500);
    Assert.assertEquals(1, builder.fetcher.getTasks().size());
    Assert.assertEquals(0, builder.dispatcher.messages.size());

    worker.addTask(task2);

    Thread.sleep(500);
    Assert.assertEquals(2, builder.fetcher.getTasks().size());

    // offset 0-35 for task2
    Assert.assertEquals(35, builder.dispatcher.messages.size());

    builder.checkpointManager.setCommitOffset(task2, 25);
    builder.checkpointManager.commitOffset();
    builder.dispatcher.resetNumber();

    worker.removeTask(task2);
    Thread.sleep(500);
    Assert.assertEquals(1, builder.fetcher.getTasks().size());

    // wait until offset commit
    Thread.sleep(500);
    worker.addTask(task2);

    // task2 shall consume messages form 25: 35
    Thread.sleep(1000);
    Assert.assertEquals(-1, builder.checkpointManager.getCheckpointInfo(task2).getStartingOffset());
    Assert.assertEquals(10, builder.dispatcher.messages.size());

    builder.dispatcher.resetNumber();
    Task task3 = new Task(testTopic1, 0, null, null, null, 15l, null);
    worker.removeTask(task2);
    Thread.sleep(200);
    worker.addTask(task3);
    Thread.sleep(500);

    // task3 shall consume messages form 15: 35
    Assert.assertEquals(15, builder.checkpointManager.getCheckpointInfo(task3).getStartingOffset());
    Assert.assertEquals(20, builder.dispatcher.messages.size());

    worker.shutdown();
  }

  @AfterTest
  public void shutdown() {
    kafka.shutdown();
    ZkStarter.stopLocalZkServer();
  }

}
