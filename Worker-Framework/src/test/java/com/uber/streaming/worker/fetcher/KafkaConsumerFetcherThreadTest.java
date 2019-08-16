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

import com.uber.streaming.worker.Dispatcher;
import com.uber.streaming.worker.Fetcher;
import com.uber.streaming.worker.Task;
import com.uber.streaming.worker.TestUtils;
import com.uber.streaming.worker.TestWorkerBuilder;
import com.uber.streaming.worker.WorkerInstance;
import com.uber.streaming.worker.clients.CheckpointManager;
import com.uber.streaming.worker.clients.KafkaCheckpointManager;
import com.uber.streaming.worker.mocks.MockSink;
import com.uber.streaming.worker.utils.ZkStarter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import kafka.server.KafkaServerStartable;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class KafkaConsumerFetcherThreadTest {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(KafkaConsumerFetcherThreadTest.class);

  private KafkaServerStartable kafka;
  private final int clusterPort = 19092;
  private final String bootstrapServer = "localhost:" + clusterPort;
  private final String clusterZk = ZkStarter.DEFAULT_ZK_STR + "/" + clusterPort;
  private final String testTopic1 = "KafkaConsumerFetcherThreadTest1";
  private final String testTopic2 = "KafkaConsumerFetcherThreadTest2";

  @BeforeTest
  public void setup() {
    kafka = TestUtils.startupKafka(clusterZk, clusterPort, Arrays.asList(testTopic1, testTopic2));
  }

  @AfterTest
  public void shutdown() {
    kafka.shutdown();
    ZkStarter.stopLocalZkServer();
  }

  @Test
  public void testConsumerFetcherThreadStartup() {
    String threadName = "testCompactConsumerFetcherThread";
    Properties consumerProps = TestUtils.createKafkaConsumerProperties(bootstrapServer);
    CheckpointManager checkpointManager = new KafkaCheckpointManager(consumerProps);

    try {
      new KafkaConsumerFetcherThread.Builder()
          .setConf(new KafkaConsumerFetcherConfig(consumerProps)).setThreadName(threadName).build();
      Assert.fail("consumerFetcherThread should failed because of checkpoint Manager is null");
    } catch (Exception e) {
      Assert.assertEquals("checkpointManager can't be null", e.getMessage());
    }
    KafkaConsumerFetcherThread consumerFetcherThread = new KafkaConsumerFetcherThread.Builder()
        .setConf(new KafkaConsumerFetcherConfig(consumerProps)).setThreadName(threadName)
        .setCheckpointManager(checkpointManager).build();

    try {
      consumerFetcherThread.start();
      Assert.fail("consumerFetcherThread should failed because of data sink not provided");
    } catch (Exception e) {
      Assert.assertEquals("data sink required", e.getMessage());
    }
  }

  @Test
  public void testConsumerFetcherThread() throws InterruptedException, IOException {
    String threadName = "testCompactConsumerFetcherThread";
    TestWorkerBuilder builder = new TestWorkerBuilder(bootstrapServer);

    WorkerInstance worker = builder.startWorkerWithKafkaConsumerFetcherThread(threadName);

    TestUtils.produceMessages(bootstrapServer, testTopic1, 10);
    Task task1 = new Task(testTopic1, 0);
    Task task2 = new Task(testTopic2, 0);

    worker.addTask(task1);
    worker.addTask(task2);

    Thread.sleep(700);
    Assert.assertEquals(2, builder.fetcher.getTasks().size());

    TestUtils.produceMessages(bootstrapServer, testTopic1, 10);
    TestUtils.produceMessages(bootstrapServer, testTopic2, 10);

    Thread.sleep(200);
    Assert.assertEquals(30, ((MockSink) builder.dispatcher).messages.size());
    // shutdown before commit offset
    worker.shutdown();
    builder.checkpointManager.close();
    LOGGER.info("Shutdown worker without commit offset");

    builder = new TestWorkerBuilder(bootstrapServer);

    worker = builder.startWorkerWithKafkaConsumerFetcherThread(threadName);
    worker.addTask(task1);
    worker.addTask(task2);

    Thread.sleep(1000);
    Assert.assertEquals(30, (builder.dispatcher).messages.size());

    Assert.assertEquals(20, builder.checkpointManager.getCheckpointInfo(task1).getFetchOffset());
    Assert.assertEquals(10, builder.checkpointManager.getCheckpointInfo(task2).getFetchOffset());

    // move task 1 to 11 and task 2 to an out of range value
    builder.checkpointManager.setCommitOffset(task1, 10);
    builder.checkpointManager.setCommitOffset(task2, 11);
    builder.checkpointManager.commitOffset(Arrays.asList(task1, task2));

    worker.shutdown();
    builder.checkpointManager.close();

    LOGGER.info("Commit offset and shutdown worker");

    worker = builder.startWorkerWithKafkaConsumerFetcherThread(threadName);
    worker.addTask(task1);
    worker.addTask(task2);

    Thread.sleep(700);
    Assert.assertEquals(20, (builder.dispatcher).messages.size());

    Assert.assertEquals(20, builder.checkpointManager.getCheckpointInfo(task1).getFetchOffset());
    Assert.assertEquals(10, builder.checkpointManager.getCheckpointInfo(task2).getFetchOffset());

    LOGGER.info("Shutting down worker");
    worker.shutdown();
  }


  @Test
  public void testConsumerFetcherThreadWrongConsumerGroup() throws IOException {
    // Start thread with default consumer group uReplicator
    String threadName = "testCompactConsumerFetcherThread";
    Properties consumerProps = TestUtils.createKafkaConsumerProperties(bootstrapServer);
    CheckpointManager checkpointManager = new KafkaCheckpointManager(consumerProps);
    Dispatcher mockSink = new MockSink(checkpointManager);

    Fetcher kafkaConsumerFetcher = new KafkaConsumerFetcherThread(threadName,
        new KafkaConsumerFetcherConfig(consumerProps), checkpointManager);

    kafkaConsumerFetcher.setNextStage(mockSink);
    kafkaConsumerFetcher.start();

    Task task1 = new Task(testTopic1, 0, "test1", "test1", null, null, null);
    try {
      kafkaConsumerFetcher.addTask(task1);
      Assert.fail("consumerFetcherThread should failed because of servicesManager not provided");

    } catch (Exception e) {
      Assert.assertEquals(
          "Consumer group for task is test1 doesn't match consumer group for fetcher thread uReplicator",
          e.getMessage());
    }

    kafkaConsumerFetcher.close();
  }
}
