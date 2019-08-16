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
package com.uber.streaming.worker;

import com.uber.streaming.worker.clients.CheckpointManager;
import com.uber.streaming.worker.clients.KafkaCheckpointManager;
import com.uber.streaming.worker.fetcher.KafkaConsumerFetcherConfig;
import com.uber.streaming.worker.fetcher.KafkaConsumerFetcherManager;
import com.uber.streaming.worker.fetcher.KafkaConsumerFetcherThread;
import com.uber.streaming.worker.mocks.MockSink;
import java.util.Properties;

public class TestWorkerBuilder {

  public Properties consumerProps;
  public CheckpointManager checkpointManager;
  public MockSink dispatcher;
  public Fetcher fetcher;

  public TestWorkerBuilder(String bootstrapServer) {
    consumerProps = TestUtils.createKafkaConsumerProperties(bootstrapServer);
  }

  public WorkerInstance startWorkerWithKafkaConsumerFetcherThread(String threadName) {
    checkpointManager = new KafkaCheckpointManager(consumerProps);
    dispatcher = new MockSink(checkpointManager);
    fetcher = new KafkaConsumerFetcherThread.Builder().setThreadName(threadName)
        .setConf(new KafkaConsumerFetcherConfig(consumerProps))
        .setCheckpointManager(checkpointManager).build();

    WorkerInstance worker = new WorkerInstance.Builder()
        .setFetcher(fetcher)
        .setDispatcher(dispatcher).build();
    worker.start();
    return worker;
  }


  public WorkerInstance startWorkerWithKafkaConsumerFetcherManager() {
    checkpointManager = new KafkaCheckpointManager(consumerProps);
    dispatcher = new MockSink(checkpointManager);
    fetcher = new KafkaConsumerFetcherManager.Builder()
        .setConf(new KafkaConsumerFetcherConfig(consumerProps))
        .setCheckpointManager(checkpointManager).build();

    WorkerInstance worker = new WorkerInstance.Builder()
        .setFetcher(fetcher)
        .setDispatcher(dispatcher).build();
    worker.start();
    return worker;
  }
}
