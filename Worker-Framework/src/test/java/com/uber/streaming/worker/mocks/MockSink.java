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
package com.uber.streaming.worker.mocks;

import com.uber.streaming.worker.Dispatcher;
import com.uber.streaming.worker.Task;
import com.uber.streaming.worker.clients.CheckpointManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockSink extends Dispatcher<List<ConsumerRecord>> {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(MockSink.class);
  public final List<ConsumerRecord> messages;
  private CheckpointManager checkpointManager;

  public MockSink(CheckpointManager checkpointManager) {
    messages = new ArrayList<>();
    this.checkpointManager = checkpointManager;
  }

  public void resetNumber() {
    messages.clear();
  }

  @Override
  public CompletableFuture<Void> enqueue(List<ConsumerRecord> dataIn, Task task) {
    checkpointManager.setCommitOffset(task, dataIn.get(dataIn.size() - 1).offset() + 1);
    messages.addAll(dataIn);
    return null;

  }
}
