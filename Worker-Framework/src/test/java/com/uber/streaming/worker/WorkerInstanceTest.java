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



import com.uber.streaming.worker.mocks.MockFetcher;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WorkerInstanceTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerInstanceTest.class);

  @Test
  public void workerInstanceTest() throws IOException {

    List<String> messagesInQueue = new ArrayList<>();
    WorkerInstance workerInstance = new WorkerInstance.Builder()
        .setFetcher(new MockFetcher())
        .setProcessor(new Processor<String, String>() {
          @Override
          public CompletableFuture<Void> enqueue(String dataIn, Task task) {
            return nextStageSink.enqueue(dataIn, task);
          }
        })
        .setDispatcher(new Dispatcher<String>() {
          @Override
          public CompletableFuture<Void> enqueue(String dataIn, Task task) {
            // dispatcher to external data source
            return CompletableFuture.runAsync(() -> messagesInQueue.add(dataIn));
          }
        })
        .build();
    workerInstance.start();
    String testTopic = "workerInstanceTest";
    workerInstance.addTask(new Task(testTopic, 0));
    Assert.assertEquals(messagesInQueue.size(), 1);
    Assert.assertEquals(messagesInQueue.get(0), "addTask-workerInstanceTest");

    workerInstance.removeTask(new Task(testTopic, 0));
    Assert.assertEquals(messagesInQueue.size(), 2);
    Assert.assertEquals(messagesInQueue.get(1), "removeTask-workerInstanceTest");

    workerInstance.shutdown();
  }
}
