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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockFetcher implements Fetcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(MockFetcher.class);

  private Sink sink;

  public MockFetcher() {
  }

  @Override
  public void start() {
    // TODO:  generate mock data
  }

  @Override
  public void close() throws IOException {

  }
  @Override
  public void addTask(Task task) {
    try {
      sink.enqueue("addTask-" + task.getTopic(), task).get();
    } catch (InterruptedException e) {
      LOGGER.error("AddTask failed");
    } catch (ExecutionException e) {
      LOGGER.error("AddTask failed");
    }
  }

  @Override
  public void removeTask(Task task) {
    try {
      sink.enqueue("removeTask-" + task.getTopic(), task).get();
    } catch (InterruptedException e) {
      LOGGER.error("removeTask failed");
    } catch (ExecutionException e) {
      LOGGER.error("removeTask failed");
    }
  }

  @Override
  public void pauseTask(Task task) {

  }

  @Override
  public void resumeTask(Task task) {

  }

  @Override
  public List<Task> getTasks() {
    return null;
  }

  @Override
  public void setNextStage(Sink sink) {
    this.sink = sink;
  }
}
