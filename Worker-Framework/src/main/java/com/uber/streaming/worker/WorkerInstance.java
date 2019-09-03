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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WorkerInstance is a service that fetch, process and dispatch data from source data store to
 * destination data store
 */
public class WorkerInstance implements Worker {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerInstance.class);

  private AtomicBoolean isRunnig = new AtomicBoolean(false);
  private final Fetcher fetcher;
  private final Processor processor;
  private final Dispatcher dispatcher;

  public WorkerInstance(
      Fetcher fetcher,
      Processor processor,
      Dispatcher dispatcher) {
    Preconditions.checkNotNull(fetcher);
    Preconditions.checkNotNull(dispatcher);
    this.fetcher = fetcher;
    this.processor = processor;
    this.dispatcher = dispatcher;
  }

  public void start() {
    if (!isRunnig.compareAndSet(false, true)) {
      LOGGER.info("WorkerInstance is running");
      return;
    }
    fetcher.start();
    if (processor != null) {
      processor.start();
    }
    dispatcher.start();
  }

  public void shutdown() throws IOException {
    if (!isRunnig.compareAndSet(true, false)) {
      LOGGER.info("WorkerInstance already shutdown");
      return;
    }
    dispatcher.close();
    if (processor != null) {
      processor.close();
    }
    fetcher.close();
  }

  /**
   * Adds task to fetcher
   *
   * @param task task info
   */
  @Override
  public void addTask(Task task) {
    fetcher.addTask(task);
  }

  /**
   * Removes task from fetcher
   *
   * @param task task info
   */
  @Override
  public void removeTask(Task task) {
    fetcher.removeTask(task);
  }

  /**
   * Updates task info. Method is reserved for update message workload, end offset.
   *
   * @param task task info
   */
  @Override
  public void updateTask(Task task) {
    throw new NotImplementedException("updateTask is not implemented");
  }

  /**
   * Gets tasks that worker is currently working on
   *
   * @return list of tasks
   */
  @Override
  public List<Task> getTasks() {
    return fetcher.getTasks();
  }

  public final static class Builder {

    private Fetcher fetcher;
    private Processor processor;
    private Dispatcher dispatcher;

    public Builder() {
    }

    public Builder setFetcher(Fetcher fetcher) {
      this.fetcher = fetcher;
      return this;
    }

    public Builder setProcessor(Processor processor) {
      this.processor = processor;
      return this;
    }

    public Builder setDispatcher(Dispatcher dispatcher) {
      this.dispatcher = dispatcher;
      return this;
    }


    public WorkerInstance build() {
      Preconditions.checkNotNull(fetcher);
      Preconditions.checkNotNull(dispatcher);

      if (processor != null) {
        fetcher.setNextStage(processor);
        processor.setNextStage(dispatcher);
      } else {
        fetcher.setNextStage(dispatcher);
      }
      return new WorkerInstance(fetcher, processor, dispatcher);
    }
  }
}
