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


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Dispatcher is an interface for worker to send data to external data source.
 *
 * @param <K> data type for messages needs to dispatch
 */
public abstract class Dispatcher<K> implements Sink<K> {

  protected final WorkerConf conf;
  protected final AtomicBoolean isRunning = new AtomicBoolean(false);

  /**
   * Constructor
   *
   * @param conf worker conf
   */
  public Dispatcher(WorkerConf conf) {
    this.conf = conf;
  }

  @Override
  public void start() {
    isRunning.set(true);
  }

  @Override
  public void close() {
    isRunning.set(false);
  }

  @Override
  public boolean isRunning() {
    return isRunning.get();
  }

  /**
   * Enqueues data and dispatch to external datasource
   */
  public abstract CompletableFuture<Void> enqueue(K dataIn, Task task);
}
