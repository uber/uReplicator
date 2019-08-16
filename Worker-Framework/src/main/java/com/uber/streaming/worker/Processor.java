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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Processor is an interface for message process, a message process should take enqueue message,
 * convert it into dequeue data type and enqueue in dispatcher. Filters can be applied during
 * message type conversion
 *
 * @param <In> enqueue data type
 * @param <Out> dequeue data type
 */
public abstract class Processor<In, Out> implements Sink<In>, Chainable<Out> {

  protected final AtomicBoolean isRunning = new AtomicBoolean(false);
  protected Sink<Out> nextStageSink;

  @Override
  public void setNextStage(Sink<Out> sink) {
    this.nextStageSink = sink;
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
}
