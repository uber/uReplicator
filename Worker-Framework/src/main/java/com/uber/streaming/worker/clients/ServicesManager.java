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
package com.uber.streaming.worker.clients;

import com.uber.streaming.worker.clients.CheckpointManager;
import com.uber.streaming.worker.clients.CheckpointInfo;
import java.io.Closeable;

/**
 * Clients is an interface to access shared services for Fetcher, Processor and Dispatcher.
 */
public interface ServicesManager extends Closeable {

  void start();

  /**
   * Accesses to checkpoint info
   * @return checkpoint manager service
   */
  CheckpointManager getCheckpointManager();

  // TODO: Topic Observer
}
