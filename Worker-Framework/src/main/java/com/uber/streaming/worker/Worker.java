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

/**
 * internal interface for worker instance
 * TODO: add dependency diagram
 *       +------------------+
 *       | Worker Interface |
 *       +------------------+
 */
interface Worker {
  /**
   * Starts worker instance
   */
  void start();

  /**
   * Stops worker instance
   */
  void shutdown() throws IOException;

  /**
   * Adds tasks to worker instance
   * @param task
   */
  void addTask(Task task);

  /**
   * Removes task to worker instance
   * @param task
   */
  void removeTask(Task task);

  /**
   * Updates task
   * @param task
   */
  void updateTask(Task task);

  /**
   * Get tasks
   * @return list of tasks
   */
  List<Task> getTasks();
}
