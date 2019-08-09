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

import java.io.Closeable;
import java.util.List;

/*
 * Fetcher is an interface for fetching data from a data source(ie. Kafka)

 * @implSpec All methods in this class must be thread safe.
 */
public interface Fetcher<T> extends Closeable, Chainable<T> {

  /**
   * initiates any long running processes that are required for fetcher implementation.
   */
  void start();

  /**
   * adds new data fetcher task
   *
   * @param task data fetch task
   */
  void addTask(Task task);

  /**
   * removes data fetch task
   *
   * @param task data fetch task
   */
  void removeTask(Task task);

  /**
   * pauses data fetch task
   *
   * @param task data fetch task
   */
  void pauseTask(Task task);

  /**
   * resumes data fetch task
   */
  void resumeTask(Task task);

  /**
   * gets data fetch tasks
   */
  List<Task> getTasks();
}
