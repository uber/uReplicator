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
package com.uber.stream.ureplicator.worker;

import com.uber.stream.ureplicator.worker.interfaces.ICheckPointManager;
import com.uber.stream.ureplicator.worker.interfaces.IMessageTransformer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProducerManager.class);

  private final Map<String, ProducerThread> producerThreadMap = new HashMap<>();
  private final WorkerInstance workerInstance;
  private final AtomicBoolean isInit = new AtomicBoolean(false);

  public ProducerManager(
      List<ConsumerIterator> consumerStream,
      Properties producerProps,
      Boolean abortOnSendFailure,
      IMessageTransformer messageTransformer,
      ICheckPointManager checkpointManager,
      WorkerInstance workerInstance) {
    this.workerInstance = workerInstance;
    this.isInit.set(false);
    String clientIdPrefix = producerProps
        .getProperty(ProducerConfig.CLIENT_ID_CONFIG, "ureplicator");
    for (int index = 0; index < consumerStream.size(); index++) {
      String threadId = String.valueOf(index);
      ProducerThread producerThread = new ProducerThread(threadId, clientIdPrefix, producerProps,
          abortOnSendFailure,
          consumerStream.get(index), messageTransformer, checkpointManager, workerInstance);
      producerThreadMap.put(threadId, producerThread);
    }
  }

  public synchronized void start() {
    if (!isInit.compareAndSet(false, true)) {
      LOGGER.error("ProducerManager already running, number of producerThread {}.", producerThreadMap.size());
      return;
    }
    for (ProducerThread thread : producerThreadMap.values()) {
      try {
        thread.start();
      } catch (Exception e) {
        LOGGER.error("Start ProducerThread {} failed, exiting uReplicator", thread.getName());
        workerInstance.cleanShutdown();
        // System.exit to make sure worker stopped completely
        System.exit(-1);
      }
    }
  }

  public synchronized void cleanShutdown() {
    for (ProducerThread thread : producerThreadMap.values()) {
      thread.shutdown();
    }
    producerThreadMap.clear();
    isInit.set(false);
  }
}
