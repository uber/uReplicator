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
package com.uber.stream.kafka.mirrormaker.controller.core;

import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.ureplicator.common.observer.ObserverCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicPartitionExpansionCallback implements ObserverCallback {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopicPartitionExpansionCallback.class);

  private final HelixMirrorMakerManager managerControllerHelix;

  public TopicPartitionExpansionCallback(HelixMirrorMakerManager managerControllerHelix) {
    this.managerControllerHelix = managerControllerHelix;
  }

  @Override
  public void onPartitionNumberChange(String topicName, int currentPartition) {
    LOGGER.info("Received partition number change on topic {}, currentPartition: {}", topicName, currentPartition);
    managerControllerHelix.expandTopicInMirrorMaker(new TopicPartition(topicName, currentPartition));
  }
}
