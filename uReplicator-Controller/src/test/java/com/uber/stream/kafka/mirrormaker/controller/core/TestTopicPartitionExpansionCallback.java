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
import org.easymock.EasyMock;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestTopicPartitionExpansionCallback {

  private final static String TOPIC_NAME = "TestTopicPartitionExpansionCallback";
  private TopicPartitionExpansionCallback topicPartitionExpansionCallback;
  private HelixMirrorMakerManager helixMirrorMakerManager;

  @BeforeTest
  public void setup() {
    helixMirrorMakerManager = EasyMock.createMock(HelixMirrorMakerManager.class);
    topicPartitionExpansionCallback = new TopicPartitionExpansionCallback(helixMirrorMakerManager);
  }

  @Test
  public void testTopicPartitionExpansionCallback() {
    helixMirrorMakerManager.expandTopicInMirrorMaker(new TopicPartition(TOPIC_NAME, 10));
    EasyMock.expectLastCall();
    EasyMock.replay(helixMirrorMakerManager);

    topicPartitionExpansionCallback.onPartitionNumberChange(TOPIC_NAME, 10);
    EasyMock.verify(helixMirrorMakerManager);
  }
}
