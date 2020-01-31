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
