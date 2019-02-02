package com.uber.stream.kafka.mirrormaker.controller.rest.resources;

import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;
import com.uber.stream.kafka.mirrormaker.controller.core.OffsetMonitor;

public class TopicParitionOffsetRestletResource extends ServerResource {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TopicParitionOffsetRestletResource.class);

  private final HelixMirrorMakerManager _helixMirrorMakerManager;

  public TopicParitionOffsetRestletResource() {
    _helixMirrorMakerManager = (HelixMirrorMakerManager) getApplication().getContext()
        .getAttributes().get(HelixMirrorMakerManager.class.toString());
  }

  @Override
  @Get
  public Representation get() {
    OffsetMonitor offsetMonitor = _helixMirrorMakerManager.getOffsetMonitor();
    final String topicName = (String) getRequest().getAttributes().get("topic");
    if (topicName == null) {
      try {
        return new StringRepresentation(offsetMonitor.getTopicToOffsetMap().toString());
      } catch (Exception e) {
        LOGGER.error("Got error during processing Get request", e);
        getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
        return new StringRepresentation(String
            .format("Failed to get lag for offset map for all partitions with exception: %s", e));
      }
    }
    final int partition = Integer.parseInt((String) getRequest().getAttributes().get("partition"));
    try {
      return new StringRepresentation(offsetMonitor.getTopicPartitionOffset(new TopicPartition(topicName, partition)).toString());
    } catch (Exception e) {
      LOGGER.error("Got error during processing Get request", e);
      getResponse().setStatus(Status.SERVER_ERROR_INTERNAL);
      return new StringRepresentation(String
          .format("Failed to get offset for topic partition: %s:%d, with exception: %s", topicName, partition, e));
    }
  }

}
