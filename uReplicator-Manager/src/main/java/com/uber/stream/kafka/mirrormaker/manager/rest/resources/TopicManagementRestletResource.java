package com.uber.stream.kafka.mirrormaker.manager.rest.resources;

import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rest API for topic management
 */
public class TopicManagementRestletResource extends ServerResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(TopicManagementRestletResource.class);

  public TopicManagementRestletResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
  }

  @Override
  @Get
  public Representation get() {
    final String topicName = (String) getRequest().getAttributes().get("topicName");

    if (topicName == null) {
      return new StringRepresentation("No topic is added in uReplicator Manager!");
    }
    return new StringRepresentation("No info for topic: " + topicName);
  }

}
