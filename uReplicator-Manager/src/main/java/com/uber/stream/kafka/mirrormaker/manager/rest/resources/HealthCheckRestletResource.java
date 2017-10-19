package com.uber.stream.kafka.mirrormaker.manager.rest.resources;

import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

/**
 * Health check servlet.
 */
public class HealthCheckRestletResource extends ServerResource {

  @Override
  @Get
  public Representation get() {
    return new StringRepresentation("OK");
  }

}
