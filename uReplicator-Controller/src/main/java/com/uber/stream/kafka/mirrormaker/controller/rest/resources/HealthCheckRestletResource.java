package com.uber.stream.kafka.mirrormaker.controller.rest.resources;

import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

/**
 * Health check servlet.
 */
public class HealthCheckRestletResource extends ServerResource {

  private final HelixMirrorMakerManager _helixMirrorMakerManager;

  public HealthCheckRestletResource() {
    _helixMirrorMakerManager = (HelixMirrorMakerManager) getApplication().getContext()
        .getAttributes().get(HelixMirrorMakerManager.class.toString());
  }

  @Override
  @Get
  public Representation get() {
    if (_helixMirrorMakerManager.isHealthy()) {
      return new StringRepresentation("OK\n");
    } else {
      getResponse().setStatus(Status.SERVER_ERROR_SERVICE_UNAVAILABLE);
      return new StringRepresentation("Unhealthy\n");
    }
  }
}
