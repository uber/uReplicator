package com.uber.stream.kafka.mirrormaker.controller.rest.resources;

import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;

/**
 * AdminRestletResource is used to control auto balancing enable/disalbe.
 *  
 * @author xiangfu
 *
 */
public class AdminRestletResource extends ServerResource {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AdminRestletResource.class);

  private final HelixMirrorMakerManager _helixMirrorMakerManager;

  public AdminRestletResource() {
    _helixMirrorMakerManager = (HelixMirrorMakerManager) getApplication().getContext()
        .getAttributes().get(HelixMirrorMakerManager.class.toString());
  }

  @Override
  @Get
  public Representation get() {
    final String opt = (String) getRequest().getAttributes().get("opt");
    if ("disable_autobalancing".equalsIgnoreCase(opt)) {
      _helixMirrorMakerManager.disableAutoBalancing();
      LOGGER.info("Disabled autobalancing!");
      return new StringRepresentation("Disabled autobalancing!\n");
    } else if ("enable_autobalancing".equalsIgnoreCase(opt)) {
      _helixMirrorMakerManager.enableAutoBalancing();
      LOGGER.info("Enabled autobalancing!");
      return new StringRepresentation("Enabled autobalancing!\n");
    }
    LOGGER.info("No valid input!");
    return new StringRepresentation("No valid input!\n");
  }

}
