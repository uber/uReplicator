package com.uber.stream.kafka.mirrormaker.manager.rest.resources;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.common.core.KafkaBrokerTopicObserver;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.manager.ManagerConf;
import com.uber.stream.kafka.mirrormaker.manager.core.ControllerHelixManager;
import com.uber.stream.kafka.mirrormaker.manager.validation.SourceKafkaClusterValidationManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Rest API for topic management
 */
public class AdminRestletResource extends ServerResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(AdminRestletResource.class);

  private final ControllerHelixManager _helixMirrorMakerManager;

  public AdminRestletResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);

    _helixMirrorMakerManager = (ControllerHelixManager) getApplication().getContext()
        .getAttributes().get(ControllerHelixManager.class.toString());
  }

  @Override
  @Get
  public Representation get() {
    final String opt = (String) getRequest().getAttributes().get("opt");
    if ("disable_autoscaling".equalsIgnoreCase(opt)) {
      _helixMirrorMakerManager.disableAutoScaling();
      LOGGER.info("Disabled autoscaling!");
      return new StringRepresentation("Disabled autoscaling!\n");
    } else if ("enable_autoscaling".equalsIgnoreCase(opt)) {
      _helixMirrorMakerManager.enableAutoScaling();
      LOGGER.info("Enabled autobalancing!");
      return new StringRepresentation("Enabled autoscaling!\n");
    } else if ("autoscaling_status".equalsIgnoreCase(opt)) {
      if (_helixMirrorMakerManager.isAutoScalingEnabled()) {
        return new StringRepresentation("enabled\n");
      } else {
        return new StringRepresentation("disabled\n");
      }
    } else if ("disable_autobalancing".equalsIgnoreCase(opt)) {
      _helixMirrorMakerManager.disableAutoBalancing();
      LOGGER.info("Disabled autobalancing!");
      return new StringRepresentation("Disabled autobalancing!\n");
    } else if ("enable_autobalancing".equalsIgnoreCase(opt)) {
      _helixMirrorMakerManager.enableAutoBalancing();
      LOGGER.info("Enabled autobalancing!");
      return new StringRepresentation("Enabled autobalancing!\n");
    } else if ("autobalancing_status".equalsIgnoreCase(opt)) {
      if (_helixMirrorMakerManager.isAutoBalancingEnabled()) {
        return new StringRepresentation("enabled\n");
      } else {
        return new StringRepresentation("disabled\n");
      }
    }
    LOGGER.info("No valid input!");
    return new StringRepresentation("No valid input!\n");
  }

}
