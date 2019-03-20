/*
 * Copyright (C) 2015-2017 Uber Technologies, Inc. (streaming-data@uber.com)
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
package com.uber.stream.kafka.mirrormaker.controller.rest.resources;

import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import com.uber.stream.kafka.mirrormaker.common.core.TopicWorkload;
import com.uber.stream.kafka.mirrormaker.common.modules.ControllerWorkloadInfo;
import com.uber.stream.kafka.mirrormaker.common.modules.WorkerWorkloadInfo;
import com.uber.stream.kafka.mirrormaker.common.modules.TopicPartitionLag;
import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.PriorityQueue;
import java.util.Set;

/**
 * AdminRestletResource is used to control auto balancing enable/disalbe.
 *
 * @author xiangfu
 */
public class AdminRestletResource extends ServerResource {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AdminRestletResource.class);

  private final HelixMirrorMakerManager _helixMirrorMakerManager;

  public AdminRestletResource() {
    _helixMirrorMakerManager = (HelixMirrorMakerManager) getApplication().getContext()
        .getAttributes().get(HelixMirrorMakerManager.class.toString());
  }

  @Post
  public Representation post() {
    final String opt = (String) getRequest().getAttributes().get("opt");
    JSONObject responseJson = new JSONObject();
    if ("disable_autobalancing".equalsIgnoreCase(opt)) {
      _helixMirrorMakerManager.disableAutoBalancing();
      LOGGER.info("Disabled autobalancing!");
      responseJson.put("opt", "disable_autobalancing");
      responseJson.put("auto_balancing", _helixMirrorMakerManager.isAutoBalancingEnabled());
    } else if ("enable_autobalancing".equalsIgnoreCase(opt)) {
      _helixMirrorMakerManager.enableAutoBalancing();
      LOGGER.info("Enabled autobalancing!");
      responseJson.put("opt", "enable_autobalancing");
      responseJson.put("auto_balancing", _helixMirrorMakerManager.isAutoBalancingEnabled());
    } else {
      LOGGER.info("No valid input!");
      responseJson.put("opt", "No valid input!");
    }
    return new StringRepresentation(responseJson.toJSONString());
  }

  @Override
  @Get
  public Representation get() {
    JSONObject responseJson = new JSONObject();
    final String opt = (String) getRequest().getAttributes().get("opt");
    if ("autobalancing_status".equalsIgnoreCase(opt)) {
      responseJson.put("auto_balancing", _helixMirrorMakerManager.isAutoBalancingEnabled());
    } else if ("workloadinfo".equalsIgnoreCase(opt)) {
      return new StringRepresentation(JSONObject.toJSONString(calculateWorkloadInfo()));
    } else {
      LOGGER.info("No valid input!");
      responseJson.put("opt", "No valid input!");
    }
    return new StringRepresentation(responseJson.toJSONString());
  }


  private ControllerWorkloadInfo calculateWorkloadInfo() {
    ControllerWorkloadInfo controllerWorkloadInfo = new ControllerWorkloadInfo();
    PriorityQueue<InstanceTopicPartitionHolder> itphList = _helixMirrorMakerManager.getCurrentServingInstance();
    int numberOfLaggingWorkers = 0;
    boolean workloadRetrieverInitialized = _helixMirrorMakerManager.getWorkloadInfoRetriever().isInitialized();
    while (!itphList.isEmpty()) {
      InstanceTopicPartitionHolder itph = itphList.poll();
      WorkerWorkloadInfo workerWorkload = new WorkerWorkloadInfo();
      Set<TopicPartition> topicPartitionSet = itph.getServingTopicPartitionSet();
      workerWorkload.setWorkerId(itph.getInstanceName());
      workerWorkload.setNumberOfTopicPartitions(topicPartitionSet.size());
      for (TopicPartition topicPartition : topicPartitionSet) {
        if (workloadRetrieverInitialized) {
          TopicWorkload topicWorkload = _helixMirrorMakerManager.getWorkloadInfoRetriever().topicWorkload(topicPartition.getTopic());
          TopicWorkload partitionWorkLoad = new TopicWorkload(topicWorkload.getBytesPerSecondPerPartition(), topicWorkload.getMsgsPerSecondPerPartition(), 1);
          workerWorkload.getTotalWorkload().add(partitionWorkLoad);
        } else {
          workerWorkload.setTotalWorkload(null);
        }

        TopicPartitionLag tpl = _helixMirrorMakerManager.calculateLagTime(topicPartition);
        if (tpl != null) {
          workerWorkload.getLaggingTopicPartition().add(tpl);
        }
      }
      if (workerWorkload.getTotalWorkload() != null) {
        controllerWorkloadInfo.getTopicWorkload().add(workerWorkload.getTotalWorkload());
        controllerWorkloadInfo.getWorkerInstances().add(workerWorkload);
      }
      if (workerWorkload.getLaggingTopicPartition().size() != 0) {
        numberOfLaggingWorkers++;
      }
    }
    controllerWorkloadInfo.setNumOfLaggingWorkers(numberOfLaggingWorkers);
    int numberOfExpectedWorkers = (int) Math.round(controllerWorkloadInfo.getTopicWorkload().getBytesPerSecond() / _helixMirrorMakerManager.getMaxWorkloadPerWorkerBytes());
    int numOfDedicatedWorkers = (int) (controllerWorkloadInfo.getWorkerInstances().size() * _helixMirrorMakerManager.getMaxDedicatedInstancesRatio());
    int laggingAdditional = numberOfLaggingWorkers < numOfDedicatedWorkers ? 0 : numberOfLaggingWorkers - numOfDedicatedWorkers;
    controllerWorkloadInfo.setNumOfExpectedWorkers(numberOfExpectedWorkers + laggingAdditional);
    return controllerWorkloadInfo;
  }
}
