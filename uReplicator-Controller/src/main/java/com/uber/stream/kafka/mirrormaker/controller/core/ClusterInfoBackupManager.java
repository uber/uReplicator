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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.uber.stream.kafka.mirrormaker.controller.ControllerConf;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.restlet.representation.StringRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This manager schedules a periodic backup task once every 24 hrs to take the
 * backup of the mirror maker controller cluster state and dump the ideal state, parition assignment
 * to two different files either to a remote git repo or a local backup file based on the config
 *
 * @author naveencherukuri
 */
public class ClusterInfoBackupManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterInfoBackupManager.class);
  private static final int STOP_TIMEOUT_SEC = 5;

  private final HelixMirrorMakerManager _helixMirrorMakerManager;
  private final ScheduledExecutorService _executorService = Executors.newSingleThreadScheduledExecutor();

  private int _timeValue = 24 * 60 * 60;
  private TimeUnit _timeUnit = TimeUnit.SECONDS;

  private final BackUpHandler _handler;
  private final ControllerConf _config;

  public ClusterInfoBackupManager(HelixMirrorMakerManager helixMirrorMakerManager,
      BackUpHandler handler,
      ControllerConf config) {
    _helixMirrorMakerManager = helixMirrorMakerManager;
    _handler = handler;
    _config = config;
  }

  public void start() {
    LOGGER.info("Trying to schedule cluster backup job at rate {} {} !", _timeValue, _timeUnit.toString());
    _executorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          dumpState();
        } catch (Exception e) {
          LOGGER.error("Failed to take backup with exception", e);
          return;
        }
        LOGGER.info("Backup taken successfully!");
      }
    }, 20, _timeValue, _timeUnit);
  }

  public void stop() {
    _executorService.shutdown();
    try {
      _executorService.awaitTermination(STOP_TIMEOUT_SEC, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.info("Stop ClusterInfoBackupManager got interrupted");
    }
    _executorService.shutdownNow();
  }

  public synchronized void dumpState() throws Exception {
    if (!_helixMirrorMakerManager.isLeader()) {
      return;
    }

    LOGGER.info("Backing up the CurrentState and the IdealState!");
    StringBuilder idealState = new StringBuilder();
    StringBuilder partitionAssignment = new StringBuilder();
    List<String> topicLists = _helixMirrorMakerManager.getTopicLists();
    if (topicLists == null || topicLists.isEmpty()) {
      LOGGER.info("No topics available to take backup");
      return;
    }

    JSONArray resultList = new JSONArray();

    for (String topicName : topicLists) {
      IdealState idealStateForTopic = _helixMirrorMakerManager.getIdealStateForTopic(topicName);
      JSONObject resultJson = new JSONObject();
      resultJson.put("topic", topicName);
      resultJson.put("idealStateMeta", idealStateForTopic);
      resultList.add(resultJson);
    }

    idealState.append(new StringRepresentation(resultList.toJSONString()));

    resultList = new JSONArray();

    for (String topicName : topicLists) {
      IdealState idealStateForTopic = _helixMirrorMakerManager.getIdealStateForTopic(topicName);
      ExternalView externalViewForTopic = _helixMirrorMakerManager.getExternalViewForTopic(topicName);
      JSONObject responseJson = TopicAssignmentViewBuilder.build(
          topicName, idealStateForTopic, externalViewForTopic);
      resultList.add(responseJson);
    }

    partitionAssignment.append(new StringRepresentation(resultList.toJSONString()));

    String envInfo = "default";
    if (_config.getEnvironment() != null && _config.getEnvironment().trim().length() > 0) {
      envInfo = _config.getEnvironment().trim();
    }
    if (_config.getHelixClusterName() != null && !_config.getHelixClusterName().trim().isEmpty()) {
      envInfo = envInfo + "-" + _config.getHelixClusterName().trim();
    }
    String idealStateFileName = "idealState-backup-" + envInfo;
    String paritionAssignmentFileName = "partitionAssgn-backup-" + envInfo;
    _handler.writeToFile(idealStateFileName, idealState.toString());
    _handler.writeToFile(paritionAssignmentFileName, partitionAssignment.toString());
  }

}
