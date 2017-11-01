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
package com.uber.stream.kafka.mirrormaker.controller;

import com.uber.stream.kafka.mirrormaker.controller.core.AutoTopicWhitelistingManager;
import com.uber.stream.kafka.mirrormaker.controller.core.ClusterInfoBackupManager;
import com.uber.stream.kafka.mirrormaker.controller.core.FileBackUpHandler;
import com.uber.stream.kafka.mirrormaker.controller.core.GitBackUpHandler;
import com.uber.stream.kafka.mirrormaker.controller.core.HelixMirrorMakerManager;
import com.uber.stream.kafka.mirrormaker.controller.core.KafkaBrokerTopicObserver;
import com.uber.stream.kafka.mirrormaker.controller.core.ManagerControllerHelix;
import com.uber.stream.kafka.mirrormaker.controller.rest.ControllerRestApplication;
import com.uber.stream.kafka.mirrormaker.controller.validation.SourceKafkaClusterValidationManager;
import com.uber.stream.kafka.mirrormaker.controller.validation.ValidationManager;
import java.util.HashMap;
import java.util.Map;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControllerInstance {

  private static final String DEST_KAFKA_CLUSTER = "destKafkaCluster";
  private static final String SRC_KAFKA_CLUSTER = "srcKafkaCluster";
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerStarter.class);
  private final ControllerConf _config;

  private final Component _component;
  private final Application _controllerRestApp;
  private final ManagerControllerHelix _managerControllerHelix;
  private final HelixMirrorMakerManager _helixMirrorMakerManager;
  private final ValidationManager _validationManager;
  private final SourceKafkaClusterValidationManager _srcKafkaValidationManager;
  private final AutoTopicWhitelistingManager _autoTopicWhitelistingManager;
  private final ClusterInfoBackupManager _clusterInfoBackupManager;
  private final Map<String, KafkaBrokerTopicObserver> _kafkaBrokerTopicObserverMap = new HashMap<>();


  public ControllerInstance(ControllerConf conf) {
    this(null, conf);
  }

  public ControllerInstance(ManagerControllerHelix managerControllerHelix, ControllerConf conf) {
    LOGGER.info("Trying to init ControllerStarter with config: {}", conf);
    _managerControllerHelix = managerControllerHelix;
    _config = conf;
    _component = new Component();
    _controllerRestApp = new ControllerRestApplication(null);
    _helixMirrorMakerManager = new HelixMirrorMakerManager(_config);
    _validationManager = new ValidationManager(_helixMirrorMakerManager);
    _srcKafkaValidationManager = getSourceKafkaClusterValidationManager();
    _autoTopicWhitelistingManager = getAutoTopicWhitelistingManager();
    if (_config.getBackUpToGit()) {
      _clusterInfoBackupManager = new ClusterInfoBackupManager(_helixMirrorMakerManager,
          new GitBackUpHandler(conf.getRemoteBackupRepo(), conf.getLocalGitRepoPath()), _config);
    } else {
      _clusterInfoBackupManager = new ClusterInfoBackupManager(_helixMirrorMakerManager,
          new FileBackUpHandler(conf.getLocalBackupFilePath()), _config);
    }
  }

  private SourceKafkaClusterValidationManager getSourceKafkaClusterValidationManager() {
    if (_config.getEnableSrcKafkaValidation()) {
      if (!_kafkaBrokerTopicObserverMap.containsKey(SRC_KAFKA_CLUSTER)) {
        _kafkaBrokerTopicObserverMap.put(SRC_KAFKA_CLUSTER,
            new KafkaBrokerTopicObserver(SRC_KAFKA_CLUSTER, _config.getSrcKafkaZkPath()));
      }
      return new SourceKafkaClusterValidationManager(
          _kafkaBrokerTopicObserverMap.get(SRC_KAFKA_CLUSTER),
          _helixMirrorMakerManager, _config.getEnableAutoTopicExpansion());
    } else {
      LOGGER.info("Not init SourceKafkaClusterValidationManager!");
      return null;
    }
  }

  public KafkaBrokerTopicObserver getSourceKafkaTopicObserver() {
    return _kafkaBrokerTopicObserverMap.get(SRC_KAFKA_CLUSTER);
  }

  private AutoTopicWhitelistingManager getAutoTopicWhitelistingManager() {
    if (_config.getEnableAutoWhitelist()) {
      if (!_kafkaBrokerTopicObserverMap.containsKey(SRC_KAFKA_CLUSTER)) {
        _kafkaBrokerTopicObserverMap.put(SRC_KAFKA_CLUSTER,
            new KafkaBrokerTopicObserver(SRC_KAFKA_CLUSTER, _config.getSrcKafkaZkPath()));
      }
      if (!_kafkaBrokerTopicObserverMap.containsKey(DEST_KAFKA_CLUSTER)) {
        _kafkaBrokerTopicObserverMap.put(DEST_KAFKA_CLUSTER,
            new KafkaBrokerTopicObserver(DEST_KAFKA_CLUSTER, _config.getDestKafkaZkPath()));
      }

      String patternToExcludeTopics = _config.getPatternToExcludeTopics();
      if (patternToExcludeTopics != null && patternToExcludeTopics.trim().length() > 0) {
        patternToExcludeTopics = patternToExcludeTopics.trim();
      } else {
        patternToExcludeTopics = "";
      }
      LOGGER.info("Pattern to exclude topics is " + patternToExcludeTopics);

      return new AutoTopicWhitelistingManager(_kafkaBrokerTopicObserverMap.get(SRC_KAFKA_CLUSTER),
          _kafkaBrokerTopicObserverMap.get(DEST_KAFKA_CLUSTER), _helixMirrorMakerManager,
          patternToExcludeTopics, _config.getWhitelistRefreshTimeInSeconds(), _config.getInitWaitTimeInSeconds());
    } else {
      LOGGER.info("Not init AutoTopicWhitelistingManager!");
      return null;
    }
  }

  public HelixMirrorMakerManager getHelixResourceManager() {
    return _helixMirrorMakerManager;
  }

  public void start() throws Exception {
    _component.getServers().add(Protocol.HTTP, Integer.parseInt(_config.getControllerPort()));
    _component.getClients().add(Protocol.FILE);
    _component.getClients().add(Protocol.JAR);
    _component.getClients().add(Protocol.WAR);

    final Context applicationContext = _component.getContext().createChildContext();

    LOGGER.info("injecting conf and resource manager to the api context");
    applicationContext.getAttributes().put(ControllerConf.class.toString(), _config);
    applicationContext.getAttributes().put(HelixMirrorMakerManager.class.toString(),
        _helixMirrorMakerManager);
    applicationContext.getAttributes().put(ValidationManager.class.toString(), _validationManager);

    if (_managerControllerHelix != null) {
      applicationContext.getAttributes().put(ManagerControllerHelix.class.toString(),
          _managerControllerHelix);
    }

    if (_srcKafkaValidationManager != null) {
      applicationContext.getAttributes().put(SourceKafkaClusterValidationManager.class.toString(),
          _srcKafkaValidationManager);

      applicationContext.getAttributes().put(KafkaBrokerTopicObserver.class.toString(),
          _kafkaBrokerTopicObserverMap.get(SRC_KAFKA_CLUSTER));
    }
    if (_autoTopicWhitelistingManager != null) {
      applicationContext.getAttributes().put(AutoTopicWhitelistingManager.class.toString(),
          _autoTopicWhitelistingManager);
    }

    _controllerRestApp.setContext(applicationContext);

    _component.getDefaultHost().attach(_controllerRestApp);

    try {
      LOGGER.info("starting helix mirror maker manager");
      _helixMirrorMakerManager.start();
      _validationManager.start();
      if (_autoTopicWhitelistingManager != null) {
        _autoTopicWhitelistingManager.start();
      }
      if (_srcKafkaValidationManager != null) {
        _srcKafkaValidationManager.start();
      }
      if (_clusterInfoBackupManager != null) {
        _clusterInfoBackupManager.start();
      }
      LOGGER.info("starting api component");
      _component.start();
    } catch (final Exception e) {
      LOGGER.error("Caught exception while starting controller", e);
      throw e;
    }
  }

  public void stop() throws Exception {
    LOGGER.info("stopping broker topic observers");
    for (String key : _kafkaBrokerTopicObserverMap.keySet()) {
      try {
        KafkaBrokerTopicObserver observer = _kafkaBrokerTopicObserverMap.get(key);
        observer.stop();
      } catch (Exception e) {
        LOGGER.error("Failed to stop KafkaBrokerTopicObserver: {}!", key);
      }
    }
    LOGGER.info("stopping api component");
    _component.stop();

    if (_clusterInfoBackupManager != null) {
      LOGGER.info("stopping cluster info backup manager");
      _clusterInfoBackupManager.stop();
    }

    if (_srcKafkaValidationManager != null) {
      LOGGER.info("stopping source Kafka validation manager");
      _srcKafkaValidationManager.stop();
    }

    if (_autoTopicWhitelistingManager != null) {
      LOGGER.info("stopping auto topic whitelisting manager");
      _autoTopicWhitelistingManager.stop();
    }

    if (_validationManager != null) {
      LOGGER.info("stopping validation manager");
      _validationManager.stop();
    }

    LOGGER.info("stopping resource manager");
    _helixMirrorMakerManager.stop();
  }
}
