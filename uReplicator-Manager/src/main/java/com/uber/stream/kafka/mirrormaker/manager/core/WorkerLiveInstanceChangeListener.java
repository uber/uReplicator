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
package com.uber.stream.kafka.mirrormaker.manager.core;

import com.uber.stream.kafka.mirrormaker.manager.ManagerConf;
import java.util.List;
import org.apache.helix.HelixManager;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * We only considering add or remove box(es), not considering the replacing.
 * For replacing, we just need to bring up a new box and give the old instanceId no auto-balancing
 * needed.
 */
public class WorkerLiveInstanceChangeListener implements LiveInstanceChangeListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerLiveInstanceChangeListener.class);

  private final WorkerHelixManager _workerHelixManager;
  private final HelixManager _helixManager;

  public WorkerLiveInstanceChangeListener(WorkerHelixManager workerHelixManager,
      HelixManager helixManager) {
    _workerHelixManager = workerHelixManager;
    _helixManager = helixManager;
  }

  @Override
  public void onLiveInstanceChange(final List<LiveInstance> liveInstances, NotificationContext changeContext) {
    LOGGER.info("WorkerLiveInstanceChangeListener.onLiveInstanceChange() wakes up!");
  }

}
