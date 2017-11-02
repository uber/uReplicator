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

import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import com.uber.stream.kafka.mirrormaker.common.core.OnlineOfflineStateModel;
import com.uber.stream.kafka.mirrormaker.common.core.TopicPartition;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle idealStates changes for new topic added and expanded.
 */
public class IdealStateBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(IdealStateBuilder.class);

  public static IdealState buildCustomIdealStateFor(String topicName,
      String partition,
      InstanceTopicPartitionHolder instance) {
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(1).setNumReplica(1)
        .setMaxPartitionsPerNode(1);

    if (instance != null) {
      customModeIdealStateBuilder.assignInstanceAndState(partition, instance.getInstanceName(), "ONLINE");
    }

    return customModeIdealStateBuilder.build();
  }

  public static IdealState buildCustomIdealStateFor(String topicName,
      String partition,
      PriorityQueue<InstanceTopicPartitionHolder> instanceToNumServingTopicPartitionMap) {

    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(1).setNumReplica(5)
        .setMaxPartitionsPerNode(1);

    int i = 0;
    for (InstanceTopicPartitionHolder instance : instanceToNumServingTopicPartitionMap) {
      if (instance != null) {
        customModeIdealStateBuilder.assignInstanceAndState(partition, instance.getInstanceName(), "ONLINE");
      }
      if (++i == 5) {
        break;
      }
    }
    return customModeIdealStateBuilder.build();
  }

}
