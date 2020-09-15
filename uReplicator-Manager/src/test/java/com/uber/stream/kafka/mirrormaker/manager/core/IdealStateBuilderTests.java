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
package com.uber.stream.kafka.mirrormaker.manager.core;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.uber.stream.kafka.mirrormaker.common.core.OnlineOfflineStateModel;
import java.util.HashSet;
import java.util.Set;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.junit.Assert;
import org.testng.annotations.Test;
import java.util.List;

public class IdealStateBuilderTests {

  @Test
  public void testResetCustomIdealStateFor() {

    List<String> instanceToReplace = ImmutableList.of("1", "2", "3", "4");
    Set<String> availableInstanceSet = ImmutableSet.of("11", "12", "13", "14");
    Set<String> partitionNames = new HashSet<>();
    int maxNumReplica = 4;
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder("testTopic");
    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(maxNumReplica).setNumReplica(maxNumReplica)
        .setMaxPartitionsPerNode(instanceToReplace.size());

    for (int i = 0; i < instanceToReplace.size(); i++) {
      partitionNames.add(String.valueOf(i));
      customModeIdealStateBuilder.assignInstanceAndState(String.valueOf(i), instanceToReplace.get(i), "ONLINE");
    }

    List<String> availableInstances = Lists.newArrayList(availableInstanceSet);
    IdealState result = IdealStateBuilder
        .resetCustomIdealStateFor(customModeIdealStateBuilder.build(), "testTopic", instanceToReplace,
            availableInstances, 40);

    Set<String> existingInstances = new HashSet<>();
    for (String partitionName : result.getPartitionSet()) {
      Assert.assertTrue(partitionNames.contains(partitionName));
      for (String instanceName : result.getInstanceStateMap(partitionName).keySet()) {
        Assert.assertFalse(existingInstances.contains(instanceName));
        existingInstances.add(instanceName);
        Assert.assertTrue(availableInstanceSet.contains(instanceName));
      }
    }
    // verify availableInstances has been removed
    Assert.assertTrue(availableInstances.size() == (availableInstanceSet.size() - instanceToReplace.size()));
  }
}
