package com.uber.stream.kafka.mirrormaker.manager.utils;

import com.uber.stream.kafka.mirrormaker.common.core.OnlineOfflineStateModel;
import com.uber.stream.kafka.mirrormaker.manager.core.IdealStateBuilder;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.junit.Assert;
import org.junit.Test;

public class IdealStateBuilderTest {

  //  public static IdealState resetCustomIdealStateFor(IdealState oldIdealState,
  //      String topicName, String oldPartition, String newPartition, String newInstanceName)
  @Test
  public void resetCustomIdealStateFor() {
    String topicName = "testTopic";
    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

    customModeIdealStateBuilder
        .setStateModel(OnlineOfflineStateModel.name)
        .setNumPartitions(2).setNumReplica(1)
        .setMaxPartitionsPerNode(2);
    customModeIdealStateBuilder.assignInstanceAndState("@phx2f@dca1-agg1@2", "2", "ONLINE");
    customModeIdealStateBuilder.assignInstanceAndState("@dca1f@dca1-agg1@2", "1", "ONLINE");

    IdealState oldIdealState  = customModeIdealStateBuilder.build();
    IdealState newIdealState  = IdealStateBuilder.resetCustomIdealStateFor(oldIdealState, topicName, "@dca1f@dca1-agg1@2", "@dca1f@dca1-agg1@13", "3");
    Assert.assertEquals(2, newIdealState.getNumPartitions());
    Assert.assertTrue(newIdealState.getPartitionSet().contains("@dca1f@dca1-agg1@13"));
    String instanceName = newIdealState.getInstanceStateMap("@dca1f@dca1-agg1@13").keySet().iterator().next();
    Assert.assertEquals("3", instanceName);

    Assert.assertTrue(newIdealState.getPartitionSet().contains("@phx2f@dca1-agg1@2"));
    instanceName = newIdealState.getInstanceStateMap("@phx2f@dca1-agg1@2").keySet().iterator().next();
    Assert.assertEquals("2", instanceName);
  }

}
