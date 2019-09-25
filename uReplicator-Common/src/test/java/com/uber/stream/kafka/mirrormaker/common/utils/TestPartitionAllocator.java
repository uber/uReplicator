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
package com.uber.stream.kafka.mirrormaker.common.utils;

import com.uber.stream.kafka.mirrormaker.common.core.InstanceTopicPartitionHolder;
import org.junit.Test;
import org.testng.Assert;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: Tboy
 */
public class TestPartitionAllocator {

    @Test
    public void testAllocate() {
        InstanceTopicPartitionHolder currentInstance1 = new InstanceTopicPartitionHolder("instance1");
        InstanceTopicPartitionHolder currentInstance2 = new InstanceTopicPartitionHolder("instance2");
        InstanceTopicPartitionHolder currentInstance3 = new InstanceTopicPartitionHolder("instance3");
        List<Integer> partitionAll = Arrays.asList(new Integer[]{0,1,2,3,4,5,6,7});
        List<InstanceTopicPartitionHolder> instanceAll = Arrays.asList(currentInstance1, currentInstance2, currentInstance3);

        List<Integer> partitionList1 = PartitionAllocator.allocate(currentInstance1, partitionAll, instanceAll);
        List<Integer> partitionList2 = PartitionAllocator.allocate(currentInstance2, partitionAll, instanceAll);
        List<Integer> partitionList3 = PartitionAllocator.allocate(currentInstance3, partitionAll, instanceAll);
        Assert.assertEquals(partitionList1, Arrays.asList(0,1,2));
        Assert.assertEquals(partitionList2, Arrays.asList(3,4,5));
        Assert.assertEquals(partitionList3, Arrays.asList(6, 7));
        Assert.assertEquals(PartitionAllocator.partitionToString(partitionList1), "0,1,2");
        Assert.assertEquals(PartitionAllocator.partitionToString(partitionList2), "3,4,5");
        Assert.assertEquals(PartitionAllocator.partitionToString(partitionList3), "6,7");
    }
}
