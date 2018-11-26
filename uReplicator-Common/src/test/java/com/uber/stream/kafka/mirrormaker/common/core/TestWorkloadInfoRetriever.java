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
package com.uber.stream.kafka.mirrormaker.common.core;

import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

public class TestWorkloadInfoRetriever {
    @Test
    public void testGetTopicWorkload() {
        WorkloadInfoRetriever retriever = new WorkloadInfoRetriever();
        LinkedList<TopicWorkload> workloadLst = new LinkedList<>();
        retriever._topicWorkloadMap.put("trip_created", workloadLst);

        // Case #1 some workload updated within 2 hours
        TopicWorkload wl = new TopicWorkload(10 * 1024, 10);
        long currentTime = System.currentTimeMillis();
        wl.setLastUpdate(currentTime - TimeUnit.HOURS.toMillis(1));
        workloadLst.add(wl);
        TopicWorkload oldWorkload =  new TopicWorkload(20 * 1024, 20);
        oldWorkload.setLastUpdate(currentTime - TimeUnit.HOURS.toMillis(9));
        workloadLst.add(oldWorkload);

        Assert.assertEquals(retriever.topicWorkload("trip_created").compareTo(wl), 0);

        workloadLst.clear();
        workloadLst.add(oldWorkload);
        Assert.assertEquals(retriever.topicWorkload("trip_created").compareTo(oldWorkload), 0);

    }
}
