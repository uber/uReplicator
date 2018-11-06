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
