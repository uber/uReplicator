package com.uber.stream.ureplicator.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class OffsetsDeltaManager extends TimerTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetsDeltaManager.class);
    private static final OffsetsDeltaManager instance = new OffsetsDeltaManager();
    private final Map<String, DeltaItem> offsetDeltas = new ConcurrentHashMap<>();
    private Jedis jedis;
    private long startTime = 0;

    private OffsetsDeltaManager() {
    }

    public static OffsetsDeltaManager getInstance() {
        return instance;
    }

//    public static void main(String[] args) {
//        OffsetsDeltaManager manager = OffsetsDeltaManager.getInstance();
//        manager.addDelta("topic", 2, 38, System.currentTimeMillis());
//        manager.addDelta("topic", 3, 3, System.currentTimeMillis());
//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        manager.addDelta("topic", 2, 39, System.currentTimeMillis());
//        try {
//            Thread.sleep(30000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }

    public void schedule(long delay, long period) {
        final Timer timer = new Timer(true);
        timer.schedule(this, delay, period);
    }

    public void setupRedis(String host) {
        jedis = new Jedis(host);
    }

    @Override
    public void run() {
        try {
            final long currentTime = System.currentTimeMillis();
            Map<String, String> deltas = getDeltas(currentTime);
            LOGGER.debug(deltas.toString());
            sendDeltas(deltas);
            startTime = currentTime;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public void addDelta(String topic, int partition, long delta, long timestamp) {
        final String key = topic + " " + partition;
        final DeltaItem prevDelta = offsetDeltas.get(key);
        if (prevDelta != null) {
            if (prevDelta.delta != delta)
                prevDelta.set(delta, timestamp);
        } else
            offsetDeltas.put(key, new DeltaItem(delta, timestamp));
    }

    private void sendDeltas(Map<String, String> deltas) {
        if (deltas.isEmpty())
            return;
        Pipeline pipeline = jedis.pipelined();
        for(Map.Entry<String, String> pair: deltas.entrySet())
            pipeline.set(pair.getKey(), pair.getValue());
        pipeline.sync();
    }

    private Map<String, String> getDeltas(long endTime) {
        final Map<String, String> deltas = new HashMap<>();
        for (Map.Entry<String, DeltaItem> entry : offsetDeltas.entrySet()) {
            final DeltaItem deltaItem = entry.getValue();
            if (deltaItem.updated >= startTime && deltaItem.updated < endTime)
                deltas.put(entry.getKey(), String.valueOf(deltaItem.delta));
        }
        return deltas;
    }

    private static class DeltaItem {
        private long delta;
        private long updated;

        public DeltaItem(long delta, long updated) {
            set(delta, updated);
        }

        void set(long delta, long updated) {
            this.delta = delta;
            this.updated = updated;
        }
    }
}
