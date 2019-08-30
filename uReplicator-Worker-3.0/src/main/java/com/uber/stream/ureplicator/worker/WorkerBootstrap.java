package com.uber.stream.ureplicator.worker;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Tboy
 */
public class WorkerBootstrap {

    public static void main(String[] args) throws Exception{

        for(String arg : args){
            System.out.println(arg);
        }

        List<String> argList = new ArrayList<>();
        argList.add("-federated_enabled");
        argList.add("true");
        argList.add("-cluster_config");
        argList.add("config/clusters.properties");
        argList.add("-consumer_config");
        argList.add("config/consumer.properties");
        argList.add("-producer_config");
        argList.add("config/producer.properties");
        argList.add("-helix_config");
        argList.add("config/helix.properties");
//        argList.add("-dstzk.config");
//        argList.add("-config/dstzk.propertiess");
        argList.add("-topic_mappings");
        argList.add("config/topicmapping.properties");
        args = argList.toArray(new String[]{});

        try {
            WorkerStarter.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
