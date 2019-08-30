package kafka.mirrormaker;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Tboy
 */
public class WorkerBootstrap2 {

    public static void main(String[] args) throws Exception{

        for(String arg : args){
            System.out.println(arg);
        }

        List<String> argList = new ArrayList<>();
        argList.add("-cluster.config");
        argList.add("config/clusters.properties");
        argList.add("-consumer.config");
        argList.add("config/consumer2.properties");
        argList.add("-producer.config");
        argList.add("config/producer2.properties");
        argList.add("-helix.config");
        argList.add("config/helix.properties");
//        argList.add("-dstzk.config");
//        argList.add("-config/dstzk.propertiess");
        argList.add("-topic.mappings");
        argList.add("config/topicmapping.properties");
        args = argList.toArray(new String[]{});

        try {
            new MirrorMakerWorker().main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
