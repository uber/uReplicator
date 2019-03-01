Worker start command example:

```
java -Dlog4j.configuration=file:config/tools-log4j.properties -XX:MaxGCPauseMillis=100 -XX:InitiatingHeapOccupancyPercent=45 -verbose:gc -Xmx5g -Xms5g -XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -server -javaagent:./bin/libs/jmxtrans-agent-1.2.4.jar=config/jmxtrans.xml -cp uReplicator-Worker/target/uReplicator-Worker-1.0.0-SNAPSHOT-jar-with-dependencies.jar kafka.mirrormaker.MirrorMakerWorker --consumer.config config/consumer.properties --producer.config config/producer.properties --helix.config config/helix.properties --dstzk.config config/dstzk.properties --topic.mappings config/topicmapping.properties
```

Option description:
```
Option                      Description
------                      -----------
--consumer.config           Consumer properties, please see config/consumer.properties
--producer.config           Producer properties, please see config/producer.properties
--helix.config              Helix properties, please see config/helix.properties
--dstzk.config              If you want to enable 1:1 partition mapping, use this property, please see config/dstzk.properties
--topic.mappings            Produce to a different topic in dst cluster, please see config/topicmapping.properties
```
