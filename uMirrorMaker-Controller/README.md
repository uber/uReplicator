Helix Mirror Maker Controller
================

This is the New Helix based Mirror Maker controller.
Helix is an open sourced cluster management framework for partitioned and
replicated distributed resources. Applying to our use case, this could help
manage topic and partition to instance mapping, which will get rid of the
rebalancing issue from the old mirror maker.

Mirror maker worker will only focus on single machine to take a list of
topic-partition to simple consumers to consume data from kafka broker. 

Quickstart
----------
1. Start Local zookeeper with port 2181

2. Start ControllerStarter.java with default.

3. Start multiple FakeInstances with given instanceId

4. Now trying to play around with Helix mirror maker controller.

    # Adding a topic into mirror maker
    $ curl  -X POST -d '{"topic":"testTopic", "numPartitions":"8"}' http://localhost:9000/topics
    	Successfully add new topic: {topic: testTopic, numPartitions: 8}

    # Expanding an existed topic into mirror maker
    $ curl  -X PUT -d '{"topic":"testTopic", "numPartitions":"16"}' http://localhost:9000/topics
    	Successfully expand topic: {topic: testTopic, numPartitions: 16}

    # Get a external view of topic from mirror maker
    $ curl  -X GET  http://localhost:9000/topics/testTopic
    	{"partitionToServerMapping":
    		{"0":["testHelixMirrorMaker01"],"1":["testHelixMirrorMaker02"],"10":["testHelixMirrorMaker04"],
    		 "11":["testHelixMirrorMaker01"],"12":["testHelixMirrorMaker02"],"13":["testHelixMirrorMaker03"],
    		 "14":["testHelixMirrorMaker04"],"15":["testHelixMirrorMaker01"],"2":["testHelixMirrorMaker03"],
    		 "3":["testHelixMirrorMaker04"],"4":["testHelixMirrorMaker01"],"5":["testHelixMirrorMaker02"],
    		 "6":["testHelixMirrorMaker03"],"7":["testHelixMirrorMaker04"],"8":["testHelixMirrorMaker02"],
    		 "9":["testHelixMirrorMaker03"]},
         "serverToNumPartitionsMapping":
         	{"testHelixMirrorMaker01":4,"testHelixMirrorMaker02":4,
         	 "testHelixMirrorMaker03":4,"testHelixMirrorMaker04":4},
         "serverToPartitionMapping":{
         	"testHelixMirrorMaker01":["0","11","15","4"],
         	"testHelixMirrorMaker02":["1","12","5","8"],
         	"testHelixMirrorMaker03":["13","2","6","9"],
         	"testHelixMirrorMaker04":["10","14","3","7"]},
         "topic":"testTopic"}

    # Delete a topic from mirror maker
    $ curl  -X DELETE  http://localhost:9000/topics/testTopic
    	Successfully finished delete topic: testTopic



License
-------

The project is licensed under the Apache 2 license.
