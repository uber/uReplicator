package com.uber.stream.ureplicator.worker;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.uber.stream.ureplicator.common.KafkaUReplicatorMetricsReporter;
import com.uber.stream.ureplicator.worker.interfaces.ICheckPointManager;
import com.uber.stream.ureplicator.worker.interfaces.IMessageTransformer;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.codehaus.jackson.annotate.JsonTypeInfo.As;
import org.easymock.EasyMock;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class ProducerThreadTests {

  private ProducerThread producerThread;
  private DefaultProducer defaultProducer;
  private ConsumerIterator consumerIterator;
  private ICheckPointManager checkPointManager;
  private WorkerInstance workerInstance;
  private IMessageTransformer messageTransformer;
  private BlockingQueue<FetchedDataChunk> channel;

  @BeforeTest
  public void setup() throws ExecutionException, InterruptedException {
    KafkaUReplicatorMetricsReporter.init(null);
    this.defaultProducer = EasyMock.createMock(DefaultProducer.class);
    this.channel = new LinkedBlockingQueue<>(3);
    this.consumerIterator = new ConsumerIterator(channel, 1);
    this.checkPointManager = EasyMock.createMock(ICheckPointManager.class);
    this.workerInstance = EasyMock.createMock(WorkerInstance.class);
    this.messageTransformer = new DefaultMessageTransformer(null, null, ImmutableMap.of());
    EasyMock.expect(defaultProducer.getMetrics()).andReturn(ImmutableMap.of());
    defaultProducer.send(EasyMock.anyObject(),EasyMock.anyInt(), EasyMock.anyLong());
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(defaultProducer.maybeFlush(EasyMock.anyBoolean())).andReturn(true).anyTimes();

    EasyMock.replay(defaultProducer);
    this.producerThread = new ProducerThread("test", defaultProducer, consumerIterator, messageTransformer,
        checkPointManager, workerInstance);
  }

  @Test
  public void testProducerThread() throws InterruptedException {
    producerThread.pollOnce();
    Assert.assertEquals(producerThread.consumedOffsets, new HashMap<>());

    PartitionOffsetInfo partitionOffsetInfo = new PartitionOffsetInfo(new TopicPartition("topic1", 1), 10L, null);
    List<ConsumerRecord> recordList = ImmutableList.of(
        new ConsumerRecord("topic1", 1, 10L, "test", "test1"),
        new ConsumerRecord("topic1", 1, 11L, "test", "test2")
    );

    FetchedDataChunk dataChunk = new FetchedDataChunk(partitionOffsetInfo, recordList);
    channel.put(dataChunk);
    Assert.assertEquals(channel.remainingCapacity(), 2);
    EasyMock.expect(checkPointManager.commitOffset(producerThread.consumedOffsets)).andReturn(false).times(2);
    EasyMock.replay(checkPointManager);

    // read first message and offset commit failed.
    producerThread.pollOnce();

    Assert.assertEquals(channel.remainingCapacity(), 3);
    Assert.assertEquals(producerThread.consumedOffsets.size(), 1);
    Assert.assertTrue(producerThread.consumedOffsets.containsKey(new TopicPartition("topic1", 1)));
    Long offset = producerThread.consumedOffsets.get(new TopicPartition("topic1", 1));
    Assert.assertEquals(offset,  new Long(11));

    // read second message and offset commit failed.
    producerThread.pollOnce();
    Assert.assertEquals(producerThread.consumedOffsets.size(), 1);
    Assert.assertTrue(producerThread.consumedOffsets.containsKey(new TopicPartition("topic1", 1)));
    offset = producerThread.consumedOffsets.get(new TopicPartition("topic1", 1));
    Assert.assertEquals(offset,  new Long(12));

    EasyMock.reset(checkPointManager);
    EasyMock.expect(checkPointManager.commitOffset(producerThread.consumedOffsets)).andReturn(true);
    EasyMock.replay(checkPointManager);

    // no new messages while retry commit offset
    producerThread.pollOnce();
    Assert.assertEquals(producerThread.consumedOffsets.size(), 0);
  }

}
