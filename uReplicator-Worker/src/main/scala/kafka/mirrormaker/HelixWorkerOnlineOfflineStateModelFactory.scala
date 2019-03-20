/**
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
package kafka.mirrormaker

import kafka.utils.Logging
import org.apache.helix.NotificationContext
import org.apache.helix.model.Message
import org.apache.helix.participant.statemachine.{StateModel, StateModelFactory}

/**
 * Helix State model for the Mirror Maker topic partition addition and deletion request.
 *
 * @param instanceId
 * @param connector
 */
class HelixWorkerOnlineOfflineStateModelFactory(final val instanceId: String,
                                                final val connector: KafkaConnector,
                                                final val topicPartitionCountObserver: TopicPartitionCountObserver)
  extends StateModelFactory[StateModel] with Logging {
  override def createNewStateModel(partitionName: String) = new OnlineOfflineStateModel(instanceId, connector)

  // register mm instance
  class OnlineOfflineStateModel(final val instanceId: String, final val connectors: KafkaConnector) extends StateModel {

    def onBecomeOnlineFromOffline(message: Message, context: NotificationContext) = {
      info("OnlineOfflineStateModel.onBecomeOnlineFromOffline for topic: "
        + message.getResourceName() + ", partition: " + message.getPartitionName()
        + " to instance: " + instanceId)
      // add topic partition on the instance
      connectors.addTopicPartition(message.getResourceName, message.getPartitionName.toInt)
      if (topicPartitionCountObserver != null) {
        topicPartitionCountObserver.addTopic(message.getResourceName)
      }
      debug("Finish OnlineOfflineStateModel.onBecomeOnlineFromOffline for topic: "
        + message.getResourceName() + ", partition: " + message.getPartitionName()
        + " to instance: " + instanceId)
    }

    def onBecomeOfflineFromOnline(message: Message, context: NotificationContext) = {
      info("OnlineOfflineStateModel.onBecomeOfflineFromOnline for topic: "
        + message.getResourceName() + ", partition: " + message.getPartitionName()
        + " to instance: " + instanceId)
      // delete topic partition on the instance
      connectors.deleteTopicPartition(message.getResourceName, message.getPartitionName.toInt, false)
      debug("Finish OnlineOfflineStateModel.onBecomeOfflineFromOnline for topic: "
        + message.getResourceName() + ", partition: " + message.getPartitionName()
        + " to instance: " + instanceId)
    }

    def onBecomeDroppedFromOffline(message: Message, context: NotificationContext) = {
      info("OnlineOfflineStateModel.onBecomeDroppedFromOffline for topic: "
        + message.getResourceName() + ", partition: " + message.getPartitionName()
        + " to instance: " + instanceId)
      // delete topic partition on the instance
      connectors.deleteTopicPartition(message.getResourceName, message.getPartitionName.toInt, true)
      debug("Finish OnlineOfflineStateModel.onBecomeDroppedFromOffline for topic: "
        + message.getResourceName() + ", partition: " + message.getPartitionName()
        + " to instance: " + instanceId)
    }
  }

}
