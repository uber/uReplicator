/**
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
package kafka.mirrormaker

import kafka.utils.Logging
import org.apache.helix.NotificationContext
import org.apache.helix.model.Message
import org.apache.helix.participant.statemachine.{StateModel, StateModelFactory}

/**
 * Manager-Worker Helix State model.
 */
class ManagerWorkerOnlineOfflineStateModelFactory(
  final val managerWorkerHelixHandler: ManagerWorkerHelixHandler) extends StateModelFactory[StateModel] with Logging {

  private final val Separator = "@"

  override def createNewStateModel(partitionName: String) = new OnlineOfflineStateModel(managerWorkerHelixHandler)

  class OnlineOfflineStateModel(final val managerWorkerHelixHandler: ManagerWorkerHelixHandler) extends StateModel {

    def onBecomeOnlineFromOffline(message: Message, context: NotificationContext) = {
      info("OnlineOfflineStateModel.onBecomeOnlineFromOffline for route: "
        + message.getResourceName() + ", partition: " + message.getPartitionName())
      val srcDst = parseSrcDstCluster(message.getResourceName)
      if (srcDst != null) {
        managerWorkerHelixHandler.handleRouteAssignmentOnline(srcDst(1), srcDst(2), message.getPartitionName())
      }
      debug("Finish OnlineOfflineStateModel.onBecomeOnlineFromOffline for route: "
        + message.getResourceName() + ", partition: " + message.getPartitionName())
    }

    def onBecomeOfflineFromOnline(message: Message, context: NotificationContext) = {
      info("OnlineOfflineStateModel.onBecomeOfflineFromOnline for route: "
        + message.getResourceName() + ", partition: " + message.getPartitionName())
      val srcDst = parseSrcDstCluster(message.getResourceName)
      if (srcDst != null) {
        managerWorkerHelixHandler.handleRouteAssignmentOffline(srcDst(1), srcDst(2), message.getPartitionName())
      }
      debug("Finish OnlineOfflineStateModel.onBecomeOfflineFromOnline for route: "
        + message.getResourceName() + ", partition: " + message.getPartitionName())
    }

    def onBecomeDroppedFromOffline(message: Message, context: NotificationContext) = {
      info("OnlineOfflineStateModel.onBecomeDroppedFromOffline for route: "
        + message.getResourceName() + ", partition: " + message.getPartitionName())
      val srcDst = parseSrcDstCluster(message.getResourceName)
      if (srcDst != null) {
        managerWorkerHelixHandler.handleRouteAssignmentOffline(srcDst(1), srcDst(2), message.getPartitionName())
      }
      debug("Finish OnlineOfflineStateModel.onBecomeDroppedFromOffline for route: "
        + message.getResourceName() + ", partition: " + message.getPartitionName())
    }

    def onBecomeOfflineFromError(message: Message, context: NotificationContext) = {
      info("OnlineOfflineStateModel.onBecomeOfflineFromError for route: "
        + message.getResourceName() + ", partition: " + message.getPartitionName())
      val srcDst = parseSrcDstCluster(message.getResourceName)
      if (srcDst != null) {
        // This can happen when onBecomeOfflineFromOnline comes after worker joins new route
        // managerWorkerHelixHandler.handleRouteAssignmentOffline(srcDst(1), srcDst(2), message.getPartitionName())
      }
      debug("Finish OnlineOfflineStateModel.onBecomeOfflineFromError for route: "
        + message.getResourceName() + ", partition: " + message.getPartitionName())
    }

    override def onBecomeDroppedFromError(message: Message, context: NotificationContext) = {
      info("OnlineOfflineStateModel.onBecomeDroppedFromError for route: "
        + message.getResourceName() + ", partition: " + message.getPartitionName())
      val srcDst = parseSrcDstCluster(message.getResourceName)
      if (srcDst != null) {
        // Temporarily keep this to see if it could happen in addition to onBecomeOfflineFromError
        managerWorkerHelixHandler.handleRouteAssignmentOffline(srcDst(1), srcDst(2), message.getPartitionName())
      }
      debug("Finish OnlineOfflineStateModel.onBecomeDroppedFromError for route: "
        + message.getResourceName() + ", partition: " + message.getPartitionName())
    }

    def parseSrcDstCluster(resourceName: String) : Array[String] = {
      if (!resourceName.startsWith(Separator)) {
        return null
      }
      val elems = resourceName.split(Separator)
      if (elems.length != 3) {
        error("Invalid route: " + resourceName)
        return null
      }
      elems
    }
  }

}
