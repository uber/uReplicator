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

import joptsimple.OptionSet
import kafka.utils.Logging

/**
 * This class handles the online-offline events for workers in Manager-Worker Helix cluster.
 *
 * @param consumerIdString
 * @param config
 */
class ManagerWorkerHelixHandler(private val workerConfig: MirrorMakerWorkerConf, private val options: OptionSet) extends Logging {
  private val HexliClusterPrefix = "controller-worker-"

  private var currentWorkerInstance: WorkerInstance = null
  private var currentSrcCluster: String = ""
  private var currentDstCluster: String = ""
  private var currentRouteId: String = ""

  def handleRouteAssignmentOnline(srcCluster: String, dstCluster: String, routeId: String) {
    info("ManagerWorkerHelixHandler.handleRouteAssignmentOnline: srcCluster=%s, dstCluster=%s, routeId=%s".format(srcCluster, dstCluster, routeId))
    this.synchronized {
      if (currentWorkerInstance != null) {
        if (!(srcCluster.equals(currentSrcCluster) && dstCluster.equals(currentDstCluster) && routeId.equals(currentRouteId))) {
          error("The worker instance has already started but with different route assignment, current srcCluster=%s, dstCluster=%s, routeId=%s"
              .format(currentSrcCluster, currentDstCluster, currentRouteId))
        } else {
          info("The worker instance has already started but with the same route assignment")
        }
      } else {
        val helixClusterName = HexliClusterPrefix + srcCluster + "-" + dstCluster + "-" + routeId
        currentWorkerInstance = new WorkerInstance(workerConfig, options, srcCluster, dstCluster, helixClusterName)
        currentSrcCluster = srcCluster
        currentDstCluster = dstCluster
        currentRouteId = routeId
        currentWorkerInstance.start()
      }
    }
  }

  def handleRouteAssignmentOffline(srcCluster: String, dstCluster: String, routeId: String) {
    info("ManagerWorkerHelixHandler.handleRouteAssignmentOffline: srcCluster=%s, dstCluster=%s, routeId=%s".format(srcCluster, dstCluster, routeId))
    this.synchronized {
      if (currentWorkerInstance != null) {
        if (!(srcCluster.equals(currentSrcCluster) && dstCluster.equals(currentDstCluster) && routeId.equals(currentRouteId))) {
          error("The worker instance has already started but with different route assignment, current srcCluster=%s, dstCluster=%s, routeId=%s"
              .format(currentSrcCluster, currentDstCluster, currentRouteId))
        } else {
          currentWorkerInstance.cleanShutdown()
          currentWorkerInstance = null
          currentSrcCluster = null
          currentDstCluster = null
          currentRouteId = null
        }
      } else {
        error("The worker instance has not started yet")
      }
    }
  }

  def stop() {
    this.synchronized {
      if (currentWorkerInstance != null) {
        currentWorkerInstance.cleanShutdown()
        currentWorkerInstance = null
      }
    }
  }
}
