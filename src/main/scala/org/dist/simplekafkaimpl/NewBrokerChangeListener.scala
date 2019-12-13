package org.dist.simplekafkaimpl

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.common.Logging
import org.dist.simplekafkaimpl.ZooKeeperClient

class NewBrokerChangeListener(testListener: NewController, zookeeperClient:ZooKeeperClient) extends IZkChildListener with Logging {
  import scala.jdk.CollectionConverters._

  override def handleChildChange(parentPath: String, currentBrokerList: util.List[String]): Unit = {
    info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.asScala.mkString(",")))
    try {

      val curBrokerIds = currentBrokerList.asScala.map(_.toInt).toSet
      val newBrokerIds = curBrokerIds -- testListener.liveBrokers.map(broker  => broker.id)
      val newBrokers = newBrokerIds.map(zookeeperClient.getBrokerInfo(_))

      newBrokers.foreach(testListener.addBroker(_))

      /*if (newBrokerIds.size > 0)
        controller.onBrokerStartup(newBrokerIds.toSeq)
*/
    } catch {
      case e: Throwable => error("Error while handling broker changes", e)
    }
  }
}