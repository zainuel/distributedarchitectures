package org.dist.simplekafkaimpl

import org.dist.queue.utils.ZkUtils.Broker

class NewController(zooKeeperClient: ZooKeeperClient) {

  var liveBrokers: Set[Broker] = Set()

  def addBroker(broker: Broker) = {
    liveBrokers += broker
  }

  def init() = {
    zooKeeperClient.subscribeBrokerChangeListener(new NewBrokerChangeListener(this, zooKeeperClient))
  }

}
