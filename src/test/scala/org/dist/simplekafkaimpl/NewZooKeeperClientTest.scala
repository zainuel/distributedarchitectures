package org.dist.simplekafkaimpl

import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.BrokerChangeListener

class ZooKeeperClientTest extends ZookeeperTestHarness {

  test("it should register a Broker") {
    val zooKeeperClient = new ZooKeeperClient(zkClient);
    val broker = new Broker(1, "127.0.0.1", 22000);
    val createResponse = zooKeeperClient.registerBroker(broker);

  }

  test("it should maintain live brokers") {
    val zooKeeperClient = new ZooKeeperClient(zkClient);
    val testListener = new NewController(zooKeeperClient, 1);
    testListener.init();

    zooKeeperClient.registerBroker(Broker(0, "10.10.10.10", 8000))
    zooKeeperClient.registerBroker(Broker(1, "10.10.10.11", 8000))
    zooKeeperClient.registerBroker(Broker(2, "10.10.10.12", 8000))

    TestUtils.waitUntilTrue(() => {
      testListener.liveBrokers.size == 3
    }, "Waiting for all brokers to get added", 1000)

    assert(testListener.liveBrokers.size == 3)
  }
}
