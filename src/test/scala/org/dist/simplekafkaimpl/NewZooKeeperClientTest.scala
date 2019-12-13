package org.dist.simplekafkaimpl

import org.assertj.core.api.Assertions
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{BrokerChangeListener, CreateTopicCommand, SimpleSocketServer}
import org.mockito.{ArgumentMatchers, Mockito}
import org.scalatest.Matchers

class NewZooKeeperClientTest extends ZookeeperTestHarness {

  test("it should register a Broker") {
    val zooKeeperClient = new NewZooKeeperClient(zkClient);
    val broker = new Broker(1, "127.0.0.1", 22000);

    val createResponse = zooKeeperClient.registerBroker(broker);

  }

  test("it should maintain live brokers") {
    val zooKeeperClient = new NewZooKeeperClient(zkClient);
    val socketServer = Mockito.mock(classOf[SimpleSocketServer])
    val testListener = new NewController(zooKeeperClient, 1, socketServer);
    testListener.init();

    zooKeeperClient.registerBroker(Broker(0, "10.10.10.10", 8000))
    zooKeeperClient.registerBroker(Broker(1, "10.10.10.11", 8000))
    zooKeeperClient.registerBroker(Broker(2, "10.10.10.12", 8000))

    TestUtils.waitUntilTrue(() => {
      testListener.liveBrokers.size == 3
    }, "Waiting for all brokers to get added", 1000)

    assert(testListener.liveBrokers.size == 3)
  }

  test("it should elect leader") {
    val zooKeeperClient = new NewZooKeeperClient(zkClient);
    val brokerId = 1

    zooKeeperClient.registerBroker(Broker(0, "10.10.10.10", 8000))

    val testListener = new NewController(zooKeeperClient, brokerId, Mockito.mock(classOf[SimpleSocketServer]));
    testListener.init();

    assert(testListener.currentLeader == brokerId)

    val listenerTwo = new NewController(zooKeeperClient, 2, Mockito.mock(classOf[SimpleSocketServer]));
    listenerTwo.init()

    assert(listenerTwo.currentLeader == testListener.currentLeader)
  }

  test("it should create a new topic") {
    val zooKeeperClient = new NewZooKeeperClient(zkClient);
    val brokerId = 1

    zooKeeperClient.registerBroker(Broker(0, "10.10.10.10", 8000))
    zooKeeperClient.registerBroker(Broker(1, "10.10.10.11", 8000))
//    zooKeeperClient.registerBroker(Broker(2, "10.10.10.12", 8000))
//    zooKeeperClient.registerBroker(Broker(3, "10.10.10.13", 8000))

    val testListener = new NewController(zooKeeperClient, brokerId, Mockito.mock(classOf[SimpleSocketServer]));
    val spyTestListener = Mockito.spy(testListener);
    spyTestListener.init();

    val createTopicCommand = new CreateTopicCommand(zooKeeperClient);
    createTopicCommand.createTopic("devices", 2, 2);

    val assignments = zooKeeperClient.getPartitionAssignmentsFor("devices")
    assert(assignments.size == 2)
  }

  test("it should call controller's callback on creating a new topic") {
    val zooKeeperClient = new NewZooKeeperClient(zkClient);
    val brokerId = 1

    zooKeeperClient.registerBroker(Broker(0, "10.10.10.10", 8000))
    zooKeeperClient.registerBroker(Broker(1, "10.10.10.11", 8000))

    val testListener = new NewController(zooKeeperClient, brokerId, Mockito.mock(classOf[SimpleSocketServer]));
    val spyTestListener = Mockito.spy(testListener);
    spyTestListener.init();

    val createTopicCommand = new CreateTopicCommand(zooKeeperClient);
    createTopicCommand.createTopic("devices", 2, 2);

    zooKeeperClient.subscribeTopicChangeListener(new NewTopicChangeHandler(zooKeeperClient, onTopicChange))

    Mockito.verify(spyTestListener).onTopicChange(ArgumentMatchers.eq("devices"), ArgumentMatchers.any())
  }
}
