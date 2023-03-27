package org.apache.openwhisk.core.scheduler.supervisor.test

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestKit, TestProbe}
import com.google.protobuf.ByteString
import com.ibm.etcd.api.Event.EventType
import com.ibm.etcd.api.{Event, KeyValue, LeaseKeepAliveResponse, TxnResponse}
import com.ibm.etcd.client.kv.KvClient.Watch
import com.ibm.etcd.client.kv.WatchUpdate
import com.ibm.etcd.client.{EtcdClient => Client}
import common.StreamLogging
import org.apache.openwhisk.core.etcd.EtcdClient
import org.apache.openwhisk.core.scheduler.SchedulingSupervisorConfig
import org.apache.openwhisk.core.scheduler.queue.trackplugin._
import org.apache.openwhisk.core.service.{WatcherService, mockWatchUpdate}
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpecLike, Matchers}

import java.lang
import java.util.concurrent.Executor
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

@RunWith(classOf[JUnitRunner])
class QueueSupervisorTests extends TestKit(ActorSystem("WatcherService"))
  with FlatSpecLike
  with Matchers
  with MockFactory
  with ScalaFutures
  with StreamLogging{

  private implicit val ec: ExecutionContextExecutor = system.dispatcher
  val client: Client = {
    val hostAndPorts = "172.17.0.1:2379"
    Client.forEndpoints(hostAndPorts).withPlainText().build()
  }

  val namespace: String = "test-namespace"
  val action: String = "test-action"
  val probe: TestProbe = TestProbe()
  val supervisorConfig: SchedulingSupervisorConfig = SchedulingSupervisorConfig(
    enableSupervisor = true,
    3,
    1,
    2,
    "AsRequested",
    2,
    2,
    "AcceptAll",
    2,
    500)
  implicit val etcdClient: EtcdClient = new MockEtcdClient(client, true)
  implicit val watcherService: ActorRef = system.actorOf(WatcherService.props(etcdClient))
  implicit val stateRegistry: StateRegistry = new MockStateRegistry(namespace, action)

  it should "Set the minWorkers equals to readyWorkers and maxWorkers" in{
    val supervisor = new QueueSupervisor(namespace,action, supervisorConfig)
    supervisor.setMinWorkers(0) shouldBe true
    supervisor.setReadyWorkers(0) shouldBe true
    supervisor.setMaxWorkers(0) shouldBe true
    supervisor.clean()
  }

  it should "Set the minWorkers equals to readyWorkers" in {
    val supervisor = new QueueSupervisor(namespace, action, supervisorConfig)
    supervisor.setReadyWorkers(1) shouldBe true
    supervisor.clean()
  }

  it should "Set the readyWorkers equals to maxWorkers" in {
    val supervisor = new QueueSupervisor(namespace, action, supervisorConfig)
    supervisor.setReadyWorkers(3) shouldBe true
    supervisor.clean()
  }

  it should "Set the minWorkers less than readyWorkers" in {
    val supervisor = new QueueSupervisor(namespace, action, supervisorConfig)
    supervisor.setMinWorkers(0) shouldBe true
    supervisor.clean()
  }

  it should "Set the maxWorkers higher than readyWorkers" in {
    val supervisor = new QueueSupervisor(namespace, action, supervisorConfig)
    supervisor.setMaxWorkers(4) shouldBe true
    supervisor.clean()
  }

  it should "Not permit to set readyWorkers higher than maxWorkers" in {
    val supervisor = new QueueSupervisor(namespace, action, supervisorConfig)
    supervisor.setReadyWorkers(4) shouldBe false
    supervisor.clean()
  }

  it should "Not permit to set maxWorkers less than than readyWorkers" in {
    val supervisor = new QueueSupervisor(namespace, action, supervisorConfig)
    supervisor.setMaxWorkers(1) shouldBe false
    supervisor.clean()
  }

  it should "Not permit to set readyWorkers less than minWorkers" in {
    val supervisor = new QueueSupervisor(namespace, action, supervisorConfig)
    supervisor.setReadyWorkers(0) shouldBe false
    supervisor.clean()
  }

  it should "Not permit to set minWorkers higher than readyWorkers" in {
    val supervisor = new QueueSupervisor(namespace, action, supervisorConfig)
    supervisor.setMinWorkers(3) shouldBe false
    supervisor.clean()
  }

  it should "Be able to change dynamically the schedule period" in {
    val supervisor = new TestQueueSupervisor(namespace, action, supervisorConfig)
    Thread.sleep(20000)
    supervisor.changeSchedulerPeriod(Duration(7, SECONDS))
    Thread.sleep(20000)
    moreOrLess(supervisor.variation, 7000, 500) shouldBe true
    Thread.sleep(7000)
    moreOrLess(supervisor.variation, 7000, 500) shouldBe true
    supervisor.changeSchedulerPeriod(Duration(18, SECONDS))
    Thread.sleep(40000)
    moreOrLess(supervisor.variation, 18000, 500) shouldBe true
    Thread.sleep(18000)
    moreOrLess(supervisor.variation, 18000, 500) shouldBe true
    supervisor.clean()
  }

  it should "Be able to call periodically the schedule function" in {
    val supervisor = new TestQueueSupervisor(namespace, action, supervisorConfig)
    Thread.sleep(3000)
    moreOrLess(supervisor.testTimestamp, System.currentTimeMillis() - 1000, 500) shouldBe true
    Thread.sleep(30000)
    moreOrLess(supervisor.testTimestamp, System.currentTimeMillis() - 1000, 500) shouldBe true
    Thread.sleep(30000)
    moreOrLess(supervisor.testTimestamp, System.currentTimeMillis() - 1000, 500) shouldBe true
    supervisor.clean()
  }


  it should "Elaborate runtime the Inter-arrival time correctly" in{
    val supervisor = new QueueSupervisor(namespace, action, supervisorConfig)
    supervisor.handleActivation( null, 0,0,0,0) shouldBe true
    math.round(supervisor.iar).toInt shouldBe 0
    Thread.sleep(58000)
    supervisor.handleActivation( null, 0,0,0,0) shouldBe true
    math.round(supervisor.iar).toInt shouldBe 1
    Thread.sleep(58000)
    supervisor.handleActivation(null, 0, 0, 0, 0) shouldBe true
    math.round(supervisor.iar).toInt shouldBe 1
    Thread.sleep(60000)
    math.round(supervisor.iar).toInt shouldBe 1
    supervisor.handleActivation(null, 0, 0, 0, 0) shouldBe true
    Thread.sleep(60000)
    math.round(supervisor.iar).toInt shouldBe 1
    supervisor.handleActivation(null, 0, 0, 0, 0) shouldBe true
    math.round(supervisor.iar).toInt shouldBe 1
    Thread.sleep(60000)
    math.round(supervisor.iar).toInt shouldBe 1
    Thread.sleep(60000)
    math.round(supervisor.iar).toInt shouldBe 0
    supervisor.clean()
  }

  it should "Elaborate runtime the Inter-arrival time correctly managing higher values" in {
    val supervisor = new QueueSupervisor(namespace, action, supervisorConfig)
    Thread.sleep(70000)
    supervisor.handleActivation(null, 0, 0, 0, 0) shouldBe true
    supervisor.handleActivation(null, 0, 0, 0, 0) shouldBe true
    math.round(supervisor.iar).toInt shouldBe 0
    Thread.sleep(60000)
    supervisor.handleActivation(null, 0, 0, 0, 0) shouldBe true
    supervisor.handleActivation(null, 0, 0, 0, 0) shouldBe true
    math.round(supervisor.iar).toInt shouldBe 1
    Thread.sleep(60000)
    supervisor.handleActivation(null, 0, 0, 0, 0) shouldBe true
    supervisor.handleActivation(null, 0, 0, 0, 0) shouldBe true
    math.round(supervisor.iar).toInt shouldBe 2
    Thread.sleep(60000)
    math.round(supervisor.iar).toInt shouldBe 2
    supervisor.handleActivation(null, 0, 0, 0, 0) shouldBe true
    supervisor.handleActivation(null, 0, 0, 0, 0) shouldBe true
    Thread.sleep(60000)
    math.round(supervisor.iar).toInt shouldBe 2
    supervisor.handleActivation(null, 0, 0, 0, 0) shouldBe true
    supervisor.handleActivation(null, 0, 0, 0, 0) shouldBe true
    math.round(supervisor.iar).toInt shouldBe 2
    Thread.sleep(60000)
    math.round(supervisor.iar).toInt shouldBe 2
    Thread.sleep( 60000 )
    math.round(supervisor.iar).toInt shouldBe 1
    Thread.sleep(60000)
    math.round(supervisor.iar).toInt shouldBe 0
    supervisor.clean()
  }

  def moreOrLess( checkTime: Long, timestamp: Long, variation: Long ): Boolean = checkTime < timestamp+variation && checkTime > timestamp-variation

  class TestQueueSupervisor(namespace: String, action: String, config: SchedulingSupervisorConfig) extends QueueSupervisor(namespace, action, config){

    var testTimestamp : Long = 0
    var variation : Long = 0
    override def schedule(localUpdateState: UpdateState, globalUpdateState: UpdateState, states: Map[String, StateInformation]): Unit = {
      if( testTimestamp == 0 )
        this.testTimestamp = System.currentTimeMillis()
      else{
        val help = System.currentTimeMillis()
        variation = help - testTimestamp
        testTimestamp = help
      }
    }
  }
  class MockStateRegistry(namespace: String, action: String) extends StateRegistry(namespace, action) {

    override def publishUpdate(value: TrackQueueSnapshot): Unit = {
    }

    override def clean(): Unit = {}

    override def getStates: Map[String, StateInformation] = {
      null
    }

    override def getUpdateStatus: UpdateState = null
  }

  class MockEtcdClient(client: Client, isLeader: Boolean)
  extends EtcdClient(client)(ec) {
    var count = 0
    var storedValues = List.empty[(String, String, Long, Long)]
    var dataMap: Map[String, String] = Map[String, String]()

    override def putTxn[T](key: String, value: T, cmpVersion: Long, leaseId: Long): Future[TxnResponse] = {
      if (isLeader) {
        storedValues = (key, value.toString, cmpVersion, leaseId) :: storedValues
      }
      Future.successful(TxnResponse.newBuilder().setSucceeded(isLeader).build())
    }

    /*
   * this method count the number of entries whose key starts with the given prefix
   */
    override def getCount(prefixKey: String): Future[Long] = {
      Future.successful {
        dataMap.count(data => data._1.startsWith(prefixKey))
      }
    }

    var watchCallbackMap: Map[String, WatchUpdate => Unit] = Map[String, WatchUpdate => Unit]()

    override def keepAliveOnce(leaseId: Long): Future[LeaseKeepAliveResponse] =
      Future.successful(LeaseKeepAliveResponse.newBuilder().setID(leaseId).build())

    /*
   * this method adds one callback for the given key in watchCallbackMap.
   *
   * Note: Currently it only supports prefix-based watch.
   */
    override def watchAllKeys(next: WatchUpdate => Unit, error: Throwable => Unit, completed: () => Unit): Watch = {

      watchCallbackMap += "" -> next
      new Watch {
        override def close(): Unit = {}

        override def addListener(listener: Runnable, executor: Executor): Unit = {}

        override def cancel(mayInterruptIfRunning: Boolean): Boolean = true

        override def isCancelled: Boolean = true

        override def isDone: Boolean = true

        override def get(): lang.Boolean = true

        override def get(timeout: Long, unit: TimeUnit): lang.Boolean = true
      }
    }

    /*
   * This method stores the data in dataMap to simulate etcd.put()
   * After then, it calls the registered watch callback for the given key
   * So we don't need to call put() to simulate watch API.
   * Expected order of calls is 1. watch(), 2.publishEvents(). Data will be stored in dataMap and
   * callbacks in the callbackMap for the given prefix will be called by publishEvents()
   *
   * Note: watch callback is currently registered based on prefix only.
   */
    def publishEvents(eventType: EventType, key: String, value: String): Unit = {
      val eType = eventType match {
        case EventType.PUT =>
          dataMap += key -> value
          EventType.PUT

        case EventType.DELETE =>
          dataMap -= key
          EventType.DELETE

        case EventType.UNRECOGNIZED => Event.EventType.UNRECOGNIZED
      }
      val event = Event
        .newBuilder()
        .setType(eType)
        .setPrevKv(
          KeyValue
            .newBuilder()
            .setKey(ByteString.copyFromUtf8(key))
            .setValue(ByteString.copyFromUtf8(value))
            .build())
        .setKv(
          KeyValue
            .newBuilder()
            .setKey(ByteString.copyFromUtf8(key))
            .setValue(ByteString.copyFromUtf8(value))
            .build())
        .build()

      // find the callbacks which has the proper prefix for the given key
      watchCallbackMap.filter(callback => key.startsWith(callback._1)).foreach { callback =>
        callback._2(new mockWatchUpdate().addEvents(event))
      }
    }
  }
}
