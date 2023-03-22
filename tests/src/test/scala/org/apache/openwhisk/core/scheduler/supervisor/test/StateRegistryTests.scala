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
import org.apache.openwhisk.core.scheduler.queue.trackplugin.{StateInformation, StateRegistry, TrackQueueSnapshot, TrackedRunning, UpdateForRenew}
import org.apache.openwhisk.core.service.{WatcherService, mockWatchUpdate}
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures

import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpecLike, Matchers}

import java.lang
import java.sql.Timestamp
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class StateRegistryTests
extends TestKit(ActorSystem("WatcherService"))
    with FlatSpecLike
    with Matchers
    with MockFactory
    with ScalaFutures
    with StreamLogging {

  private implicit val ec: ExecutionContextExecutor = system.dispatcher
  private val probe: TestProbe = TestProbe()
  private val client: Client = {
    val hostAndPorts = "172.17.0.1:2379"
    Client.forEndpoints(hostAndPorts).withPlainText().build()
  }

  private implicit val etcdClient : EtcdClient = new MockEtcdClient(client, true)
  private implicit val containers : Set[String] = Set[String]().empty
  private implicit val watcherService: ActorRef = system.actorOf(WatcherService.props(etcdClient))
  private val namespace = "test-namespace"
  private val namespace2 = "test-namespace-2"
  private val action = "test-action"

  logging.info(this, "TESTING SINGLE INSTANCE BEHAVIOR")
  val snapshot: TrackQueueSnapshot = TrackQueueSnapshot(initialized = false, new AtomicInteger(0), 0, containers, containers, 0, 0, 0, 0, Option(0), 0, 0, TrackedRunning, null)
  val snapshot2: TrackQueueSnapshot = TrackQueueSnapshot(initialized = true, new AtomicInteger(0), 0, containers, containers, 0, 0, 0, 0, Option(0), 0, 0, TrackedRunning, null)
  val snapshot3: TrackQueueSnapshot = TrackQueueSnapshot(initialized = true, new AtomicInteger(1), 0, containers, containers, 0, 0, 0, 0, Option(0), 0, 0, TrackedRunning, null)

  //  TEST TO BE EXECUTED WITHOUT FORKED TASKS. StateRegistry operates on a singleton class which produces unpredictable results on the tests
  it should "Have a consistent initialization state" in{
    val stateRegistry = new StateRegistry(namespace, action)
    stateRegistry.getStates.isEmpty shouldBe true
    val localValue = stateRegistry.getUpdateStatus
    localValue.update shouldBe false
    localValue.lastUpdate.before(new Timestamp(System.currentTimeMillis())) shouldBe true
    val globalValue =  StateRegistry.getUpdateStatus(namespace, action)
    globalValue.update shouldBe false
    globalValue.lastUpdate.before(new Timestamp(System.currentTimeMillis())) shouldBe true
    stateRegistry.clean()
  }

  it should "Manage removal of a not present state" in{
    val stateRegistry = new StateRegistry(namespace, action)
    stateRegistry.clean()
    val localValue = stateRegistry.getUpdateStatus
    localValue.update shouldBe false
    Thread.sleep(50)
    localValue.lastUpdate.before(new Timestamp(System.currentTimeMillis())) shouldBe true

  }

  it should "Add a first update and notify it" in {
    val stateRegistry = new StateRegistry(namespace, action)
    val prevTime = stateRegistry.getUpdateStatus.lastUpdate
    stateRegistry.publishUpdate(snapshot)
    val localValue = stateRegistry.getUpdateStatus
    val equivState = new StateInformation(snapshot)
    equivState.timestamp = localValue.lastUpdate.getTime+1
    localValue.update shouldBe true
    localValue.lastUpdate.after(prevTime) shouldBe true
    val states : Map[String,StateInformation] = stateRegistry.getStates
    states.size shouldBe 1
    states(s"$namespace--$action").check(equivState) shouldBe UpdateForRenew
    stateRegistry.clean()
  }

  it should "Remove an update already stored" in {
    val stateRegistry = new StateRegistry(namespace, action)
    stateRegistry.publishUpdate(snapshot)
    stateRegistry.clean()
    val localValue = stateRegistry.getUpdateStatus
    localValue.update shouldBe false
    val values = stateRegistry.getStates
    values.size shouldBe 0
    stateRegistry.clean()
  }

  it should "Reset the local status after a state extraction" in{
    val stateRegistry = new StateRegistry(namespace, action)
    stateRegistry.publishUpdate(snapshot)
    stateRegistry.getStates
    val localValue = stateRegistry.getUpdateStatus
    localValue.update shouldBe false
    stateRegistry.clean()
  }

  it should "Add a new fresh update and notify it" in {
    val stateRegistry = new StateRegistry(namespace, action)
    stateRegistry.publishUpdate(snapshot)
    val prevValue = stateRegistry.getUpdateStatus
    Thread.sleep(50)
    stateRegistry.getStates
    stateRegistry.publishUpdate(snapshot2)
    val localValue = stateRegistry.getUpdateStatus
    localValue.update shouldBe true
    localValue.lastUpdate.after(prevValue.lastUpdate) shouldBe true
    val equivState = new StateInformation(snapshot2)
    equivState.timestamp = localValue.lastUpdate.getTime + 1
    val states = stateRegistry.getStates
    states.size shouldBe 1
    states(s"$namespace--$action").check(equivState) shouldBe UpdateForRenew
    stateRegistry.clean()
  }

  it should "Never add an old update" in {
    val stateRegistry = new StateRegistry(namespace, action)
    val testUpdate = new StateInformation(snapshot2)
    Thread.sleep(50)
    stateRegistry.publishUpdate(snapshot)
    StateRegistry.addUpdate( namespace, action, testUpdate) shouldBe false
    stateRegistry.getUpdateStatus.update shouldBe true
    StateRegistry.getUpdateStatus(namespace,action).update shouldBe false
    stateRegistry.getStates
    StateRegistry.addUpdate( namespace, action, testUpdate) shouldBe false
    stateRegistry.getUpdateStatus.update shouldBe false
    StateRegistry.getUpdateStatus(namespace,action).update shouldBe false
    stateRegistry.clean()
  }

  it should "Not add duplicated updates but refreshing the local timestamp" in{
    val stateRegistry = new StateRegistry(namespace, action)
    stateRegistry.publishUpdate(snapshot)
    val time = stateRegistry.getUpdateStatus.lastUpdate
    Thread.sleep(100)
    stateRegistry.getStates
    stateRegistry.publishUpdate(snapshot)
    val update = stateRegistry.getUpdateStatus
    update.update shouldBe false
    update.lastUpdate.after(time) shouldBe false
    stateRegistry.getStates.get(s"$namespace--$action").get.timestamp > time.getTime shouldBe true
    stateRegistry.clean()
  }

  it should "Have a consistent initialization state from other StateRegistry instances" in {
    val stateRegistry = new StateRegistry(namespace, action)
    val stateRegistry2 = new StateRegistry(namespace2, action)
    stateRegistry.getStates.isEmpty shouldBe true
    stateRegistry2.getStates.isEmpty shouldBe true
    val localValue = stateRegistry.getUpdateStatus
    val secondLocalValue = stateRegistry2.getUpdateStatus
    Thread.sleep(50)
    localValue.update shouldBe false
    localValue.lastUpdate.before(new Timestamp(System.currentTimeMillis())) shouldBe true
    secondLocalValue.update shouldBe false
    secondLocalValue.lastUpdate.before(new Timestamp(System.currentTimeMillis())) shouldBe true
    val globalValue = StateRegistry.getUpdateStatus(namespace, action)
    val globalValue2 = StateRegistry.getUpdateStatus(namespace2, action)
    globalValue.update shouldBe false
    globalValue.lastUpdate.before(new Timestamp(System.currentTimeMillis())) shouldBe true
    globalValue2.update shouldBe false
    globalValue2.lastUpdate.before(new Timestamp(System.currentTimeMillis())) shouldBe true
    stateRegistry.clean()
    stateRegistry2.clean()

  }

  it should "Add a first update and notify it from other StateRegistry perspective" in {
    val stateRegistry = new StateRegistry(namespace, action)
    val stateRegistry2 = new StateRegistry(namespace2, action)
    val prevTime = stateRegistry2.getUpdateStatus.lastUpdate
    stateRegistry.publishUpdate(snapshot)
    val localValue = stateRegistry2.getUpdateStatus
    val globalValue = StateRegistry.getUpdateStatus(namespace, action)
    val globalValue2 = StateRegistry.getUpdateStatus(namespace2, action)
    val equivState = new StateInformation(snapshot)
    equivState.timestamp = globalValue.lastUpdate.getTime + 1
    localValue.update shouldBe false
    localValue.lastUpdate.after(prevTime) shouldBe false
    globalValue.update shouldBe false
    globalValue2.update shouldBe true
    val states: Map[String, StateInformation] = stateRegistry2.getStates
    states.size shouldBe 1
    states.get(s"$namespace--$action").get.check(equivState) shouldBe UpdateForRenew
    stateRegistry.clean()
    stateRegistry2.clean()
  }

  it should "Manage removal of a not present state from other StateRegistry perspective" in {
    var stateRegistry = new StateRegistry(namespace, action)
    val stateRegistry2 = new StateRegistry(namespace2, action)
    stateRegistry.clean()
    stateRegistry.getUpdateStatus.update shouldBe false
    stateRegistry2.getUpdateStatus.update shouldBe false
    stateRegistry = new StateRegistry(namespace, action)
    stateRegistry2.getUpdateStatus.update shouldBe false
    stateRegistry.publishUpdate(snapshot)
    StateRegistry.getUpdateStatus(namespace2, action).update shouldBe true
    stateRegistry2.getStates
    StateRegistry.getUpdateStatus(namespace2, action).update shouldBe false
    stateRegistry.clean()
    StateRegistry.getUpdateStatus(namespace2, action).update shouldBe true
    stateRegistry2.clean()
  }

  it should "Removal of an update should be notified to others StateRegistry instances" in{
    val stateRegistry = new StateRegistry(namespace, action)
    val stateRegistry2 = new StateRegistry(namespace2, action)
    stateRegistry.publishUpdate(snapshot)
    StateRegistry.getUpdateStatus(namespace2, action).update shouldBe true
    stateRegistry2.getStates
    StateRegistry.getUpdateStatus(namespace2, action).update shouldBe false
    stateRegistry.clean()
    StateRegistry.getUpdateStatus(namespace2, action).update shouldBe true
    stateRegistry2.clean()
  }

  it should "Reset the local status after a state extraction only on the given instance" in {
    val stateRegistry = new StateRegistry(namespace, action)
    val stateRegistry2 = new StateRegistry(namespace2, action)
    stateRegistry.publishUpdate(snapshot)
    StateRegistry.getUpdateStatus(namespace, action).update shouldBe false
    StateRegistry.getUpdateStatus(namespace2, action).update shouldBe true
    stateRegistry.getStates
    val localValue = stateRegistry.getUpdateStatus
    localValue.update shouldBe false
    StateRegistry.getUpdateStatus(namespace, action).update shouldBe false
    StateRegistry.getUpdateStatus(namespace2, action).update shouldBe true
    stateRegistry2.getStates
    StateRegistry.getUpdateStatus(namespace, action).update shouldBe false
    StateRegistry.getUpdateStatus(namespace2, action).update shouldBe false
    stateRegistry.clean()
    stateRegistry2.clean()
  }

  class MockEtcdClient(client: Client, isLeader: Boolean, leaseNotFound: Boolean = false, failedCount: Int = 1)
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
