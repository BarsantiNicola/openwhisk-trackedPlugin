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
import org.apache.openwhisk.core.scheduler.queue.trackplugin.{StateRegistry, TrackQueueSnapshot, TrackedRunning, UpdateState}
import org.apache.openwhisk.core.service.{WatcherService, mockWatchUpdate}
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpecLike, Matchers}
import java.lang
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
  private implicit var containers : Set[String] = Set[String]().empty
  private implicit val watcherService: ActorRef = system.actorOf(WatcherService.props(etcdClient))
  private val namespace = "test-namespace"
  private val namespace2 = "test-namespace-2"
  private val action = "test-action"

  logging.info(this, "TESTING SINGLE INSTANCE BEHAVIOR")

  it should "Respect the single and multihost registry behavior" in {
    val stateRegistry = new StateRegistry(namespace, action)

    val snapshot: TrackQueueSnapshot = TrackQueueSnapshot(initialized = false, new AtomicInteger(0), 0, containers, 0, 0, 0, 0, Option(0), 0, 0, TrackedRunning, null)
    val snapshot2: TrackQueueSnapshot = TrackQueueSnapshot(initialized = true, new AtomicInteger(0), 0, containers, 0, 0, 0, 0, Option(0), 0, 0, TrackedRunning, null)
    val snapshot3: TrackQueueSnapshot = TrackQueueSnapshot(initialized = true, new AtomicInteger(1), 0, containers, 0, 0, 0, 0, Option(0), 0, 0, TrackedRunning, null)

    logging.info(this, "It should have no updates after initialization..")
    val firstLocalResult: UpdateState = stateRegistry.getUpdateStatus
    val firstGlobalResult: UpdateState = StateRegistry.getUpdateStatus(namespace, action)
    firstLocalResult.update shouldBe false
    firstGlobalResult.update shouldBe false
    logging.info(this, "Done")

    logging.info(this, "It should have a local update after a publish and It should not have a global " +
      "update(global update must show only updated coming from others")
    stateRegistry.publishUpdate(snapshot)
    val secondLocalResult: UpdateState = stateRegistry.getUpdateStatus
    val secondGlobalResult: UpdateState = StateRegistry.getUpdateStatus(namespace, action)
    secondLocalResult.update shouldBe true
    secondGlobalResult.update shouldBe false
    logging.info(this, "Done")

    logging.info(this, "It should remove the update flag after getting the state registry")
    stateRegistry.getStates
    val thirdLocalResult: UpdateState = stateRegistry.getUpdateStatus
    val thirdGlobalResult: UpdateState = StateRegistry.getUpdateStatus(namespace, action)
    thirdLocalResult.update shouldBe false
    thirdGlobalResult.update shouldBe false
    logging.info(this, "Done")

    logging.info(this, "Adding a snapshot without changes from the stored one must not activate the update flag")
    stateRegistry.publishUpdate(snapshot)
    val forthLocalResult: UpdateState = stateRegistry.getUpdateStatus
    val forthGlobalResult: UpdateState = StateRegistry.getUpdateStatus(namespace, action)
    forthLocalResult.update shouldBe false
    forthGlobalResult.update shouldBe false
    forthLocalResult.lastUpdate shouldBe thirdLocalResult.lastUpdate
    forthGlobalResult.lastUpdate shouldBe thirdGlobalResult.lastUpdate
    logging.info(this, "Done")

    logging.info(this, "Adding a different snapshot should activate the update flag and change the lastUpdate timestamp")
    stateRegistry.publishUpdate(snapshot2)
    val fifthLocalResult: UpdateState = stateRegistry.getUpdateStatus
    val fifthGlobalResult: UpdateState = StateRegistry.getUpdateStatus(namespace, action)
    fifthLocalResult.update shouldBe true
    fifthGlobalResult.update shouldBe false
    fifthLocalResult.lastUpdate should not be forthLocalResult.lastUpdate
    fifthGlobalResult.lastUpdate should not be forthGlobalResult.lastUpdate
    logging.info(this, "Done")

    logging.info(this, "TESTING INTERACTIONS BETWEEN TWO LOCAL STATE-REGISTRIES")

    val secondStateRegistry = new StateRegistry(namespace2, action)

    logging.info(this, "A newly created stateregistry should see an update status if previously a publish has been made")
    stateRegistry.getUpdateStatus.update shouldBe true
    secondStateRegistry.getUpdateStatus.update shouldBe true
    logging.info(this, "Done")

    logging.info(this, "It should remove the update flag after getting the state registry only in the involved instance")
    stateRegistry.getStates
    stateRegistry.getUpdateStatus.update shouldBe false
    secondStateRegistry.getUpdateStatus.update shouldBe true

    secondStateRegistry.getStates
    stateRegistry.getUpdateStatus.update shouldBe false
    secondStateRegistry.getUpdateStatus.update shouldBe false
    logging.info(this, "Done")

    logging.info(this, "It should have a local update after a publish and It should not have a global " +
      "update(global update must show only updated coming from others")
    secondStateRegistry.publishUpdate(snapshot2)
    StateRegistry.getUpdateStatus(namespace, action).update shouldBe true
    StateRegistry.getUpdateStatus(namespace2, action).update shouldBe false
    logging.info(this, "Done")

    logging.info(this, "Adding a snapshot without changes from the stored one must not activate the update flag and change previously update status")
    stateRegistry.publishUpdate(snapshot2)
    stateRegistry.getUpdateStatus.update shouldBe false
    StateRegistry.getUpdateStatus(namespace, action).update shouldBe true
    StateRegistry.getUpdateStatus(namespace2, action).update shouldBe false
    logging.info(this, "Done")

    logging.info(this, "It should manage the states independently")
    stateRegistry.publishUpdate(snapshot3)
    stateRegistry.getUpdateStatus.update shouldBe true
    StateRegistry.getUpdateStatus(namespace, action).update shouldBe true
    StateRegistry.getUpdateStatus(namespace2, action).update shouldBe true

    secondStateRegistry.getStates
    StateRegistry.getUpdateStatus(namespace, action).update shouldBe true
    StateRegistry.getUpdateStatus(namespace2, action).update shouldBe false
    secondStateRegistry.getUpdateStatus.update shouldBe false
    stateRegistry.getUpdateStatus.update shouldBe true

    stateRegistry.getStates
    StateRegistry.getUpdateStatus(namespace, action).update shouldBe false
    StateRegistry.getUpdateStatus(namespace2, action).update shouldBe false
    secondStateRegistry.getUpdateStatus.update shouldBe false
    stateRegistry.getUpdateStatus.update shouldBe false
    logging.info(this, "Done")
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
