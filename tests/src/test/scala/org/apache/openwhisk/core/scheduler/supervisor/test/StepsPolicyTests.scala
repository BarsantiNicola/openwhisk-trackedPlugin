/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.apache.openwhisk.core.entity.Parameters
import org.apache.openwhisk.core.etcd.EtcdClient
import org.apache.openwhisk.core.scheduler.SchedulingSupervisorConfig
import org.apache.openwhisk.core.scheduler.queue.trackplugin._
import org.apache.openwhisk.core.scheduler.queue.{AddContainer, DecisionResults, Skip}
import org.apache.openwhisk.core.service.{WatcherService, mockWatchUpdate}
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpecLike, Matchers}

import java.lang
import java.util.concurrent.{Executor, TimeUnit}
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class StepsPolicyTests extends TestKit(ActorSystem("WatcherService"))
  with FlatSpecLike
  with Matchers
  with MockFactory
  with ScalaFutures
  with StreamLogging  {

  private implicit val ec: ExecutionContextExecutor = system.dispatcher
  val client: Client = {
    val hostAndPorts = "172.17.0.1:2379"
    Client.forEndpoints(hostAndPorts).withPlainText().build()
  }

  val namespace : String = "test-namespace"
  val action : String    = "test-action"
  val probe: TestProbe = TestProbe()

  implicit val etcdClient: EtcdClient = new MockEtcdClient(client, true)
  implicit val watcherService: ActorRef = system.actorOf(WatcherService.props(etcdClient))

  it should "Manage minWorkers, maxWorkers, readyWorkers equal to 0" in{
    val stateRegistry: StateRegistry = new MockStateRegistry(namespace, action)
    val config = SchedulingSupervisorConfig(enableSupervisor = true, 0, 0, 0, "Steps", 2, 0, "AcceptAll", 2, 500, "Consolidation")
    val supervisor: QueueSupervisor = new QueueSupervisor(namespace, action, config, Parameters.apply(), stateRegistry, etcdClient)
    val parameters: (Set[String], Int, Int, Set[String]) = createEnv(0, 0, 0, 0, 1, 0, supervisor)
    supervisor.elaborate(parameters._1,parameters._2,parameters._3,parameters._4, None) shouldBe DecisionResults(Skip,0)
    supervisor.elaborate(parameters._1,parameters._2,parameters._3+1,parameters._4, None) shouldBe DecisionResults(Skip,0)
    supervisor.elaborate(parameters._1,parameters._2,parameters._3+2,parameters._4, None) shouldBe DecisionResults(Skip,0)
    supervisor.clean()
  }

  it should "Manage minWorkers, maxWorkers, readyWorkers equal to 1" in{
    val stateRegistry: StateRegistry = new MockStateRegistry(namespace, action)
    val config = SchedulingSupervisorConfig(enableSupervisor = true, 1, 1, 1, "Steps", 2, 0, "AcceptAll", 2, 500, "Consolidation")
    val supervisor: QueueSupervisor = new QueueSupervisor(namespace, action, config, Parameters.apply(), stateRegistry, etcdClient)
    val parameters: (Set[String], Int, Int, Set[String]) = createEnv(0, 0, 0, 0, 1, 0, supervisor)
    supervisor.elaborate(parameters._1, parameters._2, parameters._3, parameters._4, None) shouldBe DecisionResults(AddContainer, 1)
    supervisor.elaborate(parameters._1, parameters._2, parameters._3 + 1, parameters._4, None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(parameters._1, parameters._2, parameters._3 + 2, parameters._4, None) shouldBe DecisionResults(Skip, 0)
    supervisor.clean()
  }

  it should "Manage minWorkers, maxWorkers, readyWorkers equal to n" in {
    val stateRegistry: StateRegistry = new MockStateRegistry(namespace, action)
    val config = SchedulingSupervisorConfig(enableSupervisor = true, 5, 5, 5, "Steps", 2, 0, "AcceptAll", 2, 500, "Consolidation")
    val supervisor: QueueSupervisor = new QueueSupervisor(namespace, action, config, Parameters.apply(), stateRegistry, etcdClient)
    val parameters: (Set[String], Int, Int, Set[String]) = createEnv(0, 0, 0, 0, 1, 0, supervisor)
    supervisor.elaborate(parameters._1, parameters._2, parameters._3, parameters._4, None) shouldBe DecisionResults(AddContainer, 2)
    supervisor.elaborate(parameters._1, parameters._2, parameters._3, parameters._4, None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A","B"), parameters._2, parameters._3, Set("A","B"), None) shouldBe DecisionResults(AddContainer, 2)
    supervisor.elaborate(Set("A","B"), parameters._2, parameters._3, Set("A","B"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A","B","C","D"), parameters._2, parameters._3, Set("A","B","C","D"), None) shouldBe DecisionResults(AddContainer, 1)
    supervisor.elaborate(Set("A","B","C","D","E"), parameters._2, parameters._3 + 1, Set("A","B","C","D","E"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A","B","C","D","E"), parameters._2, parameters._3, Set("A","B","C","D","E"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.clean()
  }

  it should "Manage containers using only maxWorkers" in{
    val stateRegistry: StateRegistry = new MockStateRegistry(namespace, action)
    val config = SchedulingSupervisorConfig(enableSupervisor = true, 5, 0, 0, "Steps", 2, 0, "AcceptAll", 2, 500, "Consolidation")
    val supervisor: QueueSupervisor = new QueueSupervisor(namespace, action, config, Parameters.apply(), stateRegistry, etcdClient)
    val parameters: (Set[String], Int, Int, Set[String]) = createEnv(0, 0, 0, 0, 0, 0, supervisor)
    supervisor.elaborate(parameters._1, parameters._2, parameters._3, parameters._4, None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(parameters._1, parameters._2, parameters._3 + 1, parameters._4, None) shouldBe DecisionResults(AddContainer, 2)
    supervisor.elaborate(parameters._1, parameters._2, parameters._3 + 2, parameters._4, None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A","B"), parameters._2, parameters._3 + 2, Set("A","B"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A","B"), parameters._2, parameters._3 + 1, Set("A","B"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A","B"), parameters._2, parameters._3 + 3, Set("A","B"), None) shouldBe DecisionResults(AddContainer, 2)
    supervisor.elaborate(Set("A","B","C"), parameters._2, parameters._3, Set("A","B","C"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A","B","C","D"), parameters._2, parameters._3, Set("A","B","C","D"), None) shouldBe DecisionResults(RemoveReadyContainer(Set("A","B")), 0)
    supervisor.elaborate(Set("C","D"), parameters._2, parameters._3, Set("C","D"), None) shouldBe DecisionResults(RemoveReadyContainer(Set("C","D")), 0)
    supervisor.clean()
  }

  it should "Manage containers using using maxWorkers and minWorkers" in {
    val stateRegistry: StateRegistry = new MockStateRegistry(namespace, action)
    val config = SchedulingSupervisorConfig(enableSupervisor = true, 5, 2, 0, "Steps", 4, 0, "AcceptAll", 2, 500, "Consolidation")
    val supervisor: QueueSupervisor = new QueueSupervisor(namespace, action, config, Parameters.apply(), stateRegistry, etcdClient)
    val parameters: (Set[String], Int, Int, Set[String]) = createEnv(0, 0, 0, 0, 0, 0, supervisor)
    supervisor.elaborate(parameters._1, parameters._2, parameters._3, parameters._4, None) shouldBe DecisionResults(AddContainer, 4)
    supervisor.elaborate(parameters._1, parameters._2, parameters._3 + 1, parameters._4, None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A", "B","C","D"), parameters._2, parameters._3 + 1, Set("A", "B","C","D"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A", "B","C","D"), parameters._2, parameters._3, Set("A", "B","C","D"), None) shouldBe DecisionResults(RemoveReadyContainer(Set("A","B")), 0)
    supervisor.elaborate(Set("C","D"), parameters._2, parameters._3, Set("C","D"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("C", "D"), parameters._2, parameters._3 + 6, Set("C", "D"), None) shouldBe DecisionResults(AddContainer, 3)
    supervisor.clean()
  }

  it should "Manage containers using using maxWorkers and readyWorkers" in {
    val stateRegistry: StateRegistry = new MockStateRegistry(namespace, action)
    val config = SchedulingSupervisorConfig(enableSupervisor = true, 5, 0, 2, "Steps", 3, 0, "AcceptAll", 2, 500, "Consolidation")
    val supervisor: QueueSupervisor = new QueueSupervisor(namespace, action, config, Parameters.apply(), stateRegistry, etcdClient)
    val parameters: (Set[String], Int, Int, Set[String]) = createEnv(0, 0, 0, 0, 0, 0, supervisor)
    supervisor.elaborate(parameters._1, parameters._2, parameters._3, parameters._4, None) shouldBe DecisionResults(AddContainer, 3)
    supervisor.elaborate(Set( "A", "B", "C"), parameters._2, parameters._3 + 1, Set( "A", "B", "C"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set( "A", "B", "C"), parameters._2, parameters._3 + 2, Set( "A", "B", "C"), None) shouldBe DecisionResults(AddContainer, 2)
    supervisor.elaborate(Set( "A", "B", "C", "D"), parameters._2, parameters._3 + 1, Set( "A", "B", "C", "D"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A", "B", "C", "D","E"), parameters._2, parameters._3 + 3, Set("A", "B", "C", "D","E"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A", "B", "C", "D","E"), parameters._2, parameters._3 + 1, Set("A", "B", "C", "D","E"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set( "A", "B", "C", "D","E"), parameters._2, parameters._3, Set( "A", "B", "C", "D","E"), None) shouldBe DecisionResults(RemoveReadyContainer(Set("E","A","B")), 0)
    supervisor.elaborate(Set( "D","E"), parameters._2, parameters._3, Set( "D","E"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set( "D","E"), parameters._2, parameters._3 + 1, Set( "C","D"), None) shouldBe DecisionResults(AddContainer, 3)
    supervisor.clean()
  }

  it should "Manage containers using using maxWorkers minWorkers and readyWorkers in every possible combination" in {
    val stateRegistry: StateRegistry = new MockStateRegistry(namespace, action)
    val config = SchedulingSupervisorConfig(enableSupervisor = true, 5, 1, 2, "Steps", 1, 0, "AcceptAll", 2, 500, "Consolidation")
    val supervisor: QueueSupervisor = new QueueSupervisor(namespace, action, config, Parameters.apply(), stateRegistry, etcdClient)
    val parameters: (Set[String], Int, Int, Set[String]) = createEnv(0, 0, 0, 0, 0, 0, supervisor)
    supervisor.elaborate(parameters._1, parameters._2, parameters._3, parameters._4, None) shouldBe DecisionResults(AddContainer, 1)
    supervisor.elaborate(parameters._1, parameters._2, parameters._3, parameters._4, None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A","B"), parameters._2, parameters._3 + 1, Set("A","B"), None) shouldBe DecisionResults(AddContainer, 1)
    supervisor.elaborate(Set("A","B"), parameters._2, parameters._3 + 4, Set("A","B"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A","B"), parameters._2, parameters._3 , Set("A","B"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A","B","C"), parameters._2, parameters._3 , Set("A","B","C"), None) shouldBe DecisionResults(RemoveReadyContainer(Set("A")), 0)
    supervisor.elaborate(Set("B","C"), parameters._2, parameters._3+5 , Set("B","C"), None) shouldBe DecisionResults(AddContainer, 1)
    supervisor.elaborate(Set("B","C","D"), parameters._2, parameters._3+5 , Set("B","C","D"), None) shouldBe DecisionResults(AddContainer, 1)
    supervisor.elaborate(Set("B","C","D","E"), parameters._2, parameters._3+5 , Set("B","C","D","E"), None) shouldBe DecisionResults(AddContainer, 1)
    supervisor.elaborate(Set("B","C","D","E","F"), parameters._2, parameters._3+5 , Set("B","C","D","E","F"), None) shouldBe DecisionResults(Skip,0)
    supervisor.clean()
  }

  it should "Respect the ready containers threshold" in{
    val stateRegistry: StateRegistry = new MockStateRegistry(namespace, action)
    val config = SchedulingSupervisorConfig(enableSupervisor = true, 4,0,2, "Steps", 2, 0, "AcceptAll", 2, 500 , "Consolidation")
    val supervisor: QueueSupervisor = new QueueSupervisor(namespace, action, config, Parameters.apply(), stateRegistry, etcdClient)
    val parameters: (Set[String], Int, Int, Set[String]) = createEnv(0, 0, 0, 0, 0, 0, supervisor)
    supervisor.elaborate(parameters._1, parameters._2, parameters._3, parameters._4, None) shouldBe DecisionResults(AddContainer, 2)
    supervisor.elaborate(Set("A","B"), parameters._2, parameters._3+1, Set("A","B"), None) shouldBe DecisionResults(AddContainer, 2)
    supervisor.elaborate(Set("A","B","C","D"), parameters._2, parameters._3+1, Set("A","B","C","D"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A","B","C","D"), parameters._2, parameters._3+1, Set("A","B","C"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A","B","C","D"), parameters._2, parameters._3+1, Set("A","B","C","D"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A","B","C","D"), parameters._2, parameters._3, Set("A","B","C","D"), None) shouldBe DecisionResults(RemoveReadyContainer(Set("A","B")), 0)
    supervisor.elaborate(Set("C","D"), parameters._2, parameters._3, Set("C","D"), None) shouldBe DecisionResults(Skip, 0)
  }

  it should "Respect the minimum containers threshold" in{
    val stateRegistry: StateRegistry = new MockStateRegistry(namespace, action)
    val config = SchedulingSupervisorConfig(enableSupervisor = true, 6,2,0, "Steps", 3, 0, "AcceptAll", 2, 500 , "Consolidation")
    val supervisor: QueueSupervisor = new QueueSupervisor(namespace, action, config, Parameters.apply(), stateRegistry, etcdClient)
    val parameters: (Set[String], Int, Int, Set[String]) = createEnv(0, 0, 0, 0, 0, 0, supervisor)
    supervisor.elaborate(parameters._1, parameters._2, parameters._3, parameters._4, None) shouldBe DecisionResults(AddContainer, 3)
    supervisor.elaborate(Set("A", "B","C"), parameters._2, parameters._3 + 1, Set("A", "B","C"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A", "B","C"), parameters._2, parameters._3+3, Set("A", "B","C"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A", "B","C"), parameters._2, parameters._3+5, Set("A", "B","C"), None) shouldBe DecisionResults(AddContainer, 3)
    supervisor.elaborate(Set("A", "B","C","D","E","F"), parameters._2, parameters._3+4, Set("A", "B","C","D","E","F"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A", "B","C","D","E","F"), parameters._2, parameters._3+3, Set("A", "B","C","D","E","F"), None) shouldBe DecisionResults(RemoveReadyContainer(Set("E","F","A")), 0)
    supervisor.elaborate(Set("B", "C", "D"), parameters._2, parameters._3, Set("B", "C", "D"), None) shouldBe DecisionResults(RemoveReadyContainer(Set("B")),0)
  }

  it should "React to a dynamic change of the parameters" in{
    val stateRegistry: StateRegistry = new MockStateRegistry(namespace, action)
    val config = SchedulingSupervisorConfig(enableSupervisor = true, 1, 1, 0, "Steps", 2, 0, "AcceptAll", 2, 500, "Consolidation")
    val supervisor: QueueSupervisor = new QueueSupervisor(namespace, action, config, Parameters.apply(), stateRegistry, etcdClient)
    val parameters: (Set[String], Int, Int, Set[String]) = createEnv(0, 0, 0, 0, 0, 0, supervisor)
    supervisor.elaborate(parameters._1, parameters._2, 1, parameters._4, None) shouldBe DecisionResults(AddContainer, 1)
    supervisor.elaborate(Set("A"), parameters._2, 1, Set("A"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.setMaxWorkers(2)
    supervisor.elaborate(Set("A"), parameters._2 , 1, Set("A"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A"), parameters._2 , 3, Set("A"), None) shouldBe DecisionResults(AddContainer, 1)
    supervisor.setMaxWorkers(1)
    supervisor.elaborate(Set("A","B"), parameters._2, 3, Set("A","B"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.elaborate(Set("A","B"), parameters._2, 0, Set("A","B"), None) shouldBe DecisionResults(RemoveReadyContainer(Set("A")), 0)
    supervisor.setMinWorkers(0)
    supervisor.elaborate(Set("B"), parameters._2, 0, Set("B"), None) shouldBe DecisionResults(RemoveReadyContainer(Set("B")), 0)
    supervisor.setReadyWorkers(1)
    supervisor.elaborate(parameters._1, parameters._2, 3, parameters._4, None) shouldBe DecisionResults(AddContainer, 1)
    supervisor.setMaxWorkers(4)
    supervisor.elaborate(Set("C"), parameters._2, 0, Set("C"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.setMinWorkers(3)
    supervisor.elaborate(Set("C"), parameters._2, 0, Set("C"), None) shouldBe DecisionResults(AddContainer, 2)
    supervisor.setReadyWorkers(3)
    supervisor.elaborate(Set("C","D","E"), parameters._2, 1, Set("C","D","E"), None) shouldBe DecisionResults(AddContainer, 1)
    supervisor.elaborate(Set("C","D","E","F"), parameters._2, 2, Set("C","D","E","F"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.setMaxWorkers(5)
    supervisor.elaborate(Set("C","D","E","F"), parameters._2, 2, Set("C","D","E","F"), None) shouldBe DecisionResults(AddContainer, 1)
    supervisor.setMinWorkers(0)
    supervisor.elaborate(Set("C","D","E","F","G"), parameters._2, 2, Set("C","D","E","F","G"), None) shouldBe DecisionResults(Skip, 0)
    supervisor.setReadyWorkers(0)
    supervisor.elaborate(Set("A","B","C","D","E"), parameters._2, 2, Set("A","B","C","D","E"), None) shouldBe DecisionResults(RemoveReadyContainer(Set("E","A")), 0)
    supervisor.elaborate(Set("B","C","D"), parameters._2, 0, Set("B","C","D"), None) shouldBe DecisionResults(RemoveReadyContainer(Set("B","C")), 0)
    supervisor.elaborate(Set("D"), parameters._2, 0, Set("D"), None) shouldBe DecisionResults(RemoveReadyContainer(Set("D")), 0)
    supervisor.setReadyWorkers(2)
    supervisor.elaborate(parameters._1, parameters._2, 0, parameters._1, None) shouldBe DecisionResults(AddContainer, 2)
    supervisor.setMinWorkers(3)
    supervisor.elaborate(Set("A","B"), parameters._2, 0, Set("A","B"), None) shouldBe DecisionResults(AddContainer, 2)
  }
  def createContainers(num:Int): Set[String] = {

    @tailrec
    def iter(num: Int, containers: Set[String]): Set[String] = {
      if (num == 0) return containers
      iter(num - 1, containers ++ List(Random.alphanumeric.take(10).mkString))
    }

    iter(num, Set[String]())
  }

  def createEnv(nAllocated: Int, nCreation: Int, ready: Int, incomingReq: Int, enqueuedReq: Int, iar: Double, supervisor: QueueSupervisor): (Set[String], Int, Int,Set[String]) = {

    if( nAllocated < ready) return null

    supervisor.inProgressCreations.set(nCreation)
    supervisor.iar = iar
    val cont = addContainers(nAllocated, Set[String]())
    ( cont, incomingReq, enqueuedReq, cont.take(ready))

  }

  def addContainers(num: Int, list: Set[String]): Set[String] = {

    @tailrec
    def iter(num: Int, containers: Set[String]): Set[String] = {
      if (num == 0) return containers
      iter(num - 1, containers ++ Set(Random.alphanumeric.take(10).mkString))
    }

    iter(num, list)
  }

  class MockStateRegistry(namespace: String, action: String) extends StateRegistry(namespace,action){

    override def publishUpdate(value: TrackQueueSnapshot, iar: Int, maxWorkers: Int, minWorkers: Int, readyWorkers: Int, containerPolicy: String, activationPolicy: String, usages: List[InvokerUsage] ): Unit = {
    }

    override def clean(): Unit = {}

    override def getStates: Map[String, StateInformation] = {
      null
    }

    override def getUpdateStatus: UpdateState = null
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

    var watchCallbackMap = Map[String, WatchUpdate => Unit]()

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

