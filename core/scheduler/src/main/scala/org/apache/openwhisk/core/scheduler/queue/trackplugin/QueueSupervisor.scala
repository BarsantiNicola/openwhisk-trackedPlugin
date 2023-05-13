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
package org.apache.openwhisk.core.scheduler.queue.trackplugin

import com.google.gson.Gson
import org.apache.openwhisk.common.{InvokerHealth, Logging}
import org.apache.openwhisk.core.connector.ActivationMessage
import org.apache.openwhisk.core.entity.Parameters
import org.apache.openwhisk.core.etcd.EtcdClient
import org.apache.openwhisk.core.scheduler.SchedulingSupervisorConfig
import org.apache.openwhisk.core.scheduler.queue.{AddContainer, AddInitialContainer, DecisionResults, Pausing, Skip}
import spray.json.DefaultJsonProtocol._

import java.util.concurrent.atomic.AtomicInteger
import java.util.{Timer, TimerTask}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, MILLISECONDS}
import org.apache.openwhisk.common.InvokerState.{Healthy, Offline, Unhealthy}
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.core.etcd.EtcdKV.InvokerKeys
import org.apache.openwhisk.core.etcd.EtcdType._
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

/**
 * Class nested to the SchedulingDecisionMaker actor to control the action containers management. The core parts of the
 * component are represented by the schedule function which can control the number of used containers by changing a set
 * of variables(minWorkers, maxWorkers, readyWorkers) and the handleActivation function which can be used to accept or
 * reject incoming workloads
 *
 * The class is called by:
 * - the MemoryQueue during TrackedRun/TrackedIdle state to delegate the ActivationMessage acceptance
 * - the SchedulingDecisionMaker after the receival of a QueueSnapshot update message to delegate its management
 *
 * The class is able to:
 * - create/remove containers
 * - accept or reject ActivationMessage requests
 * - performing an automatic management of the containers accordingly to a set of parameters
 *
 * @param namespace name of the invocation namespace assigned to the memoryQueue
 * @param action    name of the action assigned to the memoryQueue
 */

class QueueSupervisor( val namespace: String, val action: String, supervisorConfig: SchedulingSupervisorConfig, val annotations: Parameters, val stateRegistry : StateRegistry, val etcdClient: EtcdClient )(implicit val logging: Logging, ec: ExecutionContext) {

  // Containers control variables
  implicit var maxWorkers: Int = annotations.getAs[Int]("max-workers").getOrElse(supervisorConfig.maxWorkers)
  //  maximum number of assignable containers to the action
  implicit var minWorkers: Int = annotations.getAs[Int]("min-workers").getOrElse(supervisorConfig.minWorkers)
  //  minimum number of assigned containers to the action
  implicit var readyWorkers: Int = annotations.getAs[Int]("ready-workers").getOrElse(supervisorConfig.readyWorkers)
  //  minimum number of containers ready to accept a request assigned to the action

  //  used to understand if a dynamic change of action annotations is happened
  private var annotatedMin : Int = minWorkers
  private var annotatedMax : Int = maxWorkers
  private var annotatedReady : Int = readyWorkers

  // Counters for internal functionalities
  private[QueueSupervisor] var rejectedRequests = new AtomicInteger(0)    //  counter of the rejected activations in last 1m
  private[QueueSupervisor] val acceptedRequests = new AtomicInteger(0)    //  last minute arrivals(used to compute iar)
  implicit val inProgressCreations: AtomicInteger = new AtomicInteger(0) //  counter of the containers in progress creations

  //  Timers for periodic tasks execution
  private[QueueSupervisor] var schedulerTimer = new Timer // periodic scheduler execution
  private[QueueSupervisor] val metricsTimer = new Timer    // periodic metrics update execution
  private var schedulerPeriod: Duration = Duration(1000, MILLISECONDS) //  can be used to change runtime the period of scheduling

  //  Internal variables
  private var maxAddingTime : Option[Long] = None  //  time used to identify an error(system unable to satisfy the containers creations)
  private var executionTime : Option[Double] = None  // used for evaluating the iar(we are interested in how many requests arrive into an execution time)
  private val creationTme : Double = 500  //  used as an adding offset for the iar computation to consider also container creation time TODO evaluated dynamically
  private var onFlushTimeout : Option[Long] = None
  private var invokersState : Option[List[InvokerUsage]] = None
  private val gson = new Gson()

  var iar: Double = 0                // [Metric] average inter-arrival rate of requests
  private[QueueSupervisor] var snapshot = Set.empty[String]  // Used to keep track of containers creation
  private[QueueSupervisor]  var containerPolicy : ContainerSchedulePolicy = annotations
    .getAs[String]("container-policy")
    .getOrElse(supervisorConfig.schedulePolicy) match {
      case "AsRequested" => AsRequested()
      case "Steps" => Steps(supervisorConfig.step)
      case "Poly" =>  Poly(supervisorConfig.grade)
      case "IPoly" => IPoly(supervisorConfig.grade)
      case "Fibonacci" => Fibonacci()
      case "All"  =>  All()
      case _ => AsRequested()
  }

  private[QueueSupervisor] var activationPolicy : ActivationSchedulePolicy = annotations
    .getAs[String]("activation-policy")
    .getOrElse(supervisorConfig.activationPolicy) match {
      case "AcceptAll" => AcceptAll()
      case "AcceptTill" => AcceptTill(supervisorConfig.maxActivationConcurrency)
      case "AcceptEvery" => AcceptEvery(supervisorConfig.maxActivationConcurrency, Duration(supervisorConfig.acceptPeriodInMillis,MILLISECONDS))
      case "RejectAll" => RejectAll()
      case _ => AcceptAll()
  }

  private[QueueSupervisor] var invokerPriorityPolicy : InvokerPriorityPolicy = annotations
    .getAs[String]("invoker-priority-policy")
    .getOrElse(supervisorConfig.invokerPriorityPolicy) match {
    case "Consolidate" => Consolidate()
    case "Balance" => Balance()
    case _ => Consolidate()
  }

  //  periodic estimation of inter-arrival rate of requests
  metricsTimer.scheduleAtFixedRate( new TimerTask { //  chosen fixed rate to have more precise rate estimations
          def run(): Unit =  executionTime match{
            case Some(value:Double) if value > 0 => iar = ( iar + (acceptedRequests.getAndSet(0) + rejectedRequests.getAndSet(0))/value)/2
            case _ => iar = 0
          }
        }, 1000, 60000 )

  //  execution of periodic scheduling, the period can be changed runtime using changeSchedulerPeriod(period)
  if( annotations.getAs[Boolean]("reactive").getOrElse(true))
      schedulerTimer.scheduleAtFixedRate( new TimerTask{
          def run(): Unit = schedule( stateRegistry.getUpdateStatus, StateRegistry.getUpdateStatus(namespace, action), stateRegistry.getStates )
      }, 1000, schedulerPeriod.toMillis )  //  first delay fixed to give time to the system to initialize itself


  invokerUpdate

  /**
   * Function to define the scheduling behavior of the action queue. It is called periodically by the instance and can interact
   * with the environment via the following set of functions:
   * - changeSchedulerPeriod: modify the period in which this function is called by the environment
   * - setMaxWorkers: change the maximum number of containers that the supervisor can allocate
   * - setMinWorkers: change the minimum number of containers that the supervisor has to assign to the action
   * - setReadyWorkers: change the minimum number of ready containers that the supervisor has to guarantee to be always available the action
   * @param localUpdateState: gives information about the queue(if some change happened, and the time passed from the last update)
   * @param globalUpdateState: gives information about the global state(if some change happened, and the time passed from the last update)
   * @param states: map with all the information available of the instantiated queues( "namespace--action" -> StateInformation )
   */
  def schedule(localUpdateState: UpdateState, globalUpdateState: UpdateState, states: Map[String, StateInformation]): Unit = {
    invokerUpdate
      //
      //  PLACE YOUR DYNAMIC SCHEDULER BEHAVIOR HERE
      //
  }

  /**
   * Function to define to handle appropriately the activation requests. It is called directly by the environment on
   * request arrival
   * @param msg the ActivationMessage received
   * @param containers the number of allocated containers
   * @param ready  the number of containers ready to accept a request
   * @param enqueued the number of request stored into the queue
   * @param incoming the number of request incoming to the system
   * @return False if the request has to be rejected, True otherwise
   */
  def handleActivation(msg: ActivationMessage, containers: Int, ready: Int, enqueued: Int, incoming: Int): Boolean = {

    val result = activationPolicy.handleActivation(msg, containers, ready, enqueued, incoming, math.round(iar) )
    if (!result)
      rejectedRequests.incrementAndGet()
    else
      acceptedRequests.incrementAndGet()
    result
  }

  private def update(annotations: Parameters): Unit = {
    val res = (
      annotations.getAs[Int]("max-workers").getOrElse(maxWorkers),
      annotations.getAs[Int]("min-workers").getOrElse(minWorkers),
      annotations.getAs[Int]("ready-workers").getOrElse(readyWorkers))

    if( res._2 != annotatedMin){
      annotatedMin = res._2
      setMinWorkers(res._2)
    }

    if( res._1 != annotatedMax){
      annotatedMax = res._1
      setMaxWorkers(res._1)
    }

    if( res._3 != annotatedReady ){
      annotatedReady = res._3
      setReadyWorkers(res._1)
    }

  }

  /**
   * Function to define to handle the initialization of the queue. The only meaningful behaviour that can be placed here
   * for me is the creation or not of some containers, remember however that the creation of a MemoryQueue is done as a
   * consequence of the arrival of a first message that needs to be processed by some container
   * @return a message compatible with DecisionResults that will be received by to the associated MemoryQueue
   */
  def initStrategy(): DecisionResults = {

    inProgressCreations.incrementAndGet()
    DecisionResults(AddInitialContainer, 1)

  }

  private def invokerUpdate: Future[Unit] = getInvokerStates()(etcdClient, ec).map { invokers =>
    invokersState = Option(invokers.map { invoker => InvokerUsage(invoker.id.instance, if (invoker.status.isUsable) invoker.id.userMemory.toMB else 0) })
    invokersState.foreach {
      usages: List[InvokerUsage] => updateInvokersPriority(invokerPriorityPolicy.compute(computeGlobalInvokersUsage(usages)))(etcdClient)
    }
  }

  /**
   * Function called by the queue during initialization phase to set the timeout after discarding requests
   * @return the time to wait expressed in milliseconds. If the return is less or equal 0 it will use the default configuration
   */
  def getTimeout: Long = {
    0
  }

  //// USABLE FUNCTIONS

  /**
   * Changes the scheduling call period without interfering with the actual execution
   * @param time new period to be applied(meaningful only if greater of 200ms, the system evolution happens with a period of 100-170ms)
   */
  def changeSchedulerPeriod( time: Duration ): Unit = {
    schedulerTimer.cancel()
    schedulerPeriod= time
    schedulerTimer = new Timer
    schedulerTimer.scheduleAtFixedRate( new TimerTask {
      def run(): Unit = schedule(stateRegistry.getUpdateStatus, StateRegistry.getUpdateStatus(namespace, action), stateRegistry.getStates)
    }, 0, schedulerPeriod.toMillis)
  }

  /**
   * Sets the number of maximum containers usable by the action. The value must be greated than the minWorker and
   * readyWorker. Note that it is accepted that the value is less than minWorker+readyWorker but this condition not guarantee
   * to have always the required readyWorkers. Also note that the required containers will not be immediately allocated but
   * require some time dependent from the environment(from 100ms to few seconds)
   * @param num The maximum number of containers to be set
   * @return True in case of success, false otherwise
   */
  def setMaxWorkers(num: Int): Boolean = num match {
    case _ if num < 0 => logging.error(this, s"[$namespace/$action] Error, bad workers value. ($num < 0). Operation aborted"); false
    case _ if num < minWorkers => logging.error(this, s"[$namespace/$action] Error, bad workers value. ($num < $minWorkers)[maxWorkers<minWorkers]. Operation aborted"); false
    case _ if num < readyWorkers => logging.error(this, s"[$namespace/$action] Error, bad workers value. ($num < $readyWorkers)[maxWorkers<readyWorkers]. Operation aborted"); false

    case _ if minWorkers + readyWorkers > num =>
      logging.warn(
        this,
        s"[$namespace/$action] Attention, the given workers not permit to guarantee the requested ready workers" +
          s"($num < ${minWorkers + readyWorkers})[maxWorkers<minWorkers+readyWorkers]")
      maxWorkers = num; true
    case _ => maxWorkers = num; true

  }

  /**
   * Sets the minimum number of containers that must be alwats allocated to the action. The value must be greated than zero
   * and less than the readyWorkers and maxWorkers. Note that it is accepted that the value is less than minWorker+readyWorker
   * but this condition not guarantee to have always the required readyWorkers
   * @param num The minimum number of containers to be set
   * @return True in case of success, false otherwise
   */
  def setMinWorkers(num: Int): Boolean = num match {
    case _ if num < 0 => logging.error(this, s"[$namespace/$action] Error, bad workers value. ($num < 0). Operation aborted"); false
    case _ if num > maxWorkers => logging.error(this, s"[$namespace/$action] Error, bad workers value. ($num > $maxWorkers)[minWorkers>maxWorkers]. Operation aborted"); false
    case _ if num + readyWorkers > maxWorkers =>
      logging.warn(
        this,
        s"[$namespace/$action] Attention, the given workers not permit to guarantee the requested ready workers." +
          s"($maxWorkers < ${num + readyWorkers})[maxWorkers<minWorkers+readyWorkers]")
      true
    case _ => minWorkers = num; true
  }

  /**
   * Sets the minimum number of containers that must be always allocated to the action and be ready to execute a task.
   * The value must be greated than zero and less than the minWorkers and maxWorkers. Note that it is accepted that the value
   * is less than minWorker+readyWorker is greater than maxWorkers but this condition not guarantee to have always the required readyWorkers
   * Also note that the required containers will not be immediately allocated but it will require some time dependent from
   * the environment(from 100ms to few seconds)
   *
   * @param num The number of ready containers to be set
   * @return True in case of success, false otherwise
   */
  def setReadyWorkers(num: Int): Boolean = num match {
    case _ if num < 0 => logging.error(this, s"[$namespace/$action] Error, bad workers value. ($num < 0). Operation aborted"); false
    case _ if num > maxWorkers => logging.error(this, s"[$namespace/$action] Error, bad workers value. ($num > $maxWorkers)[readyWorkers>maxWorkers]. Operation aborted"); false
    case _ if num < minWorkers => logging.error(this, s"[$namespace/$action] Error, bad workers value. ($num < $minWorkers)[readyWorkers<minWorkers]. Operation aborted"); false
    case _ if minWorkers + num > maxWorkers =>
      logging.warn(
        this,
        s"[$namespace/$action] Attention, the given workers not permit to guarantee the requested ready workers." +
          s"($maxWorkers < ${minWorkers + num})[maxWorkers<minWorkers+readyWorkers]")
      readyWorkers = num; true
    case _ => readyWorkers = num; true
  }

  ////  INTERNAL FUNCTIONS[TO NOT BE USED]

  /**
   * Evaluate the variation of the assigned containers respect to a snapshot.
   * The function is used to compute the variation of contaienrs and understand which inProgress creation are terminated
   * This is necessary because containers has different times respect to the scheduler periodicity. Moreover we cannot
   * just count the containers, many operation can happen in the call period and it is not rare that a container is destroyed
   * while another is created messing the inProgress Counter
   * @param containers A set of containerId to be compared with the snapshot
   * @return The number of containerId not present in the snapshot
   */
  private def computeAddedContainers( containers: Set[String] ): Int = {
    var counter : Int = 0
    containers.foreach{ containerId => if( !snapshot.contains( containerId )) counter+=1}
    counter
  }

  /**
   * Core container management function. It will interact with the associated queue to add/remove containers
   * to respect the minimum/maximum/ready-Workers parameters basing on the information given by the underlying environment
   * @param containers A set of containerId representing the containers associated with the action
   * @param incoming   A value computed by the environment representing the incoming requests(already received but still
   *                   under management)
   * @param enqueued   A value computed by the environment representing the number of enqueued requests
   * @return Returns three types of messages: AddContainer(num)[Add num containers], RemoveReadyContainers(num)[Remove num containers], SKip[do nothing]
   */
  def elaborate( containers: Set[String], incoming: Int, enqueued: Int, readyContainers: Set[String], et: Option[Double]) : DecisionResults = {

    executionTime = et match{
      case Some(value:Double) => Option(60000/value)
      case _ => Option(60000/creationTme)
    }

    val i_iat :Int = math.round(iar).toInt  // Average interarrival rate of the requests
    logging.info(this, s"[Framework-Analysis][$namespace/$action][Data] { 'kind': 'supervisor-state', 'containers': ${containers.size}, 'iar': $i_iat, 'incoming': $incoming, 'enqueued': $enqueued, 'ready': ${readyContainers.size}, 'timestamp': ${System.currentTimeMillis()}}")

    val difference = computeAddedContainers(containers) //  evaluation of added containers from the last call

    if( difference > 0 ){  //  if some containers are added means that some inProgress creation are terminated

      maxAddingTime = None
      //  cannot happen just a check
      if( inProgressCreations.get() - difference  >= 0 ) {
        inProgressCreations.getAndAdd( (-1)*difference )
      }else {
        logging.error(this, s"[$namespace/$action] Error during container elaboration, too many containers are added to the system")
        inProgressCreations.set(0)
      }

    }else{

      //  we have some requests to create containers in elaboration
      if( inProgressCreations.get() > 0 )
        maxAddingTime match{  //  however too much time is passed without any container created
          case Some(time: Long) if time < System.currentTimeMillis() && containers.isEmpty => return DecisionResults(Pausing, 0)
          case _ =>
      }
    }

    snapshot = containers  //  make new snapshot for the next elaborate() call
    val inProgressCreationsCount = inProgressCreations.get()

    containerPolicy.grant( minWorkers, readyWorkers, maxWorkers, containers.size, readyContainers, inProgressCreationsCount, i_iat, enqueued, incoming) match{
      case DecisionResults(AddContainer, value) =>
        inProgressCreations.addAndGet(value)
        maxAddingTime = Option(System.currentTimeMillis()+300000)  // set a limit of 5m for the containers creation
        DecisionResults(AddContainer,value)
      case value => value
    }
  }

  /**
   * Called on MemoryQueue destroy. It will stop the timers and propagate the clean to the StateRegistry
   */
  def clean(): Unit = {

    stateRegistry.clean()
    metricsTimer.cancel()
    schedulerTimer.cancel()

  }


  /**
   * Function called periodically by the underlying environment for propagate the queue state. The given information
   * will be stored and eventually propagated to the other schedulers. At the same time the information are used
   * for the internal management of the QueueSupervisor(add remove/container, update inProgress creations)
   * @param snapshot Snapshot given by the memoryQueue which describe its state
   * @return DecisionResults(AddContainer,num) to add containers
   *         DecisionReults(RemoveReadyContainers(ids),0) to remove containers
   *         DecisionResults(Skip,0) to do nothing
   */
  def delegate( snapshot: TrackQueueSnapshot, annotations: Parameters ): DecisionResults = {

    this.update(annotations)

    stateRegistry.publishUpdate(
      snapshot,
      math.round(iar).toInt,
      maxWorkers,
      minWorkers,
      readyWorkers,
      containerPolicy.toString,
      activationPolicy.toString,
      invokersState.getOrElse(List.empty[InvokerUsage]))

    this.elaborate(
      snapshot.currentContainers.diff(snapshot.onRemoveContainers),
      snapshot.incomingMsgCount.get(),
      snapshot.currentMsgCount,
      snapshot.readyContainers.diff(snapshot.onRemoveContainers),
      snapshot.averageDuration
    )

  }



  private def computeGlobalInvokersUsage(personal: List[InvokerUsage]): List[InvokerUsage] = {

    if( stateRegistry.getUpdateStatus.update || StateRegistry.getUpdateStatus(namespace, action).update){
        stateRegistry.getStates
          .map{ record =>record._2.invokersState.toList}
          .reduce{(x,y) => x++y}
          .groupBy{_.invokerId}
          .map{ merged => InvokerUsage(merged._1, merged._2.map(_.usage).sum)}.toList

    }else
      personal
  }

  /**
   * Used as a block for the Idle condition queue state. In the default configuration the queue will became idle and
   * after some time it will automatically remove it, however this gives problem when is our system that blocks requests.
   * The solution is to let the queue consider not only the number of enqueued requests but also the number of recently(1m)
   * rejected requests. They have to be 0 both to pass to idle state
   * @return
   */
  def activationsRecentlyRejected() : Int = rejectedRequests.get()

  def failedJob() : Unit = inProgressCreations.decrementAndGet()

  def tryResolveFlush(): DecisionResults = {
    onFlushTimeout match {
      case Some(timeout: Long) if System.currentTimeMillis() > timeout =>
        onFlushTimeout = Option(System.currentTimeMillis() + 60000)
        inProgressCreations.set(1)
        DecisionResults(AddContainer, 1)
      case None => onFlushTimeout = Option(System.currentTimeMillis() + 60000)
        inProgressCreations.set(1)
        DecisionResults(AddContainer, 1)
      case _ => DecisionResults(Skip, 0)
    }
  }

  private def getInvokerStates()(implicit etcdClient: EtcdClient, ec: ExecutionContext): Future[List[InvokerHealth]] = {
      etcdClient
        .getPrefix(InvokerKeys.prefix)
        .map { res =>
          res.getKvsList.asScala
            .map { kv =>
              InvokerResourceMessage
                .parse(kv.getValue.toString(StandardCharsets.UTF_8))
                .map { resourceMessage =>
                  val status = resourceMessage.status match {
                    case Healthy.asString => Healthy
                    case Unhealthy.asString => Unhealthy
                    case _ => Offline
                  }

                  val temporalId = InvokerKeys.getInstanceId(kv.getKey.toString(StandardCharsets.UTF_8))
                  val invoker = temporalId.copy(
                    userMemory = resourceMessage.freeMemory.MB,
                    busyMemory = Some(resourceMessage.busyMemory.MB),
                    tags = resourceMessage.tags,
                    dedicatedNamespaces = resourceMessage.dedicatedNamespaces)
                  InvokerHealth(invoker, status)
                }
                .getOrElse(InvokerHealth(InvokerInstanceId(kv.getKey, userMemory = 0.MB), Offline))
            }
            .filter(i => i.status.isUsable)
            .toList
        }
    }

  private def updateInvokersPriority(priorities: List[InvokerPriority])(implicit etcdClient: EtcdClient) : Unit = {
    logging.info(this, s"[Framework-Analysis][Data] {'kind': 'priority-update', 'update': '${gson.toJson(priorities.toSet)}', 'timestamp': ${System.currentTimeMillis()}}")
    priorities.foreach{ priority =>
      etcdClient.put( s"$namespace-$action-priority-${priority.invokerId}", priority.priority.toString)
    }
  }

}

case class InvokerPriority(invokerId: Int, priority: Long)



