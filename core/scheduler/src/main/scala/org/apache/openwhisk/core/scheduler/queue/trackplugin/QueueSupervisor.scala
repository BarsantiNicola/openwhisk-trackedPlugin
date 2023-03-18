package org.apache.openwhisk.core.scheduler.queue.trackplugin

import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.connector.ActivationMessage
import org.apache.openwhisk.core.scheduler.queue.{AddContainer, AddInitialContainer, DecisionResults, Skip}

import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Timer, TimerTask}
import scala.concurrent.duration.{Duration, SECONDS}

case class UpdateState( update: Boolean, lastUpdate: Timestamp )

sealed trait SchedulePolicy
case object Monoblock extends SchedulePolicy  //  removes/adds immediately the requested containers

//  removes/adds the requested containers using a maximum fixed number of containers
case class Steps(num: Int) extends SchedulePolicy

//  removes/adds the requested containers in an exponential fashion(at every recursion the number of containers increases)
case object Exponential extends SchedulePolicy

//  the opposite of the exponential policy, at every recursion the number of containers decreases exponentially
case object Logarithmic extends SchedulePolicy

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
class QueueSupervisor( val namespace: String, val action: String )( implicit val logging: Logging, val stateRegistry : StateRegistry ) {

  // Containers control variables
  var maxWorkers : Int = 2   //  maximum number of assignable containers to the action
  var minWorkers : Int = 0   //  minimum number of assigned containers to the action
  var readyWorkers : Int = 0 //  minimum number of containers ready to accept a request assigned to the action

  // Counters for internal functionalities
  private[QueueSupervisor] var rejectedRequests = new AtomicInteger(0)    //  counter of the rejected activations in last 1m
  private[QueueSupervisor] val acceptedRequests = new AtomicInteger(0)    //  last minute arrivals(used to compute iar)
  val inProgressCreations = new AtomicInteger(0) //  counter of the containers in progress creations

  //  Timers for periodic tasks execution
  private[QueueSupervisor] val schedulerTimer = new Timer  // periodic scheduler execution
  private[QueueSupervisor] val metricsTimer = new Timer    // periodic metrics update execution
  private val schedulerPeriod : Duration = Duration( 30, SECONDS )  //  can be used to change runtime the period of scheduling

  //  Internal variables
  var iar: Double = 0                // [Metric] average inter-arrival rate of requests
  private[QueueSupervisor] var snapshot = Set.empty[String]  // Used to keep track of containers creation
  var policy : (SchedulePolicy, SchedulePolicy) = (Monoblock,Monoblock)

  private val timeout_test = System.currentTimeMillis()+180000

  //  periodic estimation of inter-arrival rate of requests
  metricsTimer.scheduleAtFixedRate( new TimerTask { //  chosen fixed rate to have more precise rate estimations
          def run(): Unit = {
            logging.debug( this, s"[$namespace/$action] Periodic IAR update : $iar -> ${(iar+acceptedRequests.get)/2}" )
            iar = ( iar + acceptedRequests.get())/2  //  moving average of IAR(not count too much on the previous results)
            acceptedRequests.set( 0 )                 //  resetting the arrival counter every 60s => arrival rate
            rejectedRequests.set( 0 )
          }
        }, 60000, 60000 )

  //  execution of periodic scheduling, the period can be changed runtime using changeSchedulerPeriod(period)
  schedulerTimer.schedule( new TimerTask{
          def run(): Unit = schedule( stateRegistry.getUpdateStatus, StateRegistry.getUpdateStatus(namespace, action), stateRegistry.getStates )
  }, 2000, schedulerPeriod.toMillis )  //  first delay fixed to give time to the system to initialize itself


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

  }

  /**
   * Function to define to handle appropriately the activation requests. It is called directly by the environment on
   * request arrival
   * @param msg the ActivationMessage received
   * @param containers the number of allocated containers
   * @param promises the number of ready to go containers
   * @return False if the request has to be rejected, True otherwise
   */
  def handleActivation(msg: ActivationMessage, containers: Int, promises: Int): Boolean = {
    logging.info(this, s"[$namespace/$action] DELEGATE_AM")
    val result = true
    if (!result)
      rejectedRequests.incrementAndGet()
    else
      acceptedRequests.incrementAndGet()
    result
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
  private def changeSchedulerPeriod( time: Duration ): Unit = {
    schedulerTimer.cancel()
    schedulerTimer.schedule( new TimerTask {
      def run(): Unit = schedule(stateRegistry.getUpdateStatus, StateRegistry.getUpdateStatus(namespace, action), stateRegistry.getStates)
    }, schedulerPeriod.toMillis, schedulerPeriod.toMillis)
  }

  /**
   * Functions to update the policy used by the containers management
   * @param addPolicy Policy that will be used to add containers
   * @param remPolicy Policy that will be used to remove containers
   */
  private def updatePolicies( addPolicy: SchedulePolicy, remPolicy: SchedulePolicy ): Unit = policy.synchronized{
    policy = (addPolicy, remPolicy)
  }
  private def updateAddPolicy = updatePolicies( _, policy._2 )
  private def updateRemPolicy = updatePolicies( policy._1 , _ )

  /**
   * Sets the number of maximum containers usable by the action. The value must be greated than the minWorker and
   * readyWorker. Note that it is accepted that the value is less than minWorker+readyWorker but this condition not guarantee
   * to have always the required readyWorkers. Also note that the required containers will not be immediately allocated but
   * require some time dependent from the environment(from 100ms to few seconds)
   * @param num The maximum number of containers to be set
   * @return True in case of success, false otherwise
   */
  private def setMaxWorkers(num: Int): Boolean = num match {
    case _ if num < 0 => logging.error(this, s"[$namespace/$action] Error, bad workers value. ($num < 0). Operation aborted"); false
    case _ if num < minWorkers => logging.error(this, s"[$namespace/$action] Error, bad workers value. ($num < $minWorkers)[maxWorkers<minWorkers]. Operation aborted"); false
    case _ if num < readyWorkers => logging.error(this, s"[$namespace/$action] Error, bad workers value. ($num < $readyWorkers)[maxWorkers<readyWorkers]. Operation aborted"); false

    case _ if minWorkers + readyWorkers > num =>
      logging.warn(
        this,
        s"[$namespace/$action] Attention, the given workers not permit to guarantee the requested ready workers" +
          s"($num < ${minWorkers + readyWorkers})[maxWorkers<minWorkers+readyWorkers]")
      maxWorkers = num;
      true
    case _ => maxWorkers = num; true

  }

  /**
   * Sets the minimum number of containers that must be alwats allocated to the action. The value must be greated than zero
   * and less than the readyWorkers and maxWorkers. Note that it is accepted that the value is less than minWorker+readyWorker
   * but this condition not guarantee to have always the required readyWorkers
   * @param num The minimum number of containers to be set
   * @return True in case of success, false otherwise
   */
  private def setMinWorkers(num: Int): Boolean = num match {
    case _ if num < 0 => logging.error(this, s"[$namespace/$action] Error, bad workers value. ($num < 0). Operation aborted"); false
    case _ if num > maxWorkers => logging.error(this, s"[$namespace/$action] Error, bad workers value. ($num > $maxWorkers)[minWorkers>maxWorkers]. Operation aborted"); false
    case _ if num < readyWorkers => logging.error(this, s"[$namespace/$action] Error, bad workers value. ($num < $readyWorkers)[minWorkers<readyWorkers]. Operation aborted"); false
    case _ if num + readyWorkers > maxWorkers =>
      logging.warn(
        this,
        s"[$namespace/$action] Attention, the given workers not permit to guarantee the requested ready workers." +
          s"($maxWorkers < ${num + readyWorkers})[maxWorkers<minWorkers+readyWorkers]");
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
  private def setReadyWorkers(num: Int): Boolean = num match {
    case _ if num < 0 => logging.error(this, s"[$namespace/$action] Error, bad workers value. ($num < 0). Operation aborted"); false
    case _ if num > maxWorkers => logging.error(this, s"[$namespace/$action] Error, bad workers value. ($num > $maxWorkers)[readyWorkers>maxWorkers]. Operation aborted"); false
    case _ if num < minWorkers => logging.error(this, s"[$namespace/$action] Error, bad workers value. ($num < $minWorkers)[readyWorkers<minWorkers]. Operation aborted"); false
    case _ if minWorkers + num > maxWorkers =>
      logging.warn(
        this,
        s"[$namespace/$action] Attention, the given workers not permit to guarantee the requested ready workers." +
          s"($maxWorkers < ${minWorkers + num})[maxWorkers<minWorkers+readyWorkers]")
      readyWorkers = num;
      true
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

    val counter : AtomicInteger = new AtomicInteger(0)
    containers.foreach{ containerId => if( snapshot.contains( containerId )) counter.getAndIncrement()}
    counter.get()
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
  def elaborate( containers: Set[String], incoming: Int, enqueued: Int ) : DecisionResults = {

    val i_iat :Int = math.round(iar).toInt  // Average interarrival rate of the requests

    val difference = computeAddedContainers(containers) //  evaluation of added containers from the last call
    if( difference > 0 ){  //  if some containers are added means that some inProgress creation are terminated

      //  cannot happen just a check
      if( inProgressCreations.get() - difference  >= 0 ) {
        inProgressCreations.getAndAdd( (-1)*difference )
      }else {
        logging.error(this, s"[$namespace/$action] Error during container elaboration, too many containers are added to the system")
        inProgressCreations.set(0)
      }
    }
    snapshot = containers  //  make new snapshot for the next elaborate() call
    val inProgress = inProgressCreations.get()

    //  TODO log only for testing purpose to be removed
    logging.info( this, s"[$namespace/$action] Container [containers: ${containers.size} queue: $enqueued iat: ${math.round(iar)} inProgress: $inProgress]")


    if( math.max(i_iat,incoming) + enqueued > containers.size + inProgress ){
      //  we haven't enough containers to manage the requests

      val neededContainersCount = math.max(i_iat,incoming) + enqueued  - containers.size - inProgress
      val neededContainers = maxWorkers - inProgress - containers.size - neededContainersCount >= 0

      //  we try to give the required containers or at least the maximum usable number
      val containersToAdd = if( neededContainers ) neededContainersCount else maxWorkers-containers.size-inProgress

      if( containersToAdd > 0 ) {
        logging.info(this, s"[$namespace/$action] ADDING $containersToAdd CONTAINERS")
        inProgressCreations.addAndGet( containersToAdd )
        DecisionResults( AddContainer, containersToAdd )
      } else
        DecisionResults(Skip, 0)

    }else{
      //  we have enough containers to manage the incoming requests

      if( containers.nonEmpty && System.currentTimeMillis() > timeout_test )
        DecisionResults(RemoveReadyContainer(containers.toSet),0)
      else
        DecisionResults(Skip,0)
    /*  val removable :Int = containers - i_iat - enqueued

      if( removable > 0 && counter.get() == 0 )
        return DecisionResults(RemoveReadyContainer(1), 0)
*/
      //

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
   * @param snapshot
   * @return
   */
  def delegate( snapshot: TrackQueueSnapshot ): DecisionResults = {
    stateRegistry.publishUpdate(snapshot)
    this.elaborate( snapshot.currentContainers, snapshot.incomingMsgCount.get(), snapshot.currentMsgCount)

  }

  /**
   * Used as a block for the Idle condition queue state. In the default configuration the queue will became idle and
   * after some time it will automatically remove it, however this gives problem when is our system that blocks requests.
   * The solution is to let the queue consider not only the number of enqueued requests but also the number of recently(1m)
   * rejected requests. They have to be 0 both to pass to idle state
   * @return
   */
  def activationsRecentlyRejected() : Int = rejectedRequests.get()



}



