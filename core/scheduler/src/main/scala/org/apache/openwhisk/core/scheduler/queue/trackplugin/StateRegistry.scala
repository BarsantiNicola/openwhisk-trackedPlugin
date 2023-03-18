package org.apache.openwhisk.core.scheduler.queue.trackplugin

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.etcd.EtcdClient
import org.apache.openwhisk.core.service.{DeleteEvent, PutEvent, WatchEndpoint, WatchEndpointInserted, WatchEndpointRemoved}
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.sql.Timestamp
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Random, Success}

//  Set of messages returned by StateInformation.equals
sealed trait CheckResult
case object UpdateForChange extends CheckResult
case object UpdateForRenew extends CheckResult
case object NotUpdate extends CheckResult

/**
 * class for storing queue information. It is just a reduction of the QueueSnapshot to not include global information
 * like existingContainerCount or inProgressContainerCount(only local are needed)
 * @param initialized : Boolean: defines if the memoryQueue is initialized
 * @param incomingMsgCount : Int : defines the number of incoming message for the namespace
 * @param currentMsgCount : Int : defines the number of messages currently enqueued
 * @param staleActivationNum Int : containers not working
 * @param existingContainerCountInNamespace : Int: total containers associated to the namespace
 * @param inProgressContainerCountInNamespace : Int: total containers in the namespace which are ending the last creation part
 * @param averageDuration : Int : mean time of an action execution
 * @param limit : Int : maximum number of activations in the queue
 * @param stateName : Int : current state of the MemoryQueue
 * @param timestamp: Long : time instant associated to the information. I used a Long, cause Timestamp gives problem to openwhisk
 */
case class StateInformation(
                             var initialized: Boolean,
                             var incomingMsgCount: Int,
                             var currentMsgCount: Int,
                             var staleActivationNum: Int,
                             var existingContainerCountInNamespace: Int,
                             var inProgressContainerCountInNamespace: Int,
                             var averageDuration: Double,
                             var limit: Int,
                             var stateName: String,
                             var timestamp: Long
                           ) extends Serializable {
  def this(snapshot: TrackQueueSnapshot) {
    this(
      snapshot.initialized,
      snapshot.incomingMsgCount.intValue(),
      snapshot.currentMsgCount,
      snapshot.staleActivationNum,
      snapshot.existingContainerCountInNamespace,
      snapshot.inProgressContainerCountInNamespace,
      snapshot.averageDuration.getOrElse(0),
      snapshot.limit,
      snapshot.stateName.toString,
      System.currentTimeMillis())
  }

  def this(data: Map[String, String]) {
    this(
      data("initialized").toBoolean,
      data("incomingMsgCount").toInt,
      data("currentMsgCount").toInt,
      data("staleActivationNum").toInt,
      data("existingContainerCountInNamespace").toInt,
      data("inProgressContainerCountInNamespace").toInt,
      data("averageDuration").toDouble,
      data("limit").toInt,
      data("stateName"),
      data("timestamp").toLong,
    )
  }

  /**
   * Function to compare the differences between two updates
   * @param update new value to be compared to
   * @return UpdateForChange if the instances are different
   *         UpdateForRenew  if the update is a copy of the previous
   *         NotUpdate       if the update is older than the stored one
   */
  def check(update: StateInformation): CheckResult = {

    if( update.timestamp < this.timestamp )
      return NotUpdate

    val needUpdate = update.initialized != this.initialized ||
      update.incomingMsgCount != this.incomingMsgCount ||
      update.currentMsgCount != this.currentMsgCount ||
      update.staleActivationNum != this.staleActivationNum ||
      update.existingContainerCountInNamespace != this.existingContainerCountInNamespace ||
      update.inProgressContainerCountInNamespace != this.inProgressContainerCountInNamespace ||
      update.averageDuration != this.averageDuration ||
      update.limit != this.limit ||
      update.stateName.compareTo(this.stateName) != 0

    if (needUpdate) UpdateForChange else UpdateForRenew

  }

  /**
   * Redefinition of the toString method to be a marshalling easy usable with the spray.json library
   * TODO to change to use POJO spray marshalling/unmarshalling
   * @return
   */
  override def toString: String = Map[String, String](
    "initialized" -> this.initialized.toString,
    "incomingMsgCount" -> this.incomingMsgCount.toString,
    "currentMsgCount" -> this.currentMsgCount.toString,
    "staleActivationNum" -> this.staleActivationNum.toString,
    "existingContainerCountInNamespace" -> this.existingContainerCountInNamespace.toString,
    "inProgressContainerCountInNamespace" -> this.inProgressContainerCountInNamespace.toString,
    "averageDuration" -> this.averageDuration.toString,
    "limit" -> this.limit.toString,
    "stateName" -> this.stateName,
    "timestamp" -> this.timestamp.toString
  ).toJson.compactPrint
}

/**
 * Class for the creation of a StateRegistry able to share state information with all the instances indendently from their
 * position(locally or on another host). The class uses the Etcd database and the WatcherService actor for creating a content
 * distribution mechanism between the various instances
 * @param namespace        namespace name associated to the MemoryQueue
 * @param action           action name associated to the MemoryQueue. Namespace+action must be unique, no checks are made
 * @param watcherService   watcherService actorRef used for the creation of WatchEndpoints for the onRequest updates
 * @param logging          logger service
 * @param actorSystem      class for the creation of actors. Used to interconnect the class with the WatchEndpoints
 * @param etcdClient       client for interacting with Etcd. Used to store data and initial data retrieve on etcd
 * @param ec               ExecutionContext used to manage Futures
 */
class StateRegistry(
                     val namespace : String,
                     val action : String
                   )(
                     implicit val watcherService: ActorRef,    //  needed to receive automatic updates from WatcherService
                     implicit val logging: Logging,
                     implicit val actorSystem: ActorSystem,
                     implicit val etcdClient: EtcdClient,   //  needed to directly interact with Etcd(store/remove data)
                     implicit val ec : ExecutionContext
                   ){


  private val schedulerId : String = if( StateRegistry.isInit ) StateRegistry.schedulerId.getOrElse("") else{
    Random.setSeed(System.currentTimeMillis())
    Random.alphanumeric.take(10).mkString
  }
  val watcherName: String = s"information-receiver-$schedulerId"

  if( !StateRegistry.isInit ) StateRegistry.init(schedulerId, watcherService)

  private var update : Boolean = StateRegistry.stateRegistry.nonEmpty
  private var lastUpdate : Timestamp = if( StateRegistry.updateRegistry.nonEmpty) StateRegistry.lastUpdate else new Timestamp(System.currentTimeMillis())


  private def forwardUpdate( value: StateInformation ): Unit = {

    etcdClient.put( s"whisk/$watcherName--$namespace--$action", value.toString ).andThen{
      case Success(_) => logging.info(this, s"[$schedulerId/$namespace/$action] Data for $namespace correctly stored on ETCD")
      case Failure(e) => logging.info(this, s"Error during storage of namespace $namespace -> ${e.toString}")
    }
  }

  def publishUpdate(value: TrackQueueSnapshot ): Unit = {
    val updateReq = new StateInformation( value )
    if (StateRegistry.addUpdate( namespace, action, updateReq)) {
      update = true
      lastUpdate = new Timestamp(System.currentTimeMillis())
      forwardUpdate(updateReq)
      logging.info(this, s"Forwarding an update from $schedulerId for $namespace:\n ${updateReq.toString}")
    }
  }

  def clean(): Unit = {
    StateRegistry.removeState( namespace, action, global = true )(etcdClient)
  }

  def getStates : Map[String,StateInformation] = {
    update = false
    StateRegistry.getStates(namespace, action)
  }

  def getUpdateStatus: UpdateState = UpdateState( update, lastUpdate )

}

/**
 * Global shared information registry between instances on the same client. The component is in charge to interact
 * with the other hosts sharing updates in order to keep in each host the same information
 */
object StateRegistry{

  //  map which associate to each $namespace--$action key the associated StateInformation
  val stateRegistry: TrieMap[String, StateInformation] = new TrieMap[String, StateInformation]()

  //  map used to manage the update status of each client interacting with the global StateRegistry
  private val updateRegistry: TrieMap[String, Boolean] = new TrieMap[String, Boolean]()

  //  timestamp of the last change on the stateRegistry
  private var lastUpdate: Timestamp = new Timestamp(System.currentTimeMillis())

  //  reference to the InformationReceiver actor which receives updates coming from other schedulers
  private var share : Option[ActorRef] = None

  //  internal variables used to share information to all the instances
  private var schedulerId: Option[String] = None
  private var watcherName: Option[String] = None

  /**
   * Function used to change the update map for the managing of the update status.
   * An function call, every instance must change its update status to true except the calling one,
   * on that we preserve the state already present(for it is not a global update but a local one)
   * @param namespace namespace of the instance requiring the update
   * @param action    action of the instance requiring the update
   */
  private def update(namespace: String, action: String): Unit = {

    //  can be first update, in the case we manually put it
    updateRegistry.getOrElseUpdate(s"$namespace--$action" , stateRegistry.nonEmpty)
    //  every instance must change its update status to true except the calling one
    //  on the calling one we preserve the state already present
    updateRegistry.foreach{ key => if (key._1 == s"$namespace--$action") key._2 else updateRegistry.replace(key._1, true )}

  }

  /**
   * Function used to retrieve the states from the registry. It must be used to access the registry because it automatically
   * adjusts the update map
   * @param namespace  name of the namespace of the calling instance
   * @param action     name of the action of the calling instance
   * @return A map containing all the avaialable state information in the form "$namespace--$action" -> StateInformation
   */
  def getStates(namespace: String, action: String): Map[String,StateInformation] = {
    updateRegistry.get(s"$namespace--$action") match{
      case Some(x) if x => updateRegistry.replace(s"$namespace--$action", false)
      case None => updateRegistry.put(s"$namespace--$action", false )
      case _ =>
    }
    StateRegistry.stateRegistry.toMap
  }

  /**
   * Function used to remove the state of an instance from the registry. It assumes that the StateRegistry instance will
   * destroyed soon and removes its information from the global shared state. The remove is considered an update
   * @param namespace  namespace name of the required instance to remove
   * @param action     action name of the required instance to remove
   */
  private def removeUpdate(namespace: String, action: String): Unit = {

    // updating all the instances
    update( namespace, action )  //  important that is used before removing the update, otherwise will reinsert it
    // removing the instance from the update map
    updateRegistry.remove(s"$namespace--$action")
    //  removing the instance state
    stateRegistry.remove(s"$namespace--$action")

  }

  /**
   * Returns the update flag associated with the given instance
   * @param namespace namespace name of the instance
   * @param action    action name of the instance
   * @return          returns an UpdateState class containing the update flag and timestamp of the last states change
   */
  def getUpdateStatus( namespace: String, action: String ): UpdateState = {

    //  if it is not present we automatically add it
    UpdateState(
      updateRegistry.getOrElse( s"$namespace--$action", {
        updateRegistry.put( s"$namespace--$action", stateRegistry.nonEmpty )
        stateRegistry.nonEmpty
      }),
      lastUpdate )
  }

  /**
   * Variable which returns if the object is initialized or not
   * @return
   */
  private def isInit : Boolean = share.orNull != null

  /**
   * Initialization function, must be colled once. Other request will not produce any effect
   * The function creates an akka actor and connect it to the WatcherService in order to receive
   * updates made by other StateRegistry remote instances
   * @param schedId          : unique id representing the scheduler
   * @param watcherService   : actorRef to the WatcherServie for the creation of a WatchEndpoint
   * @param etcdClient       : instance of a connector class to operate directly on Etcd
   * @param logging          : logger
   * @param ec               : ExecutionContext for the Future management
   * @param actorSystem      : instance of ActorSystem for the creation of akka actors
   */
  def init( schedId : String, watcherService : ActorRef )
          (implicit etcdClient: EtcdClient, logging: Logging, ec:ExecutionContext, actorSystem: ActorSystem): Unit = {

    //  we have to consider synchronization between StateRegistry instances
    share.synchronized {
      if( share.isEmpty ){
        this.schedulerId = Option(schedId)
        this.watcherName = Option(s"information-receiver-$schedId")

        //  creation of the akka actor for manage the remote updates
        share = Option(actorSystem.actorOf(InformationReceiver.props(schedId, watcherService), schedId))

        //  initialization of the stateRegistry with the information already available on etcd
        //  whisk/information-receier- is the prefix common to all the keys creates by the StateRegistry
        etcdClient.getPrefix(s"whisk/information-receiver-").map {
          result =>
            result.getKvsList.forEach {
                  //  key parsing => [0]= schedulerId, [1] = namespace, [2] = action
              key => val values = key.getKey.toString.replace("whisk/information-receiver-","").split("--")
                StateRegistry.addUpdate( values(1), values(2), new StateInformation(key.getValue.toString.parseJson.convertTo[Map[String, String]]))}
        }
      }
    }
  }

  /**
   * Function for add an update into the local state registry
   *
   * @param namespace name of the memoryQueue
   * @param updateMsg data to be stored including a timestamp to manage critical run conditions(with kafka cannot happen but
   *                  who knows how the platform can evolve, some comments on the code suggest they wanna remove the kafka usage)
   * @return returns false in case the update doesn't change the registry state(the given information are a duplication of what the registry already has)
   *         true otherwise
   */
  def addUpdate( namespace: String, action: String, updateMsg: StateInformation ): Boolean = {
      var forward : Boolean = false
      stateRegistry.getOrElseUpdate(s"$namespace--$action", {
        lastUpdate = new Timestamp(System.currentTimeMillis())
        update(namespace, action)
        forward = true
        updateMsg
      }).check(updateMsg) match {
        case UpdateForChange => stateRegistry.replace(s"$namespace--$action", updateMsg)
              lastUpdate = new Timestamp(System.currentTimeMillis())
              update(namespace, action)
              forward = true
        case UpdateForRenew => stateRegistry.replace(s"$namespace--$action", updateMsg)
        case NotUpdate =>
      }
      forward
    }

  /**
   * Removes a state from the registry and on the Etcd datastore
   * @param namespace  namespace name to remove
   * @param action     action name to remove
   * @param etcdClient client for interact with etcd
   * @param global     indicates if it has to remove the value from etcd
   */
  def removeState(namespace: String, action: String, global: Boolean)(implicit etcdClient: EtcdClient): Unit = {
      removeUpdate( namespace, action )
      if( global ) etcdClient.del(s"whisk/information-receiver-${schedulerId.get}--$namespace--$action")
  }
}

/**
 * Akka Actor for the capture of updates on the StateRegistry keys. It creates a WatchEndpoint
 * placing itself as a receiver for any changes in the keys used by the instances. Whenever an update
 * come, if the update comes from the same scheduler it discard it, otherwise it uses it to update the registry
 *
 * TODO Future improvement must create a new standalone watcherservice which not send updates coming from the same scheduler
 * @param schedulerId    unique id representing the scheduler instance
 * @param watcherService needed for the creation of a watchendpoint
 * @param logging        logger
 * @param etcdClient     required for the creation of a watchendpoint
 * @param ec             needed for future management
 */
class InformationReceiver(
                           val schedulerId : String,       //  uniquely identify the scheduler
                           val watcherService: ActorRef    //  needed to receive automatic updates from WatcherService
                         )(
                           implicit val logging: Logging,
                           implicit val etcdClient: EtcdClient,   //  needed to directly interact with Etcd(store/remove data)
                           implicit val ec : ExecutionContext
                         ) extends Actor{

  //  creation of a watchEndpoint for receiving updates on key change
  //  we are using the prefix mode which permits to check a set of keys with a common prefix
  watcherService ! WatchEndpoint( "whisk/information-receiver-", "", isPrefix = true, schedulerId, Set(PutEvent,DeleteEvent))
  override def receive: Receive = {

    //  a key has been inserted or changed
    case req: WatchEndpointInserted =>
      //  key parsing => [0]= schedulerId, [1] = namespace, [2] = action
      val parsedKey = req.key.replace("whisk/information-receiver-","").split("--")
      //  we receive updates also from ourself, so we accept only messages from other schedulers
      if ( parsedKey(0).compareTo(schedulerId) != 0 ) StateRegistry.addUpdate(
        parsedKey(1),
        parsedKey(2),
        new StateInformation(req.value.parseJson.convertTo[Map[String, String]]))

    //  a key has been removed(state-registry removal)
    case req: WatchEndpointRemoved =>
      //  key parsing => [0]= schedulerId, [1] = namespace, [2] = action
      val parsedKey = req.key.replace("whisk/information-receiver-","").split("--")
      //  we receive updates also from ourself, so we accept only messages from other schedulers
      if (parsedKey(0).compareTo(schedulerId) != 0 ) StateRegistry.removeState( parsedKey(1), parsedKey(2), global = false)

    //  cannot happen
    case _ => logging.warn(this, s"[$schedulerId] Received an unexpected message. Discarding it")
  }
}

object InformationReceiver{
  def props(schedulerId: String, watcherService: ActorRef)
           (implicit logging: Logging, etcdClient: EtcdClient, ec: ExecutionContext):
  Props = {Props(new InformationReceiver(schedulerId, watcherService))}
}