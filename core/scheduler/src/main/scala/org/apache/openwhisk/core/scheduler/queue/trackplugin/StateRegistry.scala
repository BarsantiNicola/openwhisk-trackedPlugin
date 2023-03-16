package org.apache.openwhisk.core.scheduler.queue.trackplugin

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.etcd.EtcdClient
import org.apache.openwhisk.core.service.{PutEvent, WatchEndpoint, WatchEndpointInserted, WatchEndpointRemoved}
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.sql.Timestamp
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Random, Success}

class CheckResult

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
 * @param inProgressContainerCountInNamespace : Int: total containers in the namespace which are executing actions
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

  def check(update: StateInformation): CheckResult = {

    if (update.timestamp < this.timestamp)
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

  Random.setSeed(System.currentTimeMillis())

  private val schedulerId = Random.alphanumeric.take(10).mkString
  private val watcherName = s"information-receiver-$schedulerId"
  private var update : Boolean = false
  private var lastUpdate : Timestamp = new Timestamp(System.currentTimeMillis())

  StateRegistry.init( schedulerId, watcherName, watcherService )

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
    StateRegistry.removeState( schedulerId, namespace, action )(etcdClient)
  }

  def getStates : Map[String,StateInformation] = {
    update = false
    StateRegistry.stateRegistry.toMap
  }

  def getUpdateStatus: UpdateState = UpdateState( update, lastUpdate )

}

object StateRegistry{

  private val stateRegistry: TrieMap[String, StateInformation] = new TrieMap[String, StateInformation]()
  private val updateRegistry: TrieMap[String, Boolean] = new TrieMap[String, Boolean]()
  private var share : Option[ActorRef] = None
  private var lastUpdate : Timestamp = new Timestamp(System.currentTimeMillis())

  private def update(namespace: String, action: String): Unit = {
    updateRegistry.foreach {
      key =>
        if (key._1 == s"$namespace--$action") false else true
    }
  }

  private def removeUpdate(namespace: String, action: String): Unit = {
    updateRegistry.remove(s"$namespace--$action")
    update(namespace, action)
  }

  def getUpdateStatus( namespace: String, action: String ): UpdateState = {
    val result = updateRegistry.getOrElse(s"$namespace--$action", true )
    updateRegistry.put(    s"$namespace--$action", false )
    UpdateState(result, lastUpdate)
  }

  def init( schedulerId : String, namespace: String, watcherService : ActorRef )(implicit etcdClient: EtcdClient, logging: Logging, ec:ExecutionContext, actorSystem: ActorSystem): Unit = {
    share.synchronized {
      if (share.isEmpty) {
        etcdClient.getPrefix(s"whisk/information-receiver-").map {
          result =>
            result.getKvsList.forEach {
              key => val values = key.getKey.toString.replace("whisk/information-receiver-","").split("--")
                StateRegistry.addUpdate(values(1), values(2), new StateInformation(key.getValue.toString.parseJson.convertTo[Map[String, String]]))}
        }
        share = Option(actorSystem.actorOf(InformationReceiver.props(schedulerId, s"$schedulerId--$namespace", watcherService), s"$schedulerId--$namespace"))
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

  def addUpdate( namespace: String, action: String, updateMsg: StateInformation )(implicit logging: Logging): Boolean = {
      if( stateRegistry.contains( s"$namespace--$action" )){

        stateRegistry.get( s"$namespace--$action" ).map{ data =>
          var forward = false
          logging.info(this, s"Update received from $namespace--$action")
          data.check(updateMsg) match {
            case UpdateForChange => stateRegistry.replace(s"$namespace--$action", updateMsg)
              lastUpdate = new Timestamp(System.currentTimeMillis())
              update(namespace, action)
              forward = true
            case UpdateForRenew => stateRegistry.replace(s"$namespace--$action", updateMsg)
            case NotUpdate =>
          }

          return forward.booleanValue() //  to prevent changes out of scope
        }.get

      }else{

        logging.info(this, s"Received first update from the MemoryQueue $namespace--$action")
        stateRegistry += (s"$namespace--$action" -> updateMsg)
        lastUpdate = new Timestamp(System.currentTimeMillis())
        update(namespace, action)
        true

      }
    }
  def removeState(schedulerId: String, namespace: String, action: String)(implicit etcdClient: EtcdClient): Unit = {
      removeUpdate( namespace, action )
      etcdClient.del(s"whisk/information-receiver-$schedulerId--$namespace--$action")
  }
}

class InformationReceiver(
                           val schedulerId : String,       //  uniquely identify the scheduler
                           val watcherName : String,       //  akka name assigned to the actor
                           val watcherService: ActorRef    //  needed to receive automatic updates from WatcherService
                         )(
                           implicit val logging: Logging,
                           implicit val etcdClient: EtcdClient,   //  needed to directly interact with Etcd(store/remove data)
                           implicit val ec : ExecutionContext
                         ) extends Actor{

  watcherService ! WatchEndpoint( "whisk/information-receiver-", "", isPrefix = true, watcherName, Set(PutEvent))
  override def receive: Receive = {

    case req: WatchEndpointInserted =>
      val keys = req.key.replace("whisk/information-receiver-","").split("--")
      logging.info(this, s" Received ADD ${keys(0)} -> ${keys(1)}")
      if (keys(0).compareTo(schedulerId) != 0 ) {
        logging.info(this, s"Applying update emitted by ${keys(0)}: ${keys(1)} -> ${req.value}")
        StateRegistry.addUpdate(keys(1),keys(2), new StateInformation(req.value.parseJson.convertTo[Map[String, String]]))
      }
    case req: WatchEndpointRemoved =>
      val keys = req.key.replace("whisk/information-receiver-","").split("--")
      logging.info(this, s" Received REM ${keys(0)} -> ${keys(1)}")
      if (keys(0).compareTo(schedulerId) != 0 ) {
        logging.info(this, s"Applying update emitted by ${keys(0)}: Removing MemoryQueue ${keys(1)}")
        StateRegistry.removeState(schedulerId, keys(1), keys(2))
      }

  }
}

object InformationReceiver{
  def props(schedulerId: String, watcherName: String, watcherService: ActorRef)
           (implicit logging: Logging, etcdClient: EtcdClient, ec: ExecutionContext):
  Props = {
    Props(new InformationReceiver(schedulerId, watcherName, watcherService))
  }
}