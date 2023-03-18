package org.apache.openwhisk.core.scheduler.queue.trackplugin

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

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.entity.FullyQualifiedEntityName
import org.apache.openwhisk.core.etcd.EtcdClient
import org.apache.openwhisk.core.scheduler.SchedulingConfig
import org.apache.openwhisk.core.scheduler.queue.{DecisionResults, Flushing, Pausing, Running, Skip}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class TrackedSchedulingDecisionMaker(
                               invocationNamespace: String,
                               action: FullyQualifiedEntityName,
                               schedulingConfig: SchedulingConfig)(implicit val actorSystem: ActorSystem, ec: ExecutionContext, logging: Logging, etcdClient: EtcdClient, watcherService: ActorRef)
  extends Actor {

  private implicit val stateRegistry: StateRegistry = new StateRegistry( invocationNamespace, action.name.name )
  private val supervisor = new QueueSupervisor( invocationNamespace, action.name.name )

  override def receive: Receive = {
    case msg: TrackQueueSnapshot =>
      decide(msg)
        .andThen {
          case Success(DecisionResults(Skip, _)) =>
          // do nothing
          case Success(result: DecisionResults) =>
            msg.recipient ! result
          case Failure(e) =>
            logging.error(this, s"failed to make a scheduling decision due to $e");
        }
    case Clean =>
      supervisor.clean()
  }

  private[queue] def decide(snapshot: TrackQueueSnapshot) = {
    val TrackQueueSnapshot(
    initialized,
    incoming,
    currentMsg,
    existing,
    inProgress,
    _,
    _,
    _,
    averageDuration,
    limit,
    _,
    stateName,
    _) = snapshot

    logging.info( this, s"State: ${snapshot.toString}")
    val totalContainers = existing.size + inProgress
    val availableMsg = currentMsg + incoming.get()
    logging.info(this, s"RAISED REPORT: $stateName ")
    if (limit <= 0) {
      logging.info(this, "Limit is behing 0: $limit" )
      // this is an error case, the limit should be bigger than 0
      stateName match {
        case Flushing => Future.successful(DecisionResults(Skip, 0))
        case _        => Future.successful(DecisionResults(Pausing, 0))
      }

    } else {

      (stateName, averageDuration) match {

        case (Running, _) =>
          logging.info(this, s"Identified Running state, forcing to TrackedThrottled")
          Future.successful(DecisionResults(EnableTrackedRun(supervisor), 0))

        // there is no container
        case (TrackedRunning, None) if totalContainers == 0 && !initialized =>

          logging.info(
            this,
            s"add one initial container if totalContainers($totalContainers) == 0 [$invocationNamespace:$action]")
          Future.successful(supervisor.initStrategy())

        case (TrackedRunning, _) => Future.successful(supervisor.delegate(snapshot))

        case (TrackedIdle, _) => Future.successful(supervisor.delegate(snapshot))
        // do nothing
        case _ =>
          Future.successful(DecisionResults(Skip, 0))
      }
    }
  }
}

object TrackedSchedulingDecisionMaker {
  def props(invocationNamespace: String, action: FullyQualifiedEntityName, schedulingConfig: SchedulingConfig)(
    implicit actorSystem: ActorSystem,
    ec: ExecutionContext,
    logging: Logging,
    etcdClient: EtcdClient,
    watcherService: ActorRef): Props = {
    Props(new TrackedSchedulingDecisionMaker(invocationNamespace, action, schedulingConfig))
  }
}
