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

import akka.actor.{Actor, ActorSystem, Props}
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.entity.FullyQualifiedEntityName
import org.apache.openwhisk.core.scheduler.queue._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class TrackedSchedulingDecisionMaker(invocationNamespace: String, action: FullyQualifiedEntityName, supervisor: QueueSupervisor )
                                    (implicit val actorSystem: ActorSystem,
                                     ec: ExecutionContext,
                                     logging: Logging)
  extends Actor {

  override def receive: Receive = {
    case msg: TrackQueueSnapshot =>
      decide(msg)
        .andThen {
          case Success(DecisionResults(Skip, _)) =>
          case Success(result: DecisionResults) => msg.recipient ! result
          case Failure(e) => logging.error(this, s"failed to make a scheduling decision due to $e");
        }

    case Clean => supervisor.clean()
  }

  private[queue] def decide(snapshot: TrackQueueSnapshot) = {
    val TrackQueueSnapshot(
    initialized,
    _, _,
    existing,
    _,
    _,
    inProgress,
    _, _, _,
    averageDuration,
    _, _,
    stateName,
    _) = snapshot

    val totalContainers = existing.size + inProgress

    (stateName, averageDuration) match {
      case (Flushing,_) => Future.successful(supervisor.tryResolveFlush())
      // we are in init state and no containers already given
      case (Running, None) if totalContainers == 0 && !initialized =>

        logging.info(
            this,
            s"add one initial container if totalContainers($totalContainers) == 0 [$invocationNamespace:$action]")
        Future.successful(supervisor.initStrategy())

      case (Running|Idle, _) => Future.successful(supervisor.delegate(snapshot))

      // do nothing
      case _ => Future.successful(DecisionResults(Skip, 0))

    }
  }
}

object TrackedSchedulingDecisionMaker {
  def props(invocationNamespace: String, action: FullyQualifiedEntityName, supervisor: QueueSupervisor)(
    implicit actorSystem: ActorSystem,
    ec: ExecutionContext,
    logging: Logging
  ): Props = {
    Props(new TrackedSchedulingDecisionMaker(invocationNamespace, action, supervisor))
  }
}
