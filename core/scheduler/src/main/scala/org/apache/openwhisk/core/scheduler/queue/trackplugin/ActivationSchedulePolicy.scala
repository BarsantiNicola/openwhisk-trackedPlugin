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

import org.apache.openwhisk.core.connector.ActivationMessage
import java.util.{Timer, TimerTask}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.Duration

/**
 * Basic class for the development of policies for the action activation requests management
 */
abstract class ActivationSchedulePolicy(){

    /**
     * Will be called automatically by the subsystem at the receival of an Activation request
     * @param msg          ActivationMessage describing the request
     * @param containers   Containers allocated to the action
     * @param readyContainers  Containers ready to accept a request
     * @param enqueued     Number of enqueued requests
     * @param incoming     number of requests that are already into the system but not managed yet(will soon call again this function)
     * @param iar          inter-arrival rate of the requests
     * @return  True to accept the request(that will be assigned to a ready containers otherwise enqueued)
     *          False reject the request with a "Too message requests" error
     */
    def handleActivation( msg: ActivationMessage, containers: Int, readyContainers: Int, enqueued: Int, incoming: Int, iar : Long  ): Boolean
}

/**
 * Policy to accept all the request up to a maximum number of enqueued
 * @param maxConcurrent maximum number of enqueued request admitted
 */
case class AcceptTill( maxConcurrent: Int ) extends ActivationSchedulePolicy{
    override def handleActivation( msg: ActivationMessage, containers: Int, readyContainers: Int, enqueued: Int, incoming: Int, iar : Long  ): Boolean = {
        maxConcurrent <= enqueued+1
    }
}

/**
 * Policy to accept a maximum number of requests into a given period
 * @param maxConcurrent maximum number of request that can be accepted into a period
 * @param period        period duration
 */
case class AcceptEvery( maxConcurrent: Int, period: Duration ) extends ActivationSchedulePolicy with AutoCloseable {

    private val accept : AtomicInteger = new AtomicInteger(maxConcurrent)
    private val timer = new Timer
    timer.scheduleAtFixedRate(new TimerTask {
        override def run(): Unit = accept.set(maxConcurrent)
    }, period.toMillis, period.toMillis)

    override def handleActivation( msg: ActivationMessage, containers: Int, readyContainers: Int, enqueued: Int, incoming: Int, iar : Long  ): Boolean = accept.decrementAndGet() >= 0

    override def close(): Unit = timer.cancel()
}

/**
 * Policy to accept all the requests
 */
case class AcceptAll() extends ActivationSchedulePolicy{
    override def handleActivation( msg: ActivationMessage, containers: Int, readyContainers: Int, enqueued: Int, incoming: Int, iar : Long  ): Boolean = true

}

/**
 * Policy to rejects all the requests
 */
case class RejectAll() extends ActivationSchedulePolicy{
    override def handleActivation( msg: ActivationMessage, containers: Int, readyContainers: Int, enqueued: Int, incoming: Int, iar : Long  ): Boolean = false
}