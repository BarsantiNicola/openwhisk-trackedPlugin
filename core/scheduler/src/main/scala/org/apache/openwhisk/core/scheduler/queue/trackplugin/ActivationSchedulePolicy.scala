package org.apache.openwhisk.core.scheduler.queue.trackplugin

import org.apache.openwhisk.core.connector.ActivationMessage

import java.util.{Timer, TimerTask}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.Duration;

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


case class AcceptTill( maxConcurrent: Int ) extends ActivationSchedulePolicy{
    override def handleActivation( msg: ActivationMessage, containers: Int, readyContainers: Int, enqueued: Int, incoming: Int, iar : Long  ): Boolean = {
        maxConcurrent <= enqueued+1
    }
}

case class AcceptEvery( maxConcurrent: Int, period: Duration ) extends ActivationSchedulePolicy with AutoCloseable {

    private val accept : AtomicInteger = new AtomicInteger(maxConcurrent)
    private val timer = new Timer
  timer.scheduleAtFixedRate(new TimerTask {
        override def run(): Unit = accept.set(maxConcurrent)
    }, period.toMillis, period.toMillis)

    override def handleActivation( msg: ActivationMessage, containers: Int, readyContainers: Int, enqueued: Int, incoming: Int, iar : Long  ): Boolean = accept.decrementAndGet() >= 0

    override def close(): Unit = timer.cancel()
}

case class AcceptAll() extends ActivationSchedulePolicy{
    override def handleActivation( msg: ActivationMessage, containers: Int, readyContainers: Int, enqueued: Int, incoming: Int, iar : Long  ): Boolean = true

}

case class RejectAll() extends ActivationSchedulePolicy{
    override def handleActivation( msg: ActivationMessage, containers: Int, readyContainers: Int, enqueued: Int, incoming: Int, iar : Long  ): Boolean = false
}