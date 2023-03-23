package org.apache.openwhisk.core.scheduler.queue.trackplugin

import org.apache.openwhisk.core.scheduler.queue.{AddContainer, DecisionResults, Skip}

/**
 * Basic class for the development of policies for the action containers management
 */
abstract class ContainerSchedulePolicy(){
  /**
   * Will be called periodically by the subsystem to adapt the containers
   * @param minWorkers    minimum number of workers that has to be allocated
   * @param readyWorkers  number of ready workers that the policy has to try to maintain
   * @param maxWorkers    maximum number of workers that can be allocated
   * @param totalContainers  total containers allocated to the action
   * @param readyContainers  ready containers present on the action
   * @param inCreationContainers  containers that will be available soon(they are on creation)
   * @param requestIar       inter-arrival rate of the requests
   * @param enqueuedRequests number of requests for the action enqueued
   * @param incomingRequests number of requests that are already into the system but not managed yet
   * @return DecisionResults(AddContainer,num) add num containers,
   *         DecisionResults(RemoveReadyContainers(list[ids]),0) remove the containers with the given containerId
   *         DecisionResults(Skip,0) do nothing
   */
  def grant( minWorkers: Int, readyWorkers: Int, maxWorkers: Int, totalContainers: Int, readyContainers: Set[String], inCreationContainers: Int, requestIar: Int, enqueuedRequests: Int, incomingRequests: Int  ): DecisionResults
}

/**
 * Adds containers basing on the incoming requests respecting a set the set of parameters minWorkers,readyWorkers and maxWorkers
 */
class AsRequested() extends ContainerSchedulePolicy{
  override  def grant( minWorkers: Int,
                       readyWorkers: Int,
                       maxWorkers: Int,
                       totalContainers: Int,
                       readyContainers: Set[String],
                       inCreationContainers: Int,
                       requestIar: Int,
                       enqueuedRequests: Int,
                       incomingRequests: Int
                     ): DecisionResults = {

    if (math.max(requestIar, incomingRequests) + enqueuedRequests > totalContainers + inCreationContainers) {
      //  we haven't enough containers to manage the requests

      //  computation of number of required containers required to manage all the requests
      val neededContainersCount = math.max(minWorkers-totalContainers-inCreationContainers, math.max(requestIar, incomingRequests) + enqueuedRequests - totalContainers - inCreationContainers).max(0)
      println(neededContainersCount)
      //  we can add containers up to maxWorkers value
      val enoughContainers = maxWorkers - inCreationContainers - totalContainers - neededContainersCount >= 0

      //  we try to give the required containers or at least the maximum usable number
      val containersToAdd = if (enoughContainers) neededContainersCount else maxWorkers - totalContainers - inCreationContainers

      val enoughReady = maxWorkers - inCreationContainers - totalContainers - containersToAdd >= readyWorkers

      containersToAdd match{
        case _ if containersToAdd == neededContainersCount && enoughReady  => DecisionResults(AddContainer, containersToAdd)
        case _ if containersToAdd == neededContainersCount && !enoughReady => DecisionResults(AddContainer,maxWorkers - inCreationContainers - totalContainers)
        case _ if containersToAdd > 0  => DecisionResults(AddContainer, containersToAdd )
        case _ => DecisionResults(Skip, 0)
      }

    } else {

      //  we have enough containers to manage the incoming requests
      val remainingReady = readyContainers.size + inCreationContainers-math.max(requestIar, incomingRequests) - enqueuedRequests
      val tooManyWorkers = totalContainers + inCreationContainers + readyWorkers - remainingReady > maxWorkers
      val notEnoughWorkers = totalContainers + inCreationContainers < minWorkers
      val interfere = readyContainers.size - math.max(requestIar, incomingRequests) - enqueuedRequests <= 0

      remainingReady match{
        case _ if remainingReady < readyWorkers && tooManyWorkers =>  DecisionResults(AddContainer, maxWorkers-totalContainers-inCreationContainers)
        case _ if remainingReady < readyWorkers && !tooManyWorkers =>  DecisionResults(AddContainer,readyWorkers-remainingReady )
        case _ if notEnoughWorkers => DecisionResults(AddContainer, minWorkers-totalContainers)
        case _ if remainingReady == readyWorkers => DecisionResults(Skip,0)
        case _ if totalContainers + inCreationContainers == minWorkers && !notEnoughWorkers => DecisionResults(Skip,0)
        case _ if !notEnoughWorkers && !interfere => DecisionResults(RemoveReadyContainer(readyContainers.take(remainingReady-readyWorkers)), 0)
        case _ => DecisionResults(Skip,0)
      }
    }
  }
}

object AsRequested {
  def apply(): AsRequested = new AsRequested()
}

/**
 * The containers are added in blocks of stepSize. In case a block cannot be allocated less containers
 * can be added. The class simply extend the AsRequested method changing the given results to adapt to a step
 * @param stepSize Dimension of the block
 */
case class Steps(stepSize: Int) extends AsRequested {
  override def grant( minWorkers: Int,
                      readyWorkers: Int,
                      maxWorkers: Int,
                      totalContainers: Int,
                      readyContainers: Set[String],
                      inCreationContainers: Int,
                      requestIar: Int,
                      enqueuedRequests: Int,
                      incomingRequests: Int
                    ): DecisionResults = {

    if( inCreationContainers != 0 )
      return DecisionResults( Skip, 0 )

    super.grant(minWorkers,
                readyWorkers,
                maxWorkers,
                totalContainers,
                readyContainers,
                inCreationContainers,
                requestIar,
                enqueuedRequests,
                incomingRequests ) match {

      case DecisionResults(AddContainer, value ) if value>stepSize => DecisionResults(AddContainer, stepSize )

      case DecisionResults(AddContainer, value ) =>
        if( totalContainers + inCreationContainers + value - stepSize <= maxWorkers ) {
          DecisionResults(AddContainer, stepSize)
        }else
          DecisionResults(AddContainer, value )

      case DecisionResults(RemoveReadyContainer(containers), 0 ) if containers.size > stepSize => DecisionResults(RemoveReadyContainer(containers.take(stepSize)), 0)

      case _ => DecisionResults( Skip, 0 )
    }
  }
}

/**
 * The containers are added incrementally with the number of steps required to manage the request
 * @param grade grade of the polynomial series to be generated(ex grade 1 => 1,2,3,4.. grade 2 => 1,4,9,16..)
 */
case class Poly(grade: Int) extends ContainerSchedulePolicy {

  private var stepCounter :Int = 1
  private var inc : Boolean = true

  override def grant( minWorkers: Int, readyWorkers: Int, maxWorkers: Int, totalContainers: Int, readyContainers: Set[String], inCreationContainers: Int, requestIar: Int, enqueuedRequests: Int, incomingRequests: Int  ): DecisionResults = {

    if( inCreationContainers != 0 )
      return DecisionResults( Skip, 0 )

    val toMaxContainerToAdd = maxWorkers-totalContainers
    val requiredContainers = (totalContainers < math.max( incomingRequests, requestIar) + enqueuedRequests ||
      totalContainers < minWorkers ||
      readyContainers.size < readyWorkers) && totalContainers >= maxWorkers

    val containersToAdd = requiredContainers match{
      case true if !inc =>
        stepCounter=1
        inc = !inc
        1
      case false if inc =>
        stepCounter=1
        inc = !inc
        1
      case _ => stepCounter+=1; math.pow( stepCounter-1, grade ).toInt;
    }
    val tooManyContainers = if( inc ) totalContainers + containersToAdd > maxWorkers else totalContainers-containersToAdd < minWorkers
    val stay = readyContainers.size-containersToAdd < readyWorkers + containersToAdd/2

    requiredContainers match {
      case true if tooManyContainers => stepCounter = 1; DecisionResults(AddContainer, toMaxContainerToAdd)
      case true => DecisionResults(AddContainer, containersToAdd)
      case false if stay => stepCounter = 1; DecisionResults(Skip, 0)
      case _ => DecisionResults( RemoveReadyContainer(readyContainers.take(containersToAdd)), 0)
    }
  }
}

case class All() extends ContainerSchedulePolicy{
  override def grant(minWorkers: Int, readyWorkers: Int, maxWorkers: Int, totalContainers: Int, readyContainers: Set[String], inCreationContainers: Int, requestIar: Int, enqueuedRequests: Int, incomingRequests: Int): DecisionResults = {

    inCreationContainers+totalContainers-maxWorkers match{
      case value if value > 0 && readyContainers.size >= value => DecisionResults(RemoveReadyContainer(readyContainers.take(value)), 0)
      case value if value < 0 => DecisionResults(AddContainer, -1*value)
      case _ => DecisionResults(Skip,0)
    }
  }
}