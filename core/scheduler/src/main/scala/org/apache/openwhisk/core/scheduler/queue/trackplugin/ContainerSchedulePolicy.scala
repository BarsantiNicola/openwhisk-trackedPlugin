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

import org.apache.openwhisk.core.scheduler.queue.{AddContainer, DecisionResults, Skip}

import scala.annotation.tailrec


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

  override def toString: String = "ContainerSchedulePolicy"
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
                       incoming: Int
                     ): DecisionResults = {

    //  check to spot if there are some containers that can be dropped
    val readyCheck = readyContainers.size - math.max(requestIar,enqueuedRequests)-readyWorkers

    //  evaluation if some containers can be added
    //  in case yes => we can add or remove containers basing on the incoming requests
    //  in case no  => we can only remove containers
    (if( totalContainers + inCreationContainers <=maxWorkers ) math.min(maxWorkers, math.max( math.max(requestIar, enqueuedRequests)+readyWorkers, minWorkers)) match{

      //  the system needs more containers
      case requiredContainers if requiredContainers > totalContainers+inCreationContainers => DecisionResults(AddContainer, requiredContainers-totalContainers-inCreationContainers)
      //  the system has already the correct amount of containers
      case requiredContainers if requiredContainers == totalContainers+inCreationContainers => DecisionResults(Skip,0)

      //  the system has more containers that what it actually needs
      case requiredContainers if requiredContainers <  totalContainers+inCreationContainers && readyCheck > 0 => DecisionResults(RemoveReadyContainer(readyContainers.take(math.min(readyCheck, readyContainers.size - minWorkers))), 0)
      case requiredContainers if requiredContainers <  totalContainers+inCreationContainers && readyCheck == 0 => DecisionResults(Skip,0)
      case requiredContainers if requiredContainers <  totalContainers+inCreationContainers && readyCheck < 0 => DecisionResults(AddContainer, math.min(maxWorkers-totalContainers-inCreationContainers, -1*readyCheck))

    } else DecisionResults(RemoveReadyContainer(readyContainers.take( totalContainers+inCreationContainers-maxWorkers)), 0)) match {

      case DecisionResults( AddContainer, 0 ) => DecisionResults(Skip, 0)
      case DecisionResults( AddContainer, value) if value < 0 => DecisionResults(Skip, 0)
      case DecisionResults( RemoveReadyContainer(set), 0) if set.isEmpty => DecisionResults(Skip, 0)
      case value => value
    }
  }

  override def toString: String = "AsRequested"
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

    val systemFree = requestIar + enqueuedRequests == 0
    def outsideScope(value: Int): Boolean = totalContainers +inCreationContainers - value == 0 ||
      totalContainers +inCreationContainers - value == math.max(readyWorkers,minWorkers)

    super.grant(minWorkers,
                readyWorkers,
                maxWorkers,
                totalContainers,
                readyContainers,
                inCreationContainers,
                requestIar,
                enqueuedRequests,
                incomingRequests ) match {

      case DecisionResults(AddContainer, value ) if value>=stepSize => DecisionResults( AddContainer, stepSize )

      case DecisionResults(AddContainer, _ ) =>
        if( totalContainers + inCreationContainers + stepSize >= maxWorkers ) {
          DecisionResults(AddContainer, maxWorkers - totalContainers - inCreationContainers)
        }else
          DecisionResults(AddContainer, stepSize )

      case DecisionResults(RemoveReadyContainer(containers), 0 ) if containers.size > stepSize => DecisionResults(RemoveReadyContainer(containers.take(stepSize)), 0)
      case DecisionResults(RemoveReadyContainer(containers), 0 ) if containers.size == stepSize => DecisionResults(RemoveReadyContainer(containers), 0)
      case DecisionResults(RemoveReadyContainer(containers),0) if containers.size < stepSize && systemFree && outsideScope(containers.size) =>
         DecisionResults(RemoveReadyContainer(containers),0)
      case _ => DecisionResults( Skip, 0 )
    }
  }

  override def toString: String = s"Steps $stepSize"
}

/**
 * It always allocate to the action maxWorkers containers
 */
case class All() extends ContainerSchedulePolicy{
  override def grant(minWorkers: Int, readyWorkers: Int, maxWorkers: Int, totalContainers: Int, readyContainers: Set[String], inCreationContainers: Int, requestIar: Int, enqueuedRequests: Int, incomingRequests: Int): DecisionResults = {

    inCreationContainers+totalContainers-maxWorkers match{
      case value if value > 0 && readyContainers.size >= value => DecisionResults(RemoveReadyContainer(readyContainers.take(value)), 0)
      case value if value > 0 => DecisionResults(RemoveReadyContainer(readyContainers),0)
      case value if value < 0 => DecisionResults(AddContainer, -1*value)
      case _ => DecisionResults(Skip,0)
    }
  }

  override def toString: String = "All"
}

/**
 * Containers are added basing on array which defines the number of containers to be allocated. Using an index, if the required
 * containers are greater/lower than the available the system moves to higher/lower indexes granting the number of containers specified
 * ex: [0,1,2,10] => if required ==1, returns 1 if required ==4, returns 10
 * Abstract class which requires only to define a method for creating the array to use
 */
abstract class BlocksPolicy extends ContainerSchedulePolicy {

    private var index = 0
    def computeAllocationArray(maxWorkers: Int ): Array[Int]

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

      if (inCreationContainers != 0)
        return DecisionResults(Skip, 0)

      val requestsToServe = requestIar +enqueuedRequests
      val offset = if( requestsToServe < math.max(minWorkers,readyWorkers) ) math.max(minWorkers, readyWorkers) else readyWorkers
      val requiredContainers = requestsToServe + offset
      val allocationArray = computeAllocationArray(maxWorkers)

      if( index >= allocationArray.length ){
        if( readyContainers.size >= totalContainers- allocationArray(allocationArray.length-1)) {
          index = allocationArray.length-1
          return DecisionResults( RemoveReadyContainer(readyContainers.take(totalContainers-allocationArray(index))),0)
        }else
          return DecisionResults(Skip,0)
      }
      val sufficientReady = if( index > 0 ) allocationArray(index)-allocationArray(index-1) +readyWorkers <= readyContainers.size else false

      (requiredContainers-allocationArray(index) match{

        case value if value > 0 && index < allocationArray.length-1 => index+=1; DecisionResults( AddContainer, allocationArray(index)-totalContainers)
        case value if value > 0 => DecisionResults( AddContainer, allocationArray(index)-totalContainers)
        case value if value < 0 && index > 0 && requiredContainers <= allocationArray(index-1) && sufficientReady => index-=1; DecisionResults( RemoveReadyContainer(readyContainers.take(allocationArray(index+1)-allocationArray(index))),0)
        case value if value < 0 && index == allocationArray.length-1 => if( totalContainers-allocationArray(index) > 0) DecisionResults(RemoveReadyContainer(readyContainers.take(totalContainers-allocationArray(index))),0) else DecisionResults(AddContainer, allocationArray(index)-totalContainers)
        case value if value < 0 && index == 0 => DecisionResults( RemoveReadyContainer(readyContainers.take(totalContainers-allocationArray(index))),0)
        case _ => DecisionResults(Skip,0)

      }) match{

        case DecisionResults(AddContainer,0 ) => DecisionResults(Skip,0)
        case DecisionResults(RemoveReadyContainer(containers),_) if containers.isEmpty => DecisionResults(Skip,0)
        case value => value

      }
    }

    override def toString: String = "BlocksContainerPolicy"
  }

/**
 * The containers are added incrementally with the number of steps required to manage the request
 * @param grade grade of the polynomial series to be generated(ex grade 1 => 1,2,3,4.. grade 2 => 1,4,9,16..)
 */
case class Poly( grade: Int ) extends BlocksPolicy{
  override def computeAllocationArray(maxWorkers: Int ): Array[Int] = {

    @tailrec
    def create(prevValue: Int, result: Array[Int]): Array[Int] = {
      if (prevValue + math.pow(result.length, grade) >= maxWorkers) return result ++ Array[Int](maxWorkers)
      create(prevValue + math.pow(result.length, grade).toInt,
        result ++ Array[Int](prevValue + math.pow(result.length, grade).toInt))
    }

    create(0, Array[Int](0))
  }

  override def toString: String = s"Poly $grade"
}

/**
 * Has a behavior opposite to the poly, while the poly adds greater blocks with the incrementing of the index the IPoly
 * decrements the block size
 * @param grade grade of the polynomial series to be used(ex grade 1 max 10 => 1, 2, 3, 4.. grade 2 => 6,3,2,1..)
 */
case class IPoly(grade: Int) extends BlocksPolicy{
  override def computeAllocationArray(maxWorkers: Int): Array[Int] = {

    @tailrec
    def poly(result: Array[Int]): (Array[Int],Int) = {
      if (math.pow(result.length, grade) >= maxWorkers) return (Array[Int](maxWorkers), maxWorkers-math.pow(result.length-1, grade).toInt)
      poly(result ++ Array[Int](math.pow(result.length, grade).toInt))
    }

    //  returns an array of polynomial values until they are less of maxWorkers and the difference not covered
    val result = poly(Array[Int](0))

    //  the remaining containers will be spreaded on all the array position(except the first)
    val exceed: Int = result._2 % result._1.length //  containers in exceed to the spreading
    val offset: Int = result._2 / result._1.length //  containers to be added on each position

    Range(0, result._1.length).map {
      case 0 => 0
      case ind@1 => result._1(ind) + offset + exceed
      case v => result._1(v) + offset
    }.toArray

  }

  override def toString: String = "IPoly $grade"
}
/**
 * Policy similar to the Poly but using the Fibonacci series which is smoother
 */
case class Fibonacci() extends BlocksPolicy{

  override def computeAllocationArray(maxWorkers: Int): Array[Int] = {
    @tailrec
    def create(value_1: Int, value_2: Int, result: Array[Int]): Array[Int] = {

      val actual = value_1 + value_2
      if( actual >= maxWorkers ) return result ++ Array[Int](maxWorkers)

      create(actual, value_1, result ++ Array[Int](actual))
    }
    create(1, 0, Array[Int](0))
  }

  override def toString: String = "Fibonacci"
}
