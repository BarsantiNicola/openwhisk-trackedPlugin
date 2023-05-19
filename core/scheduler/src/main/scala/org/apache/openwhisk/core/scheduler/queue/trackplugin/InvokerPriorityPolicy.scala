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


import scala.collection.immutable.TreeSet

case class ContainerInvoker(containerId: String, invokerId: Int)

abstract class InvokerPriorityPolicy{

  implicit val ordering: Ordering[InvokerPriority] = (x: InvokerPriority, y: InvokerPriority) => {
    y.priority - x.priority match {
      case 0 => y.invokerId - x.invokerId
      case value if value != 0 => value.toInt
    }
  }

  var priorities : TreeSet[InvokerPriority] = TreeSet.empty[InvokerPriority]

  def compute(usages: List[InvokerUsage]): List[InvokerPriority] = {
    priorities = TreeSet.empty[InvokerPriority] ++ computePriorities(usages)
    priorities.toList
  }

  def computePriorities(usages: List[InvokerUsage]): List[InvokerPriority]

  def selectRemove(containers: Set[String], number: Int, associations: Set[ContainerInvoker]): Set[String] = {
    var toRemove : Set[String] = Set.empty[String]
    val availableAssociations = associations.filter( p => containers.contains(p.containerId))
    for (priority <- priorities){
      val filtered = availableAssociations.filter( _.invokerId == priority.invokerId).map{ _.containerId }
      filtered.size + toRemove.size - number match{
        case 0 => return toRemove ++ filtered
        case value if value < 0 => toRemove = toRemove ++ filtered
        case value if value > 0 => return toRemove ++ filtered.take(value)
      }
    }
    if(toRemove.isEmpty && containers.nonEmpty)
      toRemove = containers.take(number)
    toRemove
  }

  override def toString = "InvokerPriorityPolicy"
}

case class Consolidate() extends  InvokerPriorityPolicy {
  override def computePriorities(usages: List[InvokerUsage]): List[InvokerPriority] = {
    var values = Set.empty[Long]
    usages.foreach(usage => values += usage.usage) //  Sets automatically removes duplicated
    val ordered = values.toList.sorted    //  move to list to be able to use indexOf function
    usages.map { usage => InvokerPriority(usage.invokerId, ordered.indexOf(usage.usage)) }
  }

  override def toString = "Consolidate"

}

case class Balance() extends  InvokerPriorityPolicy {

  override def computePriorities(usages: List[InvokerUsage]): List[InvokerPriority] = {
    var values = Set.empty[Long]
    usages.foreach(usage => values += usage.usage)
    val ordered = values.toList.sorted(Ordering.Long.reverse)
    usages.map { usage => InvokerPriority(usage.invokerId, ordered.indexOf(usage.usage)) }
  }

  override def toString = "Balance"
}
