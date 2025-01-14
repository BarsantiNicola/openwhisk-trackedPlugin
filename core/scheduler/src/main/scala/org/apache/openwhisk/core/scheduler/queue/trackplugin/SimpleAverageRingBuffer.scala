package org.apache.openwhisk.core.scheduler.queue.trackplugin

object SimpleAverageRingBuffer {
  def apply(maxSize: Int) = new SimpleAverageRingBuffer(maxSize)
}

/**
 * This buffer provides the average of the given elements.
 * The number of elements are limited and the first element is removed if the maximum size is reached.
 * Since it is based on the Vector, its operation takes effectively constant time.
 * For more details, please visit https://docs.scala-lang.org/overviews/collections/performance-characteristics.html
 *
 * @param maxSize the maximum size of the buffer
 */
class SimpleAverageRingBuffer(private val maxSize: Int) {
  private var elements = Vector.empty[Double]
  private var sum = 0.0

  def nonEmpty: Boolean = elements.nonEmpty

  def average: Double = {
      sum / size
  }

  def add(el: Double): Unit = synchronized {
    if (elements.size == maxSize) {
      sum = sum + el - elements.head
      elements = elements.tail :+ el
    } else {
      sum += el
      elements = elements :+ el
    }
  }

  def size(): Int = elements.size
}
