/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util

import java.io.Serializable
import java.util.{PriorityQueue => JPriorityQueue}

import scala.collection.JavaConverters._
import scala.collection.generic.Growable

/**
 * Bounded priority queue. This class wraps the original PriorityQueue
 * class and modifies it such that only the top K elements are retained.
 * The top K elements are defined by an implicit Ordering[A].
 */
private[spark] class BoundedPriorityQueue[A](maxSize: Int)(implicit ord: Ordering[A])
  extends Iterable[A] with Growable[A] with Serializable {

  /***
    * 元素个数为maxSize的优先级队列，使用ord进行排序
    */
  private val underlying = new JPriorityQueue[A](maxSize, ord)

  /***
    * 返回迭代队列中的元素的迭代器
    * @return
    */
  override def iterator: Iterator[A] = underlying.iterator.asScala

  /***
    * 返回队列中的元素个数
    * @return
    */
  override def size: Int = underlying.size

  /***
    * 将xs集合加入到队列中，具体做法是：
    * 遍历xs集合的每个元素，调用BoundedProrityQueue中+=操作，保持队列的大小不超过maxSize
    *
    * @param xs
    * @return
    */
  override def ++=(xs: TraversableOnce[A]): this.type = {
    xs.foreach { this += _ }
    this
  }

  /***
    * 往队列中添加一个元素，保持队列中的元素个数不会超过maxSize个
    * @param elem
    * @return
    */
  override def +=(elem: A): this.type = {
    if (size < maxSize) {
      underlying.offer(elem)
    } else {

      //如果当前队列中的元素是maxSize，那么需要将队列中的一个元素剔除
      maybeReplaceLowest(elem)
    }
    this
  }

  override def +=(elem1: A, elem2: A, elems: A*): this.type = {
    this += elem1 += elem2 ++= elems
  }

  override def clear() { underlying.clear() }

  /***
    *
    * @param a
    * @return
    */
  private def maybeReplaceLowest(a: A): Boolean = {
    //队列头元素是出队的第一个元素
    val head = underlying.peek()

    /***
      * 如果第一个元素存在并且元素a大于队列头元素，那么将队列头元素出队，将元素a加入到队列中
      */
    if (head != null && ord.gt(a, head)) {
      underlying.poll()
      underlying.offer(a)
    } else {
      false
    }
  }
}
