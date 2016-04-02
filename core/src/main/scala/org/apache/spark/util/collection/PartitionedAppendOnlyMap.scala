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

package org.apache.spark.util.collection

import java.util.Comparator

import org.apache.spark.util.collection.WritablePartitionedPairCollection._

/**
 * Implementation of WritablePartitionedPairCollection that wraps a map in which the keys are tuples
 * of (partition ID, K)
 *
 *  从SizeTrackingAppendOnlyMap的泛型参数中可以看出，AppendOnlyMap中存放的键值类型是((Int,K),V)
  *
  * @tparam K
  * @tparam V
  */
private[spark] class PartitionedAppendOnlyMap[K, V]
  extends SizeTrackingAppendOnlyMap[(Int, K), V] with WritablePartitionedPairCollection[K, V] {

  /***
    * 调用partitionedDestructiveSortedIterator获得分区优先排序的Iterator
    *
    *
    * @param keyComparator 按照Key排序的比较器，可能为None，如果为None，则表示仅进行按分区ID排序；如果不为None，则首先进行分区
    *                      内排序，而后按照Key进行排序
    * @return
    */
  def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)] = {
    /** *
      * 如果keyComparator为None，那么comparator就是调用partitionComparator方法返回的Comparator
      * 如果keyComparator不为None，那么通过 keyComparator.map(partitionKeyComparator)语句调用partitionKeyComparator方法，参数是keyComparator
      */
    val comparator = keyComparator.map(partitionKeyComparator).getOrElse(partitionComparator)

    /** *
      * 获取到comparator，对数据进行排序(破坏了底层的数据结构）
      * 注意：排序的数据类型是(Int,K)，也就是说调用destructiveSortedIterator时，destructiveSortedIterator需要的K是这里的(Int,K)
      *
      */
    destructiveSortedIterator(comparator)
  }

  /** *
    * 实现了WritablePartitionedPairCollection的insert方法，它的实现是调用AppendOnlyMap的update方法(Key的类型是(Int,K))
    * @param partition
    * @param key
    * @param value
    */
  def insert(partition: Int, key: K, value: V): Unit = {
    update((partition, key), value)
  }
}
