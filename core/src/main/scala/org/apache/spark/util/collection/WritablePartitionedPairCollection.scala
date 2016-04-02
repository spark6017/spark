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

import org.apache.spark.storage.DiskBlockObjectWriter

/**
 * A common interface for size-tracking collections of key-value pairs that
 *
 *  - Have an associated partition for each key-value pair.
 *  - Support a memory-efficient sorted iterator
 *  - Support a WritablePartitionedIterator for writing the contents directly as bytes.
 */
private[spark] trait WritablePartitionedPairCollection[K, V] {
  /**
   * Insert a key-value pair with a partition into the collection
   */
  def insert(partition: Int, key: K, value: V): Unit

  /**
   * Iterate through the data in order of partition ID and then the given comparator. This may
   * destroy the underlying collection.
   *
   * 首先根据分区ID排序，其后对Key进行排序，它在destructiveSortedWritablePartitionedIterator方法中调用
   * destructiveSortedWritablePartitionedIterator方法像是模板方法，它调用子类实现的destructiveSortedWritablePartitionedIterator
    *
    * @param keyComparator
    * @return
    */
  def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)]

  /**
   * Iterate through the data and write out the elements instead of returning them. Records are
   * returned in order of their partition ID and then the given comparator.
   * This may destroy the underlying collection.
   */
  def destructiveSortedWritablePartitionedIterator(keyComparator: Option[Comparator[K]])
    : WritablePartitionedIterator = {

    /***
      * 获取排序后的数据迭代器
      */
    val it = partitionedDestructiveSortedIterator(keyComparator)

    /** *
      *  创建WritablePartitionedIterator的实现类，它是对it迭代器的封装，类似适配器模式
      */
    new WritablePartitionedIterator {
      /** *
        * 构造WritablePartitionedIterator对象时，读取第一个元素存放在cur变量
        */
      private[this] var cur = if (it.hasNext) it.next() else null

      /** *
        * 将当前元素cur的K和V写到磁盘，注意并没有将partition的id信息写到磁盘
        * @param writer
        */
      def writeNext(writer: DiskBlockObjectWriter): Unit = {
        writer.write(cur._1._2, cur._2)

        /** *
          * 写完当前元素，移动指针到下一个元素，可见hasNext只负责判断，而不负责指针移动，因为hasNext操作是一个幂等操作
          * （可以随便调用）
          */
        cur = if (it.hasNext) it.next() else null
      }

      /** *
        * 检查WritablePartitionedIterator是否还有元素，如果有，则调用writeNext方法写到磁盘
        * @return
        */
      def hasNext(): Boolean = cur != null

      /** *
        * 因为元素是按照分区ID排序的，因此，遍历的逻辑是一个分区一个分区的逐次进行遍历
        * @return
        */
      def nextPartition(): Int = cur._1._1
    }
  }
}

private[spark] object WritablePartitionedPairCollection {
  /**
   * A comparator for (Int, K) pairs that orders them by only their partition ID.
   * 根据分区ID进行排序, partitionComparator返回的Comparator是对类型为(Int,K)的元素进行排序
   */
  def partitionComparator[K]: Comparator[(Int, K)] = new Comparator[(Int, K)] {
    override def compare(a: (Int, K), b: (Int, K)): Int = {
      a._1 - b._1
    }
  }

  /**
   * A comparator for (Int, K) pairs that orders them both by their partition ID and a key ordering.
   *
   * 首先根据partitionId进行比较，如果partitionId相同，就根据key进行比较
   *
   * 通过这个comparator，得到的结果是，不同的partition之间，比如A和B，A分区中的数据要么全部大于B分区的数据，A分区中的数据要么全部小于A分区的数据
   *
   * 分区内的数据按照key进行排序
   *
   * partitionKeyComparator返回的Comparator是对类型为(Int,K)的元素进行排序
   */
  def partitionKeyComparator[K](keyComparator: Comparator[K]): Comparator[(Int, K)] = {

    /** *
      * 返回一个比较器，首先按照分区ID排序，如果分区ID不等，则直接返回；
      * 否则分区内按照Key进行排序
      */
    new Comparator[(Int, K)] {
      override def compare(a: (Int, K), b: (Int, K)): Int = {
        val partitionDiff = a._1 - b._1
        if (partitionDiff != 0) {
          partitionDiff
        } else {
          keyComparator.compare(a._2, b._2)
        }
      }
    }
  }
}

/**
 * Iterator that writes elements to a DiskBlockObjectWriter instead of returning them. Each element
 * has an associated partition.
  * 遍历元素并写到磁盘中(writeNext方法的实现)
  */
private[spark] trait WritablePartitionedIterator {
  def writeNext(writer: DiskBlockObjectWriter): Unit

  def hasNext(): Boolean

  def nextPartition(): Int
}
