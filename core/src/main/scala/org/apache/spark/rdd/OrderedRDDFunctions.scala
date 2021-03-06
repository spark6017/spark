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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Logging, Partitioner, RangePartitioner}
import org.apache.spark.annotation.DeveloperApi

/**
 *  <K,V>类型的RDD的K是可排序的，可排序的含义是指，存在一个隐式转换，将类型K转换为Ordered类型， Ordering[K]
 *
 *  所有的基本类型都是可排序的，原因是Ordering[Int],Ordering[String]都是Scala语言内置的
 *
 *  用户可以自定义比较逻辑，比如实现大小写不敏感的Ordering[String]
 *
 * Extra functions available on RDDs of (key, value) pairs where the key is sortable through
 * an implicit conversion. They will work with any key type `K` that has an implicit `Ordering[K]`
 * in scope. Ordering objects already exist for all of the standard primitive types. Users can also
 * define their own orderings for custom types, or to override the default ordering. The implicit
 * ordering that is in the closest scope will be used.
 *
 * {{{
 *   import org.apache.spark.SparkContext._
 *
 *   val rdd: RDD[(String, Int)] = ...
 *   implicit val caseInsensitiveOrdering = new Ordering[String] {
 *     override def compare(a: String, b: String) = a.toLowerCase.compare(b.toLowerCase)
 *   }
 *
 *   // Sort by key, using the above case insensitive ordering.
 *   rdd.sortByKey()
 * }}}
 */
class OrderedRDDFunctions[K : Ordering : ClassTag,
                          V: ClassTag,
                          P <: Product2[K, V] : ClassTag] @DeveloperApi() (
    self: RDD[P])
  extends Logging with Serializable
{
  private val ordering = implicitly[Ordering[K]]

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   *
   * sortByKey是全局排序还是局部排序？  sortByKey是全局排序，为什么说是全局排序，因为
   *      1. 因为sortByKey使用RangePartitioner进行划分分区，也就是说，A分区的数据肯定比B分区的数据要么全部大，要么全部小(数据大小是按照Key的维度来进行比较的)
   *      2. 又因为是sortByKey，那么分区内继续排序？会继续根据Key排序，这也正是sortByKey的含义： 首先在Shuffle Write阶段实现分区间有序(分区内不保证有序)，在Shuffle Read
   *      阶段再实现分区内有序
   *
    * 注意：sortByKey可以指定分区数，但是不能指定分区算法，因为分区算法是固定的RangePartitioner
    *
    * @param ascending
    * @param numPartitions sortByKey可以指定分区数，默认的分区数是调用该方法的RDD的分区数
    * @return
    */
  // TODO: this currently doesn't work on P other than Tuple2!
  def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length)
      : RDD[(K, V)] = self.withScope
  {
    val part = new RangePartitioner(numPartitions, self, ascending)

    /**
     *  sortByKey得到的RDD是一个ShuffledRDD
     *  1. 分区算法是RangePartitioner
     *  2. KeyOrdering是排序算法是ordering
     *  3. ShuffledRDD并没有指定mapSideCombine
     *  4. ShuffledRDD也没有指定aggregator
     *
     *  因为是RangePartitioner，那么数据经过分区后，A，B两个分区，A分区的数据要么全部大于B分区的数据；A分区的数据要么全部小于B分区的数据
     */
    new ShuffledRDD[K, V, V](self, part)
      .setKeyOrdering(if (ascending) ordering else ordering.reverse)
  }

  /**
   * Repartition the RDD according to the given partitioner and, within each resulting partition,
   * sort records by their keys.
   *
   * This is more efficient than calling `repartition` and then sorting within each partition
   * because it can push the sorting down into the shuffle machinery.
   *
    *  根据partitioner进行数据重新Repartition，能够保证分区内数据有序，但不保证分区间的数据有序
    *
    * @param partitioner
    * @return
    */
  def repartitionAndSortWithinPartitions(partitioner: Partitioner): RDD[(K, V)] = self.withScope {
    new ShuffledRDD[K, V, V](self, partitioner).setKeyOrdering(ordering)
  }

  /**
   * Returns an RDD containing only the elements in the the inclusive range `lower` to `upper`.
   * If the RDD has been partitioned using a `RangePartitioner`, then this operation can be
   * performed efficiently by only scanning the partitions that might contain matching elements.
   * Otherwise, a standard `filter` is applied to all partitions.
   */
  def filterByRange(lower: K, upper: K): RDD[P] = self.withScope {

    /***
      * 判断元素ｋ是否在lower和upper之间
      * @param k
      * @return
      */
    def inRange(k: K): Boolean = ordering.gteq(k, lower) && ordering.lteq(k, upper)

    val rddToFilter: RDD[P] = self.partitioner match {
        //如果原始RDD使用的是RangePartitioner，那么得到lower和upper所在的partition id，因为不知道RangePartitioner是升序分区，还是降序分区
        //所以，l和u的值谁大谁小不确定
      case Some(rp: RangePartitioner[K, V]) => {
        val partitionIndicies = (rp.getPartition(lower), rp.getPartition(upper)) match {
          case (l, u) => Math.min(l, u) to Math.max(l, u)
        }

        /***
          * PartitionPruningRDD的Pruning表示裁剪的意思，
          * create方法的两个参数：原始RDD以及要保留的分区ID
          */
        PartitionPruningRDD.create(self, partitionIndicies.contains)
      }
      case _ =>
        self
    }

    /***
      * 如果原始RDD采用的是RangerPartitioner，那么rddToFilter这个RDD包含的分区是原始RDD的分区的子集
      * 如果原始RDD采用的是其它的分区算法，那么rddToFilter这个RDD就是原始的RDD
      */
    rddToFilter.filter { case (k, v) => inRange(k) }
  }

}
