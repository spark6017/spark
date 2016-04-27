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

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.Serializer

private[spark] class ShuffledRDDPartition(val idx: Int) extends Partition {
  override val index: Int = idx
  override def hashCode(): Int = idx
}

/**
 * :: DeveloperApi ::
 * The resulting RDD from a shuffle (e.g. repartitioning of data).
 * @param prev the parent RDD.
 * @param part the partitioner used to partition the RDD
 * @tparam K the key class.
 * @tparam V the value class.
 * @tparam C the combiner class.
 */
// TODO: Make this return RDD[Product2[K, C]] or have some way to configure mutable pairs
@DeveloperApi
class ShuffledRDD[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient var prev: RDD[_ <: Product2[K, V]],
    part: Partitioner)
  extends RDD[(K, C)](prev.context, Nil) {

  private var serializer: Option[Serializer] = None

  private var keyOrdering: Option[Ordering[K]] = None

  private var aggregator: Option[Aggregator[K, V, C]] = None

  private var mapSideCombine: Boolean = false

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer) */
  def setSerializer(serializer: Serializer): ShuffledRDD[K, V, C] = {
    this.serializer = Option(serializer)
    this
  }

  /** Set key ordering for RDD's shuffle. */
  def setKeyOrdering(keyOrdering: Ordering[K]): ShuffledRDD[K, V, C] = {
    this.keyOrdering = Option(keyOrdering)
    this
  }

  /** Set aggregator for RDD's shuffle. */
  def setAggregator(aggregator: Aggregator[K, V, C]): ShuffledRDD[K, V, C] = {
    this.aggregator = Option(aggregator)
    this
  }

  /** Set mapSideCombine flag for RDD's shuffle. */
  def setMapSideCombine(mapSideCombine: Boolean): ShuffledRDD[K, V, C] = {
    this.mapSideCombine = mapSideCombine
    this
  }

  /**
   * 获得该ShuffleRDD的依赖，它只有一个依赖，类型是ShuffleDependency。ShuffleRDD构造时aggregator可能不为None，而mapSideCombine可能为None
   * （参见PairRDDFunctions的combineXXX方法）
   *
   * getDependencies这个方法是说该RDD如何依赖它的父RDD，是窄依赖还是宽依赖。
   * ShuffleDependency看它的构造函数属性，有六个参数
    *
    *
    * new ShuffleDependency操作会生成一个shuffle id，而getDependencies是唯一一个创建new ShuffleDependency的位置
   *
   *
   *
   * 以reduceByKey为例，reduceByKey会生成一个ShuffledRDD，那么当调用该ShuffledRDD的getDependencies就会创建ShuffleDependency
   *
   * 问题：getDependencies是什么时候调到的？
   *
   * @return
   */
  override def getDependencies: Seq[Dependency[_]] = {
    List(new ShuffleDependency(prev, part, serializer, keyOrdering, aggregator, mapSideCombine))
  }

  /** *
    * ShuffledRDD的分区器是构造ShuffledRDD时指定的
    */
  override val partitioner = Some(part)

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](part.numPartitions)(i => new ShuffledRDDPartition(i))
  }

  /***
    * shuffle数据的数据本地性，假如Shuffle Map Stage的数据写到ABC三台机器，那么最后reduce任务运行在ABC机器上，
    * 避免跨机器拉取数据
    * @param partition
    * @return
    */
  override protected def getPreferredLocations(partition: Partition): Seq[String] = {
    /***
      * 获取MapOutputTrackerMaster
      */
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    tracker.getPreferredLocationsForShuffle(dep, partition.index)
  }

  /***
    * 对于ShuffledRDD，从磁盘读取Shuffled的数据
    * @param split
    * @param context
    * @return
    */
  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]

    /**
     * 获取Shuffle Reader，获取Shuffle Reader时需要提供的参数
     * 1. ShuffleHandle，记录了shuffle Id
     * 2. split，这是reduce任务要拉取数据的partition id
     */
   val reader =  SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
    val r = reader.read
    r.asInstanceOf[Iterator[(K, C)]]
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
