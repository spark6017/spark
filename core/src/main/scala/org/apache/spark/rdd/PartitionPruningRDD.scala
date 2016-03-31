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

import org.apache.spark.{NarrowDependency, Partition, TaskContext}
import org.apache.spark.annotation.DeveloperApi

/***
  *
  * @param idx 本Partition的idx
  * @param parentSplit 父RDD的Partition对象，Partition的index函数返回该Partition的index
  */
private[spark] class PartitionPruningRDDPartition(idx: Int, val parentSplit: Partition)
  extends Partition {
  /***
    * 实现Partition的index函数
    */
  override val index = idx
}


/**
 * Represents a dependency between the PartitionPruningRDD and its parent. In this
 * case, the child RDD contains a subset of partitions of the parents'.
  *
  * RDD剪裁使用的依赖，RDD B的分区是RDD A分区的子集
  *
  * @param rdd
  * @param partitionFilterFunc
  * @tparam T
  */
private[spark] class PruneDependency[T](rdd: RDD[T], partitionFilterFunc: Int => Boolean)
  extends NarrowDependency[T](rdd) {

  /***
    * 获取PartitionPruningRDD的分区列表，每个元素是PartitionPruningRDDPartition
    */

  val partitionsReserverd =  rdd.partitions.filter(s => partitionFilterFunc(s.index))

  //partitions的结果是一个数组集合，数组的元素是PartitionPruningRDDPartition
  @transient
  val partitions: Array[Partition] = partitionsReserverd.zipWithIndex.map { case(split, idx) => new PartitionPruningRDDPartition(idx, split) : Partition }

  /***
    * 根据指定的分区ID获取原始RDD对应的分区
    * @param partitionId a partition of the child RDD
    * @return the partitions of the parent RDD that the child partition depends upon
    */
  override def getParents(partitionId: Int): List[Int] = {
    List(partitions(partitionId).asInstanceOf[PartitionPruningRDDPartition].parentSplit.index)
  }
}


/**
 * :: DeveloperApi ::
 * A RDD used to prune RDD partitions/partitions so we can avoid launching tasks on
 * all partitions. An example use case: If we know the RDD is partitioned by range,
 * and the execution DAG has a filter on the key, we can avoid launching tasks
 * on partitions that don't have the range covering the key.
  *
  * @param prev 要剪裁的RDD
  * @param partitionFilterFunc 要剪裁的RDD保留哪些partition的函数
  * @param ev$1
  * @tparam T
  */
@DeveloperApi
class PartitionPruningRDD[T: ClassTag](
    prev: RDD[T],
    partitionFilterFunc: Int => Boolean)
  extends RDD[T](prev.context, List(new PruneDependency(prev, partitionFilterFunc))) {

  /***
    * 返回的是原始RDD的Partition
    * @param split
    * @param context
    * @return
    */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    firstParent[T].iterator(
      split.asInstanceOf[PartitionPruningRDDPartition].parentSplit, context)
  }

  /***
    *通过依赖关系获取Partition
    * @return
    */
  override protected def getPartitions: Array[Partition] =
    dependencies.head.asInstanceOf[PruneDependency[T]].partitions
}


@DeveloperApi
object PartitionPruningRDD {

  /**
   * Create a PartitionPruningRDD. This function can be used to create the PartitionPruningRDD
   * when its type T is not known at compile time.
    *
    * @param rdd 要裁剪的RDD
    * @param partitionFilterFunc 是否保留某分区的函数，如果保留前三分区，那么函数就是(index:Int)=>index <= 2
    * @tparam T
    * @return
    */
  def create[T](rdd: RDD[T], partitionFilterFunc: Int => Boolean): PartitionPruningRDD[T] = {
    new PartitionPruningRDD[T](rdd, partitionFilterFunc)(rdd.elementClassTag)
  }
}
