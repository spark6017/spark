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

import java.io.{IOException, ObjectOutputStream}

import scala.reflect.ClassTag

import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.util.Utils

/***
  * Zip操作形成的RDD的分区(Partition）对应的实现类
  * @param idx 分区ID
  * @param rdds 该分区所属的RDD依赖的RDD集合
  * @param preferredLocations 该分区的所有数据优先位置
  */
private[spark] class ZippedPartitionsPartition(
    idx: Int,
    @transient private val rdds: Seq[RDD[_]],
    @transient val preferredLocations: Seq[String])
  extends Partition {

  /***
    * 实现Partition类的index方法
    */
  override val index: Int = idx

  /***
    * 获取该分区对应的依赖的父RDDs的分区
    */
  var partitionValues = rdds.map(rdd => rdd.partitions(idx))

  /***
    * 获取该分区所对应的依赖的父RDDs的分区
    * @return
    */
  def partitions: Seq[Partition] = partitionValues

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    partitionValues = rdds.map(rdd => rdd.partitions(idx))
    oos.defaultWriteObject()
  }
}

/***
  *
  * 对于Zip Partition RDD，随意该RDD可能依赖于两个、三个、四个RDD。每个分区依赖于其它每个RDD的一个分区，
  * 这种多对一的依赖情况，仍然是一个One2OneDependency
  * @param sc
  * @param rdds 该ZippedPartitionsBaseRDD依赖的RDD，如果a.zip(b),那么rdds就是数组[a,b]
  * @param preservesPartitioning
  * @param ev$1
  * @tparam V
  */
private[spark] abstract class ZippedPartitionsBaseRDD[V: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[_]],
    preservesPartitioning: Boolean = false)
  extends RDD[V](sc, rdds.map(x => new OneToOneDependency(x))) {

  /***
    * 如果保持分区，那么去第一个RDD的分区器
    */
  override val partitioner =
    if (preservesPartitioning) firstParent[Any].partitioner else None

  /***
    * zip两个RDD，那么两个RDD的分区数目要相同，同时每个分区的元素也要相同
    * @return
    */
  override def getPartitions: Array[Partition] = {

    /***
      * 所有RDD的分区个数要一致
      */
    val numParts = rdds.head.partitions.length
    if (!rdds.forall(rdd => rdd.partitions.length == numParts)) {
      throw new IllegalArgumentException("Can't zip RDDs with unequal numbers of partitions")
    }

    /***
      * 为数组填充数据
      */
    Array.tabulate[Partition](numParts) { i =>

      /***
        * 求该RDD依赖的所有RDD的第i个分区的优先位置，返回值是一个长度为numParts的数组
        * prefs是Seq[Array[String]]类型
        */
    val prefs = rdds.map(rdd => rdd.preferredLocations(rdd.partitions(i)))

      /***
        * Check whether there are any hosts that match all RDDs; otherwise return the union
        * 对所有的prefereredLocation求交集，
        *
        * 如果exactMatchLocations表示所有的分区的公共的优先位置
        */
      val exactMatchLocations = prefs.reduce((x, y) => x.intersect(y))

      /***
        * 如果有公共的数据位置，那么返回公共的数据位置。如果没有公共的数据位置，那么返回并集（prefs.flatten.distinct）
        */
      val locs = if (!exactMatchLocations.isEmpty) exactMatchLocations else prefs.flatten.distinct

      /***
        * 构造ZippedPartitionsPartition分区
        */
      new ZippedPartitionsPartition(i, rdds, locs)
    }
  }

  /***
    * ZippedPartitionsRDD的优先数据位置
    * @param s
    * @return
    */
  override def getPreferredLocations(s: Partition): Seq[String] = {
    s.asInstanceOf[ZippedPartitionsPartition].preferredLocations
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}

/***
  *
  * @param sc
  * @param f
  * @param rdd1
  * @param rdd2
  * @param preservesPartitioning
  * @param ev$1
  * @param ev$2
  * @param ev$3
  * @tparam A
  * @tparam B
  * @tparam V
  */
private[spark] class ZippedPartitionsRDD2[A: ClassTag, B: ClassTag, V: ClassTag](
    sc: SparkContext,
    var f: (Iterator[A], Iterator[B]) => Iterator[V],
    var rdd1: RDD[A],
    var rdd2: RDD[B],
    preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2), preservesPartitioning) {

  /***
    * 实现RDD的抽象方法，该方法在ZippedPartitionsBaseRDD中没有实现，因此需要在ZippedPartitionsRDD2中实现
    * @param s ZippedPartitionsRDD2的分区对象，它是ZippedPartitionsPartition类型的Partition
    * @param context
    * @return
    */
  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    /***
      * 取ZippedPartitionsPartition关联的父RDDs的分区
      */
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions

    /***
      * 获取到rdd1数据集合对应的Iterator以及rdd2数据集合对应的Iterator，然后调用f函数
      * f函数，可以查看RDD的zip方法
      */
    f(rdd1.iterator(partitions(0), context), rdd2.iterator(partitions(1), context))
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    f = null
  }
}

/***
  * 将三个RDD进行zip得到的RDD，类型为ZippedPartitionsRDD3
  * @param sc
  * @param f
  * @param rdd1
  * @param rdd2
  * @param rdd3
  * @param preservesPartitioning
  * @param ev$1
  * @param ev$2
  * @param ev$3
  * @param ev$4
  * @tparam A
  * @tparam B
  * @tparam C
  * @tparam V
  */
private[spark] class ZippedPartitionsRDD3
  [A: ClassTag, B: ClassTag, C: ClassTag, V: ClassTag](
    sc: SparkContext,
    var f: (Iterator[A], Iterator[B], Iterator[C]) => Iterator[V],
    var rdd1: RDD[A],
    var rdd2: RDD[B],
    var rdd3: RDD[C],
    preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2, rdd3), preservesPartitioning) {

  /***
    * f函数作用于三个RDD的分区数据集合对应的Iterator
    * @param s
    * @param context
    * @return
    */
  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(rdd1.iterator(partitions(0), context),
      rdd2.iterator(partitions(1), context),
      rdd3.iterator(partitions(2), context))
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    rdd3 = null
    f = null
  }
}

/***
  * 四个RDD进行zip得到的RDD
  * @param sc
  * @param f
  * @param rdd1
  * @param rdd2
  * @param rdd3
  * @param rdd4
  * @param preservesPartitioning
  * @param ev$1
  * @param ev$2
  * @param ev$3
  * @param ev$4
  * @param ev$5
  * @tparam A
  * @tparam B
  * @tparam C
  * @tparam D
  * @tparam V
  */
private[spark] class ZippedPartitionsRDD4
  [A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag](
    sc: SparkContext,
    var f: (Iterator[A], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V],
    var rdd1: RDD[A],
    var rdd2: RDD[B],
    var rdd3: RDD[C],
    var rdd4: RDD[D],
    preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2, rdd3, rdd4), preservesPartitioning) {

  /***
    *
    * @param s
    * @param context
    * @return
    */
  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    //首先获取s分区对应的其它四个RDD的分区，
    //这里是多对一的分区情况，还是OneToOneDependency
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(rdd1.iterator(partitions(0), context),
      rdd2.iterator(partitions(1), context),
      rdd3.iterator(partitions(2), context),
      rdd4.iterator(partitions(3), context))
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    rdd3 = null
    rdd4 = null
    f = null
  }
}
