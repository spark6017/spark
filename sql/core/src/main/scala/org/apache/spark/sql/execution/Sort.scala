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

package org.apache.spark.sql.execution

import org.apache.spark.{InternalAccumulator, SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, OrderedDistribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Performs (external) sorting. 排序物理计划(可能是外排序)
  * 如果global为true，表示全局排序；如果为false，表示分区内排序
  *
 *
 * @param global when true performs a global sort of all partitions by shuffling the data first
 *               if necessary.
 * @param testSpillFrequency Method for configuring periodic spilling in unit tests. If set, will
 *                           spill every `frequency` records.
 */
case class Sort(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan,
    testSpillFrequency: Int = 0)
  extends UnaryNode {

  /**
   * 本算子的输出属性，Sort算子的输出属性依赖于child的output
   * @return
   */
  override def output: Seq[Attribute] = child.output

  /**
   *  这个算子执行结果(Output)分区数据的排序性，即本算子的输出是带有排序特性的
   * @return
   */
  override def outputOrdering: Seq[SortOrder] = sortOrder

  /**
   *  Child Distribution,如果是全量排序，那么要求孩子节点的Distribution是OrderedDistribution[SortOrders]
    *  如果是局部排序，那么对孩子物理计划的输出不要特定的分布
   * @return
   */
  override def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  override private[sql] lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

  /***
    *
    * @return
    */
  protected override def doExecute(): RDD[InternalRow] = {

//    for (i <-1 to 10) {
//      println("------------------------------------------------------------")
//    }
    val schema = child.schema

    /***
      * child物理计划的输出属性，类型是Seq[Attribute]
      */
    val childOutput = child.output

    val dataSize = longMetric("dataSize")
    val spillSize = longMetric("spillSize")

    /***
      *
      */
    child.execute().mapPartitionsInternal { iter =>

        /**
        * 创建对UnsafeRow进行排序的Ordering对象,childOutput是子物理计划的输出属性,
        * newOrdering是Ordering[InternalRow]类型的对象，它有compare方法,用于对InternalRow进行排序
          * 问题： 对UnsafeRow如何排序？
          *
        */
    val ordering = newOrdering(sortOrder, childOutput)

      // The comparator for comparing prefix,为什么只取sortOrder的第一个元素？
      val boundSortExpression : SortOrder = BindReferences.bindReference(sortOrder.head, childOutput)

      // 创建prefix comparator
      val prefixComparator = SortPrefixUtils.getPrefixComparator(boundSortExpression)

      // The generator for prefix
      val sortPrefixes = Seq(SortPrefix(boundSortExpression))
      val prefixProjection = UnsafeProjection.create(sortPrefixes)

      /***
        * 实现UnsafeExternalRowSorter.PrefixComputer接口
        */
      val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
        override def computePrefix(row: InternalRow): Long = {
          val prefix = prefixProjection.apply(row)
          val ret = prefix.getLong(0)
          ret
        }
      }

      val pageSize = SparkEnv.get.memoryManager.pageSizeBytes

      /***
        * 对于SparkSQL而眼，创建UnsafeExternalRowSorter对UnsafeRow进行排序，
        * UnsafeExternalRowSorter包装了UnsafeExternalSorter
        */
      val sorter = new UnsafeExternalRowSorter(
        schema, ordering, prefixComparator, prefixComputer, pageSize)

      if (testSpillFrequency > 0) {
        sorter.setTestSpillFrequency(testSpillFrequency)
      }

      val metrics = TaskContext.get().taskMetrics()
      // Remember spill data size of this task before execute this operator so that we can
      // figure out how many bytes we spilled for this operator.
      val spillSizeBefore = metrics.memoryBytesSpilled


      /**
        * 调用UnsafeExternalRowSorter的sort方法完成排序
        * 问题：
        * 1. 当内存空间不足时，spill到磁盘的逻辑是在哪里执行的？
        *
        */
      val sortedIterator = sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])

      dataSize += sorter.getPeakMemoryUsage
      spillSize += metrics.memoryBytesSpilled - spillSizeBefore
      metrics.incPeakExecutionMemory(sorter.getPeakMemoryUsage)

      sortedIterator
    }
  }
}
