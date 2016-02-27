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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.execution.metric.LongSQLMetric

/**
  * An iterator used to evaluate [[AggregateFunction]]. It assumes the input rows have been
  * sorted by values of [[groupingExpressions]].
  *
  * 是一个Iterator，需要实现hasNext和next方法
  *
  *
  * SortBasedAggregation分为两个阶段，Partial和Final，它们都需要SortBasedAggregation，那么也就需要创建两个SortBasedAggregationIterator
  *
  * 那么两个阶段都会调用SortBasedAggregationIterator的hasNext和next方法
  *
  *
  *
  * @param groupingExpressions 分组表达式
  * @param valueAttributes 孩子物理计划的attributes
  * @param inputIterator  分区内的数据,是InternalRow的Iterator
  * @param aggregateExpressions 封装了aggregate function的expression
  * @param aggregateAttributes aggregate function对应的attribute。对于select count（1） as total_count from dual，那么attribute
  * @param initialInputBufferOffset 这个offset是干啥的？
  * @param resultExpressions
  * @param newMutableProjection 一个函数，入参是Seq[Expression]和Seq[Attribute]，返回值一个函数，这个函数的入参是空，返回值是MutableProjection
  * @param numInputRows
  * @param numOutputRows
  */
class SortBasedAggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    valueAttributes: Seq[Attribute],
    inputIterator: Iterator[InternalRow],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    numInputRows: LongSQLMetric,
    numOutputRows: LongSQLMetric)
  extends AggregationIterator(
    groupingExpressions,
    valueAttributes,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection) {

  /**
    * Creates a new aggregation buffer and initializes buffer values
    * for all aggregate functions.
   *
   * 所谓的Buffer或者AggregateBuffer其实是一个MutableRow，为什么是一个MutableRow
   * 为什么是Row？原因是每个聚合函数都是一个值，比如Count, Max,Min, Avg, 所以一行就可以放下
   * 为什么是Mutable的Row?原因是每个聚合函数都需要对结果按行聚合，
    */
  private def newBuffer: MutableRow = {

    /**
     * AggregateBuffer还有Schema？原因是每个Buffer是一个Row，所以需要有列信息
     * 这个列信息是存放在AggregateFunction的aggBufferAttributes中的
     */
    val bufferSchema = aggregateFunctions.flatMap(_.aggBufferAttributes)
    val bufferRowSize: Int = bufferSchema.length

    val genericMutableBuffer = new GenericMutableRow(bufferRowSize)
    val useUnsafeBuffer = bufferSchema.map(_.dataType).forall(UnsafeRow.isMutable)

    val buffer = if (useUnsafeBuffer) {
      val unsafeProjection =
        UnsafeProjection.create(bufferSchema.map(_.dataType))
      unsafeProjection.apply(genericMutableBuffer)
    } else {
      genericMutableBuffer
    }
    initializeBuffer(buffer)
    buffer
  }

  ///////////////////////////////////////////////////////////////////////////
  // Mutable states for sort based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // The partition key of the current partition.
  private[this] var currentGroupingKey: UnsafeRow = _

  // The partition key of next partition.
  private[this] var nextGroupingKey: UnsafeRow = _

  // The first row of next partition.
  private[this] var firstRowInNextGroup: InternalRow = _

  // Indicates if we has new group of rows from the sorted input iterator
  /**
    * 所谓的New Group是分组的新值吗？比如group by class_id，如果class_id有三个值，那么就有三个group？
    *
    */
  private[this] var sortedInputHasNewGroup: Boolean = false

  // The aggregation buffer used by the sort-based aggregation.
  private[this] val sortBasedAggregationBuffer: MutableRow = newBuffer

  // An SafeProjection to turn UnsafeRow into GenericInternalRow, because UnsafeRow can't be
  // compared to MutableRow (aggregation buffer) directly.
  private[this] val safeProj: Projection = FromUnsafeProjection(valueAttributes.map(_.dataType))

  /***
    * 初始化sortedInputHasNewGroup

    */
  protected def initialize(): Unit = {
    if (inputIterator.hasNext) {
      initializeBuffer(sortBasedAggregationBuffer)
      val inputRow = inputIterator.next()

      /***
        * 创建grouping key
        */
      nextGroupingKey = groupingProjection(inputRow).copy()
      firstRowInNextGroup = inputRow.copy()
      numInputRows += 1
      sortedInputHasNewGroup = true
    } else {
      // This inputIter is empty.
      sortedInputHasNewGroup = false
    }
  }

  /***
    * 构造SortBasedAggregationIterator调用initialize方法
    */
  initialize()

  /**
    * Processes rows in the current group. It will stop when it find a new group.
    *
    * 更新sortedInputHasNewGroup
    *
    */
  protected def processCurrentSortedGroup(): Unit = {
    currentGroupingKey = nextGroupingKey
    // Now, we will start to find all rows belonging to this group.
    // We create a variable to track if we see the next group.
    /**所谓的一个Group，就是grouping key是一样的那些记录**/
    var findNextPartition = false
    // firstRowInNextGroup is the first row of this group. We first process it.
    val proj = safeProj(firstRowInNextGroup)
    processRow(sortBasedAggregationBuffer, proj)

    // The search will stop when we see the next group or there is no
    // input row left in the iter.
    while (!findNextPartition && inputIterator.hasNext) {

      /**获得要处理的Row**/
      val currentRow = inputIterator.next()
      // Get the grouping key.
      val groupingKey = groupingProjection(currentRow)
      numInputRows += 1

      // Check if the current row belongs the current input row.
      if (currentGroupingKey == groupingKey) {
        val proj = safeProj(currentRow)
        processRow(sortBasedAggregationBuffer, proj)
      } else {
        // We find a new group.
        findNextPartition = true
        nextGroupingKey = groupingKey.copy()
        firstRowInNextGroup = currentRow.copy()
      }
    }
    // We have not seen a new group. It means that there is no new row in the input
    // iter. The current group is the last group of the iter.
    if (!findNextPartition) {
      sortedInputHasNewGroup = false
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Iterator's public methods
  ///////////////////////////////////////////////////////////////////////////

  override final def hasNext: Boolean = {
    val hasNext = sortedInputHasNewGroup
    hasNext
  }

  /***
    * 读取下一个元素，返回UnsafeRow，这里是处理所有相同的grouping key，返回一个UnsafeRow
    * 也就是说，next方法中有处理current sorted group
    * @return
    */
  override final def next(): UnsafeRow = {

    /**
      * 如果还有新组
      */
    if (hasNext) {
      // Process the current group.
      /**
        * 处理当前组
        */
      processCurrentSortedGroup()
      // Generate output row for the current group.
      val outputRow = generateOutput(currentGroupingKey, sortBasedAggregationBuffer)
      // Initialize buffer values for the next group.
      initializeBuffer(sortBasedAggregationBuffer)
      numOutputRows += 1
      outputRow
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  def outputForEmptyGroupingKeyWithoutInput(): UnsafeRow = {
    initializeBuffer(sortBasedAggregationBuffer)
    generateOutput(UnsafeRow.createFromByteArray(0, 0), sortBasedAggregationBuffer)
  }
}
