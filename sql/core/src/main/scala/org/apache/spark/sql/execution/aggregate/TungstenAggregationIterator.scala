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

import org.apache.spark.{InternalAccumulator, Logging, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.execution.{UnsafeFixedWidthAggregationMap, UnsafeKVExternalSorter}
import org.apache.spark.sql.execution.metric.LongSQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.KVIterator

/**
 *
 * 基于Tungsten的Aggregation，它接受的输入是Iterator[UnsafeRow]
 *  An iterator used to evaluate aggregate functions. It operates on [[UnsafeRow]]s.
 *
 * This iterator first uses hash-based aggregation to process input rows. It uses
 * a hash map to store groups and their corresponding aggregation buffers. If we
 * this map cannot allocate memory from memory manager, it spill the map into disk
 * and create a new one. After processed all the input, then merge all the spills
 * together using external sorter, and do sort-based aggregation.
 *
 *  聚合流程：
 *  首先对输入的unsafe rows进行hash-based的聚合，具体做法是使用Hash Map来存放group keys和对应的aggregation buffer的对应关系，如果Map中还没有group key，那么
 *  将初始化一个Aggregation buffer，然后将<groupKey,aggbuffer>加入到map中；如果group key已经存在于map中，那么取出对一个的aggregation buffer，将它进行upadte操作
 *
 *  如果hash map使得内存空间不足(比如重复的group key很少导致每个group key，都会在map中建立一个groupkey和buffer的对应关系)，那么将hash map的数据spill到磁盘，然后创建一个新的hash map继续进行hash based聚合，如果内存不够再spill到磁盘
 *  重复上面的过程直到输入的所有unsafe rows处理完成，然后使用external sorter对spill到磁盘的数据进行merge，然后执行sort-based聚合
 *
 *
 * The process has the following step:
 *  - Step 0: Do hash-based aggregation.
 *  - Step 1: Sort all entries of the hash map based on values of grouping expressions and
 *            spill them to disk.
 *  - Step 2: Create a external sorter based on the spilled sorted map entries and reset the map.
 *  - Step 3: Get a sorted [[KVIterator]] from the external sorter.
 *  - Step 4: Repeat step 0 until no more input.
 *  - Step 5: Initialize sort-based aggregation on the sorted iterator.
 * Then, this iterator works in the way of sort-based aggregation.
 *
 * 聚合执行流程：
 * 第0步：执行hash-based聚合
 * 第1步：如果内存空间不足，那么将hash-based聚合使用到的Hash Map中的数据spill到磁盘，在spill前需要进行排序操作，它是根据分组列的值进行排序的
 * 第2步：基于spill到磁盘上以排序的map entries创建一个external sorter，然后重新创建第一步用到的Hash Map
 * 第3步：基于第二步创建的external sorter，获取一个KVIterator
 * 第4步：重复第0步到第4步
 * 第5步：初始化sort base aggregation
 * 问题：
 * 1. 第二步创建的external sorter和第三步创建的sorted KVIterator用来干啥了？
 * 2. 因为第0步到第3步是repeat的，那么也就是说针对每次spill都会产生一个external sorter？
 *
 *
 * The code of this class is organized as follows:
 *  - Part 1: Initializing aggregate functions.
 *  - Part 2: Methods and fields used by setting aggregation buffer values,
 *            processing input rows from inputIter, and generating output
 *            rows.
 *  - Part 3: Methods and fields used by hash-based aggregation.
 *  - Part 4: Methods and fields used when we switch to sort-based aggregation.
 *  - Part 5: Methods and fields used by sort-based aggregation.
 *  - Part 6: Loads input and process input rows.
 *  - Part 7: Public methods of this iterator.
 *  - Part 8: A utility function used to generate a result when there is no
 *            input and there is no grouping expression.
 *
  * @param groupingExpressions expressions for grouping keys
  * @param aggregateExpressions  [[AggregateExpression]] containing [[AggregateFunction]]s with mode [[Partial]], [[PartialMerge]], or [[Final]].
  * @param aggregateAttributes the attributes of the aggregateExpressions'outputs when they are stored in the final aggregation buffer.
  * @param initialInputBufferOffset
  * @param resultExpressions expressions for generating output rows.
  * @param newMutableProjection the function used to create mutable projections.
  * @param originalInputAttributes attributes of representing input rows from `inputIter
  * @param inputIter the iterator containing input [[UnsafeRow]]s.
  * @param testFallbackStartsAt The threshold when switching to sort based aggregation(can be used to force hash based aggregation to switch to sort based aggregation)
  * @param numInputRows the number of the input rows from inputIter
  * @param numOutputRows the number of the output rows after aggregation by tungsten aggregation
  * @param dataSize
  * @param spillSize
  */
class TungstenAggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    originalInputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow],
    testFallbackStartsAt: Option[Int],
    numInputRows: LongSQLMetric,
    numOutputRows: LongSQLMetric,
    dataSize: LongSQLMetric,
    spillSize: LongSQLMetric)
  extends AggregationIterator(
    groupingExpressions,
    originalInputAttributes,
    aggregateExpressions,
    aggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection) with Logging {

  ///////////////////////////////////////////////////////////////////////////
  // Part 1: Initializing aggregate functions.
  ///////////////////////////////////////////////////////////////////////////

  // Remember spill data size of this task before execute this operator so that we can
  // figure out how many bytes we spilled for this operator.
  //=================================常量01=================================
  private val spillSizeBefore = TaskContext.get().taskMetrics().memoryBytesSpilled

  ///////////////////////////////////////////////////////////////////////////
  // Part 2: Methods and fields used by setting aggregation buffer values,
  //         processing input rows from inputIter, and generating output
  //         rows.
  ///////////////////////////////////////////////////////////////////////////

  // Creates a new aggregation buffer and initializes buffer values.
  // This function should be only called at most two times (when we create the hash map,
  // and when we create the re-used buffer for sort-based aggregation).
  private def createNewAggregationBuffer(): UnsafeRow = {
    val bufferSchema = aggregateFunctions.flatMap(_.aggBufferAttributes)
    val buffer: UnsafeRow = UnsafeProjection.create(bufferSchema.map(_.dataType))
      .apply(new GenericMutableRow(bufferSchema.length))
    // Initialize declarative aggregates' buffer values
    expressionAggInitialProjection.target(buffer)(EmptyRow)
    // Initialize imperative aggregates' buffer values

    //收集imperativeAggregate function,然后调用ImperativeAggregate的initialize方法，
    //注意是收集ImperativeAggregate，而不是DeclarativeAggregate
    aggregateFunctions.collect { case f: ImperativeAggregate => f }.foreach(_.initialize(buffer))
    buffer
  }

  /** *
    * Creates a function used to generate output rows.
    * @return 返回结果是一个(UnsafeRow,MutableRow)=>UnsafeRow的函数
    */
  override protected def generateResultProjection(): (UnsafeRow, MutableRow) => UnsafeRow = {
    val modes = aggregateExpressions.map(_.mode).distinct

    //如果是partial aggregation
    if (modes.nonEmpty && !modes.contains(Final) && !modes.contains(Complete)) {
      // Fast path for partial aggregation, UnsafeRowJoiner is usually faster than projection
      val groupingAttributes = groupingExpressions.map(_.toAttribute)
      val bufferAttributes = aggregateFunctions.flatMap(_.aggBufferAttributes)
      val groupingKeySchema = StructType.fromAttributes(groupingAttributes)
      val bufferSchema = StructType.fromAttributes(bufferAttributes)
      val unsafeRowJoiner = GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)

      /**
       * 当前的UnsafeRow与currentBuffer进行join?
       */
      (currentGroupingKey: UnsafeRow, currentBuffer: MutableRow) => {
        unsafeRowJoiner.join(currentGroupingKey, currentBuffer.asInstanceOf[UnsafeRow])
      }
    }

    //如果是final aggregation，则调用super的generateResultProjection
    else {
      super.generateResultProjection()
    }
  }

  // An aggregation buffer containing initial buffer values. It is used to
  // initialize other aggregation buffers.

  //=================================常量02=================================
  private[this] val initialAggregationBuffer: UnsafeRow = createNewAggregationBuffer()

  ///////////////////////////////////////////////////////////////////////////
  // Part 3: Methods and fields used by hash-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // This is the hash map used for hash-based aggregation. It is backed by an
  // UnsafeFixedWidthAggregationMap and it is used to store
  // all groups and their corresponding aggregation buffers for hash-based aggregation.

  //=================================常量03=================================
  private[this] val hashMap = new UnsafeFixedWidthAggregationMap(
    initialAggregationBuffer,
    StructType.fromAttributes(aggregateFunctions.flatMap(_.aggBufferAttributes)),
    StructType.fromAttributes(groupingExpressions.map(_.toAttribute)),
    TaskContext.get().taskMemoryManager(),
    1024 * 16, // initial capacity
    TaskContext.get().taskMemoryManager().pageSizeBytes,
    false // disable tracking of performance metrics
  )

  // The function used to read and process input rows. When processing input rows,
  // it first uses hash-based aggregation by putting groups and their buffers in
  // hashMap. If there is not enough memory, it will multiple hash-maps, spilling
  // after each becomes full then using sort to merge these spills, finally do sort
  // based aggregation.
  // fallbackStartsAt默认值是Integer.MAX_VALUE
  private def processInputs(fallbackStartsAt: Int): Unit = {
    ///没有分组表达式，比如select count(name) from TBL_STUDENT,如果没有分区表达式，只需要一个分组
    if (groupingExpressions.isEmpty) {
      // If there is no grouping expressions, we can just reuse the same buffer over and over again.
      // Note that it would be better to eliminate the hash map entirely in the future.
      val groupingKey = groupingProjection.apply(null)

      //hashMap是谁和谁之间的map?
      val buffer: UnsafeRow = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)

      /** *
        * 遍历所有的元素，调用processRow进行聚合
        */
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        numInputRows += 1
        processRow(buffer, newInput)
      }
    } else {
      var i = 0
      //遍历每个元素
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        numInputRows += 1

        //获取分组的Key
        val groupingKey = groupingProjection.apply(newInput)
        var buffer: UnsafeRow = null

        //如果i小于fallbackStartsAt，表示继续使用hashMap进行聚合
        if (i < fallbackStartsAt) {
          buffer = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
        }

        //如果buffer为null，表示什么情况？
        if (buffer == null) {
          val sorter = hashMap.destructAndCreateExternalSorter()
          if (externalSorter == null) {
            externalSorter = sorter
          } else {
            //UnsafeKVExternalSorter进行merge
            externalSorter.merge(sorter)
          }
          i = 0
          //重新构造hash based aggregation buffer
          buffer = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
          if (buffer == null) { /**内存不足**/
            // failed to allocate the first page
            throw new OutOfMemoryError("No enough memory for aggregation")
          }
        }
        processRow(buffer, newInput)
        i += 1
      }

      //如果externalSorter不为空，则switch到sort based aggregation，
      //问题：externalSorter在什么地方初始化的？
      if (externalSorter != null) {
        val sorter = hashMap.destructAndCreateExternalSorter()
        externalSorter.merge(sorter)
        hashMap.free()

        switchToSortBasedAggregation()
      }
    }
  }

  // The iterator created from hashMap. It is used to generate output rows when we
  // are using hash-based aggregation.

  //=================================变量04=================================
  private[this] var aggregationBufferMapIterator: KVIterator[UnsafeRow, UnsafeRow] = null

  // Indicates if aggregationBufferMapIterator still has key-value pairs.

  //=================================变量05=================================
  private[this] var mapIteratorHasNext: Boolean = false

  ///////////////////////////////////////////////////////////////////////////
  // Part 4: Methods and fields used when we switch to sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // This sorter is used for sort-based aggregation. It is initialized as soon as
  // we switch from hash-based to sort-based aggregation. Otherwise, it is not used.

  //=================================变量06=================================
  private[this] var externalSorter: UnsafeKVExternalSorter = null

  /**
   * Switch to sort-based aggregation when the hash-based approach is unable to acquire memory.
   *
   * hash-based需要的内存比sort-based需要的大
    *
    * switchToSortBasedAggregation调用时，externalSorter不为null
   */
  private def switchToSortBasedAggregation(): Unit = {
    logInfo("falling back to sort based aggregation.")

    // Basically the value of the KVIterator returned by externalSorter
    // will be just aggregation buffer, so we rewrite the aggregateExpressions to reflect it.
    val newExpressions = aggregateExpressions.map {
      case agg @ AggregateExpression(_, Partial, _) =>
        agg.copy(mode = PartialMerge)
      case agg @ AggregateExpression(_, Complete, _) =>
        agg.copy(mode = Final)
      case other => other
    }
    val newFunctions = initializeAggregateFunctions(newExpressions, 0)
    val newInputAttributes = newFunctions.flatMap(_.inputAggBufferAttributes)
    sortBasedProcessRow = generateProcessRow(newExpressions, newFunctions, newInputAttributes)

    // Step 5: Get the sorted iterator from the externalSorter.
    sortedKVIterator = externalSorter.sortedIterator()

    // Step 6: Pre-load the first key-value pair from the sorted iterator to make
    // hasNext idempotent.
    sortedInputHasNewGroup = sortedKVIterator.next()

    // Copy the first key and value (aggregation buffer).
    if (sortedInputHasNewGroup) {
      val key = sortedKVIterator.getKey
      val value = sortedKVIterator.getValue
      nextGroupingKey = key.copy()
      currentGroupingKey = key.copy()
      firstRowInNextGroup = value.copy()
    }

    // Step 7: set sortBased to true.
    sortBased = true
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 5: Methods and fields used by sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // Indicates if we are using sort-based aggregation. Because we first try to use
  // hash-based aggregation, its initial value is false.
  private[this] var sortBased: Boolean = false

  // The KVIterator containing input rows for the sort-based aggregation. It will be
  // set in switchToSortBasedAggregation when we switch to sort-based aggregation.
  private[this] var sortedKVIterator: UnsafeKVExternalSorter#KVSorterIterator = null

  // The grouping key of the current group.
  private[this] var currentGroupingKey: UnsafeRow = null

  // The grouping key of next group.
  private[this] var nextGroupingKey: UnsafeRow = null

  // The first row of next group.
  private[this] var firstRowInNextGroup: UnsafeRow = null

  // Indicates if we has new group of rows from the sorted input iterator.
  private[this] var sortedInputHasNewGroup: Boolean = false

  // The aggregation buffer used by the sort-based aggregation.
  private[this] val sortBasedAggregationBuffer: UnsafeRow = createNewAggregationBuffer()

  // The function used to process rows in a group
  private[this] var sortBasedProcessRow: (MutableRow, InternalRow) => Unit = null

  // Processes rows in the current group. It will stop when it find a new group.
  private def processCurrentSortedGroup(): Unit = {
    // First, we need to copy nextGroupingKey to currentGroupingKey.
    currentGroupingKey.copyFrom(nextGroupingKey)
    // Now, we will start to find all rows belonging to this group.
    // We create a variable to track if we see the next group.
    var findNextPartition = false
    // firstRowInNextGroup is the first row of this group. We first process it.
    sortBasedProcessRow(sortBasedAggregationBuffer, firstRowInNextGroup)

    // The search will stop when we see the next group or there is no
    // input row left in the iter.
    // Pre-load the first key-value pair to make the condition of the while loop
    // has no action (we do not trigger loading a new key-value pair
    // when we evaluate the condition).
    var hasNext = sortedKVIterator.next()
    while (!findNextPartition && hasNext) {
      // Get the grouping key and value (aggregation buffer).
      val groupingKey = sortedKVIterator.getKey
      val inputAggregationBuffer = sortedKVIterator.getValue

      // Check if the current row belongs the current input row.
      if (currentGroupingKey.equals(groupingKey)) {
        sortBasedProcessRow(sortBasedAggregationBuffer, inputAggregationBuffer)

        hasNext = sortedKVIterator.next()
      } else {
        // We find a new group.
        findNextPartition = true
        // copyFrom will fail when
        nextGroupingKey.copyFrom(groupingKey)
        firstRowInNextGroup.copyFrom(inputAggregationBuffer)
      }
    }
    // We have not seen a new group. It means that there is no new row in the input
    // iter. The current group is the last group of the sortedKVIterator.
    if (!findNextPartition) {
      sortedInputHasNewGroup = false
      sortedKVIterator.close()
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 6: Loads input rows and setup aggregationBufferMapIterator if we
  //         have not switched to sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Start processing input rows,
   *
   * processInputs是在TungstenAggregationIterator构造函数中调用的
    *
    * sortBased变量会在processInputs进行赋值，处理过程中如果切换到sort based aggregation,那么会将sortBased置为true
   */
  processInputs(testFallbackStartsAt.getOrElse(Int.MaxValue))

  // If we did not switch to sort-based aggregation in processInputs,
  // we pre-load the first key-value pair from the map (to make hasNext idempotent).
  if (!sortBased) {
    // First, set aggregationBufferMapIterator.
    aggregationBufferMapIterator = hashMap.iterator()
    // Pre-load the first key-value pair from the aggregationBufferMapIterator.
    mapIteratorHasNext = aggregationBufferMapIterator.next()
    // If the map is empty, we just free it.
    if (!mapIteratorHasNext) {
      hashMap.free()
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 7: Iterator's public methods.
  ///////////////////////////////////////////////////////////////////////////

  //是否还有元素
  override final def hasNext: Boolean = {
    val a = sortBased && sortedInputHasNewGroup
    val b = !sortBased && mapIteratorHasNext
    a || b
  }

  override final def next(): UnsafeRow = {
    if (hasNext) {
      val res = if (sortBased) {
        // Process the current group.
        processCurrentSortedGroup()
        // Generate output row for the current group.
        val outputRow = generateOutput(currentGroupingKey, sortBasedAggregationBuffer)
        // Initialize buffer values for the next group.
        sortBasedAggregationBuffer.copyFrom(initialAggregationBuffer)

        outputRow
      } else {
        // We did not fall back to sort-based aggregation.
        val result =
          generateOutput(
            aggregationBufferMapIterator.getKey,
            aggregationBufferMapIterator.getValue)

        // Pre-load next key-value pair form aggregationBufferMapIterator to make hasNext
        // idempotent.
        mapIteratorHasNext = aggregationBufferMapIterator.next()

        if (!mapIteratorHasNext) {
          // If there is no input from aggregationBufferMapIterator, we copy current result.
          val resultCopy = result.copy()
          // Then, we free the map.
          hashMap.free()

          resultCopy
        } else {
          result
        }
      }

      // If this is the last record, update the task's peak memory usage. Since we destroy
      // the map to create the sorter, their memory usages should not overlap, so it is safe
      // to just use the max of the two.
      if (!hasNext) {
        val mapMemory = hashMap.getPeakMemoryUsedBytes
        val sorterMemory = Option(externalSorter).map(_.getPeakMemoryUsedBytes).getOrElse(0L)
        val peakMemory = Math.max(mapMemory, sorterMemory)
        val metrics = TaskContext.get().taskMetrics()
        dataSize += peakMemory
        spillSize += metrics.memoryBytesSpilled - spillSizeBefore
        metrics.incPeakExecutionMemory(peakMemory)
      }
      numOutputRows += 1
      res
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 8: Utility functions
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Generate a output row when there is no input and there is no grouping expression.
   */
  def outputForEmptyGroupingKeyWithoutInput(): UnsafeRow = {
    if (groupingExpressions.isEmpty) {
      sortBasedAggregationBuffer.copyFrom(initialAggregationBuffer)
      // We create a output row and copy it. So, we can free the map.
      val resultCopy =
        generateOutput(UnsafeRow.createFromByteArray(0, 0), sortBasedAggregationBuffer).copy()
      hashMap.free()
      resultCopy
    } else {
      throw new IllegalStateException(
        "This method should not be called when groupingExpressions is not empty.")
    }
  }
}
