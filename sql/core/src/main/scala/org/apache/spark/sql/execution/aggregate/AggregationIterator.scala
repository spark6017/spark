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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._

/** *
  * The base class of [[SortBasedAggregationIterator]] and [[TungstenAggregationIterator]].
  * It mainly contains two parts:
  * 1. It initializes aggregate functions.
  * 2. It creates two functions, `processRow` and `generateOutput` based on [[AggregateMode]] of
  *    its aggregate functions. `processRow` is the function to handle an input. `generateOutput`
  *    is used to generate result.
  *
  * @param groupingExpressions 分组表达式
  * @param inputAttributes attributes of representing input rows from `inputIter
  * @param aggregateExpressions  [[AggregateExpression]] containing [[AggregateFunction]]s with mode [[Partial]], [[PartialMerge]], or [[Final]].
  * @param aggregateAttributes the attributes of the aggregateExpressions'outputs when they are stored in the final aggregation buffer.
  * @param initialInputBufferOffset
  * @param resultExpressions
  * @param newMutableProjection 它是一个函数类型，函数入参是 (Seq[Expression], Seq[Attribute]),函数的返回值仍然是一个函数，这个函数
  *                             的入参是()，返回值是MutableProjection
  */
abstract class AggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    inputAttributes: Seq[Attribute],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection))
  extends Iterator[UnsafeRow] with Logging {

  ///////////////////////////////////////////////////////////////////////////
  // Initializing functions.
  ///////////////////////////////////////////////////////////////////////////

  /**
    * The following combinations of AggregationMode are supported:
    * - Partial
    * - PartialMerge (for single distinct)
    * - Partial and PartialMerge (for single distinct)
    * - Final
    * - Complete (for SortBasedAggregate with functions that does not support Partial)
    * - Final and Complete (currently not used)
    *
    * TODO: AggregateMode should have only two modes: Update and Merge, AggregateExpression
    * could have a flag to tell it's final or not.
    */
  /////============================初始化块01============================
  {
    /** *
      * 计算出聚合表达式中的聚合模式并对聚合模式的检查：
      * 1.  所有的聚合表达式最多有两个mode
      * 2. 所有的聚合表达式必须是Partial、PartialMerge的子集或者是Final、Complete的子集
      */
    val modes = aggregateExpressions.map(_.mode).distinct.toSet
    require(modes.size <= 2,
      s"$aggregateExpressions are not supported because they have more than 2 distinct modes.")
    require(modes.subsetOf(Set(Partial, PartialMerge)) || modes.subsetOf(Set(Final, Complete)),
      s"$aggregateExpressions can't have Partial/PartialMerge and Final/Complete in the same time.")
  }

  /** *
    * Initialize all AggregateFunctions by binding references if necessary and set inputBufferOffset and mutableBufferOffset.
    *
    * 问题：binding references表示什么意思？
    *
    * @param expressions 聚合表达式的集合
    * @param startingInputBufferOffset
    * @return 聚合函数的集合
    */
  protected def initializeAggregateFunctions(
      expressions: Seq[AggregateExpression],
      startingInputBufferOffset: Int): Array[AggregateFunction] = {
    var mutableBufferOffset = 0
    var inputBufferOffset: Int = startingInputBufferOffset

    /** *
      * 每个聚合表达式转换为一个聚合函数
      */
    val functions = new Array[AggregateFunction](expressions.length)
    var i = 0

    /** *
      * 遍历所有的聚合表达式
      */
    while (i < expressions.length) {

      /** *
        * 取出聚合表达式中的聚合函数，它是AggregateFunction类型
        */
      val func = expressions(i).aggregateFunction

      /** *
        * 对AggregateFunction进行reference binding
        */
      val funcWithBoundReferences: AggregateFunction = expressions(i).mode match {

        /**
         * 如果是指令式聚合函数且聚合模式是Partial或者Complete，那么调用 BindReferences.bindReference对函数进行绑定
         */
        case Partial | Complete if func.isInstanceOf[ImperativeAggregate] =>
          // We need to create BoundReferences if the function is not an
          // expression-based aggregate function (it does not support code-gen) and the mode of
          // this function is Partial or Complete because we will call eval of this
          // function's children in the update method of this aggregate function.
          // Those eval calls require BoundReferences to work.
          BindReferences.bindReference(func, inputAttributes)

        /**
          * 对于其它情况
          */
        case _ =>
          // We only need to set inputBufferOffset for aggregate functions with mode
          // PartialMerge and Final.
          val updatedFunc = func match {
            case function: ImperativeAggregate =>
              function.withNewInputAggBufferOffset(inputBufferOffset)
            case function => function
          }
          inputBufferOffset += func.aggBufferSchema.length
          updatedFunc
      }


      val funcWithUpdatedAggBufferOffset = funcWithBoundReferences match {
        case function: ImperativeAggregate =>
          // Set mutableBufferOffset for this function. It is important that setting
          // mutableBufferOffset happens after all potential bindReference operations
          // because bindReference will create a new instance of the function.
          function.withNewMutableAggBufferOffset(mutableBufferOffset)
        case function => function
      }
      mutableBufferOffset += funcWithUpdatedAggBufferOffset.aggBufferSchema.length
      functions(i) = funcWithUpdatedAggBufferOffset
      i += 1
    }
    functions
  }

  /////============================常量01============================
  /** *
    * 调用initializeAggregateFunctions对聚合表达式中的聚合函数进行初始化
    */
  protected val aggregateFunctions: Array[AggregateFunction] =
  {
    val a = initializeAggregateFunctions(aggregateExpressions, initialInputBufferOffset)
    a
  }

  // Positions of those imperative aggregate functions in allAggregateFunctions.
  // For example, we have func1, func2, func3, func4 in aggregateFunctions, and
  // func2 and func3 are imperative aggregate functions.
  // ImperativeAggregateFunctionPositions will be [1, 2].
  /////============================常量02============================
  /** *
    * 查找出所有的指令式聚合函数
    */
  protected[this] val allImperativeAggregateFunctionPositions: Array[Int] = {
    val positions = new ArrayBuffer[Int]()
    var i = 0
    while (i < aggregateFunctions.length) {
      aggregateFunctions(i) match {
        case agg: DeclarativeAggregate =>
        case _ => positions += i
      }
      i += 1
    }
    val a = positions.toArray
    a
  }

  // The projection used to initialize buffer values for all expression-based aggregates.
  /////============================常量03============================
  /** *
    * 用于为declarative based aggregations设置初始值
    */
  protected[this] val expressionAggInitialProjection = {

    /** *
      * DeclarativeAggregate的initialValues是一个表达式，比如Count函数的initialValues返回的是Literal(0)
      */
    val initExpressions = aggregateFunctions.flatMap {
      case ae: DeclarativeAggregate => {
        val a = ae.initialValues
        a
      }
      // For the positions corresponding to imperative aggregate functions, we'll use special
      // no-op expressions which are ignored during projection code-generation.
      case i: ImperativeAggregate => {
        val a = Seq.fill(i.aggBufferAttributes.length)(NoOp)
        a
      }
    }

    /**
      * 调用传入的newMutableProjection方法
      * newMutableProjection函数的真实定义是在SortBasedAggregate的doExecute方法中
      *
      */
    newMutableProjection(initExpressions, Nil)()
  }

  // All imperative AggregateFunctions.
  /////============================常量04============================
  /** *
    *
    */
  protected[this] val allImperativeAggregateFunctions: Array[ImperativeAggregate] =
  {
    val a = allImperativeAggregateFunctionPositions
      .map(aggregateFunctions)
      .map(_.asInstanceOf[ImperativeAggregate])
    a
  }

  /**
   * 返回处理input row的函数，
   * @param expressions 类型为AggregateExpression的集合
   * @param functions 类型为AggregateFunction的集合
   * @param inputAttributes
   * @return 一个函数，函数类型是(MutableRow, InternalRow) => Unit，MutableRow是aggregation buffer，InternalRow是当前处理的row
   */
  protected def generateProcessRow(
      expressions: Seq[AggregateExpression],
      functions: Seq[AggregateFunction],
      inputAttributes: Seq[Attribute]): (MutableRow, InternalRow) => Unit = {
    val joinedRow = new JoinedRow
    if (expressions.nonEmpty) {
      /**
        * 1. 获得mergeExpressions，
        */
      val mergeExpressions = functions.zipWithIndex.flatMap {

        /**
          * 如果是DeclarativeAggregate(AggregateFunction是DeclarativeAggregate的子类)
          */
        case (ae: DeclarativeAggregate, i) =>
          expressions(i).mode match {
            /**
              * 如果是Partial和Complete状态，那么使用的是updateExpressions
              */
            case Partial | Complete => {
              val mode = expressions(i).mode
              val e =  ae.updateExpressions
              e
            }

            /**
              * 如果是PartialMerge或者Final，那么调用mergeExpression
              */
            case PartialMerge | Final => {
              val mode = expressions(i).mode
              val e = ae.mergeExpressions
              e
            }
          }

        /**
          * 如果是AggregateFunction，全部填充NoOP？
          */
        case (agg: AggregateFunction, _) => Seq.fill(agg.aggBufferAttributes.length)(NoOp)
      }


      /**
        * 2. 获得updateFunction,只对ImperativeAggregate类型的AggregateFunction起作用
        */
      val updateFunctions = functions.zipWithIndex.collect {
        case (ae: ImperativeAggregate, i) =>
          expressions(i).mode match {

            /**
              * 如果是Partial或者Complete Mode，那么updateFunction就是(buffer: MutableRow, row: InternalRow) => ae.update(buffer, row)
              * 意思是以aggregateBuffer和InternalRow为参数，调用ImperativeAggregate的update方法
              */
            case Partial | Complete =>
              (buffer: MutableRow, row: InternalRow) => ae.update(buffer, row)

            /**
              * 如果是PartialMerge或者Final Mode，那么updateFunction就是(buffer: MutableRow, row: InternalRow) => ae.merge(buffer, row)
              *  意思是以aggregateBuffer和InternalRow为参数，调用ImperativeAggregate的merge方法
              */
            case PartialMerge | Final =>
              (buffer: MutableRow, row: InternalRow) => ae.merge(buffer, row)
          }
      }


      // This projection is used to merge buffer values for all expression-based aggregates.
      val aggregationBufferSchema = functions.flatMap(_.aggBufferAttributes)

      /***
        *mergeExpressions是Seq[Expression]
        */
      val updateProjection =
        newMutableProjection(mergeExpressions, aggregationBufferSchema ++ inputAttributes)()

      /***
        * 返回值，updateProjection需要使用mergeExpressions，因此方法体中的updateProjection.target也会用到mergeExpression
        */
      (currentBuffer: MutableRow, row: InternalRow) => {
        // Process all expression-based aggregate functions.


        val target = updateProjection.target(currentBuffer)
        target(joinedRow(currentBuffer, row))
        // Process all imperative aggregate functions.
        var i = 0
        while (i < updateFunctions.length) {
          updateFunctions(i)(currentBuffer, row)
          i += 1
        }
      }
    } else {
      // Grouping only.
      (currentBuffer: MutableRow, row: InternalRow) => {}
    }
  }

  /////============================常量05============================
  /** *
    * processRow是一个函数，它在构造AggregateIterator时就初始化完成
    * processRow是个值函数，入参是(MutableRow,InternalRow)，返回值是Unit
    */
  protected val processRow: (MutableRow, InternalRow) => Unit =
  {
    //调用generateProcessRow创建
    val a = generateProcessRow(aggregateExpressions, aggregateFunctions, inputAttributes)
    a
  }

  /**
    * 分组投影
    */
  /////============================常量06============================
  protected val groupingProjection: UnsafeProjection = {
    val a = groupingExpressions
    val b = inputAttributes
    val c = UnsafeProjection.create(a, b)
    c
  }

  /**
    * 将分组表达式转换为Attributes
    */
  /////============================常量07============================
  protected val groupingAttributes = {
    val a = groupingExpressions.map(_.toAttribute)
    a
  }

    /**
     *  Initializing the function used to generate the output row.
     *  调用AggregateFunction的evaluateExpression方法，做最后的计算
     */
  protected def generateResultProjection(): (UnsafeRow, MutableRow) => UnsafeRow = {
    val joinedRow = new JoinedRow
    val modes = aggregateExpressions.map(_.mode).distinct
    val bufferAttributes = aggregateFunctions.flatMap(_.aggBufferAttributes)
    if (modes.contains(Final) || modes.contains(Complete)) {
      val evalExpressions = aggregateFunctions.map {
        case ae: DeclarativeAggregate => ae.evaluateExpression
        case agg: AggregateFunction => NoOp
      }
      val aggregateResult = new SpecificMutableRow(aggregateAttributes.map(_.dataType))
      val expressionAggEvalProjection = newMutableProjection(evalExpressions, bufferAttributes)()
      expressionAggEvalProjection.target(aggregateResult)

      val resultProjection =
        UnsafeProjection.create(resultExpressions, groupingAttributes ++ aggregateAttributes)

      (currentGroupingKey: UnsafeRow, currentBuffer: MutableRow) => {
        // Generate results for all expression-based aggregate functions.
        expressionAggEvalProjection(currentBuffer)
        // Generate results for all imperative aggregate functions.
        var i = 0
        while (i < allImperativeAggregateFunctions.length) {
          aggregateResult.update(
            allImperativeAggregateFunctionPositions(i),
            allImperativeAggregateFunctions(i).eval(currentBuffer))
          i += 1
        }
        resultProjection(joinedRow(currentGroupingKey, aggregateResult))
      }
    } else if (modes.contains(Partial) || modes.contains(PartialMerge)) {
      val resultProjection = UnsafeProjection.create(
        groupingAttributes ++ bufferAttributes,
        groupingAttributes ++ bufferAttributes)
      (currentGroupingKey: UnsafeRow, currentBuffer: MutableRow) => {
        resultProjection(joinedRow(currentGroupingKey, currentBuffer))
      }
    } else {
      // Grouping-only: we only output values based on grouping expressions.
      val resultProjection = UnsafeProjection.create(resultExpressions, groupingAttributes)
      (currentGroupingKey: UnsafeRow, currentBuffer: MutableRow) => {
        resultProjection(currentGroupingKey)
      }
    }
  }

  /**
   * 返回值是一个函数
   */
  /////============================常量08============================
  protected val generateOutput: (UnsafeRow, MutableRow) => UnsafeRow =
  {
    val a = generateResultProjection()
    a
  }

  /** Initializes buffer values for all aggregate functions. */
  protected def initializeBuffer(buffer: MutableRow): Unit = {
    expressionAggInitialProjection.target(buffer)(EmptyRow)
    var i = 0
    while (i < allImperativeAggregateFunctions.length) {
      allImperativeAggregateFunctions(i).initialize(buffer)
      i += 1
    }
  }
}
