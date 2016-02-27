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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.sequenceOption
import org.apache.spark.sql.types._

/**
  * The mode of an [[AggregateFunction]].
  * AggregateFunction的聚合模式
  *
  * */
private[sql] sealed trait AggregateMode

/**
 * An [[AggregateFunction]] with [[Partial]] mode is used for partial aggregation.
 * This function updates the given aggregation buffer（问题3：aggregation buffer就是指的接收聚合结果的Row吧） with the original input of this
 * function（问题1：original input指的是什么）. When it has processed all input rows（问题2：all input rows指的是什么）, the aggregation buffer is returned.
  *
 */
private[sql] case object Partial extends AggregateMode

/**
 * An [[AggregateFunction]] with [[PartialMerge]] mode is used to merge aggregation buffers
 * containing intermediate results(问题：此处的中间结果指的是什么？) for this function.
 * This function updates the given aggregation buffer by merging multiple aggregation buffers.（多个aggregation buffer进行合并）
  *
 * When it has processed all input rows, the aggregation buffer is returned.
 */
private[sql] case object PartialMerge extends AggregateMode

/**
 * An [[AggregateFunction]] with [[Final]] mode is used to merge aggregation buffers
 * containing intermediate results for this function and then generate final result.
 * This function updates the given aggregation buffer by merging multiple aggregation buffers.
 * When it has processed all input rows, the final result of this function is returned.
 *
 * 在PartialMerge基础之上求final result
 */
private[sql] case object Final extends AggregateMode

/**
 * An [[AggregateFunction]] with [[Complete]] mode is used to evaluate this function directly
 * from original input rows without any partial aggregation.
 * This function updates the given aggregation buffer with the original input of this
 * function. When it has processed all input rows, the final result of this function is returned.
 *
 * Complete mode不需要做partial aggregation
 */
private[sql] case object Complete extends AggregateMode

/**
 * A place holder expressions used in code-gen, it does not change the corresponding value
 * in the row.
 */
private[sql] case object NoOp extends Expression with Unevaluable {
  override def nullable: Boolean = true
  override def dataType: DataType = NullType
  override def children: Seq[Expression] = Nil
}

/**
  * A container for an [[AggregateFunction]] with its [[AggregateMode]] and a field
  * (`isDistinct`) indicating if DISTINCT keyword is specified for this function.
  *
  * @param aggregateFunction
  * @param mode
  * @param isDistinct
  */
private[sql] case class AggregateExpression(
    aggregateFunction: AggregateFunction,
    mode: AggregateMode,
    isDistinct: Boolean)
  extends Expression
  with Unevaluable {

  override def children: Seq[Expression] = aggregateFunction :: Nil
  override def dataType: DataType = aggregateFunction.dataType
  override def foldable: Boolean = false
  override def nullable: Boolean = aggregateFunction.nullable

  override def references: AttributeSet = {
    val childReferences = mode match {
      case Partial | Complete => aggregateFunction.references.toSeq
      case PartialMerge | Final => aggregateFunction.aggBufferAttributes
    }

    AttributeSet(childReferences)
  }

  override def prettyString: String = aggregateFunction.prettyString

  override def toString: String = s"($aggregateFunction,mode=$mode,isDistinct=$isDistinct)"

  override def sql: String = aggregateFunction.sql(isDistinct)
}

/**
 * AggregateFunction is the superclass of two aggregation function interfaces:
 *
 *  - [[ImperativeAggregate]] is for aggregation functions that are specified in terms of
 *    initialize(), update(), and merge() functions that operate on Row-based aggregation buffers.
 *  - [[DeclarativeAggregate]] is for aggregation functions that are specified using
 *    Catalyst expressions.
 *
 * In both interfaces, aggregates must define the schema ([[aggBufferSchema]]) and attributes
 * ([[aggBufferAttributes]]) of an aggregation buffer which is used to hold partial aggregate
 * results. At runtime, multiple aggregate functions are evaluated by the same operator using a
 * combined aggregation buffer which concatenates the aggregation buffers of the individual
 * aggregate functions.
 *
 * Code which accepts [[AggregateFunction]] instances should be prepared to handle both types of
 * aggregate functions.
 */
sealed abstract class AggregateFunction extends Expression with ImplicitCastInputTypes {

  /** An aggregate function is not foldable.
    * 问题：聚合函数为什么不是可折叠的，原因是聚合函数不是常亮，所以不能在解析阶段进行计算
    * */
  final override def foldable: Boolean = false

  /**
    * The schema of the aggregation buffer，它是根据aggBufferAttributes计算来的
    *
    * */
  def aggBufferSchema: StructType

  /**
    *
    * Attributes of fields in aggBufferSchema.
    *
    * 所谓的aggBuffer其实是一个MutableRow，而MutableRow是需要有Schema信息的，这个函数就是定义
    * 这个MutableRow的Schema信息，可以查看Count
    *
    */
  def aggBufferAttributes: Seq[AttributeReference]

  /**
   * Attributes of fields in input aggregation buffers (immutable aggregation buffers that are
   * merged with mutable aggregation buffers in the merge() function or merge expressions).
   * These attributes are created automatically by cloning the [[aggBufferAttributes]].
   */
  def inputAggBufferAttributes: Seq[AttributeReference]

  /**
   * Indicates if this function supports partial aggregation.
   * Currently Hive UDAF is the only one that doesn't support partial aggregation.
   *
   * 何谓partial aggregation？支持局部聚合的意思是说可以按照先在分区内进行聚合，再进行分区间合并，最后再进行final的evaluate
   */
  def supportsPartial: Boolean = true

  /**
   * Result of the aggregate function when the input is empty. This is currently only used for the
   * proper rewriting of distinct aggregate functions.
   */
  def defaultResult: Option[Literal] = None

  /**
   * Wraps this [[AggregateFunction]] in an [[AggregateExpression]] because
   * [[AggregateExpression]] is the container of an [[AggregateFunction]], aggregation mode,
   * and the flag indicating if this aggregation is distinct aggregation or not.
   * An [[AggregateFunction]] should not be used without being wrapped in
   * an [[AggregateExpression]].
    *
    * AggregateFunction虽然没有继承自AggregateExpression，但是AggregateFunction可以转换为AggregateExpression
   */
  def toAggregateExpression(): AggregateExpression = toAggregateExpression(isDistinct = false)

  /**
   * Wraps this [[AggregateFunction]] in an [[AggregateExpression]] and set isDistinct
   * field of the [[AggregateExpression]] to the given value because
   * [[AggregateExpression]] is the container of an [[AggregateFunction]], aggregation mode,
   * and the flag indicating if this aggregation is distinct aggregation or not.
   * An [[AggregateFunction]] should not be used without being wrapped in
   * an [[AggregateExpression]].
   *
    * 问题： 在将AggregateFunction转换为AggregateExpression时，为什么AggregateMode是Complete的？
   * AggregateMode是什么概念？
   */
  def toAggregateExpression(isDistinct: Boolean): AggregateExpression = {
    AggregateExpression(aggregateFunction = this, mode = Complete, isDistinct = isDistinct)
  }

  def sql(isDistinct: Boolean): String = {
    val distinct = if (isDistinct) "DISTINCT " else " "
    s"$prettyName($distinct${children.map(_.sql).mkString(", ")})"
  }
}

/**
 * API for aggregation functions that are expressed in terms of imperative initialize(), update(),
 * and merge() functions which operate on Row-based aggregation buffers.
 *
 * Within these functions, code should access fields of the mutable aggregation buffer by adding the
 * bufferSchema-relative field number to `mutableAggBufferOffset` then using this new field number
 * to access the buffer Row. This is necessary because this aggregation function's buffer is
 * embedded inside of a larger shared aggregation buffer when an aggregation operator evaluates
 * multiple aggregate functions at the same time.
 *
 * We need to perform similar field number arithmetic when merging multiple intermediate
 * aggregate buffers together in `merge()` (in this case, use `inputAggBufferOffset` when accessing
 * the input buffer).
 *
 * Correct ImperativeAggregate evaluation depends on the correctness of `mutableAggBufferOffset` and
 * `inputAggBufferOffset`, but not on the correctness of the attribute ids in `aggBufferAttributes`
 * and `inputAggBufferAttributes`.
 *
 * @see [[DeclarativeAggregate]]
 */
abstract class ImperativeAggregate extends AggregateFunction with CodegenFallback {

  /**
   * The offset of this function's first buffer value in the underlying shared mutable aggregation
   * buffer.
   *
   * For example, we have two aggregate functions `avg(x)` and `avg(y)`, which share the same
   * aggregation buffer. In this shared buffer, the position of the first buffer value of `avg(x)`
   * will be 0 and the position of the first buffer value of `avg(y)` will be 2:
   * {{{
   *          avg(x) mutableAggBufferOffset = 0
   *                  |
   *                  v
   *                  +--------+--------+--------+--------+
   *                  |  sum1  | count1 |  sum2  | count2 |
   *                  +--------+--------+--------+--------+
   *                                    ^
   *                                    |
   *                     avg(y) mutableAggBufferOffset = 2
   * }}}
   */
  protected val mutableAggBufferOffset: Int

  /**
   * Returns a copy of this ImperativeAggregate with an updated mutableAggBufferOffset.
   * This new copy's attributes may have different ids than the original.
   */
  def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate

  /**
   * The offset of this function's start buffer value in the underlying shared input aggregation
   * buffer. An input aggregation buffer is used when we merge two aggregation buffers together in
   * the `update()` function and is immutable (we merge an input aggregation buffer and a mutable
   * aggregation buffer and then store the new buffer values to the mutable aggregation buffer).
   *
   * An input aggregation buffer may contain extra fields, such as grouping keys, at its start, so
   * mutableAggBufferOffset and inputAggBufferOffset are often different.
   *
   * For example, say we have a grouping expression, `key`, and two aggregate functions,
   * `avg(x)` and `avg(y)`. In the shared input aggregation buffer, the position of the first
   * buffer value of `avg(x)` will be 1 and the position of the first buffer value of `avg(y)`
   * will be 3 (position 0 is used for the value of `key`):
   * {{{
   *          avg(x) inputAggBufferOffset = 1
   *                   |
   *                   v
   *          +--------+--------+--------+--------+--------+
   *          |  key   |  sum1  | count1 |  sum2  | count2 |
   *          +--------+--------+--------+--------+--------+
   *                                     ^
   *                                     |
   *                       avg(y) inputAggBufferOffset = 3
   * }}}
   */
  protected val inputAggBufferOffset: Int

  /**
   * Returns a copy of this ImperativeAggregate with an updated mutableAggBufferOffset.
   * This new copy's attributes may have different ids than the original.
   */
  def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate

  // Note: although all subclasses implement inputAggBufferAttributes by simply cloning
  // aggBufferAttributes, that common clone code cannot be placed here in the abstract
  // ImperativeAggregate class, since that will lead to initialization ordering issues.

  /**
   * Initializes the mutable aggregation buffer located in `mutableAggBuffer`.
   *
   * Use `fieldNumber + mutableAggBufferOffset` to access fields of `mutableAggBuffer`.
   */
  def initialize(mutableAggBuffer: MutableRow): Unit

  /**
   * Updates its aggregation buffer, located in `mutableAggBuffer`, based on the given `inputRow`.
   *
   * Use `fieldNumber + mutableAggBufferOffset` to access fields of `mutableAggBuffer`.
   */
  def update(mutableAggBuffer: MutableRow, inputRow: InternalRow): Unit

  /**
   * Combines new intermediate results from the `inputAggBuffer` with the existing intermediate
   * results in the `mutableAggBuffer.`
   *
   * Use `fieldNumber + mutableAggBufferOffset` to access fields of `mutableAggBuffer`.
   * Use `fieldNumber + inputAggBufferOffset` to access fields of `inputAggBuffer`.
   */
  def merge(mutableAggBuffer: MutableRow, inputAggBuffer: InternalRow): Unit
}

/**
 * API for aggregation functions that are expressed in terms of Catalyst expressions.
 *
 * When implementing a new expression-based aggregate function, start by implementing
 * `bufferAttributes`, defining attributes for the fields of the mutable aggregation buffer. You
 * can then use these attributes when defining `updateExpressions`, `mergeExpressions`, and
 * `evaluateExpressions`.
 *
 * Please note that children of an aggregate function can be unresolved (it will happen when
 * we create this function in DataFrame API). So, if there is any fields in
 * the implemented class that need to access fields of its children, please make
 * those fields `lazy val`s.
 *
 *
 * 何为声明式Aggregate? 基于表达式的Aggregate,它继承自AggregateFunction，而AggregateFunction继承自Expression
  * 常见的聚合函数，如Count、Max、Min、Average、Sum都是继承自DeclarativeAggregate
 *
 * 跟map-side combine的思路差不多
 *        initialize
 *       update
 *       merge
 */
abstract class DeclarativeAggregate
  extends AggregateFunction
  with Serializable
  with Unevaluable {

  /**
   * Expressions for initializing empty aggregation buffers.
   *
   * 1. 初始化一个Aggregation Buffer用于聚合操作
   */
  val initialValues: Seq[Expression]

  /**
   * Expressions for updating the mutable aggregation buffer based on an input row.
   *
   * 2. 分区内聚合: 给定Input Row，更新Aggregation Buffer
   */
  val updateExpressions: Seq[Expression]

  /**
   * A sequence of expressions for merging two aggregation buffers together. When defining these
   * expressions, you can use the syntax `attributeName.left` and `attributeName.right` to refer
   * to the attributes corresponding to each of the buffers being merged (this magic is enabled
   * by the [[RichAttribute]] implicit class).
   *
   * 3. 分区间进行聚合： 给定两个Aggregation Buffer，将两个Aggregation Buffer的结果进行聚合
   */
  val mergeExpressions: Seq[Expression]

  /**
   * An expression which returns the final value for this aggregate function. Its data type should
   * match this expression's [[dataType]].
   *
   * 4. 最终完成聚合表达式的计算，各个分区的结果聚合到一起
   */
  val evaluateExpression: Expression

  /**
    * An expression-based aggregate's bufferSchema is derived from bufferAttributes.
    *
    * 基于表达式的聚合，它的aggregation buffer的schema信息是从aggBufferAttributes获取的
    *
    * */
  final override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  /**
    * Clone aggBufferAttributes
    */
  final lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  /**
   * A helper class for representing an attribute used in merging two
   * aggregation buffers. When merging two buffers, `bufferLeft` and `bufferRight`,
   * we merge buffer values and then update bufferLeft. A [[RichAttribute]]
   * of an [[AttributeReference]] `a` has two functions `left` and `right`,
   * which represent `a` in `bufferLeft` and `bufferRight`, respectively.
   */
  implicit class RichAttribute(a: AttributeReference) {
    /** Represents this attribute at the mutable buffer side. */
    def left: AttributeReference = a

    /** Represents this attribute at the input buffer side (the data value is read-only). */
    def right: AttributeReference = inputAggBufferAttributes(aggBufferAttributes.indexOf(a))
  }
}

