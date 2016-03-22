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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 *
 * @param requiredChildDistributionExpressions 该物理计划需要child数据分布的表达式
 * @param groupingExpressions 分组表达式，比如group by class_id
 * @param aggregateExpressions 聚合表达式，聚合表达式包含了聚合函数 比如select count(id) from tbl_student group by class_id ---统计每个班的人数
 * @param aggregateAttributes  聚合属性
 * @param initialInputBufferOffset
 * @param resultExpressions 结果表达式，resultExpressions是改写后的
 * @param child
 */
case class SortBasedAggregate(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression], /**对于select count(name) from dual group by classId, 那么groupingExpressions是AttributeReference的集合，AttributeReference的字符串表示是classId#3*/
    aggregateExpressions: Seq[AggregateExpression], /**第一个进来的AggregateExpression是count(name#1), mode=Partial,isDistinct=false**/
    aggregateAttributes: Seq[Attribute],/**count#6L,元素类型是AttributeReference*/
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryNode {

  /**
    * 将每个AggregateFunction的aggBufferAttributes展开
    * 因为aggregateBufferAttributes是SortBasedAggregate的成员变量，因此在实例化SortBasedAggregate时，对aggregateBufferAttributes进行初始化
    */
  private[this] val aggregateBufferAttributes = {
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
  }

  /**
    * 类型是AttributeSet，AttributeSet的构造参数是AggregateAttributes（集合）
    * @return
    */
  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
      AttributeSet(resultExpressions.diff(groupingExpressions).map(_.toAttribute)) ++
      AttributeSet(aggregateBufferAttributes)

  override private[sql] lazy val metrics = Map(
    "numInputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  /**
    * 该物理计划的输出是一个Attribute集合，它是从resultExpressions转换得来
    * @return
    */
  override def output: Seq[Attribute] = {
    resultExpressions.map(_.toAttribute)
  }

  /**
    * 在Exchange的apply方法使用(apply方法调用Exchange的ensureDistributionAndOrdering)
    * @return
    */
  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.length == 0 => AllTuples :: Nil
      case Some(exprs) if exprs.length > 0 => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  /**
    * 在Exchange的apply方法使用(apply方法调用Exchange的ensureDistributionAndOrdering)
    *
    * 因为这是Sort Based Aggregate，因此需要Child物理计划是已经排好序的，
    * 所谓的Sort Based Aggregate，只是对分组列进行排序，然后对排好序的结果进行聚合
    * @return
    */
  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    groupingExpressions.map(SortOrder(_, Ascending)) :: Nil
  }

  /**
    * 输出结果按照分组表达式输出排序，也就是说group by class_Id，那么结果将按照class_id进行排序,
    * 也就是group by操作的最终结果是以group by key进行排序
    * @return
    */
  override def outputOrdering: Seq[SortOrder] = {
    groupingExpressions.map(SortOrder(_, Ascending))
  }

  /**
   * 返回的是一个RDD[InternalRow]，为什么返回的是一个Iterator？不是Iterator，而是RDD[InternalRow]
   * 原因是mapPartitionsInternal接受一个将Iterator转换为另一个Iterator的方法（这是mapPartition的操作，分区内转换）
   * @return
   */
  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val numInputRows = longMetric("numInputRows")
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitionsInternal { iter =>
      // Because the constructor of an aggregation iterator will read at least the first row,
      // we need to get the value of iter.hasNext first.
      val hasInput = iter.hasNext
      if (!hasInput && groupingExpressions.nonEmpty) {
        // This is a grouped aggregate and the input iterator is empty,
        // so return an empty iterator.
        Iterator[UnsafeRow]()
      } else {
        /**
         * groupingExpressions
         * resultExpressions
         * aggregateExpressions
         * aggregateAttributes
         */
        val outputIter = new SortBasedAggregationIterator(
          groupingExpressions,
          child.output, /**孩子物理计划的输出属性(Seq[Attribute])**/
          iter, /**依赖的RDD的分区数据**/
          aggregateExpressions,
          aggregateAttributes,
          initialInputBufferOffset,
          resultExpressions,
          (expressions, inputSchema) =>
            newMutableProjection(expressions, inputSchema, subexpressionEliminationEnabled),
          numInputRows,
          numOutputRows)

        /**
          * 如果没有输入数据并且分组表达式为空
          */
        if (!hasInput && groupingExpressions.isEmpty) {
          // There is no input and there is no grouping expressions.
          // We need to output a single row as the output.
          numOutputRows += 1
          Iterator[UnsafeRow](outputIter.outputForEmptyGroupingKeyWithoutInput())
        } else {
          outputIter
        }
      }
    }
  }

  override def simpleString: String = {
    val allAggregateExpressions = aggregateExpressions

    val keyString = groupingExpressions.mkString("[", ",", "]")
    val functionString = allAggregateExpressions.mkString("[", ",", "]")
    val outputString = output.mkString("[", ",", "]")
    s"SortBasedAggregate(key=$keyString, functions=$functionString, output=$outputString)"
  }
}
