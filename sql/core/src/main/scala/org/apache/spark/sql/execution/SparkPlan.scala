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

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.metric.{LongSQLMetric, SQLMetric}
import org.apache.spark.sql.types.DataType

/**
 * The base class for physical operators.
 */
abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging with Serializable {

  /**
   * A handle to the SQL Context that was used to create this plan.   Since many operators need
   * access to the sqlContext for RDD operations or configuration this field is automatically
   * populated by the query planning infrastructure.
   */
  @transient
  protected[spark] final val sqlContext = SQLContext.getActive().getOrElse(null)

  protected def sparkContext = sqlContext.sparkContext

  // sqlContext will be null when we are being deserialized on the slaves.  In this instance
  // the value of subexpressionEliminationEnabled will be set by the desserializer after the
  // constructor has run.
  val subexpressionEliminationEnabled: Boolean = if (sqlContext != null) {
    sqlContext.conf.subexpressionEliminationEnabled
  } else {
    false
  }

  /**
   * Whether the "prepare" method is called.
   */
  private val prepareCalled = new AtomicBoolean(false)

  /** Overridden make copy also propogates sqlContext to copied plan. */
  override def makeCopy(newArgs: Array[AnyRef]): SparkPlan = {
    SQLContext.setActive(sqlContext)
    super.makeCopy(newArgs)
  }

  /**
   * Return all metadata that describes more details of this SparkPlan.
   */
  private[sql] def metadata: Map[String, String] = Map.empty

  /**
   * Return all metrics containing metrics of this SparkPlan.
   */
  private[sql] def metrics: Map[String, SQLMetric[_, _]] = Map.empty

  /**
   * Return a LongSQLMetric according to the name.
   */
  private[sql] def longMetric(name: String): LongSQLMetric =
    metrics(name).asInstanceOf[LongSQLMetric]

  // TODO: Move to `DistributedPlan`
  /**
    * Specifies how data is partitioned across different nodes in the cluster.
    * 本物理计划输出的RDD的数据是什么样的分区策略, Partitioning有一个numPartitions属性，表示分区的个数
    *
    * */
  def outputPartitioning: Partitioning = UnknownPartitioning(0) // TODO: WRONG WIDTH!

  /**
    * Specifies any partition requirements on the input data for this operator.
    * 本物理计划要求孩子物理计划输入的RDD的数据分布性
    *
    * */
  def requiredChildDistribution: Seq[Distribution] =
    Seq.fill(children.size)(UnspecifiedDistribution)

  /**
    * Specifies how data is ordered in each partition.
    * 本物理计划输出的RDD的数据的排序性
    *
    * */
  def outputOrdering: Seq[SortOrder] = Nil

  /**
    * Specifies sort order for each partition requirements on the input data for this operator.
    * 本物理计划要求孩子物理计划输出的RDD的数据的排序性
    *
    * */
  def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq.fill(children.size)(Nil)

  /**
   * Returns the result of this query as an RDD[InternalRow] by delegating to doExecute
   * after adding query plan information to created RDDs for visualization.
   * Concrete implementations of SparkPlan should override doExecute instead.
   *
   * SparkSQL物理计划的execute方法返回一个RDD[InternalRow]，它是一个final方法，首先执行prepare方法，然后执行doExecute方法
   */
  final def execute(): RDD[InternalRow] = {
    RDDOperationScope.withScope(sparkContext, nodeName, false, true) {
      prepare()
      doExecute()
    }
  }

  /**
   * Prepare a SparkPlan for execution. It's idempotent.
   */
  final def prepare(): Unit = {
    if (prepareCalled.compareAndSet(false, true)) {
      doPrepare()
      children.foreach(_.prepare())
    }
  }

  /**
   * Overridden by concrete implementations of SparkPlan. It is guaranteed to run before any
   * `execute` of SparkPlan. This is helpful if we want to set up some state before executing the
   * query, e.g., `BroadcastHashJoin` uses it to broadcast asynchronously.
   *
   * Note: the prepare method has already walked down the tree, so the implementation doesn't need
   * to call children's prepare methods.
   */
  protected def doPrepare(): Unit = {}

  /**
   * Overridden by concrete implementations of SparkPlan.
   * Produces the result of the query as an RDD[InternalRow]
   */
  protected def doExecute(): RDD[InternalRow]

  /**
   * Runs this query returning the result as an array.
   */
  def executeCollect(): Array[InternalRow] = {
    execute().map(_.copy()).collect()
  }

  /**
   * Runs this query returning the result as an array, using external Row format.
   */
  def executeCollectPublic(): Array[Row] = {
    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    executeCollect().map(converter(_).asInstanceOf[Row])
  }

  /**
   * Runs this query returning the first `n` rows as an array.
   *
   * This is modeled after RDD.take but never runs any job locally on the driver.
   */
  def executeTake(n: Int): Array[InternalRow] = {
    if (n == 0) {
      return new Array[InternalRow](0)
    }

    val childRDD = execute().map(_.copy())

    val buf = new ArrayBuffer[InternalRow]
    val totalParts = childRDD.partitions.length
    var partsScanned = 0
    while (buf.size < n && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      var numPartsToTry = 1L
      if (partsScanned > 0) {
        // If we didn't find any rows after the first iteration, just try all partitions next.
        // Otherwise, interpolate the number of partitions we need to try, but overestimate it
        // by 50%.
        if (buf.size == 0) {
          numPartsToTry = totalParts - 1
        } else {
          numPartsToTry = (1.5 * n * partsScanned / buf.size).toInt
        }
      }
      numPartsToTry = math.max(0, numPartsToTry)  // guard against negative num of partitions

      val left = n - buf.size
      val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
      val sc = sqlContext.sparkContext
      val res = sc.runJob(childRDD, (it: Iterator[InternalRow]) => it.take(left).toArray, p)

      res.foreach(buf ++= _.take(n - buf.size))
      partsScanned += p.size
    }

    buf.toArray
  }

  private[this] def isTesting: Boolean = sys.props.contains("spark.testing")

  /***
    *
    * @param expressions
    * @param inputSchema
    * @param useSubexprElimination
    * @return 类型为()=>MutableProjection的函数
    */
  protected def newMutableProjection(
      expressions: Seq[Expression],
      inputSchema: Seq[Attribute],
      useSubexprElimination: Boolean = false): () => MutableProjection = {
    log.info(s"Creating MutableProj: $expressions, inputSchema: $inputSchema")
    try {
      GenerateMutableProjection.generate(expressions, inputSchema, useSubexprElimination)
    } catch {
      case e: Exception =>
        if (isTesting) {
          throw e
        } else {
          log.error("Failed to generate mutable projection, fallback to interpreted", e)
          () => new InterpretedMutableProjection(expressions, inputSchema)
        }
    }
  }

  /**
   * 动态生成条件过滤代码
   * @param expression
   * @param inputSchema
   * @return
   */
  protected def newPredicate(
      expression: Expression, inputSchema: Seq[Attribute]): (InternalRow) => Boolean = {
    try {
      GeneratePredicate.generate(expression, inputSchema)
    } catch {
      case e: Exception =>
        if (isTesting) {
          throw e
        } else {
          log.error("Failed to generate predicate, fallback to interpreted", e)
          InterpretedPredicate.create(expression, inputSchema)
        }
    }
  }

  /**
    * 通过代码生成的方式创建 Ordering[InternalRow]
    * @param order
    * @param inputSchema
    * @return 返回针对InternalRow进行排序的Ordering对象
    */
  protected def newOrdering(
      order: Seq[SortOrder], inputSchema: Seq[Attribute]): Ordering[InternalRow] = {
    try {
      GenerateOrdering.generate(order, inputSchema)
    } catch {
      case e: Exception =>
        if (isTesting) {
          throw e
        } else {
          log.error("Failed to generate ordering, fallback to interpreted", e)
          new InterpretedOrdering(order, inputSchema)
        }
    }
  }

  /**
   * Creates a row ordering for the given schema, in natural ascending order.
   */
  protected def newNaturalAscendingOrdering(dataTypes: Seq[DataType]): Ordering[InternalRow] = {
    val order: Seq[SortOrder] = dataTypes.zipWithIndex.map {
      case (dt, index) => new SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    }
    newOrdering(order, Seq.empty)
  }
}

private[sql] trait LeafNode extends SparkPlan {
  override def children: Seq[SparkPlan] = Nil
  override def producedAttributes: AttributeSet = outputSet
}

private[sql] trait UnaryNode extends SparkPlan {
  def child: SparkPlan

  override def children: Seq[SparkPlan] = child :: Nil

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

private[sql] trait BinaryNode extends SparkPlan {
  def left: SparkPlan
  def right: SparkPlan

  override def children: Seq[SparkPlan] = Seq(left, right)
}
