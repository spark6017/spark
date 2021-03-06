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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, UnaryNode, UnsafeFixedWidthAggregationMap}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.{DecimalType, StructType}
import org.apache.spark.unsafe.KVIterator

/**
 *
 * @param requiredChildDistributionExpressions
 * @param groupingExpressions 分组表达式，group by 子句
 * @param aggregateExpressions 聚合表达式，每个aggregate expression封装了一个aggregate function
 * @param aggregateAttributes 聚合属性(何解？难道用于聚合的列？)
 * @param initialInputBufferOffset
 * @param resultExpressions
 * @param child  如果是final mode的TungstenAggregate，那么child就是partial mode的TungstenAggregate
 */
case class TungstenAggregate(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryNode with CodegenSupport {

  private[this] val aggregateBufferAttributes = {
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
  }

  require(TungstenAggregate.supportsAggregate(aggregateBufferAttributes))

  override private[sql] lazy val metrics = Map(
    "numInputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"),
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

  /** *
    * TungstenAggregate物理计划输出的数据的结构(由Seq[Attribute]表达)
    * 它是根据resultExpression表示的
 *
    * @return
    */
  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
    AttributeSet(resultExpressions.diff(groupingExpressions).map(_.toAttribute)) ++
    AttributeSet(aggregateBufferAttributes)

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.length == 0 => AllTuples :: Nil
      case Some(exprs) if exprs.length > 0 => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  //
  //
  // testFallbackStartsAt是一个整型数字Option,默认是Null，如果配置了这个选项，那么就取配置值

  /** *
    * This is for testing. We force TungstenAggregationIterator to fall back to sort-based aggregation once it has processed a given number of input rows.
    *
    * 当TungstenAggregationIterator处理了testFallbackStartsAt个input rows后就是用sort based aggregation
    *
    * 问题：
    * 如果testFallbackStartsAt为None表示不切换到sort base aggregation? testFallbackStartsAt为None，那么TungstenAggregationIterator会使用默认值(Integer.MAX_VALUE)
    *
    */
  private val testFallbackStartsAt: Option[Int] = {
    sqlContext.getConf("spark.sql.TungstenAggregate.testFallbackStartsAt", null) match {
      case null | "" => None
      case fallbackStartsAt => Some(fallbackStartsAt.toInt)
    }
  }

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val numInputRows = longMetric("numInputRows")
    val numOutputRows = longMetric("numOutputRows")
    val dataSize = longMetric("dataSize")
    val spillSize = longMetric("spillSize")

    child.execute().mapPartitions { iter =>

      val hasInput = iter.hasNext
      if (!hasInput && groupingExpressions.nonEmpty) {
        // This is a grouped aggregate and the input iterator is empty,
        // so return an empty iterator.
        Iterator.empty
      } else {

        /** *
          * 分区内数据集合(iter)转换位TungstenAggregationIterator
          */
        val aggregationIterator =
          new TungstenAggregationIterator(
            groupingExpressions,
            aggregateExpressions,
            aggregateAttributes,
            initialInputBufferOffset,
            resultExpressions,
            (expressions, inputSchema) =>
              newMutableProjection(expressions, inputSchema, subexpressionEliminationEnabled),
            child.output,
            iter,
            testFallbackStartsAt, /**tungsten aggregation处理多少条rows后切换到sort based aggregation的*/
            numInputRows,
            numOutputRows,
            dataSize,
            spillSize)
        if (!hasInput && groupingExpressions.isEmpty) {
          numOutputRows += 1
          Iterator.single[UnsafeRow](aggregationIterator.outputForEmptyGroupingKeyWithoutInput())
        } else {
          aggregationIterator
        }
      }
    }
  }

  // all the mode of aggregate expressions
  private val modes = aggregateExpressions.map(_.mode).distinct

  override def supportCodegen: Boolean = {
    // ImperativeAggregate is not supported right now
    !aggregateExpressions.exists(_.aggregateFunction.isInstanceOf[ImperativeAggregate])
  }

  override def upstream(): RDD[InternalRow] = {
    child.asInstanceOf[CodegenSupport].upstream()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    if (groupingExpressions.isEmpty) {
      doProduceWithoutKeys(ctx)
    } else {
      doProduceWithKeys(ctx)
    }
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    if (groupingExpressions.isEmpty) {
      doConsumeWithoutKeys(ctx, input)
    } else {
      doConsumeWithKeys(ctx, input)
    }
  }

  // The variables used as aggregation buffer
  private var bufVars: Seq[ExprCode] = _

  private def doProduceWithoutKeys(ctx: CodegenContext): String = {
    val initAgg = ctx.freshName("initAgg")
    ctx.addMutableState("boolean", initAgg, s"$initAgg = false;")

    // generate variables for aggregation buffer
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
    val initExpr = functions.flatMap(f => f.initialValues)
    bufVars = initExpr.map { e =>
      val isNull = ctx.freshName("bufIsNull")
      val value = ctx.freshName("bufValue")
      ctx.addMutableState("boolean", isNull, "")
      ctx.addMutableState(ctx.javaType(e.dataType), value, "")
      // The initial expression should not access any column
      val ev = e.gen(ctx)
      val initVars = s"""
         | $isNull = ${ev.isNull};
         | $value = ${ev.value};
       """.stripMargin
      ExprCode(ev.code + initVars, isNull, value)
    }

    // generate variables for output
    val bufferAttrs = functions.flatMap(_.aggBufferAttributes)
    val (resultVars, genResult) = if (modes.contains(Final) || modes.contains(Complete)) {
      // evaluate aggregate results
      ctx.currentVars = bufVars
      val aggResults = functions.map(_.evaluateExpression).map { e =>
        BindReferences.bindReference(e, bufferAttrs).gen(ctx)
      }
      // evaluate result expressions
      ctx.currentVars = aggResults
      val resultVars = resultExpressions.map { e =>
        BindReferences.bindReference(e, aggregateAttributes).gen(ctx)
      }
      (resultVars, s"""
        | ${aggResults.map(_.code).mkString("\n")}
        | ${resultVars.map(_.code).mkString("\n")}
       """.stripMargin)
    } else if (modes.contains(Partial) || modes.contains(PartialMerge)) {
      // output the aggregate buffer directly
      (bufVars, "")
    } else {
      // no aggregate function, the result should be literals
      val resultVars = resultExpressions.map(_.gen(ctx))
      (resultVars, resultVars.map(_.code).mkString("\n"))
    }

    val doAgg = ctx.freshName("doAggregateWithoutKey")
    ctx.addNewFunction(doAgg,
      s"""
         | private void $doAgg() throws java.io.IOException {
         |   // initialize aggregation buffer
         |   ${bufVars.map(_.code).mkString("\n")}
         |
         |   ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
         | }
       """.stripMargin)

    s"""
       | if (!$initAgg) {
       |   $initAgg = true;
       |   $doAgg();
       |
       |   // output the result
       |   $genResult
       |
       |   ${consume(ctx, resultVars)}
       | }
     """.stripMargin
  }

  private def doConsumeWithoutKeys(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    // only have DeclarativeAggregate
    val functions = aggregateExpressions.map(_.aggregateFunction.asInstanceOf[DeclarativeAggregate])
    val inputAttrs = functions.flatMap(_.aggBufferAttributes) ++ child.output
    val updateExpr = aggregateExpressions.flatMap { e =>
      e.mode match {
        case Partial | Complete =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].updateExpressions
        case PartialMerge | Final =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].mergeExpressions
      }
    }
    ctx.currentVars = bufVars ++ input
    // TODO: support subexpression elimination
    val aggVals = updateExpr.map(BindReferences.bindReference(_, inputAttrs).gen(ctx))
    // aggregate buffer should be updated atomic
    val updates = aggVals.zipWithIndex.map { case (ev, i) =>
      s"""
         | ${bufVars(i).isNull} = ${ev.isNull};
         | ${bufVars(i).value} = ${ev.value};
       """.stripMargin
    }

    s"""
       | // do aggregate
       | ${aggVals.map(_.code).mkString("\n")}
       | // update aggregation buffer
       | ${updates.mkString("")}
     """.stripMargin
  }

  private val groupingAttributes = groupingExpressions.map(_.toAttribute)
  private val groupingKeySchema = StructType.fromAttributes(groupingAttributes)
  private val declFunctions = aggregateExpressions.map(_.aggregateFunction)
    .filter(_.isInstanceOf[DeclarativeAggregate])
    .map(_.asInstanceOf[DeclarativeAggregate])
  private val bufferAttributes = declFunctions.flatMap(_.aggBufferAttributes)
  private val bufferSchema = StructType.fromAttributes(bufferAttributes)

  // The name for HashMap
  private var hashMapTerm: String = _

  /**
    * This is called by generated Java class, should be public.
   *
   * 由动态生成的Java Class调用，是哪个Java Class，调用时需要提供TungsgtenAggregate的实例，在什么地方调用？搜索下
   *
    * @return 返回UnsafeFixedWidthAggregationMap （固定长度的Aggregation，何解？只用于类型固定的Aggregation）
    */
  def createHashMap(): UnsafeFixedWidthAggregationMap = {
    // create initialized aggregate buffer
    val initExpr = declFunctions.flatMap(f => f.initialValues)
    val initialBuffer = UnsafeProjection.create(initExpr)(EmptyRow)

    // create hashMap
    new UnsafeFixedWidthAggregationMap(
      initialBuffer,
      bufferSchema,
      groupingKeySchema,
      TaskContext.get().taskMemoryManager(),
      1024 * 16, // initial capacity
      TaskContext.get().taskMemoryManager().pageSizeBytes,
      false // disable tracking of performance metrics
    )
  }

  /**
    * This is called by generated Java class, should be public.
    */
  def createUnsafeJoiner(): UnsafeRowJoiner = {
    GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)
  }


  /**
    * Update peak execution memory, called in generated Java class.
    */
  def updatePeakMemory(hashMap: UnsafeFixedWidthAggregationMap): Unit = {
    val mapMemory = hashMap.getPeakMemoryUsedBytes
    val metrics = TaskContext.get().taskMetrics()
    metrics.incPeakExecutionMemory(mapMemory)
  }

  /** *
    * 在生成的代码中会调用TungstenAggregate的createHashMap方法
    * 问题：doProduceWithKeys在什么地方调用？
    *
    * @param ctx
    * @return 返回生成的代码字符串
    */
  private def doProduceWithKeys(ctx: CodegenContext): String = {
    /**
     * freshName用于添加前缀以及将唯一的ID作为后缀，这里用于生成一个变量名
     */
    val initAgg = ctx.freshName("initAgg")

    /** *
      * 创建一个成员变量，类型boolean，变量名为$initAgg, 语句是赋值为false
      */
    ctx.addMutableState("boolean", initAgg, s"$initAgg = false;")

    /** *
      *  create hashMap
      *
      *  首先创建一个成员变量，变量的名字是plan，赋值为当前对象(TungstenAggregate)
      */
    val thisPlan = ctx.addReferenceObj("plan", this)

    /** *
      * 创建一个名称为hashMap的成员变量
      */
    hashMapTerm = ctx.freshName("hashMap")

    /** *
      * 添加一个成员变量，类型为UnsafeFixedWidthAggregationMap，变量名称为$hashMapTerm, 赋值语句是调用TungstenAggregate的createHashMap
      */
    val hashMapClassName = classOf[UnsafeFixedWidthAggregationMap].getName
    ctx.addMutableState(hashMapClassName, hashMapTerm, s"$hashMapTerm = $thisPlan.createHashMap();")

    /** *
      * Create a name for iterator from HashMap
      * 创建一个类型为mapIter的变量，这个变量的类型是KVIterator[UnsafeRow, UnsafeRow]，没有赋值语句
      */
    val iterTerm = ctx.freshName("mapIter")
    ctx.addMutableState(classOf[KVIterator[UnsafeRow, UnsafeRow]].getName, iterTerm, "")

    /** *
      * generate code for output
      *
      * 创建aggKey和aggBuffer变量
      *
      */
    val keyTerm = ctx.freshName("aggKey")
    val bufferTerm = ctx.freshName("aggBuffer")


    /** *
      * 如果聚合是Final或者Complete阶段
      */
    val outputCode = if (modes.contains(Final) || modes.contains(Complete)) {
      // generate output using resultExpressions
      ctx.currentVars = null
      ctx.INPUT_ROW = keyTerm

      /** *
        *
        */
      val keyVars = groupingExpressions.zipWithIndex.map { case (e, i) =>
          BoundReference(i, e.dataType, e.nullable).gen(ctx)
      }
      ctx.INPUT_ROW = bufferTerm
      val bufferVars = bufferAttributes.zipWithIndex.map { case (e, i) =>
        BoundReference(i, e.dataType, e.nullable).gen(ctx)
      }
      // evaluate the aggregation result
      ctx.currentVars = bufferVars
      val aggResults = declFunctions.map(_.evaluateExpression).map { e =>
        BindReferences.bindReference(e, bufferAttributes).gen(ctx)
      }
      // generate the final result
      ctx.currentVars = keyVars ++ aggResults
      val inputAttrs = groupingAttributes ++ aggregateAttributes
      val resultVars = resultExpressions.map { e =>
        BindReferences.bindReference(e, inputAttrs).gen(ctx)
      }
      s"""
       ${keyVars.map(_.code).mkString("\n")}
       ${bufferVars.map(_.code).mkString("\n")}
       ${aggResults.map(_.code).mkString("\n")}
       ${resultVars.map(_.code).mkString("\n")}

       ${consume(ctx, resultVars)}
       """

    }

    /** *
      * 如果聚合是Partial或者PartialMerge阶段
      */
    else if (modes.contains(Partial) || modes.contains(PartialMerge)) {
      // This should be the last operator in a stage, we should output UnsafeRow directly
      /** *
        * 创建变量unsafeRowJoiner
        */
      val joinerTerm = ctx.freshName("unsafeRowJoiner")

      /** *
        * 创建类型为UnsafeRowJoiner的成员变量，变量名unsafeRowJoiner，调用TungstenAggregate的createUnsafeJoiner对它进行初始化
        */
      ctx.addMutableState(classOf[UnsafeRowJoiner].getName, joinerTerm,
        s"$joinerTerm = $thisPlan.createUnsafeJoiner();")



      /** *
        * 创建名为resultRow的变量，变量类型是UnsafeRow，赋值是调用UnsafeRowJoiner的join方法，
        * 问题：join的两个参数$keyTerm和$bufferTerm什么时候赋值的？
        *
        */
      val resultRow = ctx.freshName("resultRow")
      s"""
       UnsafeRow $resultRow = $joinerTerm.join($keyTerm, $bufferTerm);
       //调用consume方法
       ${consume(ctx, null, resultRow)}
       """

    }

    /** *
      * 其它阶段
      */
    else {
      // generate result based on grouping key
      ctx.INPUT_ROW = keyTerm
      ctx.currentVars = null
      val eval = resultExpressions.map{ e =>
        BindReferences.bindReference(e, groupingAttributes).gen(ctx)
      }
      s"""
       ${eval.map(_.code).mkString("\n")}
       ${consume(ctx, eval)}
       """
    }
/**
    *println("=================================>Output Code Begin<===============================")
    *println(outputCode)
    *println("=================================>Output Code End<===============================")

    **/
    val doAgg = ctx.freshName("doAggregateWithKeys")
    ctx.addNewFunction(doAgg,
      s"""
        private void $doAgg() throws java.io.IOException {
          ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}

          $iterTerm = $hashMapTerm.iterator();
        }
       """)

    s"""
     if (!$initAgg) {
       $initAgg = true;
       $doAgg();
     }

     // output the result
     while ($iterTerm.next()) {
       UnsafeRow $keyTerm = (UnsafeRow) $iterTerm.getKey();
       UnsafeRow $bufferTerm = (UnsafeRow) $iterTerm.getValue();
       $outputCode
     }

     $thisPlan.updatePeakMemory($hashMapTerm);
     $hashMapTerm.free();
     """
  }

  private def doConsumeWithKeys( ctx: CodegenContext, input: Seq[ExprCode]): String = {

    // create grouping key
    ctx.currentVars = input
    val keyCode = GenerateUnsafeProjection.createCode(
      ctx, groupingExpressions.map(e => BindReferences.bindReference[Expression](e, child.output)))
    val key = keyCode.value
    val buffer = ctx.freshName("aggBuffer")

    // only have DeclarativeAggregate
    val updateExpr = aggregateExpressions.flatMap { e =>
      e.mode match {
        case Partial | Complete =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].updateExpressions
        case PartialMerge | Final =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate].mergeExpressions
      }
    }

    val inputAttr = bufferAttributes ++ child.output
    ctx.currentVars = new Array[ExprCode](bufferAttributes.length) ++ input
    ctx.INPUT_ROW = buffer
    // TODO: support subexpression elimination
    val evals = updateExpr.map(BindReferences.bindReference(_, inputAttr).gen(ctx))
    val updates = evals.zipWithIndex.map { case (ev, i) =>
      val dt = updateExpr(i).dataType
      ctx.updateColumn(buffer, dt, i, ev, updateExpr(i).nullable)
    }

    s"""
     // generate grouping key
     ${keyCode.code}
     UnsafeRow $buffer = $hashMapTerm.getAggregationBufferFromUnsafeRow($key);
     if ($buffer == null) {
       // failed to allocate the first page
       throw new OutOfMemoryError("No enough memory for aggregation");
     }

     // evaluate aggregate function
     ${evals.map(_.code).mkString("\n")}
     // update aggregate buffer
     ${updates.mkString("\n")}
     """
  }

  /** *
    * TunstenAggregate物理计划的字符串表示
 *
    * @return
    */
  override def simpleString: String = {

    /** *
      * 聚合表达式
      */
    val allAggregateExpressions = aggregateExpressions

    testFallbackStartsAt match {

      /** *
        * 如果不使用fall back on sort based aggregation模式
        */
      case None =>
        val keyString = groupingExpressions.mkString("[", ",", "]")
        val functionString = allAggregateExpressions.mkString("[", ",", "]")
        val outputString = output.mkString("[", ",", "]")
        s"TungstenAggregate(key=$keyString, functions=$functionString, output=$outputString)"

      /** *
        * 如果使用了fall back on sort based aggregation模式
        */
      case Some(fallbackStartsAt) =>
        s"TungstenAggregateWithControlledFallback $groupingExpressions " +
          s"$allAggregateExpressions $resultExpressions fallbackStartsAt=$fallbackStartsAt"
    }
  }
}

object TungstenAggregate {
  /***
    * 是否支持TungstenAggregate, Spark SQL支持TungstenAggregation的条件是什么？
    *
    * 传入的是aggregate buffer的attributes，aggregate buffer是一个有schema的unsafe row，用于存放聚合结果的schema信息
    * 对于select count(age) from TBL_STUDENT group  by classId,那么aggregateBufferAttributes是的AttributeReference集合,如果count#6L
    *
    * @param aggregateBufferAttributes aggregateBuffer这个row对应的attribute
    * @return
    */
  def supportsAggregate(aggregateBufferAttributes: Seq[Attribute]): Boolean = {
    /** *
      *  通过调用 StructType.fromAttributes将Seq[Attribute]转换为Schema，aggregationBufferSchema的类型是StructType
      */
    val aggregationBufferSchema = StructType.fromAttributes(aggregateBufferAttributes)

    /** *
      * 给定aggregationBufferSchema，判断是否支持Tungsten Aggregation。基本数据类型都是mutable的
      */
    val supportsAggregate = UnsafeFixedWidthAggregationMap.supportsAggregationBufferSchema(aggregationBufferSchema)
    supportsAggregate

    //强制SortBasedAggregation
    false
  }
}
