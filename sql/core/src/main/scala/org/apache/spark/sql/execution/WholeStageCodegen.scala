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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.util.Utils

/**
  * An interface for those physical operators that support codegen.
  */
trait CodegenSupport extends SparkPlan {

  /**
    * Whether this SparkPlan support whole stage codegen or not.
    */
  def supportCodegen: Boolean = true

  /**
    * Which SparkPlan is calling produce() of this one. It's itself for the first SparkPlan.
    */
  private var parent: CodegenSupport = null

  /**
    * Returns the RDD of InternalRow which generates the input rows.
   *
   * 获得输入RDD？这个跟SparkPlan的execute方法就什么分别？
   *
    */
  def upstream(): RDD[InternalRow]

  /**
    * Returns Java source code to process the rows from upstream.
    */
  def produce(ctx: CodegenContext, parent: CodegenSupport): String = {
    this.parent = parent
    ctx.freshNamePrefix = nodeName
    doProduce(ctx)
  }

  /**
    * Generate the Java source code to process, should be overrided by subclass to support codegen.
    *
    * doProduce() usually generate the framework, for example, aggregation could generate this:
    *
    *   if (!initialized) {
    *     # create a hash map, then build the aggregation hash map
    *     # call child.produce()
    *     initialized = true;
    *   }
    *   while (hashmap.hasNext()) {
    *     row = hashmap.next();
    *     # build the aggregation results
    *     # create varialbles for results
    *     # call consume(), wich will call parent.doConsume()
    *   }
    */
  protected def doProduce(ctx: CodegenContext): String

  /**
    * Consume the columns generated from current SparkPlan, call it's parent.
    */
  def consume(ctx: CodegenContext, input: Seq[ExprCode], row: String = null): String = {
    if (input != null) {
      assert(input.length == output.length)
    }
    parent.consumeChild(ctx, this, input, row)
  }

  /**
    * Consume the columns generated from it's child, call doConsume() or emit the rows.
    */
  def consumeChild(
      ctx: CodegenContext,
      child: SparkPlan,
      input: Seq[ExprCode],
      row: String = null): String = {
    ctx.freshNamePrefix = nodeName
    if (row != null) {
      ctx.currentVars = null
      ctx.INPUT_ROW = row
      val evals = child.output.zipWithIndex.map { case (attr, i) =>
        BoundReference(i, attr.dataType, attr.nullable).gen(ctx)
      }
      s"""
         | ${evals.map(_.code).mkString("\n")}
         | ${doConsume(ctx, evals)}
       """.stripMargin
    } else {
      doConsume(ctx, input)
    }
  }

  /**
    * Generate the Java source code to process the rows from child SparkPlan.
    *
    * This should be override by subclass to support codegen.
    *
    * For example, Filter will generate the code like this:
    *
    *   # code to evaluate the predicate expression, result is isNull1 and value2
    *   if (isNull1 || value2) {
    *     # call consume(), which will call parent.doConsume()
    *   }
    */
  protected def doConsume(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    throw new UnsupportedOperationException
  }
}


/**
  * InputAdapter is used to hide a SparkPlan from a subtree that support codegen.
  *
  * This is the leaf node of a tree with WholeStageCodegen, is used to generate code that consumes
  * an RDD iterator of InternalRow.
  */
case class InputAdapter(child: SparkPlan) extends LeafNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doPrepare(): Unit = {
    child.prepare()
  }

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def supportCodegen: Boolean = false

  /**
   * InputAdapter封装的child物理计划，这里的upstream就是调用的child(SparkPlan)的execute方法
   * @return
   */
  override def upstream(): RDD[InternalRow] = {
   val rdd = child.execute()
    rdd
  }

  override def doProduce(ctx: CodegenContext): String = {
    val exprs = output.zipWithIndex.map(x => new BoundReference(x._2, x._1.dataType, true))
    val row = ctx.freshName("row")
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    val columns = exprs.map(_.gen(ctx))
    s"""
       | while (input.hasNext()) {
       |   InternalRow $row = (InternalRow) input.next();
       |   ${columns.map(_.code).mkString("\n")}
       |   ${consume(ctx, columns)}
       | }
     """.stripMargin
  }

  override def simpleString: String = "INPUT"
}

/**
  * WholeStageCodegen compile a subtree of plans that support codegen together into single Java
  * function.
  *
  * Here is the call graph of to generate Java source (plan A support codegen, but plan B does not):
  *
  *   WholeStageCodegen       Plan A               FakeInput        Plan B
  * =========================================================================
  *
  * -> execute()
  *     |
  *  doExecute() --------->   upstream() -------> upstream() ------> execute()
  *     |
  *      ----------------->   produce()
  *                             |
  *                          doProduce()  -------> produce()
  *                                                   |
  *                                                doProduce()
  *                                                   |
  *                                                consume()
  *                        consumeChild() <-----------|
  *                             |
  *                          doConsume()
  *                             |
  *  consumeChild()  <-----  consume()
  *
  * SparkPlan A should override doProduce() and doConsume().
  *
  * doCodeGen() will create a CodeGenContext, which will hold a list of variables for input,
  * used to generated code for BoundReference.
  */
case class WholeStageCodegen(plan: CodegenSupport, children: Seq[SparkPlan])
  extends SparkPlan with CodegenSupport {

  override def supportCodegen: Boolean = false

  override def output: Seq[Attribute] = plan.output
  override def outputPartitioning: Partitioning = plan.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = plan.outputOrdering

  override def doPrepare(): Unit = {
    plan.prepare()
  }

  override def doExecute(): RDD[InternalRow] = {
    val ctx = new CodegenContext
    val code = plan.produce(ctx, this)
    val references = ctx.references.toArray
    val source = s"""
      public Object generate(Object[] references) {
        return new GeneratedIterator(references);
      }

      class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {

        private Object[] references;
        ${ctx.declareMutableStates()}
        ${ctx.declareAddedFunctions()}

        public GeneratedIterator(Object[] references) {
         this.references = references;
         ${ctx.initMutableStates()}
        }

        protected void processNext() throws java.io.IOException {
         $code
        }
      }
      """

    // try to compile, helpful for debug
    // println(s"${CodeFormatter.format(source)}")
    CodeGenerator.compile(source)

    val upstream = plan.upstream();
    upstream.mapPartitions { iter =>

      val clazz = CodeGenerator.compile(source)
      val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
      buffer.setInput(iter)
      new Iterator[InternalRow] {
        override def hasNext: Boolean = {
          val hasNext = buffer.hasNext
          hasNext
        }
        override def next: InternalRow = {
          val row = buffer.next()
          row
        }
      }
    }
  }

  override def upstream(): RDD[InternalRow] = {
    throw new UnsupportedOperationException
  }

  override def doProduce(ctx: CodegenContext): String = {
    throw new UnsupportedOperationException
  }

  override def consumeChild(
      ctx: CodegenContext,
      child: SparkPlan,
      input: Seq[ExprCode],
      row: String = null): String = {

    if (row != null) {
      // There is an UnsafeRow already
      s"""
         | currentRow = $row;
         | return;
       """.stripMargin
    } else {
      assert(input != null)
      if (input.nonEmpty) {
        val colExprs = output.zipWithIndex.map { case (attr, i) =>
          BoundReference(i, attr.dataType, attr.nullable)
        }
        // generate the code to create a UnsafeRow
        ctx.currentVars = input
        val code = GenerateUnsafeProjection.createCode(ctx, colExprs, false)
        s"""
           | ${code.code.trim}
           | currentRow = ${code.value};
           | return;
         """.stripMargin
      } else {
        // There is no columns
        s"""
           | currentRow = unsafeRow;
           | return;
         """.stripMargin
      }
    }
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      builder: StringBuilder): StringBuilder = {
    if (depth > 0) {
      lastChildren.init.foreach { isLast =>
        val prefixFragment = if (isLast) "   " else ":  "
        builder.append(prefixFragment)
      }

      val branch = if (lastChildren.last) "+- " else ":- "
      builder.append(branch)
    }

    builder.append(simpleString)
    builder.append("\n")

    plan.generateTreeString(depth + 2, lastChildren :+ false :+ true, builder)
    if (children.nonEmpty) {
      children.init.foreach(_.generateTreeString(depth + 1, lastChildren :+ false, builder))
      children.last.generateTreeString(depth + 1, lastChildren :+ true, builder)
    }

    builder
  }

  override def simpleString: String = "WholeStageCodegen"
}


/**
  * Find the chained plans that support codegen, collapse them together as WholeStageCodegen.
  */
private[sql] case class CollapseCodegenStages(sqlContext: SQLContext) extends Rule[SparkPlan] {

  private def supportCodegen(e: Expression): Boolean = e match {
    case e: LeafExpression => true
    // CodegenFallback requires the input to be an InternalRow
    case e: CodegenFallback => false
    case _ => true
  }

  private def supportCodegen(plan: SparkPlan): Boolean = plan match {
    case plan: CodegenSupport if plan.supportCodegen =>
      val willFallback = plan.expressions.exists(_.find(e => !supportCodegen(e)).isDefined)
      // the generated code will be huge if there are too many columns
      val haveManyColumns = plan.output.length > 200
      !willFallback && !haveManyColumns

      //Disable WholeStageCodegen operator for now
      false
    case _ => false
  }

  /**
    * 插入一个物理计划，WholeStageCodegen
    * @param plan
    * @return
    */
  def apply(plan: SparkPlan): SparkPlan = {
    if (sqlContext.conf.wholeStageEnabled) {
      plan.transform {
        /**
          * 类型为CodegenSupport的plan并且支持Codegen，同时存在支持Codegen的children
          */
        case plan: CodegenSupport if supportCodegen(plan) &&
          // Whole stage codegen is only useful when there are at least two levels of operators that
          // support it (save at least one projection/iterator).
          (Utils.isTesting || plan.children.exists(supportCodegen)) =>

          var inputs = ArrayBuffer[SparkPlan]()
          val combined = plan.transform {
            case p if !supportCodegen(p) =>
              val input = apply(p)  // collapse them recursively
              inputs += input
              InputAdapter(input)
          }.asInstanceOf[CodegenSupport]
          WholeStageCodegen(combined, inputs)
      }
    } else {
      plan
    }
  }
}
