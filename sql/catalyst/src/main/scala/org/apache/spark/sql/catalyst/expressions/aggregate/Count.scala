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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/** *
  * Count是基于DeclarativeAggregate
  *
  * 对于select count(name) from tbl_student;如果name为空，那么count将不对它进行计数
  * @param children
  */
case class Count(children: Seq[Expression]) extends DeclarativeAggregate {

  override def nullable: Boolean = false

  /**
   * Count的返回值是Long类型
   * @return
   */
  override def dataType: DataType = LongType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq.fill(children.size)(AnyDataType)

  private lazy val count = AttributeReference("count", LongType, nullable = false)()

  override lazy val aggBufferAttributes = count :: Nil

  /**
   * 初值是Literal(0)的Seq
   */
  override lazy val initialValues = Seq(
    /* count = */ Literal(0L)
  )

  /**
    * 更新表达式，即使更新count
    */
  override lazy val updateExpressions = {
    /**
     * 过滤出nullable的child集合
     */
    val nullableChildren = children.filter(_.nullable)
    if (nullableChildren.isEmpty) {
      Seq(
        /* count = */ count + 1L
      )
    } else {
      //将nullableChildren的所有元素map成IsNull，然后对这些IsNull做reduce操作(使用Or运算符)
      val condition = nullableChildren.map(IsNull).reduce(Or)
      Seq(
        /* count = */ If(condition, count, count + 1L)
      )
    }
  }

  override lazy val mergeExpressions = Seq(
    /* count = */ count.left + count.right
  )

  /**
   * 结果表达式，count
   */
  override lazy val evaluateExpression = count

  override def defaultResult: Option[Literal] = Option(Literal(0L))
}

object Count {
  def apply(child: Expression): Count = Count(child :: Nil)
}
