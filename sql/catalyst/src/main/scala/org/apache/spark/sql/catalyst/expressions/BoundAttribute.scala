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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._

/**
 * A bound reference points to a specific slot in the input tuple, allowing the actual value
 * to be retrieved more efficiently.  However, since operations like column pruning can change
 * the layout of intermediate tuples, BindReferences should be run after all such transformations.
 */
case class BoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
  extends LeafExpression {

  override def toString: String = s"BoundReference,input[ordinal: <$ordinal>, dataType:<${dataType.simpleString}>]"

  // Use special getter for primitive types (for UnsafeRow)
  override def eval(input: InternalRow): Any = {
    if (input.isNullAt(ordinal)) {
      null
    } else {
      dataType match {
        case BooleanType => input.getBoolean(ordinal)
        case ByteType => input.getByte(ordinal)
        case ShortType => input.getShort(ordinal)
        case IntegerType | DateType => input.getInt(ordinal)
        case LongType | TimestampType => input.getLong(ordinal)
        case FloatType => input.getFloat(ordinal)
        case DoubleType => input.getDouble(ordinal)
        case StringType => input.getUTF8String(ordinal)
        case BinaryType => input.getBinary(ordinal)
        case CalendarIntervalType => input.getInterval(ordinal)
        case t: DecimalType => input.getDecimal(ordinal, t.precision, t.scale)
        case t: StructType => input.getStruct(ordinal, t.size)
        case _: ArrayType => input.getArray(ordinal)
        case _: MapType => input.getMap(ordinal)
        case _ => input.get(ordinal, dataType)
      }
    }
  }

  override def genCode(ctx: CodegenContext, ev: ExprCode): String = {
    val javaType = ctx.javaType(dataType)
    val value = ctx.getValue(ctx.INPUT_ROW, dataType, ordinal.toString)
    if (ctx.currentVars != null && ctx.currentVars(ordinal) != null) {
      ev.isNull = ctx.currentVars(ordinal).isNull
      ev.value = ctx.currentVars(ordinal).value
      ""
    } else if (nullable) {
      s"""
        boolean ${ev.isNull} = ${ctx.INPUT_ROW}.isNullAt($ordinal);
        $javaType ${ev.value} = ${ev.isNull} ? ${ctx.defaultValue(dataType)} : ($value);
      """
    } else {
      ev.isNull = "false"
      s"""
        $javaType ${ev.value} = $value;
      """
    }
  }
}

object BindReferences extends Logging {

  /***
    * 绑定Reference，将AttributeReference转换为BoundReference
    * @param expression 要进行绑定的表达式，A是Expression的子类
    * @param input 输入的属性集合
    * @param allowFailures
    * @tparam A
    * @return
    */
  def bindReference[A <: Expression](
      expression: A,
      input: Seq[Attribute],
      allowFailures: Boolean = false): A = {

    /**
      * 递归遍历表达式树，遇到AttributeReference就转换为BoundReference
      */
    expression.transform { case a: AttributeReference =>
      attachTree(a, "Binding attribute") {
        val ordinal = input.indexWhere(_.exprId == a.exprId)
        val attribute = input(ordinal)
        if (ordinal == -1) {
          if (allowFailures) {
            a
          } else {
            sys.error(s"Couldn't find $a in ${input.mkString("[", ",", "]")}")
          }
        } else {
          BoundReference(ordinal, a.dataType, attribute.nullable)
        }
      }
    }.asInstanceOf[A] // Kind of a hack, but safe.  TODO: Tighten return type when possible.
  }
}
