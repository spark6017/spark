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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.unsafe.sort.{PrefixComparator, PrefixComparators}

object SortPrefixUtils {

  /**
   * A dummy prefix comparator which always claims that prefixes are equal. This is used in cases
   * where we don't know how to generate or compare prefixes for a SortOrder.
   */
  private object NoOpPrefixComparator extends PrefixComparator {
    override def compare(prefix1: Long, prefix2: Long): Int = 0
  }

  /***
    * 根据sortOrder的dataType获取内置的PrefixComparator，目前支持的类型有
    * 1. String
    * 2. Binary
    * 3. Boolean、Byte、Short、Integer、Long、Date、Timestamp
    * 4. Decimal
    * 5. Float
    * 6. Double
    *
    * @param sortOrder 对于指定的SortOrder，获取其Prefix Comparator. 一个SortOrder表达式对应一个PrefixComparator
    * @return
    */
  def getPrefixComparator(sortOrder: SortOrder): PrefixComparator = {
    val dataType = sortOrder.dataType

    //不同数据类型的SortOrder表达式，使用不同的前缀比较器
    //比如升序的字符串前缀比较器：PrefixComparators.STRING
    //比如升序的布尔、整数、长整型、日期类型、时间戳类型的前缀比较器:  PrefixComparators.LONG
    dataType match {
      case StringType =>
        if (sortOrder.isAscending) PrefixComparators.STRING else PrefixComparators.STRING_DESC
      case BinaryType =>
        if (sortOrder.isAscending) PrefixComparators.BINARY else PrefixComparators.BINARY_DESC
      case BooleanType | ByteType | ShortType | IntegerType | LongType | DateType | TimestampType =>
        if (sortOrder.isAscending) PrefixComparators.LONG else PrefixComparators.LONG_DESC
      case dt: DecimalType if dt.precision - dt.scale <= Decimal.MAX_LONG_DIGITS =>
        if (sortOrder.isAscending) PrefixComparators.LONG else PrefixComparators.LONG_DESC
      case FloatType | DoubleType =>
        if (sortOrder.isAscending) PrefixComparators.DOUBLE else PrefixComparators.DOUBLE_DESC
      case dt: DecimalType =>
        if (sortOrder.isAscending) PrefixComparators.DOUBLE else PrefixComparators.DOUBLE_DESC
      case _ => NoOpPrefixComparator
    }
  }

  /**
   * Creates the prefix comparator for the first field in the given schema, in ascending order.
   */
  def getPrefixComparator(schema: StructType): PrefixComparator = {
    if (schema.nonEmpty) {
      val field = schema.head
      getPrefixComparator(SortOrder(BoundReference(0, field.dataType, field.nullable), Ascending))
    } else {
      new PrefixComparator {
        override def compare(prefix1: Long, prefix2: Long): Int = 0
      }
    }
  }

  /**
   * Creates the prefix computer for the first field in the given schema, in ascending order.
   *
   * 根据Schema获取PrefixComputer
   *
   * 问题：如果Schema有多个列，那么究竟根据那个列计算PrefixComputer？答：根据schema指定的第一列
    *
    * @param schema
    * @return
    */
  def createPrefixGenerator(schema: StructType): UnsafeExternalRowSorter.PrefixComputer = {
    if (schema.nonEmpty) {

      /** *
        * 取schema的第一个列作为计算prefix的依据
        */
      val boundReference = BoundReference(0, schema.head.dataType, nullable = true)
      val prefixProjection = UnsafeProjection.create(SortPrefix(SortOrder(boundReference, Ascending)))

      new UnsafeExternalRowSorter.PrefixComputer {
        override def computePrefix(row: InternalRow): Long = {
          prefixProjection.apply(row).getLong(0)
        }
      }
    } else {

      /** *
        * 如果schema为空，那么返回的prefix的值都是空，也就是说，在这种情况下，先根据prefix进行排序后record排序的价值失效(只能根据record排序)
        */
      new UnsafeExternalRowSorter.PrefixComputer {
        override def computePrefix(row: InternalRow): Long = 0
      }
    }
  }
}
