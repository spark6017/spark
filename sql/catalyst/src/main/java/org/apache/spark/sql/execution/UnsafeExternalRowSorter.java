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

package org.apache.spark.sql.execution;

import java.io.IOException;

import scala.collection.Iterator;
import scala.math.Ordering;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.util.AbstractScalaRowIterator;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.collection.unsafe.sort.PrefixComparator;
import org.apache.spark.util.collection.unsafe.sort.RecordComparator;
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter;
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterIterator;

/**
 * final class,既没有继承其它类，也没有被其它类继承
 *
 * UnsafeExternalRowSorter封装了
 * 1. UnsafeExternalSorter
 * 2. PrefixComputer
 *
 * UnsafeExternalSorter内部封装了PrefixComputer和RowComparator
 * 1. PrefixComputer是UnsafeExternalRowSorter封装的PrefixComputer
 * 2. RowComparator是UnsafeExternalRowSorter创建UnsafeExternalSorter时先构造出来的
 */
final class UnsafeExternalRowSorter {

  /**
   * If positive, forces records to be spilled to disk at the given frequency (measured in numbers
   * of records). This is only intended to be used in tests.
   */
  private int testSpillFrequency = 0;

  private long numRowsInserted = 0;

  private final StructType schema;
  private final PrefixComputer prefixComputer;

  /**
   * Unsafe External Sorter
   *
   * @see [[ExternalSorter]]
   */
  private final UnsafeExternalSorter sorter;

  /***
   * 静态内部类，用于计算UnsafeRow的prefix
   */
  public abstract static class PrefixComputer {
    abstract long computePrefix(InternalRow row);
  }

  /**
   * UnsafeExternalRowSorter构造函数，
   *
   * 调用UnsafeExternalSorter的create方法创建UnsafeExternalSorter
   *
   * @param schema
   * @param ordering
   * @param prefixComparator
   * @param prefixComputer
   * @param pageSizeBytes
     * @throws IOException
     */
  public UnsafeExternalRowSorter(
      StructType schema,
      Ordering<InternalRow> ordering,
      PrefixComparator prefixComparator,
      PrefixComputer prefixComputer,
      long pageSizeBytes) throws IOException {
    this.schema = schema;
    this.prefixComputer = prefixComputer;
    final SparkEnv sparkEnv = SparkEnv.get();
    final TaskContext taskContext = TaskContext.get();

      /***
       * 创建UnsafeExternalSorter
       * 创建RowComparator，传给UnsafeExternalSorter。
       * 因为UnsafeExternalRowSorter是与Spark SQL相关的(有row的概念)，因此在UnsafeExternalRowSorter中负责创建RowComparator
       * 对于UnsafeExternalSorter而言不是与Spark SQL绑定的（也就是说不是绑定到Row上的)，因此需要实际的使用者提供这个RecordComparator的实现
       */
    sorter = UnsafeExternalSorter.create(
      taskContext.taskMemoryManager(),
      sparkEnv.blockManager(),
      taskContext,
      new RowComparator(ordering, schema.length()),
      prefixComparator,
      /* initialSize */ 4096,
      pageSizeBytes
    );
  }

  /**
   * Forces spills to occur every `frequency` records. Only for use in tests.
   */
  @VisibleForTesting
  void setTestSpillFrequency(int frequency) {
    assert frequency > 0 : "Frequency must be positive";
    testSpillFrequency = frequency;
  }

  /**
   * 插入记录，如果空间不够则进行Spill?testSpillFrequency大于0并且每插入testSpillFrequency个数的row才进行spill
   * testSpillFrequency默认是0，只有在测试时才会使用sorter.spill
   * @param row
   * @throws IOException
     */
  @VisibleForTesting
  void insertRow(UnsafeRow row) throws IOException {
    /***
     * 在UnsafeExternalRowSorter中计算出要插入的unsafe row的prefix以便进行基于prefix的排序
     */
    final long prefix = prefixComputer.computePrefix(row);

      /***
       * 调用UnsafeExternalSorter的insertRecord方法向sorter中插入record
       * 插入的数据包含两部分：
       * 1. 指向这个record数据的record address
       * 2. 这个record的key prefix
       *
       * prefix和这个record引用放到一起，从而在进行row比较时，尽量避免获取record数据本身(record指针和record数据本身可能没有放到一起)
       *
       */
    sorter.insertRecord(
      row.getBaseObject(),
      row.getBaseOffset(),
      row.getSizeInBytes(),
      prefix
    );
    numRowsInserted++;

    //在生产环境，不支持spill
    if (testSpillFrequency > 0 && (numRowsInserted % testSpillFrequency) == 0) {
      sorter.spill();
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  public long getPeakMemoryUsage() {
    return sorter.getPeakMemoryUsedBytes();
  }

  private void cleanupResources() {
    sorter.cleanupResources();
  }

  /**
   * 是否会做内存数据和磁盘数据做merge,会做归并排序
   * @return 对孩子物理计划输出的数据(Iterator)进行Sort物理计划转换后得到的Iterator
   * @throws IOException
     */
  @VisibleForTesting
  Iterator<UnsafeRow> sort() throws IOException {
    try {

        /***
         * 调用UnsafeExternalSorter的getSortedIterator获得UnsafeSorterIterator
         * sortedIterator是SpillableIterator，SpillableIterator封装了UnsafeInMemorySorter排序后的iterator
         */
      final UnsafeSorterIterator sortedIterator = sorter.getSortedIterator();
      if (!sortedIterator.hasNext()) {
        // Since we won't ever call next() on an empty iterator, we need to clean up resources
        // here in order to prevent memory leaks.
        cleanupResources();
      }


      /***
       * 进行一个封装
       */
      return new AbstractScalaRowIterator<UnsafeRow>() {

        private final int numFields = schema.length();
        private UnsafeRow row = new UnsafeRow(numFields);

        /***
         * 调用sortedIterator判断是否有数据
         * @return
           */
        @Override
        public boolean hasNext() {
          return sortedIterator.hasNext();
        }

        /***
         *
         * @return
           */
        @Override
        public UnsafeRow next() {
          try {
            /***
             * 首先加载
             */
            sortedIterator.loadNext();

              /***
               * 更新row值
               */
            row.pointTo(
              sortedIterator.getBaseObject(),
              sortedIterator.getBaseOffset(),
              sortedIterator.getRecordLength());

            /**
             * 如果是最后一个row，那么cleanup
             */
            if (!hasNext()) {
              UnsafeRow copy = row.copy(); // so that we don't have dangling pointers to freed page
              row = null; // so that we don't keep references to the base object
              cleanupResources();
              return copy;
            } else {
              return row;
            }
          } catch (IOException e) {
            cleanupResources();
            // Scala iterators don't declare any checked exceptions, so we need to use this hack
            // to re-throw the exception:
            Platform.throwException(e);
          }
          throw new RuntimeException("Exception should have been re-thrown in next()");
        };
      };
    } catch (IOException e) {
      cleanupResources();
      throw e;
    }
  }


  /**
   * 对UnsafeRow集合进行排序,返回排序后的Iterator<UnsafeRow>
   * @param inputIterator
   * @return
   * @throws IOException
     */
  public Iterator<UnsafeRow> sort(Iterator<UnsafeRow> inputIterator) throws IOException {

    //第一步： 将数据插入到sorter中，可能需要spill到磁盘,
    //在这一步没有进行排序，只是插入数据
    while (inputIterator.hasNext()) {
      UnsafeRow row = inputIterator.next();
      insertRow(row);
    }

    //第二步：调用sort方法完成merge操作
    return sort();
  }

  /***
   * RecordComparator的Record的含义在这里有一定的体现，Record是广泛意义上的数据记录，而不仅仅单指Row
   * Row只是Record的一种情况
   */
  private static final class RowComparator extends RecordComparator {
    private final Ordering<InternalRow> ordering;
    private final int numFields;
    private final UnsafeRow row1;
    private final UnsafeRow row2;

    public RowComparator(Ordering<InternalRow> ordering, int numFields) {
      this.numFields = numFields;
      this.row1 = new UnsafeRow(numFields);
      this.row2 = new UnsafeRow(numFields);
      this.ordering = ordering;
    }

    /***
     * 基于Ordering[InternalRow]进行排序，注意排序的Ordering的元素类型是Ordering[InternalRow]
     * 而row1和row2是UnsafeRow类型的，
     * 也就是说，需要把UnsafeRow转换为InternalRow? 不需要，因为UnsafeRow本身是InternalRow的子类
     *
     *
     * @param baseObj1
     * @param baseOff1
     * @param baseObj2
     * @param baseOff2
       * @return
       */
    @Override
    public int compare(Object baseObj1, long baseOff1, Object baseObj2, long baseOff2) {
      // TODO: Why are the sizes -1?
      row1.pointTo(baseObj1, baseOff1, -1);
      row2.pointTo(baseObj2, baseOff2, -1);
      return ordering.compare(row1, row2);
    }
  }
}
