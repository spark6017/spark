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

package org.apache.spark.util.collection.unsafe.sort;

import java.util.Comparator;

import org.apache.avro.reflect.Nullable;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.util.collection.Sorter;

/**
 * Sorts records using an AlphaSort-style key-prefix sort. This sort stores pointers to records
 * alongside a user-defined prefix of the record's sorting key. When the underlying sort algorithm
 * compares records, it will first compare the stored key prefixes; if the prefixes are not equal,
 * then we do not need to traverse the record pointers to compare the actual records. Avoiding these
 * random memory accesses improves cache hit rates.
 *
 * 说明0：
 * UnsafeInMemorySorter封装了RecordComparator和PrefixComparator
 * 说明1：
 * UnsafeInMemorySorter实现了基于RecordPointerAndKeyPrefix进行排序的SortComparator
 *
 *
 * 如下内容来自：
 * The actual sorting is implemented by UnsafeInMemorySorter.
 * Most consumers will not use this directly, but instead will use UnsafeExternalSorter,a class which implements a sort that can spill to disk in response to memory pressure.
 * Internally, UnsafeExternalSorter creates UnsafeInMemorySorters to perform sorting and uses UnsafeSortSpillReader/Writer to spill and read back runs of sorted records and UnsafeSortSpillMerger to merge multiple sorted spills into a single sorted iterator.
 * This external sorter integrates with Spark's existing ShuffleMemoryManager for controlling spillin
 *
 */
public final class UnsafeInMemorySorter {

  /***
   * 排序算法的实现，基于KeyPrefix和RecordPointer的排序算法
   */
  private static final class SortComparator implements Comparator<RecordPointerAndKeyPrefix> {

    private final RecordComparator recordComparator;
    private final PrefixComparator prefixComparator;
    private final TaskMemoryManager memoryManager;

    /***
     * SortComparator的构造函数
     * @param recordComparator Record排序器
     * @param prefixComparator KeyPrefix排序器
     * @param memoryManager
       */
    public SortComparator(
        RecordComparator recordComparator,
        PrefixComparator prefixComparator,
        TaskMemoryManager memoryManager) {
      this.recordComparator = recordComparator;
      this.prefixComparator = prefixComparator;
      this.memoryManager = memoryManager;
    }

    /**
     * 首先进行key prefix的排序，然后进行record的排序
     * @param r1
     * @param r2
       * @return
       */
    @Override
    public int compare(RecordPointerAndKeyPrefix r1, RecordPointerAndKeyPrefix r2) {

      //基于key prefix排序
      final int prefixComparisonResult = prefixComparator.compare(r1.keyPrefix, r2.keyPrefix);

      //如果prefix比较结果不相等，那么无需进行record数据自身的比较
      if (prefixComparisonResult != 0) {
        return prefixComparisonResult;
      }

      //根据record pointer取出base object的地址
      final Object baseObject1 = memoryManager.getPage(r1.recordPointer);
      final Object baseObject2 = memoryManager.getPage(r2.recordPointer);

      //取出每个record的offset地址
      final long baseOffset1 = memoryManager.getOffsetInPage(r1.recordPointer) + 4; // skip length
      final long baseOffset2 = memoryManager.getOffsetInPage(r2.recordPointer) + 4; // skip length

      //问题：record的长度是多少？在哪里记录着？通过baseObject和baseOffset，可以得到存放数据的地址，那么数据本身占用多少字节，在哪里获取？

      //调用RecordComparator进行compare
      return recordComparator.compare(baseObject1, baseOffset1, baseObject2, baseOffset2);

    }
  }

  private final MemoryConsumer consumer;
  private final TaskMemoryManager memoryManager;
  @Nullable
  private final Sorter<RecordPointerAndKeyPrefix, LongArray> sorter;
  @Nullable
  private final Comparator<RecordPointerAndKeyPrefix> sortComparator;

  /**
   * Within this buffer, position {@code 2 * i} holds a pointer pointer to the record at
   * index {@code i}, while position {@code 2 * i + 1} in the array holds an 8-byte key prefix.
   *
   *
   * 偶数位置放record指针，奇数位置放record的key prefix值
   */
  private LongArray array;

  /**
   * The position in the sort buffer where new records can be inserted.
   */
  private int pos = 0;

  /***
   * 对于UnsafeInMemorySorter而言，它的第一个参数是MemoryConsumer，也就是说，它构造时需要的是Memory Consumer对象，不管外围的consumer究竟是什么
   * 也就是说，要使用UnsafeInMemorySorter，需要消费者实现MemoryConsumer抽象类
   * UnsafeExternalSorter实现了MemoryConsumer，在UnsafeExternalSorter创建UnsafeInMemorySorter时，传入了this(UnsafeExternalSorter本身)
   *
   *
   * 这个构造函数使用默认的排序数组LongArray(调用consumer.allocateArray(initialSize * 2), allocateArray是在MemoryConsumer类中创建的)
   * @param consumer
   * @param memoryManager
   * @param recordComparator
   * @param prefixComparator
   * @param initialSize
     */
  public UnsafeInMemorySorter(
    final MemoryConsumer consumer,
    final TaskMemoryManager memoryManager,
    final RecordComparator recordComparator,
    final PrefixComparator prefixComparator,
    int initialSize) {
    this(consumer, memoryManager, recordComparator, prefixComparator,consumer.allocateArray(initialSize * 2));
  }

  /***
   *
   * @param consumer
   * @param memoryManager
   * @param recordComparator
   * @param prefixComparator
     * @param array 内存LongArray
     */
  public UnsafeInMemorySorter(
    final MemoryConsumer consumer,
      final TaskMemoryManager memoryManager,
      final RecordComparator recordComparator,
      final PrefixComparator prefixComparator,
      LongArray array) {
    this.consumer = consumer;
    this.memoryManager = memoryManager;
    /**
     * 如果recordComparator不为null，则创建SortComparator
     */
    if (recordComparator != null) {
      this.sorter = new Sorter<>(UnsafeSortDataFormat.INSTANCE);
      this.sortComparator = new SortComparator(recordComparator, prefixComparator, memoryManager);
    } else {
      this.sorter = null;
      this.sortComparator = null;
    }
    this.array = array;
  }

  /**
   * Free the memory used by pointer array.
   */
  public void free() {
    if (consumer != null) {
      consumer.freeArray(array);
      array = null;
    }
  }

  public void reset() {
    pos = 0;
  }

  /**
   * @return the number of records that have been inserted into this sorter.
   */
  public int numRecords() {
    return pos / 2;
  }

  public long getMemoryUsage() {
    return array.size() * 8L;
  }

  public boolean hasSpaceForAnotherRecord() {
    return pos + 2 <= array.size();
  }

  public void expandPointerArray(LongArray newArray) {
    if (newArray.size() < array.size()) {
      throw new OutOfMemoryError("Not enough memory to grow pointer array");
    }
    Platform.copyMemory(
      array.getBaseObject(),
      array.getBaseOffset(),
      newArray.getBaseObject(),
      newArray.getBaseOffset(),
      array.size() * 8L);
    consumer.freeArray(array);
    array = newArray;
  }

  /**
   * Inserts a record to be sorted. Assumes that the record pointer points to a record length
   * stored as a 4-byte integer, followed by the record's bytes.
   *
   * record pointer指向的地址是存放length长度的4个字节，再这4个字节之后的length个字节是record数据的二进制表示
   *
   * 将record指针和record的key prefix放到一起（连续的内存地址）
   *
   * @param recordPointer pointer to a record in a data page, encoded by {@link TaskMemoryManager}.
   * @param keyPrefix a user-defined key prefix
   */
  public void insertRecord(long recordPointer, long keyPrefix) {
    if (!hasSpaceForAnotherRecord()) {
      expandPointerArray(consumer.allocateArray(array.size() * 2));
    }
    array.set(pos, recordPointer);
    pos++;
    array.set(pos, keyPrefix);
    pos++;
  }

  /***
   * 在UnsafeInMemorySorter中实现SortedIterator
   */
  public final class SortedIterator extends UnsafeSorterIterator {

    private final int numRecords;
    private int position;
    private Object baseObject;
    private long baseOffset;
    private long keyPrefix;
    private int recordLength;

    private SortedIterator(int numRecords) {
      this.numRecords = numRecords;
      this.position = 0;
    }

    /***
     * SortedIterator的clone操作
     * @return
     */
    public SortedIterator clone() {
      SortedIterator iter = new SortedIterator(numRecords);
      iter.position = position;
      iter.baseObject = baseObject;
      iter.baseOffset = baseOffset;
      iter.keyPrefix = keyPrefix;
      iter.recordLength = recordLength;
      return iter;
    }

    @Override
    public int getNumRecords() {
      return numRecords;
    }

    @Override
    public boolean hasNext() {
      return position / 2 < numRecords;
    }

    @Override
    public void loadNext() {
      // This pointer points to a 4-byte record length, followed by the record's bytes
      final long recordPointer = array.get(position);
      baseObject = memoryManager.getPage(recordPointer);
      baseOffset = memoryManager.getOffsetInPage(recordPointer) + 4;  // Skip over record length
      recordLength = Platform.getInt(baseObject, baseOffset - 4);
      keyPrefix = array.get(position + 1);
      position += 2;
    }

    @Override
    public Object getBaseObject() { return baseObject; }

    @Override
    public long getBaseOffset() { return baseOffset; }

    @Override
    public int getRecordLength() { return recordLength; }

    @Override
    public long getKeyPrefix() { return keyPrefix; }
  }

  /**
   * Return an iterator over record pointers in sorted order. For efficiency, all calls to
   * {@code next()} will return the same mutable object.
   */
  public SortedIterator getSortedIterator() {
    if (sorter != null) {
      sorter.sort(array, 0, pos / 2, sortComparator);
    }
    return new SortedIterator(pos / 2);
  }
}
