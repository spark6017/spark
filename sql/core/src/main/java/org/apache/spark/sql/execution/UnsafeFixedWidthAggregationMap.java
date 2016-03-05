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

import org.apache.spark.SparkEnv;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.KVIterator;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.map.BytesToBytesMap;
import org.apache.spark.unsafe.memory.MemoryLocation;

import java.io.IOException;

/**
 * Unsafe-based HashMap for performing aggregations where the aggregated values are fixed-width.
 *
 * This map supports a maximum of 2 billion keys.
 *
 *
 * 聚合函数结果类型长度固定的聚合Map
 *
 */
public final class UnsafeFixedWidthAggregationMap {

  /**
   * An empty aggregation buffer, encoded in UnsafeRow format. When inserting a new key into the
   * map, we copy this buffer and use it as the value.
   *
   * emptyAggregationBuffer是一个未初始化的bytes数组，因为是final的，因此它唯一的初始化位置是构造函数
   */
  private final byte[] emptyAggregationBuffer;

  /***
   * 聚合结果数据行的Schema
   */
  private final StructType aggregationBufferSchema;

  /***
   * 分组Key的Schema
   */
  private final StructType groupingKeySchema;

  /**
   * Encodes grouping keys as UnsafeRows.
   *
   * 把grouping keys编码为UnsafeRows，如何做到的？
   */
  private final UnsafeProjection groupingKeyProjection;

  /**
   * A hashmap which maps from opaque bytearray keys to bytearray values.
   */
  private final BytesToBytesMap map;

  /**
   * Re-used pointer to the current aggregation buffer
   *
   * 因为Aggregation Buffer是一个UnsafeRow，使用currentAggregationBuffer指向当前的aggregation buffer
   *
   * 何为Re-used pointer？
   */
  private final UnsafeRow currentAggregationBuffer;

  private final boolean enablePerfMetrics;

  /**
   * @return true if UnsafeFixedWidthAggregationMap supports aggregation buffers with the given
   *         schema, false otherwise.
   *         聚合函数的结果类型
   *
   */
  public static boolean supportsAggregationBufferSchema(StructType schema) {

    /***
     *  遍历所有的列,所有的列的数据类型都要求是Mutable的
     *
     */
    StructField[] fields = schema.fields();
    for (StructField field: fields) {
      DataType dataType = field.dataType();
      boolean isMutable = UnsafeRow.isMutable(dataType);
      if (!isMutable) {
        return false;
      }
    }
    return true;
  }

  /**
   * Create a new UnsafeFixedWidthAggregationMap.
   *
   * @param emptyAggregationBuffer the default value for new keys (a "zero" of the agg. function)
   * @param aggregationBufferSchema the schema of the aggregation buffer, used for row conversion.
   * @param groupingKeySchema the schema of the grouping key, used for row conversion.
   * @param taskMemoryManager the memory manager used to allocate our Unsafe memory structures.
   * @param initialCapacity the initial capacity of the map (a sizing hint to avoid re-hashing).
   * @param pageSizeBytes the data page size, in bytes; limits the maximum record size.
   * @param enablePerfMetrics if true, performance metrics will be recorded (has minor perf impact)
   */
  public UnsafeFixedWidthAggregationMap(
      InternalRow emptyAggregationBuffer,
      StructType aggregationBufferSchema,
      StructType groupingKeySchema,
      TaskMemoryManager taskMemoryManager,
      int initialCapacity,
      long pageSizeBytes,
      boolean enablePerfMetrics) {
    this.aggregationBufferSchema = aggregationBufferSchema;
    this.currentAggregationBuffer = new UnsafeRow(aggregationBufferSchema.length());
    this.groupingKeyProjection = UnsafeProjection.create(groupingKeySchema);
    this.groupingKeySchema = groupingKeySchema;
    this.map =
      new BytesToBytesMap(taskMemoryManager, initialCapacity, pageSizeBytes, enablePerfMetrics);
    this.enablePerfMetrics = enablePerfMetrics;

    // Initialize the buffer for aggregation value
    final UnsafeProjection valueProjection = UnsafeProjection.create(aggregationBufferSchema);

    /***
     * 初始化空的aggregation buffer
     */
    this.emptyAggregationBuffer = valueProjection.apply(emptyAggregationBuffer).getBytes();
  }

  /**
   * Return the aggregation buffer for the current group. For efficiency, all calls to this method
   * return the same object. If additional memory could not be allocated, then this method will
   * signal an error by returning null.
   */
  public UnsafeRow getAggregationBuffer(InternalRow groupingKey) {
    final UnsafeRow unsafeGroupingKeyRow = this.groupingKeyProjection.apply(groupingKey);

    return getAggregationBufferFromUnsafeRow(unsafeGroupingKeyRow);
  }

    /***
     * 根据Grouping Key(由Unsafe Row表示)得到其对应的Aggregation Buffer(也是由Unsafe Row表示)
     * @param unsafeGroupingKeyRow grouping key对应的UnsafeRow
     * @return grouping key在map中对应的aggregation buffer(由UnsafeRow表示)
     */
  public UnsafeRow getAggregationBufferFromUnsafeRow(UnsafeRow unsafeGroupingKeyRow) {
    // Probe our map using the serialized key

    /**
     * 因为grouping key是一个UnsafeRow，所以可以获取UnsafeRow能够获取的一切信息，包括baseObject、baseOffset、sizeInBytes
     */
    Object baseObject =  unsafeGroupingKeyRow.getBaseObject();
    long baseOffset = unsafeGroupingKeyRow.getBaseOffset();
    int sizeInBytes = unsafeGroupingKeyRow.getSizeInBytes();

    /***
     * 根据baseObject、baseOffset和sizeInBytes获取一个BytesToBytesMap.Location对象，
     * 注意：map是BytesToBytesMap类型的，也就是说，UnsafeFixedWidthAggregationMap底层使用BytesToBytesMap完成操作
     */
    final BytesToBytesMap.Location loc = map.lookup(baseObject,baseOffset, sizeInBytes);


    /***
     * 如果grouping key在BytesToBytesMap中没有定义
     */
    if (!loc.isDefined()) {
      // This is the first time that we've seen this grouping key, so we'll insert a copy of the
      // empty aggregation buffer into the map:
      /***
       * 首先插入一个空的aggregation buffer(UnsafeRow)，下面将值写入
       */
      boolean putSucceeded = loc.putNewKey(baseObject, baseOffset, sizeInBytes, emptyAggregationBuffer, Platform.BYTE_ARRAY_OFFSET, emptyAggregationBuffer.length);
      //如果插入失败，表示没有足够的内存了
      if (!putSucceeded) {
        return null;
      }
    }

    /***
     * Reset the pointer to point to the value that we just stored or looked up:
     * currentAggregationBuffer指向value的地址(这里的value就是aggregate buffer对应得到UnsafeRow，因为aggregate buffer是fixed width的，因此可以进行修改
     * 而无需修改数据的长度)
     *
     */
    final MemoryLocation address = loc.getValueAddress();
    baseObject = address.getBaseObject();
    baseOffset = address.getBaseOffset();
    int valueLength = loc.getValueLength();
    currentAggregationBuffer.pointTo(baseObject, baseOffset, valueLength);

    /***
     * currentAggregationBuffer作为getAggregationBufferFromUnsafeRow的返回值，目前并没有对他进行update，这个应该是在方法之外进行操作
     */
    return currentAggregationBuffer;
  }

  /**
   * Returns an iterator over the keys and values in this map. This uses destructive iterator of
   * BytesToBytesMap. So it is illegal to call any other method on this map after `iterator()` has
   * been called.
   *
   * For efficiency, each call returns the same object.
   */
  public KVIterator<UnsafeRow, UnsafeRow> iterator() {
    return new KVIterator<UnsafeRow, UnsafeRow>() {

      private final BytesToBytesMap.MapIterator mapLocationIterator =
        map.destructiveIterator();
      private final UnsafeRow key = new UnsafeRow(groupingKeySchema.length());
      private final UnsafeRow value = new UnsafeRow(aggregationBufferSchema.length());

      @Override
      public boolean next() {
        if (mapLocationIterator.hasNext()) {
          final BytesToBytesMap.Location loc = mapLocationIterator.next();
          final MemoryLocation keyAddress = loc.getKeyAddress();
          final MemoryLocation valueAddress = loc.getValueAddress();
          key.pointTo(
            keyAddress.getBaseObject(),
            keyAddress.getBaseOffset(),
            loc.getKeyLength()
          );
          value.pointTo(
            valueAddress.getBaseObject(),
            valueAddress.getBaseOffset(),
            loc.getValueLength()
          );
          return true;
        } else {
          return false;
        }
      }

      @Override
      public UnsafeRow getKey() {
        return key;
      }

      @Override
      public UnsafeRow getValue() {
        return value;
      }

      @Override
      public void close() {
        // Do nothing.
      }
    };
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  public long getPeakMemoryUsedBytes() {
    return map.getPeakMemoryUsedBytes();
  }

  /**
   * Free the memory associated with this map. This is idempotent and can be called multiple times.
   */
  public void free() {
    map.free();
  }

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public void printPerfMetrics() {
    if (!enablePerfMetrics) {
      throw new IllegalStateException("Perf metrics not enabled");
    }
    System.out.println("Average probes per lookup: " + map.getAverageProbesPerLookup());
    System.out.println("Number of hash collisions: " + map.getNumHashCollisions());
    System.out.println("Time spent resizing (ns): " + map.getTimeSpentResizingNs());
    System.out.println("Total memory consumption (bytes): " + map.getTotalMemoryConsumption());
  }

  /**
   * Sorts the map's records in place, spill them to disk, and returns an [[UnsafeKVExternalSorter]]
   *
   * Note that the map will be reset for inserting new records, and the returned sorter can NOT be used
   * to insert records.
   *
   * destructAndCreateExternalSorter做的事情
   * 1. map中的聚合数据经排序后spill到磁盘
   * 1. map被reset，释放占用的空间，然后复用插入新的records
   * 2. 返回的UnsafeKVExternalSorter,不能用于插入新的record，只能用于指示本地聚合时hash map spill到磁盘的数据
   */
  public UnsafeKVExternalSorter destructAndCreateExternalSorter() throws IOException {
    org.apache.spark.storage.BlockManager  blockManager = SparkEnv.get().blockManager();
    long pageSizeBytes =  map.getPageSizeBytes();

    UnsafeKVExternalSorter sorter =  new UnsafeKVExternalSorter(groupingKeySchema, aggregationBufferSchema,blockManager, pageSizeBytes, map);
    return sorter;
  }
}
