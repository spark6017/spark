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

package org.apache.spark.unsafe.memory;

import javax.annotation.concurrent.GuardedBy;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.spark.unsafe.Platform;

/**
 * A simple {@link MemoryAllocator} that can allocate up to 16GB using a JVM long primitive array.
 *
 * In on-heap mode, memory addresses are identified by the combination of a base Object and an offset within that object
 *
 * 负责堆上内存的申请与释放，申请的内存包装成MemoryBlock，释放时也以MemoryBlock为参数
 * MemoryBlock记录了memory的绝对地址(off heap模式)或者on heap对象以及对象内的偏移量(on heap模式)
 */
public class HeapMemoryAllocator implements MemoryAllocator {

  @GuardedBy("this")

  /***
   * 内存size和MemoryBlock之间的映射关系，为什么会有这种设计？如果size非常零散，那么bufferPoolsBySize将会非常大
   */
  private final Map<Long, LinkedList<WeakReference<MemoryBlock>>> bufferPoolsBySize =
    new HashMap<>();

  private static final int POOLING_THRESHOLD_BYTES = 1024 * 1024;

  /**
   * Returns true if allocations of the given size should go through the pooling mechanism and
   * false otherwise.
   *
   *
   */
  private boolean shouldPool(long size) {
    // Very small allocations are less likely to benefit from pooling.
    return size >= POOLING_THRESHOLD_BYTES;
  }

  /***
   * 在堆上内存分配size字节的内存,size字节是有限制的
   * @param size
   * @return
   * @throws OutOfMemoryError
   */
  @Override
  public MemoryBlock allocate(long size) throws OutOfMemoryError {

    /***
     * 首先从内存池里获取满足条件的内存，
     * free内存时，bufferPoolsBySize会记录释放的内存
     */
    if (shouldPool(size)) {
      synchronized (this) {
        final LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
        if (pool != null) {
          while (!pool.isEmpty()) {
            final WeakReference<MemoryBlock> blockReference = pool.pop();
            final MemoryBlock memory = blockReference.get();
            if (memory != null) {
              assert (memory.size() == size);
              return memory;
            }
          }
          bufferPoolsBySize.remove(size);
        }
      }
    }

    /***
     * 因为(size + 7)/8是整数，因此对size有大小限制， (size + 7) /8 <= 2^32 - 1
     *
     * 加入分配8个字节，那么array是长度为1的数组；如果是10个字节，那么将array是长度为2的数组，8个字节对齐
     * 如果分配24个字节，那么array是长度为3的数组；如果是30个字节，那让array是长度为4的数组，8个字节对齐
     * 所以，这里通过创建long数组完成分配内存空间，或者说，申请内存空间的过程就是创建long数组的过程，实际分配的字节数是8对齐的
     */
    long[] array = new long[(int) ((size + 7) / 8)];
    return new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
  }

  /***
   * 释放on heap占用的内存，如果要释放的内存大于1M则将它归还到pool里；否则什么也不干，为什么？
   *
   * @param memory
   */
  @Override
  public void free(MemoryBlock memory) {
    //
    final long size = memory.size();

    /***
     * 需要加到pool中，只有大于1M的内存块才加入到pool中
     */
    if (shouldPool(size)) {
      synchronized (this) {
        LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
        /***
         * 如果还没有size大小的memory block加入到pool中，那么首先创建pool并加入到bufferPoolsBySize中
         */
        if (pool == null) {
          pool = new LinkedList<>();
          bufferPoolsBySize.put(size, pool);
        }

        /***
         * 将memory加入到pool中
         */
        pool.add(new WeakReference<>(memory));
      }
    } else {
      // Do nothing
    }
  }
}
