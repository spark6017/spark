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

import org.apache.spark.unsafe.Platform;

/**
 * A simple {@link MemoryAllocator} that uses {@code Unsafe} to allocate off-heap memory.
 *
 * In off-heap mode, memory is addressed directly with 64-bit long addresses.
 *
 * Tungsten内存管理分为off heap和on heap两种模式，UnsafeMemoryAllocator负责off heap上的内存申请与释放
 */
public class UnsafeMemoryAllocator implements MemoryAllocator {

  /***
   * 在off heap上分配size字节的内存，内存量是否有限制？
   * @param size
   * @return
   * @throws OutOfMemoryError
   */
  @Override
  public MemoryBlock allocate(long size) throws OutOfMemoryError {
    /***
     * address记录了申请的内存绝对地址
     */
    long address = Platform.allocateMemory(size);

    /***
     * 包装为MemoryBlock
     */
    return new MemoryBlock(null, address, size);
  }

  /***
   * 如果是off heap模式，那么调用Platform的freeMemory方法，参数是内存的绝对地址(memory的offset变量记录了绝对地址)
   * @param memory
   */
  @Override
  public void free(MemoryBlock memory) {
    assert (memory.obj == null) :
      "baseObject not null; are you trying to use the off-heap allocator to free on-heap memory?";
    Platform.freeMemory(memory.offset);
  }
}
