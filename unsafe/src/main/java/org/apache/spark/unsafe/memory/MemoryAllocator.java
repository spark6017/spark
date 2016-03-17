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


/***
 * 负责内存申请与释放，申请的内存包装为MemoryBlock，释放时也以MemoryBlock为参数
 * 在Tungsten内存管理中，内存分为on heap和off heap模式，通过Memory Block将二者联系起来
 * 同时，MemoryBlock在TaskMemoryManager也当做page使用，一个page就是内存空间，最大16G
 */
public interface MemoryAllocator {

  /**
   * Allocates a contiguous block of memory. Note that the allocated memory is not guaranteed
   * to be zeroed out (call `zero()` on the result if this is necessary).
   *
   * 问题：不保证分配的内存是zero out的？zero out是什么意思？
   */
  MemoryBlock allocate(long size) throws OutOfMemoryError;

  /***
   * off heap和on heap内存的释放由子类完成
   * @param memory
   */
  void free(MemoryBlock memory);

  /***
   * 堆外内存Allocator，使用Unsafe进行管理，申请和释放
   */
  MemoryAllocator UNSAFE = new UnsafeMemoryAllocator();

  /***
   * 堆内存Allocator
   */
  MemoryAllocator HEAP = new HeapMemoryAllocator();
}
