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

import javax.annotation.Nullable;

import org.apache.spark.unsafe.Platform;

/**
 * A consecutive block of memory, starting at a {@link MemoryLocation} with a fixed size.
 *
 * MemoryBlock继承自MemoryLocation是非常low的设计，它应该组合MemoryLocation，因为MemoryBlock包含有address+size的信息，因此：
 * MemoryBlock has a Location，but  it is hard to say that MemoryBlock is a location
 * MemoryBlock is a Location
 */
public class MemoryBlock extends MemoryLocation {

  /***
   * 内存字节数
   */
  private final long length;

  /**
   * Optional page number;
   * Used when this MemoryBlock represents a page allocated by a TaskMemoryManager. This field is public so that it can be modified by the TaskMemoryManager,
   * which lives in a different package.
   *
   * MemoryBlock也可以当做TaskMemoryManager分配的内存page使用，又一个很low的设计
   *
   * 这里把MemoryBlock用做一个内存page， pageNumber记录了TaskMemoryManager持有的MemoryBlock集合的下标
   *
   */
  public int pageNumber = -1;

  public MemoryBlock(@Nullable Object obj, long offset, long length) {
    super(obj, offset);
    this.length = length;
  }

  /**
   * Returns the size of the memory block.
   */
  public long size() {
    return length;
  }

  /**
   * Creates a memory block pointing to the memory used by the long array.
   */
  public static MemoryBlock fromLongArray(final long[] array) {
    return new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, array.length * 8);
  }
}
