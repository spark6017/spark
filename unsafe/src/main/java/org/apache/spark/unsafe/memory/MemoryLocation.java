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

/**
 * A memory location. Tracked either by a memory address (with off-heap allocation),
 * or by an offset from a JVM object (in-heap allocation).
 *
 * on-heap内存，那么obj代表的是申请的内存空间，通常为字节数组，在这种情况下，offset指的是obj的offset，对于字节数组而言，就是Platform.BYTE_ARRAY_OFFSET
 * off-heap内存，那么obj为空，offset指的是内存空间的决定地址
 *
 * MemoryLocation只指定了内存的地址，但是没有指定内存的size。MemoryLocation这个类名起的正如其名，只有内存的location信息，没有size信息
 */
public class MemoryLocation {

  @Nullable
  Object obj;

  long offset;


  public MemoryLocation(@Nullable Object obj, long offset) {
    this.obj = obj;
    this.offset = offset;
  }

  public MemoryLocation() {
    this(null, 0);
  }

  public void setObjAndOffset(Object newObj, long newOffset) {
    this.obj = newObj;
    this.offset = newOffset;
  }

  /***
   * 如果是off heap模式，那么obj为null
   * @return
   */
  public final Object getBaseObject() {
    return obj;
  }

  public final long getBaseOffset() {
    return offset;
  }
}
