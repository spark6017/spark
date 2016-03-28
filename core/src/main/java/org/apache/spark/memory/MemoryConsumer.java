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

package org.apache.spark.memory;

import java.io.IOException;

import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * An memory consumer of TaskMemoryManager, which support spilling.
 *
 * Note: this only supports allocation / spilling of Tungsten memory.
 */
public abstract class MemoryConsumer {

  protected final TaskMemoryManager taskMemoryManager;
  private final long pageSize;
  protected long used;

  protected MemoryConsumer(TaskMemoryManager taskMemoryManager, long pageSize) {
    this.taskMemoryManager = taskMemoryManager;
    this.pageSize = pageSize;
  }

  protected MemoryConsumer(TaskMemoryManager taskMemoryManager) {
    this(taskMemoryManager, taskMemoryManager.pageSizeBytes());
  }

  /**
   * Returns the size of used memory in bytes.
   */
  long getUsed() {
    return used;
  }

  /**
   * Force spill during building.
   *
   * For testing.
   */
  public void spill() throws IOException {
    spill(Long.MAX_VALUE, this);
  }

  /**
   * Spill some data to disk to release memory, which will be called by TaskMemoryManager
   * when there is not enough memory for the task.
   *
   * This should be implemented by subclass.
   *
   * Note: In order to avoid possible deadlock, should not call acquireMemory() from spill().
   *
   * Note: today, this only frees Tungsten-managed pages.
   *
   *
   * MemoryConsumer提供了spill抽象方法，也就是说，MemoryConsumer需要控制在内存不够时的spill到磁盘的逻辑
   *
   * @param size the amount of memory should be released          TaskMemoryManager请求当前MemoryConsumer(this对象)为trigger释放size字节的内存空间
   * @param trigger the MemoryConsumer that trigger this spilling    TaskMemoryManager请求当前MemoryConsumer释放size的内存空间给trigger这个MemoryConsumer使用
   * @return the amount of released memory in bytes 实际释放的内存字节数
   * @throws IOException
   */
  public abstract long spill(long size, MemoryConsumer trigger) throws IOException;

  /**
   * Allocates a LongArray of `size`.
   *
   * LongArray的构造函数是MemoryBlock
   */
  public LongArray allocateArray(long size) {

    //字节数是size*8
    long required = size * 8L;
    //申请MemoryBlock
    MemoryBlock page = taskMemoryManager.allocatePage(required, this);
    if (page == null || page.size() < required) {
      long got = 0;
      if (page != null) {
        got = page.size();
        taskMemoryManager.freePage(page, this);
      }
      taskMemoryManager.showMemoryUsage();
      throw new OutOfMemoryError("Unable to acquire " + required + " bytes of memory, got " + got);
    }
    used += required;
    return new LongArray(page);
  }

  /**
   * Frees a LongArray.
   */
  public void freeArray(LongArray array) {
    freePage(array.memoryBlock());
  }

  /**
   * Allocate a memory block with at least `required` bytes.
   *
   * Throws IOException if there is not enough memory.
   *
   * @throws OutOfMemoryError
   */
  protected MemoryBlock allocatePage(long required) {
    MemoryBlock page = taskMemoryManager.allocatePage(Math.max(pageSize, required), this);
    if (page == null || page.size() < required) {
      long got = 0;
      if (page != null) {
        got = page.size();
        taskMemoryManager.freePage(page, this);
      }
      taskMemoryManager.showMemoryUsage();
      throw new OutOfMemoryError("Unable to acquire " + required + " bytes of memory, got " + got);
    }
    used += page.size();
    return page;
  }

  /**
   *
   */

  /***
   * Free a memory block.
   *
   *  释放一个内存page
   * @param page
   */
  protected void freePage(MemoryBlock page) {
    used -= page.size();
    taskMemoryManager.freePage(page, this);
  }
}
