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

package org.apache.spark.util.collection

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.memory.{MemoryMode, TaskMemoryManager}

/**
 * Spills contents of an in-memory collection to disk when the memory threshold
 * has been exceeded.
 */
private[spark] trait Spillable[C] extends Logging {
  /**
   * Spills the current in-memory collection to disk, and releases the memory.
   * 只数将数据spill到磁盘，没说数据的排序情况
   *
   * @param collection collection to spill to disk
   */
  protected def spill(collection: C): Unit

  // Number of elements read from input since last spill
  protected def elementsRead: Long = _elementsRead

  // Called by subclasses every time a record is read
  // It's used for checking spilling frequency
  protected def addElementsRead(): Unit = { _elementsRead += 1 }

  // Memory manager that can be used to acquire/release memory
  protected[this] def taskMemoryManager: TaskMemoryManager

  // Initial threshold for the size of a collection before we start tracking its memory usage
  // For testing only

  /***
    * Shuffle Spill的阀值初始值为5M
    */
  private[this] val initialMemoryThreshold: Long =
    SparkEnv.get.conf.getLong("spark.shuffle.spill.initialMemoryThreshold", 5 * 1024 * 1024)

  /** *
    * Force this collection to spill when there are this many elements in memory（For testing only）
    * 该配置可以用于测试spill，只要读取的元素个数超过这个值，则进行spill
    */
  private[this] val numElementsForceSpillThreshold: Long =
    SparkEnv.get.conf.getLong("spark.shuffle.spill.numElementsForceSpillThreshold", Long.MaxValue)

  // Threshold for this collection's size in bytes before we start tracking its memory usage
  // To avoid a large number of small spills, initialize this to a value orders of magnitude > 0
  private[this] var myMemoryThreshold = initialMemoryThreshold

  // Number of elements read from input since last spill
  private[this] var _elementsRead = 0L

  // Number of bytes spilled in total
  private[this] var _memoryBytesSpilled = 0L

  // Number of spills
  private[this] var _spillCount = 0

  /**
   * Spills the current in-memory collection to disk if needed. Attempts to acquire more
   * memory before spilling.
    *
    * Spill到磁盘，
    * 1. Spill到磁盘的数据的文件位置放在哪里
    * 2. 写到磁盘上的文件的格式是什么样的
   *
   * @param collection collection to spill to disk
   * @param currentMemory estimated size of the collection in bytes
   * @return true if `collection` was spilled to disk; false otherwise
   */
  protected def maybeSpill(collection: C, currentMemory: Long): Boolean = {
    var shouldSpill = false

    /**
      * 判断是否需要Spill，读取的文件数是32的整数倍，并且当前使用的内存大于等于设置的内存法制
      */
    if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
      // Claim up to double our current memory from the shuffle memory pool

      /***
        * 申请内存，因为currentMemory >= myMemoryThreshold,因此，申请的内存量是大于currentMemory的，
        * 也就说，申请的内存不小于PartitionedAppendOnlyMap当前的内存使用量，是Map的容量翻倍
        */
      val amountToRequest = 2 * currentMemory - myMemoryThreshold

      /***
        * 申请内存
        */
      val granted =
        taskMemoryManager.acquireExecutionMemory(amountToRequest, MemoryMode.ON_HEAP, null)

      /**
        * 分配给我的可用的内存容量是myMemoryThreshold + granted
        */
      myMemoryThreshold += granted
      // If we were granted too little memory to grow further (either tryToAcquire returned 0,
      // or we already had more memory than myMemoryThreshold), spill the current collection

      /**
        * 如果申请完内存后，当前使用的内存量依然比我可用的内存量大，那么需要Spill
        */
      shouldSpill = currentMemory >= myMemoryThreshold
    }

    /**
      * ShouldSpill依赖于两方面的因素
      * 1. 当前需要的内存大于我可用的内存
      * 2. 已读取的元素个数大于设置的读取元素个数就Spill的阀值(可用于测试)
      */
    shouldSpill = shouldSpill || _elementsRead > numElementsForceSpillThreshold
    // Actually spill
    if (shouldSpill) {
      shouldSpill = true
      _spillCount += 1
      logSpillage(currentMemory)

      /***
        * 将集合spill到磁盘，spill前是否要进行集合的排序操作？即spill到磁盘是否是排序的
        */
      spill(collection)

      /**
        * 本此spill前读取到的elements个数还原为0
        */
      _elementsRead = 0

      /**
        * Spill到磁盘的字节数，从内存spill到磁盘的字节数
        */
      _memoryBytesSpilled += currentMemory

      /***
        * 释放已经申请的资源，恢复到初始值
        */
      releaseMemory()
    }
    shouldSpill
  }

  /**
   * @return number of bytes spilled in total
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled

  /**
   * Release our memory back to the execution pool so that other tasks can grab it.
   */
  def releaseMemory(): Unit = {
    // The amount we requested does not include the initial memory tracking threshold
    //释放myMemoryThreshold - initialMemoryThreshold字节的内存
    taskMemoryManager.releaseExecutionMemory(
      myMemoryThreshold - initialMemoryThreshold, MemoryMode.ON_HEAP, null)

    /***
      * 将myMemoryThreshold还原为initialMemoryThreshold，也就是说myMemoryThreshold的初始值就是initialMemoryThreshold
      */
    myMemoryThreshold = initialMemoryThreshold
  }

  /**
   * Prints a standard log message detailing spillage.
   *
   * @param size number of bytes spilled
   */
  @inline private def logSpillage(size: Long) {
    val threadId = Thread.currentThread().getId
    logInfo("Thread %d spilling in-memory map of %s to disk (%d time%s so far)"
      .format(threadId, org.apache.spark.util.Utils.bytesToString(size),
        _spillCount, if (_spillCount > 1) "s" else ""))
  }
}
