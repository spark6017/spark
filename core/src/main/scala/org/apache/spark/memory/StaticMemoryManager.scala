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

package org.apache.spark.memory

import org.apache.spark.SparkConf
import org.apache.spark.storage.BlockId

/**
 * A [[MemoryManager]] that statically partitions the heap space into disjoint regions.
 *
 * 对execution memory和storage memory进行静态划分的内存管理器
 *
 * execution memory的比例由spark.shuffle.memoryFraction进行配置
 * storage memory的比例由spark.storage.memoryFraction进行配置
 *
 * 这两块内存是相互独立的，互相不能借用，即使出现execution memory内存不足而storage memory非常充足的情况
 *
 * The sizes of the execution and storage regions are determined through
 * `spark.shuffle.memoryFraction` and `spark.storage.memoryFraction` respectively. The two
 * regions are cleanly separated such that neither usage can borrow memory from the other.
  *
 *
 * 问题：在指定execution memory和stoage memory的大小时，为什么需要显式的说是on heap execution memory？
  * @param conf
  * @param maxOnHeapExecutionMemory 堆上可用的最大execution memory
  * @param maxStorageMemory 最大的storage memory
  * @param numCores 并行度，并行的Task共享内存，也就是说每个Task的可用内存是有限制的
  */
private[spark] class StaticMemoryManager(
    conf: SparkConf,
    maxOnHeapExecutionMemory: Long,
    override val maxStorageMemory: Long,
    numCores: Int)
  extends MemoryManager(
    conf,
    numCores,
    maxStorageMemory,
    maxOnHeapExecutionMemory) {


  /** *
    * 由StaticMemoryManager进行最大execution memory和最大storage memory的计算
    * 调用StaticMemoryManager object的getMaxExecutionMemory和getMaxStorageMemory进行计算(注意：在计算时，跟内核数是没有关系的)
    *
    * @param conf
    * @param numCores
    */
  def this(conf: SparkConf, numCores: Int) {
    this(
      conf,
      StaticMemoryManager.getMaxExecutionMemory(conf),
      StaticMemoryManager.getMaxStorageMemory(conf),
      numCores)
  }

  /** *
    *  Max number of bytes worth of blocks to evict when unrolling
    *  问题：何为unroll memory？
    *  最大的unroll memory是可配置的，配置参数是spark.storage.unrollFraction，默认0.2
    *  最大的unroll memory = 最大的storage memory * 0.2
    */
  private val maxUnrollMemory: Long = {
    (maxStorageMemory * conf.getDouble("spark.storage.unrollFraction", 0.2)).toLong
  }

  /**
   * 申请storage memory
   * @param blockId 申请内存空间的blockId，比如RDDBlock、BroadcastBlock
   * @param numBytes 申请的内存字节数
   * @return whether all N bytes were successfully granted.
   */
  override def acquireStorageMemory(blockId: BlockId, numBytes: Long): Boolean = synchronized {
    if (numBytes > maxStorageMemory) {
      // Fail fast if the block simply won't fit
      logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our " +
        s"memory limit ($maxStorageMemory bytes)")
      false
    } else {

      /** *
        *  调用StorageMemoryPool进行内存分配
        *  问题：如果分配成功，实际存放blockId的操作是哪里完成的？
        *
        */
      storageMemoryPool.acquireMemory(blockId, numBytes)
    }
  }

  /** *
    * 申请unroll memory，问题：何为unroll memory
    * @param blockId
    * @param numBytes
    * @return whether all N bytes were successfully granted.
    */
  override def acquireUnrollMemory(blockId: BlockId, numBytes: Long): Boolean = synchronized {
    val currentUnrollMemory = storageMemoryPool.memoryStore.currentUnrollMemory
    val freeMemory = storageMemoryPool.memoryFree
    // When unrolling, we will use all of the existing free memory, and, if necessary,
    // some extra space freed from evicting cached blocks. We must place a cap on the
    // amount of memory to be evicted by unrolling, however, otherwise unrolling one
    // big block can blow away the entire cache.
    val maxNumBytesToFree = math.max(0, maxUnrollMemory - currentUnrollMemory - freeMemory)
    // Keep it within the range 0 <= X <= maxNumBytesToFree
    val numBytesToFree = math.max(0, math.min(maxNumBytesToFree, numBytes - freeMemory))
    storageMemoryPool.acquireMemory(blockId, numBytes, numBytesToFree)
  }

  private[memory]
  override def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    memoryMode match {
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.acquireMemory(numBytes, taskAttemptId)
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.acquireMemory(numBytes, taskAttemptId)
    }
  }
}


/** *
  *  StaticMemoryManager的半生对象，用于计算最大的storage memory和最大的execution memory
  */
private[spark] object StaticMemoryManager {

  /**
   * Return the total amount of memory available for the storage region, in bytes.
   * 问题：当在Executor中执行Runtime.getRuntime.maxMemory方法时，方法返回的内存是多少？
   * 也就说，假如指定--executor-memory 4G,而实际物理内存是32G，那么在executor执行任务时， Runtime.getRuntime.maxMemory返回多少
   *
   *   结论：默认情况下：
   *   execution memory是可用内存的54%
   *   storage memory是可用内存的16%
   *   也就是说，execution memory + storage memory = 可用内存的70%，其余30%留作它用
   *
    * @param conf
    * @return
    */
  private def getMaxStorageMemory(conf: SparkConf): Long = {
    //获取当前进程JVM可用内存，可以通过spark.testing.memory进行配置
    val systemMaxMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)

    //storage memory占可用内存的比例，默认0.6
    val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)

    //安全系数
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)

    //可用内存*storage memory比例*安全系数=可用内存*0.54
    (systemMaxMemory * memoryFraction * safetyFraction).toLong
  }

  /**
   * Return the total amount of memory available for the execution region, in bytes.
   */
  private def getMaxExecutionMemory(conf: SparkConf): Long = {
    val systemMaxMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)

    //execution memory占可用内存的比例，默认0.2
    val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)

    //execution memory的安全系数，默认是0.8
    val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", 0.8)

    //可用内存*execution memory的比例*安全系数 = 可用内存*0.16
    (systemMaxMemory * memoryFraction * safetyFraction).toLong
  }

}
