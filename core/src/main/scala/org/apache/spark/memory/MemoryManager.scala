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

import javax.annotation.concurrent.GuardedBy

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.storage.{BlockId, MemoryStore}
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.memory.MemoryAllocator

/**
 * An abstract memory manager that enforces how memory is shared between execution and storage.
 *
 *  决定execution memory和storage memory共享模式的内存管理器
 *  execution memory指的是Task在运行过程中用到的shuffle、join、sort和Aggregation，它们在计算过程中会使用内存作为buffer
 *  storage memory指的是用于缓存的内存，包括RDD cache、broadcast数据以及任务分发所用到的内存
 *
 *  注意：每个JVM只有一个内存管理器，因为Executor是内存的实际使用者，也就是说，每个Executor有唯一一个MemoryManager，称为ExecutorMemoryManager
 *
 * In this context, execution memory refers to that used for computation in shuffles, joins,
 * sorts and aggregations, while storage memory refers to that used for caching and propagating
 * internal data across the cluster. There exists one MemoryManager per JVM.
  *
  * @param conf
  * @param numCores 并行度，并行的Task共享内存，也就是说每个Task的可用内存是有限制的
  * @param storageMemory 存储用的内存大小
  * @param onHeapExecutionMemory 构造MemoryManager时，传入的是堆上的executionMemory大小(问题：execution memory是否可以是堆外内存？)
  */
private[spark] abstract class MemoryManager(
    conf: SparkConf,
    numCores: Int,
    storageMemory: Long,
    onHeapExecutionMemory: Long) extends Logging {

  // -- Methods related to memory allocation policies and bookkeeping ------------------------------

  @GuardedBy("this")
  protected val storageMemoryPool = new StorageMemoryPool(this)
  @GuardedBy("this")
  protected val onHeapExecutionMemoryPool = new ExecutionMemoryPool(this, "on-heap execution")
  @GuardedBy("this")
  protected val offHeapExecutionMemoryPool = new ExecutionMemoryPool(this, "off-heap execution")

  storageMemoryPool.incrementPoolSize(storageMemory)
  onHeapExecutionMemoryPool.incrementPoolSize(onHeapExecutionMemory)
  offHeapExecutionMemoryPool.incrementPoolSize(conf.getSizeAsBytes("spark.memory.offHeap.size", 0))

  /**
   * Total available memory for storage, in bytes. This amount can vary over time, depending on
   * the MemoryManager implementation.
   * In this model, this is equivalent to the amount of memory not occupied by execution.
   */
  def maxStorageMemory: Long

  /**
   * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
   */
  final def setMemoryStore(store: MemoryStore): Unit = synchronized {
    storageMemoryPool.setMemoryStore(store)
  }

  /**
   *
   * @return
   */

  /***
    * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
    *
    * 尝试将numBytes字节的数据写入缓存，
    * 1. 如果storage memory不够用则从execution memory借用
    * 2. 如果借用过来的内存加上可用的storage memory仍然不能满足，那么尝试删除存在于storage memory上的RDD block
    *
    * @param blockId
    * @param numBytes
    * @return whether all N bytes were successfully granted.
    */
  def acquireStorageMemory(blockId: BlockId, numBytes: Long): Boolean

  /**
   * Acquire N bytes of memory to unroll the given block, evicting existing ones if necessary.
   *
   * This extra method allows subclasses to differentiate behavior between acquiring storage
   * memory and acquiring unroll memory. For instance, the memory management model in Spark
   * 1.5 and before places a limit on the amount of space that can be freed from unrolling.
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireUnrollMemory(blockId: BlockId, numBytes: Long): Boolean

  /**
   * Try to acquire up to `numBytes` of execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   */
  private[memory]
  def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long

  /**
   * Release numBytes of execution memory belonging to the given task.
   *
   *  是否numBytes字节的内存，根据不同的memory mode，调用不同的方法(其实是同一个类的相同方法)
    *
   *  这里只是清除记录task使用内存情况的数据结构，并不是具体的物理内存释放操作
    * @param numBytes
    * @param taskAttemptId 使用内存的ID
    * @param memoryMode
    */
  private[memory]
  def releaseExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Unit = synchronized {
    memoryMode match {
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
    }
  }

  /**
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
   * @return the number of bytes freed.
   */
  private[memory] def releaseAllExecutionMemoryForTask(taskAttemptId: Long): Long = synchronized {
    onHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId) +
      offHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId)
  }

  /**
   * Release N bytes of storage memory.
   * 从StorageMemoryPool中释放内存，如何高效的释放内存，让GC可以很快的回收掉
   */
  def releaseStorageMemory(numBytes: Long): Unit = synchronized {
    storageMemoryPool.releaseMemory(numBytes)
  }

  /**
   * Release all storage memory acquired.
   */
  final def releaseAllStorageMemory(): Unit = synchronized {
    storageMemoryPool.releaseAllMemory()
  }

  /**
   * Release N bytes of unroll memory.
   */
  final def releaseUnrollMemory(numBytes: Long): Unit = synchronized {
    releaseStorageMemory(numBytes)
  }

  /**
   * Execution memory currently in use, in bytes.
   */
  final def executionMemoryUsed: Long = synchronized {
    onHeapExecutionMemoryPool.memoryUsed + offHeapExecutionMemoryPool.memoryUsed
  }

  /**
   * Storage memory currently in use, in bytes.
   */
  final def storageMemoryUsed: Long = synchronized {
    storageMemoryPool.memoryUsed
  }

  /**
   * Returns the execution memory consumption, in bytes, for the given task.
   * on heap和off heap一共使用的内存
   * 问题：一个Task可能同时使用on heap memory和off heap memory
   */
  private[memory] def getExecutionMemoryUsageForTask(taskAttemptId: Long): Long = synchronized {
    onHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId) +
      offHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId)
  }

  // -- Fields related to Tungsten managed memory -------------------------------------------------

  /**
   * Tracks whether Tungsten memory will be allocated on the JVM heap or off-heap using
   * sun.misc.Unsafe.
   *
   * On Heap or  Off Heap
   *
   * 如果是Off Heap，那么需要设置Off Heap的容量
   */
  final val tungstenMemoryMode: MemoryMode = {
    /** *
      * 配置参数spark.memory.offHeap.enabled用于标识TungstenMemory使用on heap模式还是off heap模式，默认是on heap模式
      * 如果开启了off heap模式，那么需要设置spark.memory.offHeap.size用于设置off heap的内存大小
      *
      * 问题：JVM启动时，堆外内存有多少是如何设置的？如果spark.memory.offHeap.size设置的值超过了物理内存，会发生什么情况？
      */
    if (conf.getBoolean("spark.memory.offHeap.enabled", false)) {
      require(conf.getSizeAsBytes("spark.memory.offHeap.size", 0) > 0,
        "spark.memory.offHeap.size must be > 0 when spark.memory.offHeap.enabled == true")
      MemoryMode.OFF_HEAP
    } else {
      MemoryMode.ON_HEAP
    }
  }

  /**
   * The default page size, in bytes.
   *
   * If user didn't explicitly set "spark.buffer.pageSize", we figure out the default value
   * by looking at the number of cores available to the process, and the total amount of memory,
   * and then divide it by a factor of safety.
   *
   * 内存管理，每页的字节数
   */
  val pageSizeBytes: Long = {

    /**
     * 每页最小1M
     */
    val minPageSize = 1L * 1024 * 1024   // 1MB

    /**
     * 每页最大6M
     */
    val maxPageSize = 64L * minPageSize  // 64MB

    /**
     * 内核数，不指定则取全部的Processors
     */
    val cores = if (numCores > 0) numCores else Runtime.getRuntime.availableProcessors()
    // Because of rounding to next power of 2, we may have safetyFactor as 8 in worst case
    val safetyFactor = 16


    /**
     * 如果是ON_HEAP,那么取
     * 如果是OFF_HEAP，那么取
     */
    val maxTungstenMemory: Long = tungstenMemoryMode match {
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.poolSize
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.poolSize
    }
    val size = ByteArrayMethods.nextPowerOf2(maxTungstenMemory / cores / safetyFactor)
    val default = math.min(maxPageSize, math.max(minPageSize, size))
    conf.getSizeAsBytes("spark.buffer.pageSize", default)
  }

  /**
   * Allocates memory for use by Unsafe/Tungsten code.
   */
  private[memory] final val tungstenMemoryAllocator: MemoryAllocator = {
    tungstenMemoryMode match {
      case MemoryMode.ON_HEAP => MemoryAllocator.HEAP
      case MemoryMode.OFF_HEAP => MemoryAllocator.UNSAFE
    }
  }
}
