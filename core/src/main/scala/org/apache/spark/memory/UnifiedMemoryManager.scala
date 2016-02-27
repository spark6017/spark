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
 * A [[MemoryManager]] that enforces a soft boundary between execution and storage such that
 * either side can borrow memory from the other.
 *
 * The region shared between execution and storage is a fraction of (the total heap space - 300MB)
 * configurable through `spark.memory.fraction` (default 0.75). The position of the boundary
 * within this space is further determined by `spark.memory.storageFraction` (default 0.5).
 * This means the size of the storage region is 0.75 * 0.5 = 0.375 of the heap space by default.
 *
 * Storage can borrow as much execution memory as is free until execution reclaims its space.
 * When this happens, cached blocks will be evicted from memory until sufficient borrowed
 * memory is released to satisfy the execution memory request.
 *
 * Similarly, execution can borrow as much storage memory as is free. However, execution
 * memory is *never* evicted by storage due to the complexities involved in implementing this.
 * The implication is that attempts to cache blocks may fail if execution has already eaten
 * up most of the storage space, in which case the new blocks will be evicted immediately
 * according to their respective storage levels.
 *
 * @param storageRegionSize Size of the storage region, in bytes.
 *                          This region is not statically reserved; execution can borrow from
 *                          it if necessary. Cached blocks can be evicted only if actual
 *                          storage memory usage exceeds this region.
 */
private[spark] class UnifiedMemoryManager private[memory] (
    conf: SparkConf,
    val maxMemory: Long,
    storageRegionSize: Long,
    numCores: Int)
  extends MemoryManager(
    conf,
    numCores,
    storageRegionSize,
    maxMemory - storageRegionSize) { /**MemoryManager的onHeapExecutionMemory=maxMemory - storageRegionSize*/

  // We always maintain this invariant:
  assert(onHeapExecutionMemoryPool.poolSize + storageMemoryPool.poolSize == maxMemory)

  /** *
    * 这是一个方法，每次调用都会进行计算
    * 统一内存管理下，最大可用的存储内存是本Executor的最大可用内存-执行内存的使用量
    * 也就是是说，最大存储内存是所有的内存减去execution memory
    * @return
    */
  override def maxStorageMemory: Long = synchronized {
    maxMemory - onHeapExecutionMemoryPool.memoryUsed
  }

  /**
   * Try to acquire up to `numBytes` of execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   */
  override private[memory] def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    assert(onHeapExecutionMemoryPool.poolSize + storageMemoryPool.poolSize == maxMemory)
    assert(numBytes >= 0)
    memoryMode match {
      case MemoryMode.ON_HEAP =>

        /**
         * Grow the execution pool by evicting cached blocks, thereby shrinking the storage pool.
         *
         * When acquiring memory for a task, the execution pool may need to make multiple
         * attempts. Each attempt must be able to evict storage in case another task jumps in
         * and caches a large block between the attempts. This is called once per attempt.
         */
        def maybeGrowExecutionPool(extraMemoryNeeded: Long): Unit = {
          if (extraMemoryNeeded > 0) {
            // There is not enough free memory in the execution pool, so try to reclaim memory from
            // storage. We can reclaim any free memory from the storage pool. If the storage pool
            // has grown to become larger than `storageRegionSize`, we can evict blocks and reclaim
            // the memory that storage has borrowed from execution.
            val memoryReclaimableFromStorage =
              math.max(storageMemoryPool.memoryFree, storageMemoryPool.poolSize - storageRegionSize)
            if (memoryReclaimableFromStorage > 0) {
              // Only reclaim as much space as is necessary and available:
              val spaceReclaimed = storageMemoryPool.shrinkPoolToFreeSpace(
                math.min(extraMemoryNeeded, memoryReclaimableFromStorage))
              onHeapExecutionMemoryPool.incrementPoolSize(spaceReclaimed)
            }
          }
        }

        /**
         * The size the execution pool would have after evicting storage memory.
         *
         * The execution memory pool divides this quantity among the active tasks evenly to cap
         * the execution memory allocation for each task. It is important to keep this greater
         * than the execution pool size, which doesn't take into account potential memory that
         * could be freed by evicting storage. Otherwise we may hit SPARK-12155.
         *
         * Additionally, this quantity should be kept below `maxMemory` to arbitrate fairness
         * in execution memory allocation across tasks, Otherwise, a task may occupy more than
         * its fair share of execution memory, mistakenly thinking that other tasks can acquire
         * the portion of storage memory that cannot be evicted.
         */
        def computeMaxExecutionPoolSize(): Long = {
          maxMemory - math.min(storageMemoryUsed, storageRegionSize)
        }

        onHeapExecutionMemoryPool.acquireMemory(
          numBytes, taskAttemptId, maybeGrowExecutionPool, computeMaxExecutionPoolSize)

      case MemoryMode.OFF_HEAP =>
        // For now, we only support on-heap caching of data, so we do not need to interact with
        // the storage pool when allocating off-heap memory. This will change in the future, though.
        offHeapExecutionMemoryPool.acquireMemory(numBytes, taskAttemptId)
    }
  }

  /** *
    *
    * @param blockId 要放到内存的BlockId
    * @param numBytes 要放到内存的字节数？还是要向StorageMemory借用的内存数？是要存放到storage memory的字节数
    * @return whether all N bytes were successfully granted.
    */
  override def acquireStorageMemory(blockId: BlockId, numBytes: Long): Boolean = synchronized {
    //保证堆上的Shuffle内存容量+RDD内存存储容量=maxMemory
    assert(onHeapExecutionMemoryPool.poolSize + storageMemoryPool.poolSize == maxMemory)
    assert(numBytes >= 0)

    /**
     * 如果要存放的内存大于最大存储内存，那么即使借光执行内存的空间也放不下，因此立即失败，
     * 这是一种判断是否应该立即失败的策略，无需判断其它条件
     * 问题：
     * maxStorageMemory是怎么计算的？
     */
    if (numBytes > maxStorageMemory) {
      // Fail fast if the block simply won't fit
      logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our " +
        s"memory limit ($maxStorageMemory bytes)")
      return false
    }

    /** *
      * 如果需要的内存容量(numBytes)大于当前可用的存储内存量，那么尝试从execution memory借用内存
      * 问题：借用多少？
      */
    if (numBytes > storageMemoryPool.memoryFree) {
      // There is not enough free memory in the storage pool, so try to borrow free memory from
      // the execution pool.
      // memoryBorrowedFromExecution记录了从on heap execution memory借用的内存量
      // memoryBorrowedFromExecution的值是on heap execution memory和numBytes的最小值
      // 如果on heap execution memory小，表示on heap execution memory全部被借完
      // 如果numBytes小，那么从on heap execution memory借用numBytes字节，在这种情况下，storage memory原来剩下可用的可以不用(因为借用的就满足需求了)
      //
      val memoryBorrowedFromExecution = Math.min(onHeapExecutionMemoryPool.memoryFree, numBytes)

      /***
        * onHeapExecutionMemoryPool被借走memoryBorrowedFromExecution字节
        */
      onHeapExecutionMemoryPool.decrementPoolSize(memoryBorrowedFromExecution)

      /***
        * storageMemoryPool借来memoryBorrowedFromExecution字节
        */
      storageMemoryPool.incrementPoolSize(memoryBorrowedFromExecution)
    }


    /***
      * 如果一开始storage memory的可用容量小于numBytes，那么storageMemoryPool此时增加了memoryBorrowedFromExecution字节
      */
    storageMemoryPool.acquireMemory(blockId, numBytes)
  }

  override def acquireUnrollMemory(blockId: BlockId, numBytes: Long): Boolean = synchronized {
    acquireStorageMemory(blockId, numBytes)
  }
}

object UnifiedMemoryManager {

  // Set aside a fixed amount of memory for non-storage, non-execution purposes.
  // This serves a function similar to `spark.memory.fraction`, but guarantees that we reserve
  // sufficient memory for the system even for small heaps. E.g. if we have a 1GB JVM, then
  // the memory used for execution and storage will be (1024 - 300) * 0.75 = 543MB by default.
  private val RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024

  def apply(conf: SparkConf, numCores: Int): UnifiedMemoryManager = {
    val maxMemory = getMaxMemory(conf)
    new UnifiedMemoryManager(
      conf,
      maxMemory = maxMemory,
      storageRegionSize =
        (maxMemory * conf.getDouble("spark.memory.storageFraction", 0.5)).toLong,
      numCores = numCores)
  }

  /**
   * Return the total amount of memory shared between execution and storage, in bytes.
   */
  private def getMaxMemory(conf: SparkConf): Long = {
    val systemMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)
    val reservedMemory = conf.getLong("spark.testing.reservedMemory",
      if (conf.contains("spark.testing")) 0 else RESERVED_SYSTEM_MEMORY_BYTES)
    val minSystemMemory = reservedMemory * 1.5
    if (systemMemory < minSystemMemory) {
      throw new IllegalArgumentException(s"System memory $systemMemory must " +
        s"be at least $minSystemMemory. Please use a larger heap size.")
    }
    val usableMemory = systemMemory - reservedMemory
    val memoryFraction = conf.getDouble("spark.memory.fraction", 0.75)
    (usableMemory * memoryFraction).toLong
  }
}
