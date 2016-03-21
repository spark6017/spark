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
 *
 *  第一点： 统一内存管理器的含义和动机
 *  统一内存管理器抹掉了StaticMemoryManager的execution和storage memory明确的边界
 * A [[MemoryManager]] that enforces a soft boundary between execution and storage such that
 * either side can borrow memory from the other.
 *
 * 第二点：统一内存的可用内存量
 * execution和storage总共的可用内存是总内存-300MB；然后剩下的内存乘以75%
 *
 * The region shared between execution and storage is a fraction of (the total heap space - 300MB)
 * configurable through `spark.memory.fraction` (default 0.75). The position of the boundary
 * within this space is further determined by `spark.memory.storageFraction` (default 0.5).
 * This means the size of the storage region is 0.75 * 0.5 = 0.375 of the heap space by default.
 *
 *  第三点：execution memory和storage memory交互之execution借用storage内存的策略
 *
 *  Storage可以借用execution所有空闲的内存。当execution收回它借用的内存时，storage的空闲内存不足以将借用的内存归还，
 *  那么Storage会进行内存evict直到execution请求的内存得到满足。
 *  问题：如果Storage借用了500M，而Execution向Storage请求800M(storage必须归还向execution借用的500M内存，在内存不够的情况下是否优先满足execution？)
 *
 * Storage can borrow as much execution memory as is free until execution reclaims its space.
 * When this happens, cached blocks will be evicted from memory until sufficient borrowed
 * memory is released to satisfy the execution memory request.
 *
 *
 * 第四点：execution memory和storage memory交互之execution借用storage内存的策略
 * execution可以向storage借用所有空闲的内存，但是storage要求execution归还的内存而execution的内存吃紧时，execution不会evict 它的内存
 * 也就是说，当execution借用了storage大部分内存时，storage用于缓存的功能会受到很大影响(问题：storage 会进行老的cached block擦除吗？)
 *
 * Similarly, execution can borrow as much storage memory as is free. However, execution
 * memory is *never* evicted by storage due to the complexities involved in implementing this.
 * The implication is that attempts to cache blocks may fail if execution has already eaten
 * up most of the storage space, in which case the new blocks will be evicted immediately
 * according to their respective storage levels.
 *
 *
 * @param conf
 * @param maxMemory
 * @param storageRegionSize Size of the storage region, in bytes.
 *                          This region is not statically reserved; execution can borrow from
 *                          it if necessary. Cached blocks can be evicted only if actual
 *                          storage memory usage exceeds this region.
  * @param numCores 并行度，并行的Task共享内存，也就是说每个Task的可用内存是有限制的
  */
private[spark] class UnifiedMemoryManager private[memory] (
    conf: SparkConf,
    val maxMemory: Long,
    storageRegionSize: Long,
    numCores: Int)
  extends MemoryManager(
    conf,
    numCores,
    storageRegionSize, /**storage memory的size，Region是什么概念**/
    maxMemory - storageRegionSize) { /**MemoryManager的onHeapExecutionMemory=maxMemory - storageRegionSize*/

  // We always maintain this invariant:
  assert(onHeapExecutionMemoryPool.poolSize + storageMemoryPool.poolSize == maxMemory)

  /** *
    * 这是一个方法，每次调用都会进行计算
    * 统一内存管理下，最大可用的存储内存是本Executor的最大可用内存-执行内存的使用量
    * 也就是是说，最大存储内存是所有的内存减去execution memory。
    * 问题：为什么可以计算max storage memory而不可以计算max execution memory？原因是只要execution的内存使用了就不能给storage使用
    * 而execution memory则不然，它可以向storage借用(storage不足，那么可以进行evict。从理论上说，execution memory的最大值就是maxMemory)
    * @return
    */
  override def maxStorageMemory: Long = synchronized {
    maxMemory - onHeapExecutionMemoryPool.memoryUsed
  }

  /**
   * 【尝试】向execution 申请numBytes字节的memory，申请execution memory需要有一个taskAttemptId表示申请execution memory的Task
   * 注意：与acquireStorageMemory返回boolean(要么申请成功或者申请失败，这是All or Nothing的语义)不同，
   * acquireExecutionMemory返回的是long，表示实际申请的字节数；返回值在0到numBytes之间
   *
   *
   * Try to acquire up to `numBytes` of execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
   *
   *  每个Task都有机会申请到1/(2*N)*maxPoolSize的内存
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.

    * @param numBytes
    * @param taskAttemptId
    * @param memoryMode 用于区分是on heap execution memory还是off heap execution memory
    * @return
    */
  override private[memory] def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Long = synchronized {
    assert(onHeapExecutionMemoryPool.poolSize + storageMemoryPool.poolSize == maxMemory)
    assert(numBytes >= 0)

    /** *
      * 根据不同的memory mode进行处理（分ON_HEAP和OFF_HEAP）
      */
    memoryMode match {
      case MemoryMode.ON_HEAP =>

        /**
         *  Execution Memory Pool进行扩容，extraMemoryNeeded就是Execution Memory Pool进行扩容的内存数量
         *  发生在execution memory不够用的情况下(比如当前剩余100K，而task需要200K，那么就需要进行executor memory的扩容)
         *
         *  所谓的库容就是向storage memory借内存
         *
         * Grow the execution pool by evicting cached blocks, thereby shrinking the storage pool.
         *
         * When acquiring memory for a task, the execution pool may need to make multiple
         * attempts. Each attempt must be able to evict storage in case another task jumps in
         * and caches a large block between the attempts. This is called once per attempt.
         *
         *
         */
        def maybeGrowExecutionPool(extraMemoryNeeded: Long): Unit = {
          if (extraMemoryNeeded > 0) {
            // There is not enough free memory in the execution pool, so try to reclaim memory from
            // storage. We can reclaim any free memory from the storage pool. If the storage pool
            // has grown to become larger than `storageRegionSize`, we can evict blocks and reclaim
            // the memory that storage has borrowed from execution.


            /** *
              * 从storage可以借的内存是storage memory的可用内存以及storage从execution借走内存的最大值
              * 假如当前free storage memory是100M，而借走的execution memory是200M，那么storage memory需要evict以将借走的execution memory归还
              *
              */
            val memoryReclaimableFromStorage =
              math.max(storageMemoryPool.memoryFree, storageMemoryPool.poolSize - storageRegionSize)


            /** *
              * 如果可以从storage借内存，实际借走的内存是理论上可借的内存memoryReclaimableFromStorage与实际需要的内存(extraMemoryNeeded)之间的较小者
              */
            if (memoryReclaimableFromStorage > 0) {
              // Only reclaim as much space as is necessary and available:
              val spaceReclaimed = storageMemoryPool.shrinkPoolToFreeSpace(
                math.min(extraMemoryNeeded, memoryReclaimableFromStorage))

              /** *
                * on heap execution memory增加容量，spaceReclaimed是从storage memory借来的
                */
              onHeapExecutionMemoryPool.incrementPoolSize(spaceReclaimed)
            }
          }
        }

        /**
         * The size the execution pool would have after evicting storage memory.
         *
         *  execution从storage借走内存后，execution pool size的最大值
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
         *
         * 最大execution pool size是maxMemory减去使用的storage memory和storageRegionSize之间的较小者
         *
         * storageMemoryUsed为200M，而storageRegionSize是100M，那么将storage借用的100M返还
         * storageMemoryUsed为50M，而storageRegionSize是200M，那么可以从storage memory借用150M
         */
        def computeMaxExecutionPoolSize(): Long = {

          maxMemory - math.min(storageMemoryUsed, storageRegionSize)
        }

        /** *
          * 调用on heap execution memory pool的acquireMemory方法
          */
        onHeapExecutionMemoryPool.acquireMemory(
          numBytes, taskAttemptId, maybeGrowExecutionPool, computeMaxExecutionPoolSize)

      /** *
        * 如果是OFF_HEAP模式，那么调用off heap execution memory pool的acquireMemory方法
        */
      case MemoryMode.OFF_HEAP =>
        // For now, we only support on-heap caching of data, so we do not need to interact with
        // the storage pool when allocating off-heap memory. This will change in the future, though.
        /** *
          * 因为目前支持在堆上缓存数据，因此在off heap模式下不需要与storage pool打交道
          */
        offHeapExecutionMemoryPool.acquireMemory(numBytes, taskAttemptId)
    }
  }

  /** *
    * 向storage memory申请内存
    * 如果free storage memory + free execution memory < numBytes，那么storage memory需要evict
    *
    * @param blockId 要放到内存的BlockId
    * @param numBytes 要存放到storage memory的字节数
    * @return whether all N bytes were successfully granted.
    */
  override def acquireStorageMemory(blockId: BlockId, numBytes: Long): Boolean = synchronized {

    /** *
      * 时刻保证on heap execution memory  pool size + storage memory pool size = max memory
      */
    assert(onHeapExecutionMemoryPool.poolSize + storageMemoryPool.poolSize == maxMemory)
    assert(numBytes >= 0)

    /**
     * 如果要存放的字节数大于storage memory最大可用值(最大可用值包括了storage可以向execution可用的内存)，因此立即失败，
     * 这是一种判断是否应该立即失败的策略，无需判断其它条件
     * 问题：
     * maxStorageMemory是怎么计算的？答：最大内存 - execution使用的内存
     */
    if (numBytes > maxStorageMemory) {
      // Fail fast if the block simply won't fit
      logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our " +
        s"memory limit ($maxStorageMemory bytes)")
      return false
    }

    /** *
      * 判断条件1：
      * 如果需要的内存量(numBytes)大于storage当前可用的存储内存量，那么从execution memory借用内存，
      * 问题1：
      * 借用多少？ Math.min(onHeapExecutionMemoryPool.memoryFree, numBytes),意思是说，如果numBytes < onHeapExecutionMemoryPool.memoryFree,那么
      * 将向execution借用numByte字节(此时storage 的free memory继续空闲)
      * 比如，free storage memory为100M，free execution memory为1000M，numBytes为300M，那么向execution借用300M，storage为block分配300M后，
      * 仍然保留100M空闲空间
      *
      *
      * 问题：借用多少？
      */
    if (numBytes > storageMemoryPool.memoryFree) {
      // There is not enough free memory in the storage pool, so try to borrow free memory from
      // the execution pool.
      // memoryBorrowedFromExecution记录了要从on heap execution memory借用的内存量
      // memoryBorrowedFromExecution的值是on heap execution memory和numBytes的最小值
      // 如果on heap execution memory小，表示on heap execution memory全部被借完
      // 如果numBytes小，那么从on heap execution memory借用numBytes字节，在这种情况下，storage memory原来剩下可用的继续空闲(因为借用的就满足需求了)
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
      *
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
    *
    * 如果系统内存1G，那么预留300M，剩余724M，再取0.75 = 724M*0.75=543M
    *
    * Runtime.getRuntime.maxMemory表示executor这个JVM进程的内存容量，因为预留+取75%的原因，如果给executor分配512M，那么实际可用的是(512-300)*75%=109M
    * 因此，在统一内存管理模式下，分配给executor的内存不能太小
    *
    *
    * 问题：如果指定--execeutor-memory 1g,那么executor进程内部调用Runtime.getRuntime.maxMemory是否得到1024？
    *
    * @param conf
    * @return storage memory 和 execution memory可用的最大内存
    */
  private def getMaxMemory(conf: SparkConf): Long = {

    //系统可用内存
    val systemMemory = conf.getLong("spark.testing.memory", Runtime.getRuntime.maxMemory)

    //预留内存给Spark应用其它代码使用(除了storage和execution memory之外)，300M
    val reservedMemory = conf.getLong("spark.testing.reservedMemory",
      if (conf.contains("spark.testing")) 0 else RESERVED_SYSTEM_MEMORY_BYTES)

    //操作系统的最小内存，300M*1.5=450M，这里有点坑，450M明显不够
    val minSystemMemory = reservedMemory * 1.5

    //如果操作系统内存不够450M，那么报错
    if (systemMemory < minSystemMemory) {
      throw new IllegalArgumentException(s"System memory $systemMemory must " +
        s"be at least $minSystemMemory. Please use a larger heap size.")
    }

    //可用内存是系统内存-保留内存
    val usableMemory = systemMemory - reservedMemory

    //取75%给storage memory 和 execution memory使用
    val memoryFraction = conf.getDouble("spark.memory.fraction", 0.75)
    (usableMemory * memoryFraction).toLong
  }

  /***
    * 如果指定了512M，那么预留300M，得到usableMemory是512-300M=212M,
    * 而可用的内存是0.75%，那么212*0.75%=53*3=159M
    * 问题：为什么UI上显示的是143.6M？
    *
    * 如果指定了512M，那么预留300M，得到usableMemory是1024-300M=724M,
    * 而可用的内存是0.75%，那么724*0.75%=181*3=543M
    * 问题：为什么UI上显示的是511.5M？
    *
    */

}
