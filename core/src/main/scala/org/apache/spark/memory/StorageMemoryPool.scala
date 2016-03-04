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

import org.apache.spark.Logging
import org.apache.spark.storage.{BlockId, MemoryStore}

/**
 * Performs bookkeeping for managing an adjustable-size pool of memory that is used for storage
 * (caching).
 *
 * @param lock a [[MemoryManager]] instance to synchronize on
 */
private[memory] class StorageMemoryPool(lock: Object) extends MemoryPool(lock) with Logging {

  @GuardedBy("lock")
  private[this] var _memoryUsed: Long = 0L

  override def memoryUsed: Long = lock.synchronized {
    _memoryUsed
  }

  private var _memoryStore: MemoryStore = _

  def memoryStore: MemoryStore = {
    if (_memoryStore == null) {
      throw new IllegalStateException("memory store not initialized yet")
    }
    _memoryStore
  }

  /**
   * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
   */
  final def setMemoryStore(store: MemoryStore): Unit = {
    _memoryStore = store
  }

  /***
    * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
    *
    *  对于static memory manager而言，是申请storage memory
    *
    * @param blockId
    * @param numBytes
    * @return
    */
  def acquireMemory(blockId: BlockId, numBytes: Long): Boolean = lock.synchronized {

    /***
      * 需要释放内存的字节数，如果numBytes - memoryFree > 0表示申请的内存大于剩余的内存，因此需要进行storage memory的evict
      */
    val numBytesToFree = math.max(0, numBytes - memoryFree)

    /***
      * 需要memory storage释放numBytesToFree字节
      */
    acquireMemory(blockId, numBytes, numBytesToFree)
  }

  /**
   * Acquire N bytes of storage memory for the given block, evicting existing ones if necessary.
   *
   * 申请内存，如果可用内存不够分配(numBytesToFree > 0)，那么需要进行evict memory。如果释放内存还不足以放下，将发生OOM？
   *
   * @param blockId the ID of the block we are acquiring storage memory for 申请内存的blockId，比如RDD、Broadcast
   * @param numBytesToAcquire the size of this block 申请的内存数
   * @param numBytesToFree the amount of space to be freed through evicting blocks(需要storage memory释放numBytesToFree字节的内存，如果是0表示不用释放)
   * @return whether all N bytes were successfully granted.
   */
  def acquireMemory(
      blockId: BlockId,
      numBytesToAcquire: Long,
      numBytesToFree: Long): Boolean = lock.synchronized {
    assert(numBytesToAcquire >= 0)
    assert(numBytesToFree >= 0)
    assert(memoryUsed <= poolSize)

    /***
      * 如果需要释放的内存字节数大于0，那么调用memoryStore.evictBlocksToFreeSpace执行实际的擦除操作,擦除操作执行后会更新_memoryUsed变量
      */
    if (numBytesToFree > 0) {
      /** *
        * 调用memoryStorage的evictBlocksToFreeSpace方法
        * 问题：memoryStore是BlockManager管理的内存存储空间，为什么StorageMemoryPool也会持有一个memoryStorage
        */
      memoryStore.evictBlocksToFreeSpace(Some(blockId), numBytesToFree)
    }
    // NOTE: If the memory store evicts blocks, then those evictions will synchronously call
    // back into this StorageMemoryPool in order to free memory. Therefore, these variables
    // should have been updated.
      /**
       *  如果此时的可用内存(memoryFree)大于等于numBytesToAcquire，表示可以为blockId分配内存空间，因此返回true，否则返回false
       */
    val enoughMemory = numBytesToAcquire <= memoryFree

    //如果内存足够，表示要将numBytesToAcquire写入内存，更新_memoryUsed变量
    if (enoughMemory) {
      _memoryUsed += numBytesToAcquire
    }

    //返回内存申请是否成功
    enoughMemory
  }

  /** *
    * 只是修改了_memoryUsed的值
    * @param size
    */
  def releaseMemory(size: Long): Unit = lock.synchronized {
    if (size > _memoryUsed) {
      logWarning(s"Attempted to release $size bytes of storage " +
        s"memory when we only have ${_memoryUsed} bytes")
      _memoryUsed = 0
    } else {
      _memoryUsed -= size
    }
  }

  def releaseAllMemory(): Unit = lock.synchronized {
    _memoryUsed = 0
  }

  /**
   * Try to shrink the size of this storage memory pool by `spaceToFree` bytes. Return the number
   * of bytes removed from the pool's capacity.
   */
  def shrinkPoolToFreeSpace(spaceToFree: Long): Long = lock.synchronized {
    // First, shrink the pool by reclaiming free memory:
    val spaceFreedByReleasingUnusedMemory = math.min(spaceToFree, memoryFree)
    decrementPoolSize(spaceFreedByReleasingUnusedMemory)
    val remainingSpaceToFree = spaceToFree - spaceFreedByReleasingUnusedMemory
    if (remainingSpaceToFree > 0) {
      // If reclaiming free memory did not adequately shrink the pool, begin evicting blocks:
      val spaceFreedByEviction = memoryStore.evictBlocksToFreeSpace(None, remainingSpaceToFree)
      // When a block is released, BlockManager.dropFromMemory() calls releaseMemory(), so we do
      // not need to decrement _memoryUsed here. However, we do need to decrement the pool size.
      decrementPoolSize(spaceFreedByEviction)
      spaceFreedByReleasingUnusedMemory + spaceFreedByEviction
    } else {
      spaceFreedByReleasingUnusedMemory
    }
  }
}
