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

package org.apache.spark.storage

import java.nio.ByteBuffer
import java.util.LinkedHashMap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.TaskContext
import org.apache.spark.memory.MemoryManager
import org.apache.spark.util.{SizeEstimator, Utils}
import org.apache.spark.util.collection.SizeTrackingVector

/***
  *
  * @param value 可以是Java对象数组，也可以是ByteBuffer(字节流)
  * @param size 内存使用量
  * @param deserialized 是否反序列化
  */
private case class MemoryEntry(value: Any, size: Long, deserialized: Boolean)

/**
 * Stores blocks in memory, either as Arrays of deserialized Java objects or as
 * serialized ByteBuffers.
  *
  * 在MemoryStore中存放block必须确保内存足够容纳下该block，若内存不足则会尝试将block写到文件中(如果也Block也支持DISK存储级别)
  *
  *
  * 将Block数据存储到从内存中，存储时存储格式可能是Java对象数组，也可能是序列化后的ByteBuffer
  * MemoryStore关联一个BlockManager和一个MemoryManager（MemoryManager）
  *
  *
  * 1. putIterator方法调用unrollSafely进行尝试
  * 2. putBytes方法调用tryToPut进行尝试
 */
private[spark] class MemoryStore(blockManager: BlockManager, memoryManager: MemoryManager)
  extends BlockStore(blockManager) {

  // Note: all changes to memory allocations, notably putting blocks, evicting blocks, and
  // acquiring or releasing unroll memory, must be synchronized on `memoryManager`!
  /***
    * 何为unroll memory？
    */
  private val conf = blockManager.conf

  /***
    * LinkedHashMap保持写入顺序
    *
    * entries这个数据结构记录了写入到本MemoryStore的数据情况
    * 每个MemoryEntry包括数据value（可以是Java对象数组也可以是二进制数据ByteBuffer）、使用的内存量以及存储的数据是原始数据还是经过序列化的数据
    */
  private val entries = new LinkedHashMap[BlockId, MemoryEntry](32, 0.75f, true)

  // A mapping from taskAttemptId to amount of memory used for unrolling a block (in bytes)
  // All accesses of this map are assumed to have manually synchronized on `memoryManager`
  /***
    * unrollMemory指的的hiunroll一个block需要的内存？
    */
  private val unrollMemoryMap = mutable.HashMap[Long, Long]()
  // Same as `unrollMemoryMap`, but for pending unroll memory as defined below.
  // Pending unroll memory refers to the intermediate memory occupied by a task
  // after the unroll but before the actual putting of the block in the cache.
  // This chunk of memory is expected to be released *as soon as* we finish
  // caching the corresponding block as opposed to until after the task finishes.
  // This is only used if a block is successfully unrolled in its entirety in
  // memory (SPARK-4777).
  private val pendingUnrollMemoryMap = mutable.HashMap[Long, Long]()

  // Initial memory to request before unrolling any block
  // 在unroll any block之前分配的unrollmemory
  private val unrollMemoryThreshold: Long =
    conf.getLong("spark.storage.unrollMemoryThreshold", 1024 * 1024)

  /** Total amount of memory available for storage, in bytes. */
  private def maxMemory: Long = memoryManager.maxStorageMemory

  if (maxMemory < unrollMemoryThreshold) {
    logWarning(s"Max memory ${Utils.bytesToString(maxMemory)} is less than the initial memory " +
      s"threshold ${Utils.bytesToString(unrollMemoryThreshold)} needed to store a block in " +
      s"memory. Please configure Spark with more memory.")
  }

  logInfo("MemoryStore started with capacity %s".format(Utils.bytesToString(maxMemory)))

  /**
   * Total storage memory used including unroll memory, in bytes.
    * 问题：
   *  1. memoryManager.storageMemoryUsed记录的是本MemoryStore使用的内存还是所有MemoryStore使用的内存
   *  2.
    *
    * */
  private def memoryUsed: Long = memoryManager.storageMemoryUsed

  /**
   * Amount of storage memory, in bytes, used for caching blocks.
   * This does not include memory used for unrolling.
   * 问题：
   *
   * 1. 从变量的命名(blocks)以及注释中可以看到，似乎指的是所有blocks占用的内存
   *
   */
  private def blocksMemoryUsed: Long = memoryManager.synchronized {
    memoryUsed - currentUnrollMemory
  }

  /** *
    * Block的大小记录在MemoryEntry的size变量中
    * @param blockId
    * @return
    */
  override def getSize(blockId: BlockId): Long = {
    entries.synchronized {
      entries.get(blockId).size
    }
  }

  /***
    * 将BlockId对应数据_bytes写入到磁盘中，因为是put二进制数据，所以，deserialized为false表示这是序列化数据
    * @param blockId
    * @param _bytes
    * @param level
    * @return
    */
  override def putBytes(blockId: BlockId, _bytes: ByteBuffer, level: StorageLevel): PutResult = {
    // Work on a duplicate - since the original input might be used elsewhere.
    val bytes = _bytes.duplicate()
    bytes.rewind()

    /***
      * 如果存储级别指定使用原始数据(未序列化)，那么首先需要进行反序列化，得到原始数据values iterator
      * 调用putIterator写入到MemoryStore中
      */
    if (level.deserialized) {
      val values = blockManager.dataDeserialize(blockId, bytes)
      val result = putIterator(blockId, values, level, returnValues = true)
      result
    }

    /** *
      * 如果存储级别是使用序列化的二进制数据，那么调用tryToPut将二进制数据写入内存
      */
    else {
      tryToPut(blockId, bytes, bytes.limit, deserialized = false)
      val result = PutResult(bytes.limit(), Right(bytes.duplicate()))
      result
    }
  }

  /**
   * Use `size` to test if there is enough space in MemoryStore. If so, create the ByteBuffer and
   * put it into MemoryStore. Otherwise, the ByteBuffer won't be created.
   *
   * The caller should guarantee that `size` is correct.
   */
  def putBytes(blockId: BlockId, size: Long, _bytes: () => ByteBuffer): PutResult = {
    // Work on a duplicate - since the original input might be used elsewhere.
    /***
      * bytes是延迟计算，tryToPut方法为什么使用()=>bytes作为参数，原因是要保持bytes的延迟计算特性，如果直接在tryToPut方法中写bytes，
      * 那么bytes将进行计算
      */
    lazy val bytes = _bytes().duplicate().rewind().asInstanceOf[ByteBuffer]

    //调用tryToPut尝试将数据缓存到内存，如果成功则返回true，否则返回false
    val putSuccess = tryToPut(blockId, () => bytes, size, deserialized = false)
    val data =
      if (putSuccess) {
        assert(bytes.limit == size)
        Right(bytes.duplicate())
      } else {
        null
      }

    /***
      * 如果写入内存失败，那么data为null
      */
    PutResult(size, data)
  }

  override def putArray(
      blockId: BlockId,
      values: Array[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    if (level.deserialized) {
      val sizeEstimate = SizeEstimator.estimate(values.asInstanceOf[AnyRef])
      tryToPut(blockId, values, sizeEstimate, deserialized = true)
      PutResult(sizeEstimate, Left(values.iterator))
    } else {
      val bytes = blockManager.dataSerialize(blockId, values.iterator)
      tryToPut(blockId, bytes, bytes.limit, deserialized = false)
      PutResult(bytes.limit(), Right(bytes.duplicate()))
    }
  }

  override def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    putIterator(blockId, values, level, returnValues, allowPersistToDisk = true)
  }

  /**
   * Attempt to put the given block in memory store.
   *
   * There may not be enough space to fully unroll the iterator in memory, in which case we
   * optionally drop the values to disk if
   *   (1) the block's storage level specifies useDisk, and
   *   (2) `allowPersistToDisk` is true.
   *
   * One scenario in which `allowPersistToDisk` is false is when the BlockManager reads a block
   * back from disk and attempts to cache it in memory. In this case, we should not persist the
   * block back on disk again, as it is already in disk store.
   */
  private[storage] def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean,
      allowPersistToDisk: Boolean): PutResult = {
    val unrolledValues = unrollSafely(blockId, values)
    unrolledValues match {
      case Left(arrayValues) =>
        // Values are fully unrolled in memory, so store them as an array
        val res = putArray(blockId, arrayValues, level, returnValues)
        PutResult(res.size, res.data)
      case Right(iteratorValues) =>
        // Not enough space to unroll this block; drop to disk if applicable
        if (level.useDisk && allowPersistToDisk) {
          logWarning(s"Persisting block $blockId to disk instead.")
          val res = blockManager.diskStore.putIterator(blockId, iteratorValues, level, returnValues)
          PutResult(res.size, res.data)
        } else {
          PutResult(0, Left(iteratorValues))
        }
    }
  }

  /***
    * 返回字节(包装于ByteBuffer中）
    * @param blockId
    * @return
    */
  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    }

    /** *
      * 如果entry中的数据是未序列化的原始数据，那么首先序列化为二进制流
      */
    else if (entry.deserialized) {
      Some(blockManager.dataSerialize(blockId, entry.value.asInstanceOf[Array[Any]].iterator))
    }

    /**
     * 如果entry中的数据已经是序列化的原始数据，那么直接返回
     */
    else {
      Some(entry.value.asInstanceOf[ByteBuffer].duplicate()) // Doesn't actually copy the data
    }
  }

  /***
    * 从内存中读取一个Block，返回一个Iterator
    * @param blockId
    * @return
    */
  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    /** *
      * 缓存的数据存放在entries这个map中，它是BlockId和MemoryEntry的映射
      */
    val entry = entries.synchronized {
      entries.get(blockId)
    }

    /** *
      * 不存在则返回None
      */
    if (entry == null) {
      None
    }

    /** *
      * 如果entry中的数据是未序列化化的，那么直接取出entry.value转换为Array的对象集合
      */
    else if (entry.deserialized) {
      Some(entry.value.asInstanceOf[Array[Any]].iterator)
    }

    /** *
      * 如果entry的数据是序列化的，那么取出entry.value转换为ByteBuffer，然后再进行反序列化
      */
    else {
      val buffer = entry.value.asInstanceOf[ByteBuffer].duplicate() // Doesn't actually copy data
      Some(blockManager.dataDeserialize(blockId, buffer))
    }
  }

  /** *
    * 从MemoryStore中删除一个Block
    * @param blockId the block to remove.
    * @return True if the block was found and removed, False otherwise.
    */
  override def remove(blockId: BlockId): Boolean = memoryManager.synchronized {
    //从entries集合中找到blockId对应的MemoryEntry，并将它删除
    val entry = entries.synchronized { entries.remove(blockId) }

    //如果村在且删除成功，那么释放entry所占用的存储内存
    if (entry != null) {
      memoryManager.releaseStorageMemory(entry.size)
      logDebug(s"Block $blockId of size ${entry.size} dropped " +
        s"from memory (free ${maxMemory - blocksMemoryUsed})")
      true
    } else {
      false
    }
  }

  override def clear(): Unit = memoryManager.synchronized {
    entries.synchronized {
      entries.clear()
    }
    unrollMemoryMap.clear()
    pendingUnrollMemoryMap.clear()
    memoryManager.releaseAllStorageMemory()
    logInfo("MemoryStore cleared")
  }

  /**
   * Unroll the given block in memory safely.
   *
   * The safety of this operation refers to avoiding potential OOM exceptions caused by
   * unrolling the entirety of the block in memory at once. This is achieved by periodically
   * checking whether the memory restrictions for unrolling blocks are still satisfied,
   * stopping immediately if not. This check is a safeguard against the scenario in which
   * there is not enough free memory to accommodate the entirety of a single block.
   *
   * This method returns either an array with the contents of the entire block or an iterator
   * containing the values of the block (if the array would have exceeded available memory).
   */
  def unrollSafely(blockId: BlockId, values: Iterator[Any]): Either[Array[Any], Iterator[Any]] = {

    // Number of elements unrolled so far
    var elementsUnrolled = 0
    // Whether there is still enough memory for us to continue unrolling this block
    var keepUnrolling = true
    // Initial per-task memory to request for unrolling blocks (bytes). Exposed for testing.
    val initialMemoryThreshold = unrollMemoryThreshold
    // How often to check whether we need to request more memory
    val memoryCheckPeriod = 16
    // Memory currently reserved by this task for this particular unrolling operation
    var memoryThreshold = initialMemoryThreshold
    // Memory to request as a multiple of current vector size
    val memoryGrowthFactor = 1.5
    // Previous unroll memory held by this task, for releasing later (only at the very end)
    val previousMemoryReserved = currentUnrollMemoryForThisTask
    // Underlying vector for unrolling the block
    var vector = new SizeTrackingVector[Any]

    // Request enough memory to begin unrolling
    keepUnrolling = reserveUnrollMemoryForThisTask(blockId, initialMemoryThreshold)

    if (!keepUnrolling) {
      logWarning(s"Failed to reserve initial memory threshold of " +
        s"${Utils.bytesToString(initialMemoryThreshold)} for computing block $blockId in memory.")
    }

    // Unroll this block safely, checking whether we have exceeded our threshold periodically
    try {
      while (values.hasNext && keepUnrolling) {
        vector += values.next()
        if (elementsUnrolled % memoryCheckPeriod == 0) {
          // If our vector's size has exceeded the threshold, request more memory
          val currentSize = vector.estimateSize()
          if (currentSize >= memoryThreshold) {
            val amountToRequest = (currentSize * memoryGrowthFactor - memoryThreshold).toLong
            keepUnrolling = reserveUnrollMemoryForThisTask(blockId, amountToRequest)
            // New threshold is currentSize * memoryGrowthFactor
            memoryThreshold += amountToRequest
          }
        }
        elementsUnrolled += 1
      }

      if (keepUnrolling) {
        // We successfully unrolled the entirety of this block
        Left(vector.toArray)
      } else {
        // We ran out of space while unrolling the values for this block
        logUnrollFailureMessage(blockId, vector.estimateSize())
        Right(vector.iterator ++ values)
      }

    } finally {
      // If we return an array, the values returned here will be cached in `tryToPut` later.
      // In this case, we should release the memory only after we cache the block there.
      if (keepUnrolling) {
        val taskAttemptId = currentTaskAttemptId()
        memoryManager.synchronized {
          // Since we continue to hold onto the array until we actually cache it, we cannot
          // release the unroll memory yet. Instead, we transfer it to pending unroll memory
          // so `tryToPut` can further transfer it to normal storage memory later.
          // TODO: we can probably express this without pending unroll memory (SPARK-10907)
          val amountToTransferToPending = currentUnrollMemoryForThisTask - previousMemoryReserved
          unrollMemoryMap(taskAttemptId) -= amountToTransferToPending
          pendingUnrollMemoryMap(taskAttemptId) =
            pendingUnrollMemoryMap.getOrElse(taskAttemptId, 0L) + amountToTransferToPending
        }
      } else {
        // Otherwise, if we return an iterator, we can only release the unroll memory when
        // the task finishes since we don't know when the iterator will be consumed.
      }
    }
  }

  /**
   * Return the RDD ID that a given block ID is from, or None if it is not an RDD block.
    *
    * 根据blockId获取rddId
   */
  private def getRddId(blockId: BlockId): Option[Int] = {
    /**
      * 如果blockId是RDDBlockId,那么返回Some，否则返回None
      */
    val blockRDD = blockId.asRDDId
    blockRDD.map(_.rddId)
  }

  /** *
    * 将value数据写入内存
    * @param blockId
    * @param value
    * @param size
    * @param deserialized
    * @return
    */
  private def tryToPut(
      blockId: BlockId,
      value: Any,
      size: Long,
      deserialized: Boolean): Boolean = {
    tryToPut(blockId, () => value, size, deserialized)
  }

  /**
   * Try to put in a set of values, if we can free up enough space. The value should either be
   * an Array if deserialized is true or a ByteBuffer otherwise. Its (possibly estimated) size
   * must also be passed by the caller.
   *
   * `value` will be lazily created. If it cannot be put into MemoryStore or disk, `value` won't be
   * created to avoid OOM since it may be a big ByteBuffer.
    *
    * value是延迟计算的，这个方法只是简单的返回延迟计算的bytes数据（在不计算数据的情况下检查是否能放下values数据）
   *
   * Synchronize on `memoryManager` to ensure that all the put requests and its associated block
   * dropping is done by only on thread at a time. Otherwise while one thread is dropping
   * blocks to free memory for one block, another thread may use up the freed space for
   * another block.
   *
   * @return whether put was successful.
   */
  private def tryToPut(
      blockId: BlockId,
      value: () => Any,
      size: Long,
      deserialized: Boolean): Boolean = {

    /* TODO: Its possible to optimize the locking by locking entries only when selecting blocks
     * to be dropped. Once the to-be-dropped blocks have been selected, and lock on entries has
     * been released, it must be ensured that those to-be-dropped blocks are not double counted
     * for freeing up more space for another block that needs to be put. Only then the actually
     * dropping of blocks (and writing to disk if necessary) can proceed in parallel. */

    memoryManager.synchronized {
      // Note: if we have previously unrolled this block successfully, then pending unroll
      // memory should be non-zero. This is the amount that we already reserved during the
      // unrolling process. In this case, we can just reuse this space to cache our block.
      // The synchronization on `memoryManager` here guarantees that the release and acquire
      // happen atomically. This relies on the assumption that all memory acquisitions are
      // synchronized on the same lock.
      releasePendingUnrollMemoryForThisTask()

      /***
        * 根据size可能需要从Execution Memory借内存
        */
      val enoughMemory = memoryManager.acquireStorageMemory(blockId, size)

      /***
        * 如果有足够的内存，那么创建MemoryEntry，调用value方法计算数据，然后加到entries集合中进行维护
        */
      if (enoughMemory) {
        // We acquired enough memory for the block, so go ahead and put it
        val entry = new MemoryEntry(value(), size, deserialized)
        entries.synchronized {
          entries.put(blockId, entry)
        }

        /** *
          * 如果数据序列化那么内存中存储的是bytes，如果没有序列化，则使用values表示内存中存储的是对象
          */
        val valuesOrBytes = if (deserialized) "values" else "bytes"
        logInfo("Block %s stored as %s in memory (estimated size %s, free %s)".format(
          blockId, valuesOrBytes, Utils.bytesToString(size), Utils.bytesToString(blocksMemoryUsed)))
      } else {
        // Tell the block manager that we couldn't put it in memory so that it can drop it to
        // disk if the block allows disk storage.
        /***
          * data是延迟计算的,
          * 因为data是延迟计算的，那么data中调用的value也不会立即执行
          */
        lazy val data = if (deserialized) {
          Left(value().asInstanceOf[Array[Any]])
        } else {
          Right(value().asInstanceOf[ByteBuffer].duplicate())
        }

        /***
          * data延时计算，dropFromMemory的()=>data保持了data的延迟加载特性
          */
        blockManager.dropFromMemory(blockId, () => data)
      }
      enoughMemory
    }
  }

  /**
    * Try to evict blocks to free up a given amount of space(由space变量表示) to store a particular block.
    * Can fail if either the block is bigger than our memory or it would require replacing
    * another block from the same RDD (which leads to a wasteful cyclic replacement pattern for
    * RDDs that don't fit into memory that we want to avoid).
    *
    * 擦除操作不会擦除BlockId表示的同一个RDD的partition
    *
    * @param blockId the ID of the block we are freeing space for, if any
    * @param space the size of this block
    * @return the amount of memory (in bytes) freed by eviction
    */
  private[spark] def evictBlocksToFreeSpace(blockId: Option[BlockId], space: Long): Long = {
    assert(space > 0)
    memoryManager.synchronized {
      var freedMemory = 0L
      //获得blockId对应的rddId
      val rddToAdd = blockId.flatMap(getRddId)

      /***
        * 选出可用evict的BlockId
        */
      val selectedBlocks = new ArrayBuffer[BlockId]
      // This is synchronized to ensure that the set of entries is not changed
      // (because of getValue or getBytes) while traversing the iterator, as that
      // can lead to exceptions.
      /***
        * 遍历HashMap集合
        */
      entries.synchronized {
        val iterator = entries.entrySet().iterator()

        /**
          * 循环的条件：
          * 1. 可用内存仍然小于申请值
          * 2. entries还有元素可供擦除
          */
        while (freedMemory < space && iterator.hasNext) {
          val pair = iterator.next()
          val blockId = pair.getKey

          /**
            * rddToAdd.isEmpty表示不是blockId对应的不是RDD？
            * rddToAdd != getRddId(blockId)表示当前遍历的blockId包含的RDD不是要进行申请内存的RDD
            */
          if (rddToAdd.isEmpty || rddToAdd != getRddId(blockId)) {
            //可用内存增加
            selectedBlocks += blockId
            freedMemory += pair.getValue.size
          }
        }
      }

      /***
        * 如果可以申请到足够的空间，那么执行实际的擦除操作
        */
      if (freedMemory >= space) {
        logInfo(s"${selectedBlocks.size} blocks selected for dropping")

        /***
          * 遍历所有要从内存卸载的block
          */
        for (blockId <- selectedBlocks) {

          //读取一个entry
          val entry = entries.synchronized { entries.get(blockId) }
          // This should never be null as only one task should be dropping
          // blocks and removing entries. However the check is still here for
          // future safety.
          if (entry != null) {

            //直接读取数据，可能是Array[Any]也可能是ByteBuffer，注意因为data不是lazy的，因此此处已经给data赋值了
            //因为entry.value放在内存中，因此这里只是把data.value的内存地址赋值给data
            val data = if (entry.deserialized) {
              Left(entry.value.asInstanceOf[Array[Any]])
            } else {
              Right(entry.value.asInstanceOf[ByteBuffer].duplicate())
            }

            /***
              * 调用BlockManager的dropFromMemory将数据从内存中干掉
              * 问题：为什么要调用BlockManager方法，为什么MemoryStore不直接提供drop内存的操作，
              * 因为在drop memory时，需要disk store参与操作，而只有BlockManager同时持有DiskBlock和MemoryBlock
              */
            blockManager.dropFromMemory(blockId, () => data)
          }
        }
        freedMemory
      } else {
        blockId.foreach { id =>
          logInfo(s"Will not store $id as it would require dropping another block " +
            "from the same RDD")
        }
        0L
      }
    }
  }

  /***
    * 检查entries是否包含指定的BlockId
    * @param blockId
    * @return
    */
  override def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.containsKey(blockId) }
  }

  private def currentTaskAttemptId(): Long = {
    // In case this is called on the driver, return an invalid task attempt id.
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)
  }

  /**
   * Reserve memory for unrolling the given block for this task.
   * @return whether the request is granted.
   */
  def reserveUnrollMemoryForThisTask(blockId: BlockId, memory: Long): Boolean = {
    memoryManager.synchronized {
      val success = memoryManager.acquireUnrollMemory(blockId, memory)
      if (success) {
        val taskAttemptId = currentTaskAttemptId()
        unrollMemoryMap(taskAttemptId) = unrollMemoryMap.getOrElse(taskAttemptId, 0L) + memory
      }
      success
    }
  }

  /**
   * Release memory used by this task for unrolling blocks.
   * If the amount is not specified, remove the current task's allocation altogether.
   */
  def releaseUnrollMemoryForThisTask(memory: Long = Long.MaxValue): Unit = {
    val taskAttemptId = currentTaskAttemptId()
    memoryManager.synchronized {
      if (unrollMemoryMap.contains(taskAttemptId)) {
        val memoryToRelease = math.min(memory, unrollMemoryMap(taskAttemptId))
        if (memoryToRelease > 0) {
          unrollMemoryMap(taskAttemptId) -= memoryToRelease
          if (unrollMemoryMap(taskAttemptId) == 0) {
            unrollMemoryMap.remove(taskAttemptId)
          }
          memoryManager.releaseUnrollMemory(memoryToRelease)
        }
      }
    }
  }

  /**
   * Release pending unroll memory of current unroll successful block used by this task
   */
  def releasePendingUnrollMemoryForThisTask(memory: Long = Long.MaxValue): Unit = {
    val taskAttemptId = currentTaskAttemptId()
    memoryManager.synchronized {
      if (pendingUnrollMemoryMap.contains(taskAttemptId)) {
        val memoryToRelease = math.min(memory, pendingUnrollMemoryMap(taskAttemptId))
        if (memoryToRelease > 0) {
          pendingUnrollMemoryMap(taskAttemptId) -= memoryToRelease
          if (pendingUnrollMemoryMap(taskAttemptId) == 0) {
            pendingUnrollMemoryMap.remove(taskAttemptId)
          }
          memoryManager.releaseUnrollMemory(memoryToRelease)
        }
      }
    }
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks across all tasks.
   */
  def currentUnrollMemory: Long = memoryManager.synchronized {
    unrollMemoryMap.values.sum + pendingUnrollMemoryMap.values.sum
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks by this task.
   */
  def currentUnrollMemoryForThisTask: Long = memoryManager.synchronized {
    unrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L)
  }

  /**
   * Return the number of tasks currently unrolling blocks.
   */
  private def numTasksUnrolling: Int = memoryManager.synchronized { unrollMemoryMap.keys.size }

  /**
   * Log information about current memory usage.
   */
  private def logMemoryUsage(): Unit = {
    logInfo(
      s"Memory use = ${Utils.bytesToString(blocksMemoryUsed)} (blocks) + " +
      s"${Utils.bytesToString(currentUnrollMemory)} (scratch space shared across " +
      s"$numTasksUnrolling tasks(s)) = ${Utils.bytesToString(memoryUsed)}. " +
      s"Storage limit = ${Utils.bytesToString(maxMemory)}."
    )
  }

  /**
   * Log a warning for failing to unroll a block.
   *
   * @param blockId ID of the block we are trying to unroll.
   * @param finalVectorSize Final size of the vector before unrolling failed.
   */
  private def logUnrollFailureMessage(blockId: BlockId, finalVectorSize: Long): Unit = {
    logWarning(
      s"Not enough space to cache $blockId in memory! " +
      s"(computed ${Utils.bytesToString(finalVectorSize)} so far)"
    )
    logMemoryUsage()
  }
}
