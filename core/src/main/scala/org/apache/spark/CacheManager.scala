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

package org.apache.spark

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.storage._

/**
 * Spark class responsible for passing RDDs partition contents to the BlockManager and making
 * sure a node doesn't load two copies of an RDD at once.
  *
  * 把一个RDD的分区保存到CacheManager中
  *
 */
private[spark] class CacheManager(blockManager: BlockManager) extends Logging {

  /** Keys of RDD partitions that are being computed/loaded. */
  private val loading = new mutable.HashSet[RDDBlockId]

  /** Gets or computes an RDD partition. Used by RDD.iterator() when an RDD is cached.
    *
    *
    *
    * */
  def getOrCompute[T](
      rdd: RDD[T],
      partition: Partition,
      context: TaskContext,
      storageLevel: StorageLevel): Iterator[T] = {

    /**
      * RDD的一个partition对应的BlockId,包括rdd的id和partition的id
      */
    val key = RDDBlockId(rdd.id, partition.index)
    logDebug(s"Looking for partition $key")

    /***
      * 从BlockManager中获取
      */
    blockManager.get(key) match {

      /***
        * 如果读到数据，则放到blockResult中，blockResult的类型是BlockResult
        */
      case Some(blockResult) =>
        // Partition is already materialized, so just return its values
        val existingMetrics = context.taskMetrics().registerInputMetrics(blockResult.readMethod)
        existingMetrics.incBytesRead(blockResult.bytes)

        /***
          * BlockResult的data是一个Iterator
          */
        val iter = blockResult.data.asInstanceOf[Iterator[T]]

        /**
          * 这里的delegate就是InterruptibleIterator构造参数的第二个iter
          */
        new InterruptibleIterator[T](context, iter) {
          override def next(): T = {
            existingMetrics.incRecordsRead(1)
            delegate.next()
          }
        }

      /***
        * 如果BlockManager中没有数据，那么进行计算再缓存
        */
      case None =>
        // Acquire a lock for loading this partition
        // If another thread already holds the lock, wait for it to finish return its results
        /**
          * 等待其它线程计算+缓存完，直接其它线程缓存的结果
          *
          * 如果返回值没有定义，那么会做加锁操作(对loading对象进行加锁)
          */
        val storedValues = acquireLockForPartition[T](key)
        if (storedValues.isDefined) {
          return new InterruptibleIterator[T](context, storedValues.get)
        }

        // Otherwise, we have to load the partition ourselves
        try {
          logInfo(s"Partition $key not found, computing it")

          /***
            * 调用RDD的computeOrReadCheckpoint计算一个结果，此处的逻辑是：
            * 如果在checkpoint之前就把数据cache了，则不会再读取checkpoint；如果在cache之前把checkpoint了，那么cache直接读取checkpoint然后放到cache中
            *
            */
          val computedValues = rdd.computeOrReadCheckpoint(partition, context)
          /***
            * 将数据写入缓存(BlockManager管理）
            */
          val cachedValues = putInBlockManager(key, computedValues, storageLevel)

          /**
            * 返回缓存的数据
            */
          new InterruptibleIterator(context, cachedValues)
        } finally {
          loading.synchronized {
            loading.remove(key)
            loading.notifyAll()
          }
        }
    }
  }

  /**
   * Acquire a loading lock for the partition identified by the given block ID.
   *
   * If the lock is free, just acquire it and return None. Otherwise, another thread is already
   * loading the partition, so we wait for it to finish and return the values loaded by the thread.
   */
  private def acquireLockForPartition[T](id: RDDBlockId): Option[Iterator[T]] = {
    loading.synchronized {
      if (!loading.contains(id)) {
        // If the partition is free, acquire its lock to compute its value
        loading.add(id)
        None
      } else {
        // Otherwise, wait for another thread to finish and return its result
        logInfo(s"Another thread is loading $id, waiting for it to finish...")
        while (loading.contains(id)) {
          try {
            loading.wait()
          } catch {
            case e: Exception =>
              logWarning(s"Exception while waiting for another thread to load $id", e)
          }
        }
        logInfo(s"Finished waiting for $id")
        val values = blockManager.get(id)
        if (!values.isDefined) {
          /* The block is not guaranteed to exist even after the other thread has finished.
           * For instance, the block could be evicted after it was put, but before our get.
           * In this case, we still need to load the partition ourselves. */
          logInfo(s"Whoever was loading $id failed; we'll try it ourselves")
          loading.add(id)
        }
        values.map(_.data.asInstanceOf[Iterator[T]])
      }
    }
  }

  /**
   * Cache the values of a partition, keeping track of any updates in the storage statuses of
   * other blocks along the way.
   *
   * The effective storage level refers to the level that actually specifies BlockManager put
   * behavior, not the level originally specified by the user. This is mainly for forcing a
   * MEMORY_AND_DISK partition to disk if there is not enough room to unroll the partition,
   * while preserving the the original semantics of the RDD as specified by the application.
   */
  private def putInBlockManager[T](
      key: BlockId,
      values: Iterator[T],
      level: StorageLevel,
      effectiveStorageLevel: Option[StorageLevel] = None): Iterator[T] = {

    val putLevel = effectiveStorageLevel.getOrElse(level)
    if (!putLevel.useMemory) {
      /*
       * This RDD is not to be cached in memory, so we can just pass the computed values as an
       * iterator directly to the BlockManager rather than first fully unrolling it in memory.
       */
      blockManager.putIterator(key, values, level, tellMaster = true, effectiveStorageLevel)
      blockManager.get(key) match {
        case Some(v) => v.data.asInstanceOf[Iterator[T]]
        case None =>
          logInfo(s"Failure to store $key")
          throw new BlockException(key, s"Block manager failed to return cached value for $key!")
      }
    } else {
      /*
       * This RDD is to be cached in memory. In this case we cannot pass the computed values
       * to the BlockManager as an iterator and expect to read it back later. This is because
       * we may end up dropping a partition from memory store before getting it back.
       *
       * In addition, we must be careful to not unroll the entire partition in memory at once.
       * Otherwise, we may cause an OOM exception if the JVM does not have enough space for this
       * single partition. Instead, we unroll the values cautiously, potentially aborting and
       * dropping the partition to disk if applicable.
       */
      blockManager.memoryStore.unrollSafely(key, values) match {
        case Left(arr) =>
          // We have successfully unrolled the entire partition, so cache it in memory
          blockManager.putArray(key, arr, level, tellMaster = true, effectiveStorageLevel)
          arr.iterator.asInstanceOf[Iterator[T]]
        case Right(it) =>
          // There is not enough space to cache this partition in memory
          val returnValues = it.asInstanceOf[Iterator[T]]
          if (putLevel.useDisk) {
            logWarning(s"Persisting partition $key to disk instead.")
            val diskOnlyLevel = StorageLevel(useDisk = true, useMemory = false,
              useOffHeap = false, deserialized = false, putLevel.replication)
            putInBlockManager[T](key, returnValues, level, Some(diskOnlyLevel))
          } else {
            returnValues
          }
      }
    }
  }

}
