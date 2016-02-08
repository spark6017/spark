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

import java.io._
import java.util.Comparator

import scala.collection.BufferedIterator
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.io.ByteStreams

import org.apache.spark.{Logging, SparkEnv, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.serializer.{DeserializationStream, Serializer}
import org.apache.spark.storage.{BlockId, BlockManager}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalAppendOnlyMap.HashComparator

/**
 * :: DeveloperApi ::
 * An append-only map that spills sorted content to disk when there is insufficient space for it
 * to grow.
 *
 * This map takes two passes over the data:
 *
 *   (1) Values are merged into combiners, which are sorted and spilled to disk as necessary
 *   (2) Combiners are read from disk and merged together
 *
 * The setting of the spill threshold faces the following trade-off: If the spill threshold is
 * too high, the in-memory map may occupy more memory than is available, resulting in OOM.
 * However, if the spill threshold is too low, we spill frequently and incur unnecessary disk
 * writes. This may lead to a performance regression compared to the normal case of using the
 * non-spilling AppendOnlyMap.
 *
 * HashMap 是 Spark shuffle read 过程中频繁使用的、用于 aggregate 的数据结构。ExternalAppendOnlyMap用于内存+磁盘
 *
 * 从ExternalAppendOnlyMap的构造函数参数可以看出，ExternalAppendOnlyMap的构造参数跟[[org.apache.spark.rdd.PairRDDFunctions]]的
 * combineByKey的参数非常类似。
 *
 * 从ExternalAppendOnlyMap的构造参数可以大概看得出来，ExternalAppendOnlyMap主要用于聚合操作(比如map side combine)
 *
 */
@DeveloperApi
class ExternalAppendOnlyMap[K, V, C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    serializer: Serializer = SparkEnv.get.serializer,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    context: TaskContext = TaskContext.get())
  extends Iterable[(K, C)]
  with Serializable
  with Logging
  with Spillable[SizeTracker] {

  if (context == null) {
    throw new IllegalStateException(
      "Spillable collections should not be instantiated outside of tasks")
  }

  // Backwards-compatibility constructor for binary compatibility
  def this(
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      serializer: Serializer,
      blockManager: BlockManager) {
    this(createCombiner, mergeValue, mergeCombiners, serializer, blockManager, TaskContext.get())
  }

  override protected[this] def taskMemoryManager: TaskMemoryManager = context.taskMemoryManager()

  /**
   * ExternalAppendOnlyMap通过currentMap持有一个AppendOnlyMap，这个是AppendOnlyMap的子类，SizeTrackingAppendOnlyMap
   */
  private var currentMap = new SizeTrackingAppendOnlyMap[K, C]

  /**
   * DiskMapIterator是干啥用的？每次 spill 完在磁盘上生成一个 spilledMap 文件，然后重新 new 出来一个 AppendOnlyMap
   */
  private val spilledMaps = new ArrayBuffer[DiskMapIterator]
  private val sparkConf = SparkEnv.get.conf

  /**
   * 磁盘BlockManager？
   */
  private val diskBlockManager = blockManager.diskBlockManager

  /**
   * Size of object batches when reading/writing from serializers.
   *
   * Objects are written in batches, with each batch using its own serialization stream. This
   * cuts down on the size of reference-tracking maps constructed when deserializing a stream.
   *
   * NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
   * grow internal data structures by growing + copying every time the number of objects doubles.
   */
  private val serializerBatchSize = sparkConf.getLong("spark.shuffle.spill.batchSize", 10000)

  // Number of bytes spilled in total
  private var _diskBytesSpilled = 0L
  def diskBytesSpilled: Long = _diskBytesSpilled

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  private val fileBufferSize =
    sparkConf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  // Write metrics for current spill
  private var curWriteMetrics: ShuffleWriteMetrics = _

  // Peak size of the in-memory map observed so far, in bytes
  private var _peakMemoryUsedBytes: Long = 0L
  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  /**
   * keyComparator是对Hash值进行比较
   */
  private val keyComparator = new HashComparator[K]
  private val ser = serializer.newInstance()

  /**
   * Number of files this map has spilled so far.
   * Exposed for testing.
   *
   * 当前已经spill的次数
   */
  private[collection] def numSpills: Int = spilledMaps.size

  /**
   * Insert the given key and value into the map.
   *
   * 将K,V插入到该Map中，它是调用insertAll方法，而insertAll方法接受一个Iterator参数，
   * 因此这里将(key,value)单个元素构造为一个Iterator传给insertAll
   */
  def insert(key: K, value: V): Unit = {
    insertAll(Iterator((key, value)))
  }

  /**
   * Insert the given iterator of keys and values into the map.
   *
   * When the underlying map needs to grow, check if the global pool of shuffle memory has
   * enough room for this to happen. If so, allocate the memory required to grow the map;
   * otherwise, spill the in-memory map to disk.
   *
   * The shuffle memory usage of the first trackMemoryThreshold entries is not tracked.
   *
   *
   * 将类型为Iterator的集合插入到当前Map中(以combine的方式)
   */
  def insertAll(entries: Iterator[Product2[K, V]]): Unit = {
    if (currentMap == null) {
      throw new IllegalStateException(
        "Cannot insert new elements into a map after calling iterator")
    }
    // An update function for the map that we reuse across entries to avoid allocating
    // a new closure each time
    var curEntry: Product2[K, V] = null
    val update: (Boolean, C) => C = (hadVal, oldVal) => {
      if (hadVal) mergeValue(oldVal, curEntry._2) else createCombiner(curEntry._2)
    }

    while (entries.hasNext) {
      curEntry = entries.next()
      val estimatedSize = currentMap.estimateSize()
      if (estimatedSize > _peakMemoryUsedBytes) {
        _peakMemoryUsedBytes = estimatedSize
      }

      /**
       * 如果spill到磁盘，那么currentMap可以重建
       */
      if (maybeSpill(currentMap, estimatedSize)) {
        currentMap = new SizeTrackingAppendOnlyMap[K, C]
      }
      currentMap.changeValue(curEntry._1, update)
      addElementsRead()
    }
  }

  /**
   * Insert the given iterable of keys and values into the map.
   *
   * When the underlying map needs to grow, check if the global pool of shuffle memory has
   * enough room for this to happen. If so, allocate the memory required to grow the map;
   * otherwise, spill the in-memory map to disk.
   *
   * The shuffle memory usage of the first trackMemoryThreshold entries is not tracked.
   *
   * 将类型为Iteratable的集合插入到该Map中
   */
  def insertAll(entries: Iterable[Product2[K, V]]): Unit = {
    insertAll(entries.iterator)
  }

  /**
   * Sort the existing contents of the in-memory map and spill them to a temporary file on disk.
   *
   * 将collection集合中的元素spill到磁盘
   */
  override protected[this] def spill(collection: SizeTracker): Unit = {
    val (blockId, file) = diskBlockManager.createTempLocalBlock()
    curWriteMetrics = new ShuffleWriteMetrics()
    var writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, curWriteMetrics)

    /**当前写到磁盘的数据数目，调用flush方法(调用了Writer的commitAndClose方法)后，重置为0*/
    var objectsWritten = 0

    // List of batch sizes (bytes) in the order they are written to disk
    val batchSizes = new ArrayBuffer[Long]

    // Flush the disk writer's contents to disk, and update relevant variables
    def flush(): Unit = {
      val w = writer
      writer = null
      w.commitAndClose()
      _diskBytesSpilled += curWriteMetrics.bytesWritten
      batchSizes.append(curWriteMetrics.bytesWritten)
      objectsWritten = 0
    }

    var success = false
    try {

      /**
       * 调用AppendOnlyMap的destructiveSortedIterator对当前Map中的数据进行排序，然后再写磁盘
       */
      val it = currentMap.destructiveSortedIterator(keyComparator)
      while (it.hasNext) {
        val kv = it.next()
        writer.write(kv._1, kv._2)
        objectsWritten += 1

        /**
         * 如果写的个数是serializerBatchSize，那么就写提交一次
         */
        if (objectsWritten == serializerBatchSize) {
          flush()
          curWriteMetrics = new ShuffleWriteMetrics()
          writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, curWriteMetrics)
        }
      }

      /**
       * 最后还有少于serializerBatchSize个数的objects没有写完，则进行最后一次flush
       */
      if (objectsWritten > 0) {
        flush()
      } else if (writer != null) {
        val w = writer
        writer = null
        w.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (!success) {
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        if (writer != null) {
          writer.revertPartialWritesAndClose()
        }
        if (file.exists()) {
          if (!file.delete()) { /**删除临时文件？*/
            logWarning(s"Error deleting ${file}")
          }
        }
      }
    }

    spilledMaps.append(new DiskMapIterator(file, blockId, batchSizes))
  }

  /**
   * Return a destructive iterator that merges the in-memory map with the spilled maps.
   * If no spill has occurred, simply return the in-memory map's iterator.
   *
   * 因为ExternalAppendOnlyMap实现了[[Iterable]]接口，因此它有iterator方法，转换为Iterator对象
   */
  override def iterator: Iterator[(K, C)] = {
    if (currentMap == null) {
      throw new IllegalStateException(
        "ExternalAppendOnlyMap.iterator is destructive and should only be called once.")
    }

    /**
     * 没有外排序，那么就返回CompletionIterator
     */
    if (spilledMaps.isEmpty) {
      CompletionIterator[(K, C), Iterator[(K, C)]](currentMap.iterator, freeCurrentMap())
    } else {

      /**
       * 如果有外排序，那么就是用ExternalIterator进行内存Map和磁盘Map的归并排序
       */
      new ExternalIterator()
    }
  }

  private def freeCurrentMap(): Unit = {
    currentMap = null // So that the memory can be garbage-collected
    releaseMemory()
  }

  /**
   * An iterator that sort-merges (K, C) pairs from the in-memory map and the spilled maps
   *
   * 内存Map和磁盘Map进行归并排序？
   */
  private class ExternalIterator extends Iterator[(K, C)] {

    // A queue that maintains a buffer for each stream we are currently merging
    // This queue maintains the invariant that it only contains non-empty buffers

    /**
     * 元素是StreamBuffer的优先级队列
     */
    private val mergeHeap = new mutable.PriorityQueue[StreamBuffer]

    // Input streams are derived both from the in-memory map and spilled maps on disk
    // The in-memory map is sorted in place, while the spilled maps are already in sorted order
    /***当前内存Map进行排序后的集合***/
    private val sortedMap = CompletionIterator[(K, C), Iterator[(K, C)]](
      currentMap.destructiveSortedIterator(keyComparator), freeCurrentMap())

      /** sortedMap和spilledMaped集合分别对应着内存和磁盘的集合
        *
        * buffered是Iterator的方法，何解？
        */
    private val inputStreams = (Seq(sortedMap) ++ spilledMaps).map(it => it.buffered)

    /**
     * 针对inputStream中的每个iterator.buffered(可重复读？)
     */
    inputStreams.foreach { it =>
      val kcPairs = new ArrayBuffer[(K, C)]
      readNextHashCode(it, kcPairs)

      /** *
        * 加入到mergeHeap对象中
        */
      if (kcPairs.length > 0) {
        mergeHeap.enqueue(new StreamBuffer(it, kcPairs))
      }
    }

    /**
     * Fill a buffer with the next set of keys with the same hash code from a given iterator. We
     * read streams one hash code at a time to ensure we don't miss elements when they are merged.
     *
     * Assumes the given iterator is in sorted order of hash code.
     *
     * 将Hash值相同的K,C写入到ArrayBuffer中
     *
     * @param it iterator to read from，这是一个排序的集合
     * @param buf buffer to write the results into
     */
    private def readNextHashCode(it: BufferedIterator[(K, C)], buf: ArrayBuffer[(K, C)]): Unit = {
      if (it.hasNext) {
        var kc = it.next()
        buf += kc
        val minHash = hashKey(kc)
        while (it.hasNext && it.head._1.hashCode() == minHash) {
          kc = it.next()
          buf += kc
        }
      }
    }

    /**
     * If the given buffer contains a value for the given key, merge that value into
     * baseCombiner and remove the corresponding (K, C) pair from the buffer.
     */
    private def mergeIfKeyExists(key: K, baseCombiner: C, buffer: StreamBuffer): C = {
      var i = 0
      while (i < buffer.pairs.length) {
        val pair = buffer.pairs(i)
        if (pair._1 == key) {
          // Note that there's at most one pair in the buffer with a given key, since we always
          // merge stuff in a map before spilling, so it's safe to return after the first we find
          removeFromBuffer(buffer.pairs, i)
          return mergeCombiners(baseCombiner, pair._2)
        }
        i += 1
      }
      baseCombiner
    }

    /**
     * Remove the index'th element from an ArrayBuffer in constant time, swapping another element
     * into its place. This is more efficient than the ArrayBuffer.remove method because it does
     * not have to shift all the elements in the array over. It works for our array buffers because
     * we don't care about the order of elements inside, we just want to search them for a key.
     */
    private def removeFromBuffer[T](buffer: ArrayBuffer[T], index: Int): T = {
      val elem = buffer(index)
      buffer(index) = buffer(buffer.size - 1)  // This also works if index == buffer.size - 1
      buffer.reduceToSize(buffer.size - 1)
      elem
    }

    /**
     * Return true if there exists an input stream that still has unvisited pairs.
     */
    override def hasNext: Boolean = mergeHeap.length > 0

    /**
     * Select a key with the minimum hash, then combine all values with the same key from all
     * input streams.
     *
     * 调用next方法，返回minKey和minCombiner
     */
    override def next(): (K, C) = {
      if (mergeHeap.length == 0) {
        throw new NoSuchElementException
      }
      // Select a key from the StreamBuffer that holds the lowest key hash
      val minBuffer = mergeHeap.dequeue()
      val minPairs = minBuffer.pairs
      val minHash = minBuffer.minKeyHash
      val minPair = removeFromBuffer(minPairs, 0)
      val minKey = minPair._1
      var minCombiner = minPair._2
      assert(hashKey(minPair) == minHash)

      // For all other streams that may have this key (i.e. have the same minimum key hash),
      // merge in the corresponding value (if any) from that stream
      val mergedBuffers = ArrayBuffer[StreamBuffer](minBuffer)
      while (mergeHeap.length > 0 && mergeHeap.head.minKeyHash == minHash) {
        val newBuffer = mergeHeap.dequeue()
        minCombiner = mergeIfKeyExists(minKey, minCombiner, newBuffer)
        mergedBuffers += newBuffer
      }

      // Repopulate each visited stream buffer and add it back to the queue if it is non-empty

      //
      mergedBuffers.foreach { buffer =>
        if (buffer.isEmpty) {
          readNextHashCode(buffer.iterator, buffer.pairs)
        }
        if (!buffer.isEmpty) {
          mergeHeap.enqueue(buffer)
        }
      }

      (minKey, minCombiner)
    }

    /**
     * A buffer for streaming from a map iterator (in-memory or on-disk) sorted by key hash.
     * Each buffer maintains all of the key-value pairs with what is currently the lowest hash
     * code among keys in the stream. There may be multiple keys if there are hash collisions.
     * Note that because when we spill data out, we only spill one value for each key, there is
     * at most one element for each key.
     *
     *  在spill前做combine使得每个Key Spill到磁盘时只有一个Value
     *  也就是说，在StreamBuffer中，相同的Key只有一个值
     *
     * StreamBuffers are ordered by the minimum key hash currently available in their stream so
     * that we can put them into a heap and sort that.
     *
     * StreamBuffer可以放到基于堆排序的优先级队列中是因为StreamBuffer是可排序的(实现了Comparable接口)，
     *
     * StreamBuffer中的所有元素的Key的hash值都是一样的，没事每个K对应的Value是不同的
     *
     *
     * StreamBuffer 里面包含的 records 需要具有相同的 hash(key)，
     */
    private class StreamBuffer(
        val iterator: BufferedIterator[(K, C)],
        val pairs: ArrayBuffer[(K, C)])
      extends Comparable[StreamBuffer] {

      def isEmpty: Boolean = pairs.length == 0

      /** *
        * StreamBuffer中的第一个元素的Key哈希值最小
        * @return
        */
      def minKeyHash: Int = {
        assert(pairs.length > 0)
        hashKey(pairs.head)
      }

      /**
       * 比较最小Hash
       * @param other
       * @return
       */
      override def compareTo(other: StreamBuffer): Int = {
        // descending order because mutable.PriorityQueue dequeues the max, not the min
        if (other.minKeyHash < minKeyHash) -1 else if (other.minKeyHash == minKeyHash) 0 else 1
      }
    }
  }

  /**
   * An iterator that returns (K, C) pairs in sorted order from an on-disk map
   *
   * 从磁盘map中获得一个排序的Iterator
   */
  private class DiskMapIterator(file: File, blockId: BlockId, batchSizes: ArrayBuffer[Long])
    extends Iterator[(K, C)]
  {
    private val batchOffsets = batchSizes.scanLeft(0L)(_ + _)  // Size will be batchSize.length + 1
    assert(file.length() == batchOffsets.last,
      "File length is not equal to the last batch offset:\n" +
      s"    file length = ${file.length}\n" +
      s"    last batch offset = ${batchOffsets.last}\n" +
      s"    all batch offsets = ${batchOffsets.mkString(",")}"
    )

    private var batchIndex = 0  // Which batch we're in
    private var fileStream: FileInputStream = null

    // An intermediate stream that reads from exactly one batch
    // This guards against pre-fetching and other arbitrary behavior of higher level streams
    private var deserializeStream = nextBatchStream()
    private var nextItem: (K, C) = null
    private var objectsRead = 0

    /**
     * Construct a stream that reads only from the next batch.
     */
    private def nextBatchStream(): DeserializationStream = {
      // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
      // we're still in a valid batch.
      if (batchIndex < batchOffsets.length - 1) {
        if (deserializeStream != null) {
          deserializeStream.close()
          fileStream.close()
          deserializeStream = null
          fileStream = null
        }

        val start = batchOffsets(batchIndex)
        fileStream = new FileInputStream(file)
        fileStream.getChannel.position(start)
        batchIndex += 1

        val end = batchOffsets(batchIndex)

        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))
        val compressedStream = blockManager.wrapForCompression(blockId, bufferedStream)
        ser.deserializeStream(compressedStream)
      } else {
        // No more batches left
        cleanup()
        null
      }
    }

    /**
     * Return the next (K, C) pair from the deserialization stream.
     *
     * If the current batch is drained, construct a stream for the next batch and read from it.
     * If no more pairs are left, return null.
     */
    private def readNextItem(): (K, C) = {
      try {
        val k = deserializeStream.readKey().asInstanceOf[K]
        val c = deserializeStream.readValue().asInstanceOf[C]
        val item = (k, c)
        objectsRead += 1
        if (objectsRead == serializerBatchSize) {
          objectsRead = 0
          deserializeStream = nextBatchStream()
        }
        item
      } catch {
        case e: EOFException =>
          cleanup()
          null
      }
    }

    override def hasNext: Boolean = {
      if (nextItem == null) {
        if (deserializeStream == null) {
          return false
        }
        nextItem = readNextItem()
      }
      nextItem != null
    }

    override def next(): (K, C) = {
      val item = if (nextItem == null) readNextItem() else nextItem
      if (item == null) {
        throw new NoSuchElementException
      }
      nextItem = null
      item
    }

    private def cleanup() {
      batchIndex = batchOffsets.length  // Prevent reading any other batch
      val ds = deserializeStream
      if (ds != null) {
        ds.close()
        deserializeStream = null
      }
      if (fileStream != null) {
        fileStream.close()
        fileStream = null
      }
      if (file.exists()) {
        if (!file.delete()) {
          logWarning(s"Error deleting ${file}")
        }
      }
    }

    context.addTaskCompletionListener(context => cleanup())
  }

  /**
   * Convenience function to hash the given (K, C) pair by the key.
   * 调用ExternalAppendOnlyMap伴生对象的hash方法求Key的Hash值
   */
  private def hashKey(kc: (K, C)): Int = ExternalAppendOnlyMap.hash(kc._1)
}


/**
 * ExternalAppendOnlyMap类的伴生对象
 */
private[spark] object ExternalAppendOnlyMap {

  /**
   * Return the hash code of the given object. If the object is null, return a special hash code.
   */
  private def hash[T](obj: T): Int = {
    if (obj == null) 0 else obj.hashCode()
  }

  /**
   * A comparator which sorts arbitrary keys based on their hash codes.
   *
   * 基于Hash Code进行排序，它实现了Java的Comparator接口，需要实现compare方法
   */
  private class HashComparator[K] extends Comparator[K] {
    def compare(key1: K, key2: K): Int = {
      val hash1 = hash(key1)
      val hash2 = hash(key2)
      if (hash1 < hash2) -1 else if (hash1 == hash2) 0 else 1
    }
  }
}
