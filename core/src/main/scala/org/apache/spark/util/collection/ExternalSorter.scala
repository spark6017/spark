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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.io.ByteStreams

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.serializer._
import org.apache.spark.storage.{BlockId, DiskBlockObjectWriter}

/**
 * Sorts and potentially merges a number of key-value pairs of type (K, V) to produce key-combiner
 * pairs of type (K, C). Uses a Partitioner to first group the keys into partitions, and then
 * optionally sorts keys within each partition using a custom Comparator. Can output a single
 * partitioned file with a different byte range for each partition, suitable for shuffle fetches.
 *
 * If combining is disabled, the type C must equal V -- we'll cast the objects at the end.
 *
 * Note: Although ExternalSorter is a fairly generic sorter, some of its configuration is tied
 * to its use in sort-based shuffle (for example, its block compression is controlled by
 * `spark.shuffle.compress`).  We may need to revisit this if ExternalSorter is used in other
 * non-shuffle contexts where we might want to use different configuration settings.
 *
 * @param aggregator optional Aggregator with combine functions to use for merging data
 * @param partitioner optional Partitioner; if given, sort by partition ID and then key
 * @param ordering optional Ordering to sort keys within each partition; should be a total ordering
 * @param serializer serializer to use when spilling to disk
 *
 * Note that if an Ordering is given, we'll always sort using it, so only provide it if you really
 * want the output keys to be sorted. In a map task without map-side combine for example, you
 * probably want to pass None as the ordering to avoid extra sorting. On the other hand, if you do
 * want to do combining, having an Ordering is more efficient than not having it.
 *
 * Users interact with this class in the following way:
 *
 * 1. Instantiate an ExternalSorter.
 *
 * 2. Call insertAll() with a set of records.
 *
 * 3. Request an iterator() back to traverse sorted/aggregated records.
 *     - or -
 *    Invoke writePartitionedFile() to create a file containing sorted/aggregated outputs
 *    that can be used in Spark's sort shuffle.
 *
 * At a high level, this class works internally as follows:
 *
 *  - We repeatedly fill up buffers of in-memory data, using either a PartitionedAppendOnlyMap if
 *    we want to combine by key, or a PartitionedPairBuffer if we don't.
 *    Inside these buffers, we sort elements by partition ID and then possibly also by key.
 *    To avoid calling the partitioner multiple times with each key, we store the partition ID
 *    alongside each record.
 *
 *  - When each buffer reaches our memory limit, we spill it to a file. This file is sorted first
 *    by partition ID and possibly second by key or by hash code of the key, if we want to do
 *    aggregation. For each file, we track how many objects were in each partition in memory, so we
 *    don't have to write out the partition ID for every element.
 *
 *  - When the user requests an iterator or file output, the spilled files are merged, along with
 *    any remaining in-memory data, using the same sort order defined above (unless both sorting
 *    and aggregation are disabled). If we need to aggregate by key, we either use a total ordering
 *    from the ordering parameter, or read the keys with the same hash code and compare them with
 *    each other for equality to merge values.
 *
 *
 *    问题：ExternalSorter如果没有aggregator和ordering是什么行为？
 *
 *    aggregator   ordering
 *    有                    有
 *    有                    无
 *    无                    无
 *
 *    解释：
 *    1. 如果ShuffleDependency定义了mapSideCombine(比如reduceByKey，groupByKey和sortByKey没有定义mapSideCombine)，那么将
 *    ShuffleDependency定义的Aggregator和KeyOrdering传给ExternalSorter
 *    2. 如果ShuffleDependency没有定义mapSideCombine(比如groupByKey和sortByKey，其中groupyByKey定义了Aggregator，而sortByKey也没有定义Aggregator)，那么既不传Aggregator，也不传KeyOrdering
 *
 *
 *  - Users are expected to call stop() at the end to delete all the intermediate files.
  *
  * @param context
  * @param aggregator
  * @param partitioner 通过该分区器确立ShuffledRDD有多少个分区，这个分区数目用于创建索引文件和数据分段
  * @param ordering Ordering[K]，对类型为K的元素进行比较
  * @param serializer
  * @tparam K
  * @tparam V
  * @tparam C
  */
private[spark] class ExternalSorter[K, V, C](
    context: TaskContext,
    aggregator: Option[Aggregator[K, V, C]] = None,
    partitioner: Option[Partitioner] = None,
    ordering: Option[Ordering[K]] = None,
    serializer: Option[Serializer] = None)
  extends Logging
  with Spillable[WritablePartitionedPairCollection[K, C]] {

  override protected[this] def taskMemoryManager: TaskMemoryManager = context.taskMemoryManager()

  private val conf = SparkEnv.get.conf

  /** *
    * map output文件给多少个分区使用
    */
  private val numPartitions = partitioner.map(_.numPartitions).getOrElse(1)

  /** *
    * 判断是否需要分区
    */
  private val shouldPartition = numPartitions > 1

  /** *
    * 给定Key获取其分区，如果需要分区，则调用partitioner的getPartition操作获取，否则返回0
    * @param key
    * @return
    */
  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.get.getPartition(key) else 0
  }

  /**
    * ExternalSorter在SortShuffleWriter中创建，SortShuffleWriter也通过SparkEnv.get.blockManager持有一个blockManager对象
    * ExternalSorter也通过SparkEnv.get.blockManager持有了一个BlockerManager对象
   *
   * 因为对于一个Executor进程而言，BlockManager是单例，因此ExternalSorter和SortShuffleWriter实际上持有的是同一个BlockManager
    */
  private val blockManager = SparkEnv.get.blockManager


  /**
    * 因为Shuffle操作仅涉及磁盘存储，因此获取磁盘Block管理器
    */
  private val diskBlockManager = blockManager.diskBlockManager
  private val ser = Serializer.getSerializer(serializer)
  private val serInstance = ser.newInstance()

  /** *
    * Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    * 看注释，貌似spark.shuffle.file.buffer也接受不但单位的数字(自动识别为字节数)?
    *
    * 写磁盘的缓存，每次向磁盘写32K
    */
  private val fileBufferSize = conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  // Size of object batches when reading/writing from serializers.
  //
  // Objects are written in batches, with each batch using its own serialization stream. This
  // cuts down on the size of reference-tracking maps constructed when deserializing a stream.
  //
  // NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
  // grow internal data structures by growing + copying every time the number of objects doubles.
  private val serializerBatchSize = conf.getLong("spark.shuffle.spill.batchSize", 10000)

  // Data structures to store in-memory objects before we spill. Depending on whether we have an
  // Aggregator set, we either put objects into an AppendOnlyMap where we combine them, or we
  // store them in an array buffer.

  /** *
    * PartitionedAppendOnlyMap<=SizeTrackingAppendOnlyMap<=AppendOnlyMap
    *
    * AppendOnlyMap的底层数据结构也是数组，但是AppendOnlyMap提供了change value的方法(用于聚合相同Key的元素)
    *
    * 问题：PartitionedAppendOnlyMap中的Partitioned如何理解？
    * 同buffer一样，PartitionedAppendOnlyMap存储的是Key是(partitionId,Key), 从它继承的SizeTrackingAppendOnlyMap[(Int, K), V]的类型声明中可以
    * 看出端倪
    */
  private var map = new PartitionedAppendOnlyMap[K, C]

  /** *
    * PartitionedPairBuffer的底层数据结构是一个Array, 这个Array将(partitionID,K)和V靠近存放，其存储格式为
    * (partiionId, K1)|V1|(partitionId, K2)|V2|(partitionId, K3)|V3
    * Partitioned何解？意思是带有Partition信息的PairBuffer？
    */
  private var buffer = new PartitionedPairBuffer[K, C]

  /**
   * 记录Shuffle到磁盘的字节数
   */
  private var _diskBytesSpilled = 0L


  def diskBytesSpilled: Long = _diskBytesSpilled

  /** *
    * Peak size of the in-memory data structure observed so far, in bytes
    * Shuffle过程需要内存缓冲，此处记录Shuffle过程使用的内存数
    */
  private var _peakMemoryUsedBytes: Long = 0L
  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  // A comparator for keys K that orders them within a partition to allow aggregation or sorting.
  // Can be a partial ordering by hash code if a total ordering is not provided through by the
  // user. (A partial ordering means that equal keys have comparator.compare(k, k) = 0, but some
  // non-equal keys also have this, so we need to do a later pass to find truly equal keys).
  // Note that we ignore this if no aggregator and no ordering are given.

  /**
   *  keyComparator，默认是ordering，如果没有指定ordering，那么比较Key的hashCode值
   *  也就是说keyComparator是一定有值的
   *  按照Key的hashCode进行排序，是什么套路？
   */
  private val keyComparator: Comparator[K] = ordering.getOrElse(new Comparator[K] {
    override def compare(a: K, b: K): Int = {
      val h1 = if (a == null) 0 else a.hashCode()
      val h2 = if (b == null) 0 else b.hashCode()
      if (h1 < h2) -1 else if (h1 == h2) 0 else 1
    }
  })

  /**
   *
   * 如果定义了ordering，不管是否定义aggregator，都使用ordering进行排序
   * 如果没有定义ordering而定义了aggregator，那么使用自定义的排序器（基于Hash值排序）
   *
   * 如果既没有定义ordering，也没有定义aggregator，那么返回None
   *
   * 问题是：什么情况下会定义ordering或者aggregator？只有在定义了mapSideCombine的情况下会进行
   *
   * @return
   */
  private def comparator: Option[Comparator[K]] = {
    if (ordering.isDefined || aggregator.isDefined) {
      Some(keyComparator)
    } else {
      None
    }
  }

  /** *
    *   Information about a spilled file. Includes sizes in bytes of "batches" written by the serializer as we periodically reset its stream, as well as number of elements in each
    *   partition, used to efficiently keep track of partitions when merging.
    * @param file
    * @param blockId
    * @param serializerBatchSizes
    * @param elementsPerPartition
    */
  private[this] case class SpilledFile(
    file: File,
    blockId: BlockId,
    serializerBatchSizes: Array[Long],
    elementsPerPartition: Array[Long])

  /** *
    * 记录了Spill的文件，每个元素是一个SpilledFile
    */
  private val spills = new ArrayBuffer[SpilledFile]

  /**
   * Number of files this sorter has spilled so far.
   * Exposed for testing.
   *
   * 该Map Task总共进行了多少次spill操作
   */
  private[spark] def numSpills: Int = spills.size

  /**
   * records是否已经有序？？
   * insertAll在插入过程中，并没有进行排序, 只有spill到磁盘时才进行排序
   * @param records
   */
  def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    // TODO: stop combining if we find that the reduction factor isn't high
    val shouldCombine = aggregator.isDefined

    /**
     * 如果定义了aggregator，那么基于PartitionedAppendOnlyMap进行combine
     */
    if (shouldCombine) {
      // Combine values in-memory first using our AppendOnlyMap
      val mergeValue = aggregator.get.mergeValue
      val createCombiner = aggregator.get.createCombiner
      var kv: Product2[K, V] = null
      /** *
        * 对相同Key进行创建或者更新
        * 问题：update操作返回的结果的类型是什么？update操作的类型是(Boolean,C)=>C
        */
      val update = (hadValue: Boolean, oldValue: C) => {
        /** *
          * 如果当前Key已经有值，那么调用mergeValue进行合并
          * 如果当前Key还没有值，那么调用createCombiner创建
          */
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }


      while (records.hasNext) {
        addElementsRead()
        kv = records.next()

        /** *
          * 调用changeValue进行combine，要么创建，要么更新
          * 第一个参数是(partitionId,K)
          */
        map.changeValue((getPartition(kv._1), kv._1), update)


        maybeSpillCollection(usingMap = true)
      }
    } else { /**如果没有定义aggregator，则使用PartitionedPairBuffer记录数据**/
      // Stick values into our buffer
      while (records.hasNext) {
        addElementsRead()
        val kv = records.next()

        /**
         * 直接插入，元素是partitionId、key、value
         */
        buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
        maybeSpillCollection(usingMap = false)
      }
    }
  }

  /**
   * Spill the current in-memory collection to disk if needed.
   *
   * @param usingMap whether we're using a map or buffer as our current in-memory collection usingMap为true表示使用PartitionedAppendOnlyMap作为
   *                 内存的数据存储，否则表示使用PartitionedPairBuffer作为内存的数据存储
   *
   *  判断是否需要spill，
   *
    */
  private def maybeSpillCollection(usingMap: Boolean): Unit = {
    var estimatedSize = 0L
    if (usingMap) {

      //对PartitionedAppendOnlyMap的大小进行采样评估
      estimatedSize = map.estimateSize()

      //如果需要spill，那么重建map
      if (maybeSpill(map, estimatedSize)) {
        map = new PartitionedAppendOnlyMap[K, C]
      }
    } else {
      //对buffer已经使用的内存空间进行评估

      estimatedSize = buffer.estimateSize()

      //如果需要spill，那么重建buffer
      if (maybeSpill(buffer, estimatedSize)) {
        buffer = new PartitionedPairBuffer[K, C]
      }
    }

    //如果预估的大小大于当前内存使用量，那么更新内存使用量
    if (estimatedSize > _peakMemoryUsedBytes) {
      _peakMemoryUsedBytes = estimatedSize
    }
  }

  /**
   * Spill our in-memory collection to a sorted file that we can merge later.
   * We add this file into `spilledFiles` to find it later.
   *
   * @param collection whichever collection we're using (map or buffer)，它有两个实现类，PartitionedAppendOnlyMap以及PartitionedPairBuffer
   *                   它们都是按照分区ID优先，其次按照Key排序
   */
  override protected[this] def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    /** *
      * Because these files may be read during shuffle, their compression must be controlled by
      *  spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
      *  createTempShuffleBlock here; see SPARK-3426 for more context.
      *
      *  创建临时的Block(blockId为temp_shuffle_ + UUID)以及临时的文件
      */
    val (blockId, file) = diskBlockManager.createTempShuffleBlock()

    // These variables are reset after each flush
    // objectsWritten记录本轮一共写入了多少个对象
    var objectsWritten: Long = 0
    var spillPerBatchMetrics: ShuffleWriteMetrics = null

    /** *
      * 通过DiskBlockObjectWriter写文件，此处可以查看写磁盘文件的方式
      * 进行还原以查看写入的数据
      */
    var writer: DiskBlockObjectWriter = null

    /** *
      * 打开指定文件的Writer
      */
    def openWriter(): Unit = {
      //没被打开过
      assert (writer == null && spillPerBatchMetrics == null)

      //新建spillPerBatchMetrics
      spillPerBatchMetrics = new ShuffleWriteMetrics

      /**
        * BlockManager的DiskWriter，传入blockId，文件，序列化实例，以及文件缓冲大小
        */
      writer = blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, spillPerBatchMetrics)
    }
    openWriter()

    // List of batch sizes (bytes) in the order they are written to disk
    val batchSizes = new ArrayBuffer[Long]

    // How many elements we have in each partition
    val elementsPerPartition = new Array[Long](numPartitions)

    /** *
      * Flush the disk writer's contents to disk, and update relevant variables.
      * The writer is closed at the end of this process, and cannot be reused.
      */
    def flush(): Unit = {
      val w = writer
      writer = null
      w.commitAndClose()
      _diskBytesSpilled += spillPerBatchMetrics.bytesWritten

      /** *
        * 每次flush需要将本轮写操作写入的字节数记录下来，写到batchSizes中
        */
      batchSizes.append(spillPerBatchMetrics.bytesWritten)

      //flush结束，将本轮spillPerBatchMetrics置为null
      spillPerBatchMetrics = null

      objectsWritten = 0
    }

    var success = false
    try {

      /** *
        *  collection有可能是PartitionedAppendOnlyMap，可有可能是PartitionedPairBuffer
        *
        *  对于PartitionedPairBuffer，如果comparator是None，那么此时PartitionedPairBuffer会使用partitionComparator进行排序
        *  得到的it是一个优先按照partition进行排序。如果Comparator不为None，则进行分区内排序的有序集合
        */
      val it = collection.destructiveSortedWritablePartitionedIterator(comparator)
      while (it.hasNext) {
        //获取当前元素所属的分区
        val partitionId = it.nextPartition()
        require(partitionId >= 0 && partitionId < numPartitions,
          s"partition Id: ${partitionId} should be in the range [0, ${numPartitions})")

        //调用writeNext写入数据
        it.writeNext(writer)

        //因为当前元素属于partitionId表示的分区，因此该分区元素个数加1
        elementsPerPartition(partitionId) += 1

        //写入的对象加1，elementsPerPartition是多轮累加的，而objectsWritten仅记录本轮的结果
        objectsWritten += 1

        if (objectsWritten == serializerBatchSize) {
          flush()
          openWriter()
        }
      }
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
          if (!file.delete()) {
            logWarning(s"Error deleting ${file}")
          }
        }
      }
    }

    /** *
      * SpilledFile构造时记录了不少信息
      * 1. 文件
      * 2. BlockId(有blockId)也可以拿到这个文件
      * 3. 一共spill多少字节的数据，它是一个数组，每个元素记录的是一个batch的字节数
      * 4. 每个Partition的元素个数
      *
      * 问题：此时并不知道每个字节spill了多少字节，仅仅知道元素个数
      *
      * 最后将该spill写到spills集合中，最后做归并排序
      */
    spills.append(SpilledFile(file, blockId, batchSizes.toArray, elementsPerPartition))
  }

  /**
   * Merge a sequence of sorted files, giving an iterator over partitions and then over elements
   * inside each partition. This can be used to either write out a new file or return data to
   * the user.
   *
   * 1. 每个spill文件都是排序的
   * 2. 返回结果是一个Iterator,这个Iterator的元素类型(partitionId, 分区数据Iterator)
   *
   * Returns an iterator over all the data written to this object, grouped by partition. For each
   * partition we then have an iterator over its contents, and these are expected to be accessed
   * in order (you can't "skip ahead" to one partition without reading the previous one).
   * Guaranteed to return a key-value pair for each partition, in order of partition ID.
    *
   *   将spill到磁盘的数据Seq[SpilledFile]以及内存内的数据inMemory进行merge
   *
    * @param spills
    * @param inMemory
    * @return
    */
  private def merge(spills: Seq[SpilledFile], inMemory: Iterator[((Int, K), C)])
      : Iterator[(Int, Iterator[Product2[K, C]])] = {

    /** *
      * 将spills中的每个元素SpillFile构造为SpillReader，SpillReader的构造参数是SpillFile
      */
    val readers = spills.map(new SpillReader(_))

    /** *
      * Iterator.buffered是什么操作？返回BufferedIterator
      */
    val inMemBuffered = inMemory.buffered

    /** *
      * iterator是Seq定义的函数，是将Seq集合转换为迭代器
      */
    (0 until numPartitions).iterator.map { p =>

      /** *
        * 针对每个分区ID，构造一个IteratorForPartition,传入partitionId以及inMemBuffered
        */
      val inMemIterator = new IteratorForPartition(p, inMemBuffered)

      /** *
        * readers是一个集合，每个元素是一个SpillReader，SpillReader的readNextPartition用于读取下一个分区
        *
        * 最后结果是，iterators是内存中属于分区p的数据以及每个spill属于分区p的数据
        */
      val iterators = readers.map(_.readNextPartition()) ++ Seq(inMemIterator)


      if (aggregator.isDefined) {
        // Perform partial aggregation across partitions
        (p, mergeWithAggregation(
          iterators, aggregator.get.mergeCombiners, keyComparator, ordering.isDefined))
      } else if (ordering.isDefined) {
        // No aggregator given, but we have an ordering (e.g. used by reduce tasks in sortByKey);
        // sort the elements without trying to merge them
        (p, mergeSort(iterators, ordering.get))
      } else {
        (p, iterators.iterator.flatten)
      }
    }
  }

  /**
   * Merge-sort a sequence of (K, C) iterators using a given a comparator for the keys.
   */
  private def mergeSort(iterators: Seq[Iterator[Product2[K, C]]], comparator: Comparator[K])
      : Iterator[Product2[K, C]] =
  {
    val bufferedIters = iterators.filter(_.hasNext).map(_.buffered)
    type Iter = BufferedIterator[Product2[K, C]]
    val heap = new mutable.PriorityQueue[Iter]()(new Ordering[Iter] {
      // Use the reverse of comparator.compare because PriorityQueue dequeues the max
      override def compare(x: Iter, y: Iter): Int = -comparator.compare(x.head._1, y.head._1)
    })
    heap.enqueue(bufferedIters: _*)  // Will contain only the iterators with hasNext = true
    new Iterator[Product2[K, C]] {
      override def hasNext: Boolean = !heap.isEmpty

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val firstBuf = heap.dequeue()
        val firstPair = firstBuf.next()
        if (firstBuf.hasNext) {
          heap.enqueue(firstBuf)
        }
        firstPair
      }
    }
  }

  /**
   * Merge a sequence of (K, C) iterators by aggregating values for each key, assuming that each
   * iterator is sorted by key with a given comparator. If the comparator is not a total ordering
   * (e.g. when we sort objects by hash code and different keys may compare as equal although
   * they're not), we still merge them by doing equality tests for all keys that compare as equal.
   */
  private def mergeWithAggregation(
      iterators: Seq[Iterator[Product2[K, C]]],
      mergeCombiners: (C, C) => C,
      comparator: Comparator[K],
      totalOrder: Boolean)
      : Iterator[Product2[K, C]] =
  {
    if (!totalOrder) {
      // We only have a partial ordering, e.g. comparing the keys by hash code, which means that
      // multiple distinct keys might be treated as equal by the ordering. To deal with this, we
      // need to read all keys considered equal by the ordering at once and compare them.
      new Iterator[Iterator[Product2[K, C]]] {
        val sorted = mergeSort(iterators, comparator).buffered

        // Buffers reused across elements to decrease memory allocation
        val keys = new ArrayBuffer[K]
        val combiners = new ArrayBuffer[C]

        override def hasNext: Boolean = sorted.hasNext

        override def next(): Iterator[Product2[K, C]] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          keys.clear()
          combiners.clear()
          val firstPair = sorted.next()
          keys += firstPair._1
          combiners += firstPair._2
          val key = firstPair._1
          while (sorted.hasNext && comparator.compare(sorted.head._1, key) == 0) {
            val pair = sorted.next()
            var i = 0
            var foundKey = false
            while (i < keys.size && !foundKey) {
              if (keys(i) == pair._1) {
                combiners(i) = mergeCombiners(combiners(i), pair._2)
                foundKey = true
              }
              i += 1
            }
            if (!foundKey) {
              keys += pair._1
              combiners += pair._2
            }
          }

          // Note that we return an iterator of elements since we could've had many keys marked
          // equal by the partial order; we flatten this below to get a flat iterator of (K, C).
          keys.iterator.zip(combiners.iterator)
        }
      }.flatMap(i => i)
    } else {
      // We have a total ordering, so the objects with the same key are sequential.
      new Iterator[Product2[K, C]] {
        val sorted = mergeSort(iterators, comparator).buffered

        override def hasNext: Boolean = sorted.hasNext

        override def next(): Product2[K, C] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          val elem = sorted.next()
          val k = elem._1
          var c = elem._2
          while (sorted.hasNext && sorted.head._1 == k) {
            val pair = sorted.next()
            c = mergeCombiners(c, pair._2)
          }
          (k, c)
        }
      }
    }
  }

  /**
   * An internal class for reading a spilled file partition by partition. Expects all the
   * partitions to be requested in order.
   */
  private[this] class SpillReader(spill: SpilledFile) {
    // Serializer batch offsets; size will be batchSize.length + 1
    val batchOffsets = spill.serializerBatchSizes.scanLeft(0L)(_ + _)

    // Track which partition and which batch stream we're in. These will be the indices of
    // the next element we will read. We'll also store the last partition read so that
    // readNextPartition() can figure out what partition that was from.
    // 我们正在处理的partition ID
    var partitionId = 0
    var indexInPartition = 0L
    var batchId = 0
    var indexInBatch = 0
    var lastPartitionId = 0

    skipToNextPartition()

    // Intermediate file and deserializer streams that read from exactly one batch
    // This guards against pre-fetching and other arbitrary behavior of higher level streams
    var fileStream: FileInputStream = null
    var deserializeStream = nextBatchStream()  // Also sets fileStream

    var nextItem: (K, C) = null
    var finished = false

    /** Construct a stream that only reads from the next batch */
    def nextBatchStream(): DeserializationStream = {
      // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
      // we're still in a valid batch.
      if (batchId < batchOffsets.length - 1) {
        if (deserializeStream != null) {
          deserializeStream.close()
          fileStream.close()
          deserializeStream = null
          fileStream = null
        }

        val start = batchOffsets(batchId)
        fileStream = new FileInputStream(spill.file)
        fileStream.getChannel.position(start)
        batchId += 1

        val end = batchOffsets(batchId)

        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))
        val compressedStream = blockManager.wrapForCompression(spill.blockId, bufferedStream)
        serInstance.deserializeStream(compressedStream)
      } else {
        // No more batches left
        cleanup()
        null
      }
    }

    /**
     * Update partitionId if we have reached the end of our current partition, possibly skipping
     * empty partitions on the way.
     */
    private def skipToNextPartition() {
      while (partitionId < numPartitions &&
          indexInPartition == spill.elementsPerPartition(partitionId)) {
        partitionId += 1
        indexInPartition = 0L
      }
    }

    /**
     * Return the next (K, C) pair from the deserialization stream and update partitionId,
     * indexInPartition, indexInBatch and such to match its location.
     *
     * If the current batch is drained, construct a stream for the next batch and read from it.
     * If no more pairs are left, return null.
     */
    private def readNextItem(): (K, C) = {
      if (finished || deserializeStream == null) {
        return null
      }
      val k = deserializeStream.readKey().asInstanceOf[K]
      val c = deserializeStream.readValue().asInstanceOf[C]
      lastPartitionId = partitionId
      // Start reading the next batch if we're done with this one
      indexInBatch += 1
      if (indexInBatch == serializerBatchSize) {
        indexInBatch = 0
        deserializeStream = nextBatchStream()
      }
      // Update the partition location of the element we're reading
      indexInPartition += 1
      skipToNextPartition()
      // If we've finished reading the last partition, remember that we're done
      if (partitionId == numPartitions) {
        finished = true
        if (deserializeStream != null) {
          deserializeStream.close()
        }
      }
      (k, c)
    }

    var nextPartitionToRead = 0

    /** *
      * 读取下一个分区的数据，返回的数据类型是Iterator[(K,C)]
      * @return
      */
    def readNextPartition(): Iterator[Product2[K, C]] = new Iterator[Product2[K, C]] {
      val myPartition = nextPartitionToRead
      nextPartitionToRead += 1

      /** *
        * 检查是否还有元素
        * @return
        */
      override def hasNext: Boolean = {
        /** *
          * 第一次调用hasNext，主动调用readNextItem方法，如果没有读到元素，那么返回false；
          * 否则判断当前partition(由myPartition标识)
          */
        if (nextItem == null) {
          nextItem = readNextItem()
          if (nextItem == null) {
            return false
          }
        }
        assert(lastPartitionId >= myPartition)
        // Check that we're still in the right partition; note that readNextItem will have returned
        // null at EOF above so we would've returned false there
        lastPartitionId == myPartition
      }

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val item = nextItem
        nextItem = null
        item
      }
    }

    // Clean up our open streams and put us in a state where we can't read any more data
    def cleanup() {
      batchId = batchOffsets.length  // Prevent reading any other batch
      val ds = deserializeStream
      deserializeStream = null
      fileStream = null
      ds.close()
      // NOTE: We don't do file.delete() here because that is done in ExternalSorter.stop().
      // This should also be fixed in ExternalAppendOnlyMap.
    }
  }

  /**
   * Return an iterator over all the data written to this object, grouped by partition and
   * aggregated by the requested aggregator. For each partition we then have an iterator over its
   * contents, and these are expected to be accessed in order (you can't "skip ahead" to one
   * partition without reading the previous one). Guaranteed to return a key-value pair for each
   * partition, in order of partition ID.
   *
   * For now, we just merge all the spilled files in once pass, but this can be modified to
   * support hierarchical merging.
   * Exposed for testing.
   */
  def partitionedIterator: Iterator[(Int, Iterator[Product2[K, C]])] = {
    val usingMap = aggregator.isDefined
    val collection: WritablePartitionedPairCollection[K, C] = if (usingMap) map else buffer
    if (spills.isEmpty) {
      // Special case: if we have only in-memory data, we don't need to merge streams, and perhaps
      // we don't even need to sort by anything other than partition ID
      if (!ordering.isDefined) {
        // The user hasn't requested sorted keys, so only sort by partition ID, not key
        groupByPartition(collection.partitionedDestructiveSortedIterator(None))
      } else {
        // We do need to sort by both partition ID and key
        groupByPartition(collection.partitionedDestructiveSortedIterator(Some(keyComparator)))
      }
    } else {
      /**
       *  Merge spilled and in-memory data
       *  如果有数据spill到磁盘(可能经历过多次Spill)，那么联通内存内的shuffle数据一起做归并排序
        */
      merge(spills, collection.partitionedDestructiveSortedIterator(comparator))
    }
  }

  /**
   * Return an iterator over all the data written to this object, aggregated by our aggregator.
   * partitionedIterator会对spill到磁盘的数据进行merge
   * ExternalSorter具有iterator方法，该方法的调用点貌似只有一处就是BlockStoreShuffleReader的read
   */
  def iterator: Iterator[Product2[K, C]] = partitionedIterator.flatMap(pair => pair._2)

  /**
   * Write all the data added into this ExternalSorter into a file in the disk store. This is
   * called by the SortShuffleWriter.
   *
   * 将Shuffle数据写到磁盘文件，这些数据在文件中是按照分区进行排序的
   *
   * @param blockId block ID to write to. The index file will be blockId.name + ".index". 一个Shuffle Output file由shuffleId,mapId唯一确定，与reducerId无关
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker) 每个分区的数据长度，
   *
   *         因为每个分区的数据长度记录在索引文件中了，为什么还需要将各个分区的数据长度存放到MapStatus中呢？
   */
  def writePartitionedFile(
      blockId: BlockId,
      outputFile: File): Array[Long] = {

    val writeMetrics = context.taskMetrics().registerShuffleWriteMetrics()

    // Track location of each range in the output file
    val lengths = new Array[Long](numPartitions)


    /***
      * spills是类型为SpilledFile的ArrayBuffer， ArrayBuffer[SpilledFile]
      * 如果Shuffle没有进行spill，那么spills.isEmpty为true
      */
    if (spills.isEmpty) {
      // Case where we only have in-memory data
      /**
        * 如果定义了aggregator，那么使用map（PartitionedAppendOnlyMap），否则使用buffer（PartitionedAppendOnlyBuffer）
       * 定义了aggregate表示要进行aggregate操作，而这个操作是借助于Map实现的
        */
      val collection = if (aggregator.isDefined) map else buffer

      /***
        * 对集合进行排序
        * 优先按照分区ID进行排序，其次对分区内的数据按照comparator指定的排序算法进行排序
        */
      val it = collection.destructiveSortedWritablePartitionedIterator(comparator)

      /** *
        * 排序后，遍历每个元素
        */
      while (it.hasNext) {
        /**
         * 每次都获取一次Writer？ 不是的，根据下面的循环逻辑，是每个Partition获取一次Writer
         *
         * 这个Writer是DiskBlockObjectWriter类型的对象，用于往磁盘文件写数据。
         * BlockManager的getDiskWriter方法的参数有blockId, outputFile以及fileBufferSize等参数
         */
        val writer = blockManager.getDiskWriter(
          blockId, outputFile, serInstance, fileBufferSize, writeMetrics)

          /** *
            *  首先获取出当前元素所属的分区，然后循环写入属于本Partition的数据(通过it.writeNext(writer)方法)
            */
        val partitionId = it.nextPartition()
        while (it.hasNext && it.nextPartition() == partitionId) {
          it.writeNext(writer)
        }
        writer.commitAndClose()

        /** *
          *  segment是FileSegment类型的对象
          */
        val segment = writer.fileSegment()

        /** *
          * lengths数据是partition id与该partition数据长度的映射
          */
        lengths(partitionId) = segment.length
      }
    } else {
      /** *
        *  We must perform merge-sort; get an iterator by partition and write everything directly.
        *  partitionedIterator方法返回一个对spill到磁盘的文件以及内存内的数据进行merge sort。
        *
        *  partitionedIterator方法返回的数据类型是(partitionId,(K,V))
        *
        */
      for ((id, elements) <- this.partitionedIterator) {
        if (elements.hasNext) {
          val writer = blockManager.getDiskWriter(
            blockId, outputFile, serInstance, fileBufferSize, writeMetrics)
          for (elem <- elements) {
            writer.write(elem._1, elem._2)
          }
          writer.commitAndClose()
          val segment = writer.fileSegment()
          lengths(id) = segment.length
        }
      }
    }

    context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)
    context.taskMetrics().incPeakExecutionMemory(peakMemoryUsedBytes)

    lengths
  }

  def stop(): Unit = {
    map = null // So that the memory can be garbage-collected
    buffer = null // So that the memory can be garbage-collected
    spills.foreach(s => s.file.delete())
    spills.clear()
    releaseMemory()
  }

  /**
   * Given a stream of ((partition, key), combiner) pairs *assumed to be sorted by partition ID*,
   * group together the pairs for each partition into a sub-iterator.
   *
   * @param data an iterator of elements, assumed to already be sorted by partition ID
   */
  private def groupByPartition(data: Iterator[((Int, K), C)])
      : Iterator[(Int, Iterator[Product2[K, C]])] =
  {
    val buffered = data.buffered
    (0 until numPartitions).iterator.map(p => (p, new IteratorForPartition(p, buffered)))
  }

  /**
   * An iterator that reads only the elements for a given partition ID from an underlying buffered
   * stream, assuming this partition is the next one to be read. Used to make it easier to return
   * partitioned iterators from our in-memory collection.
    *
    * @param partitionId
    * @param data 定义成BufferedIterator的目的何在？有几个分区就有几个IteratorForPartition,那么
   *             这几个分区共享这个类型为BufferedIterator的data，每个分区都会读取一遍BufferedIterator相应的数据
   *             因此，可以将data遍历完
    */
  private[this] class IteratorForPartition(partitionId: Int, data: BufferedIterator[((Int, K), C)])
    extends Iterator[Product2[K, C]]
  {
    /** *
      * data的第一个元素(元组)的partitionId是指定的partitionId
      * @return
      */
    override def hasNext: Boolean = data.hasNext && data.head._1._1 == partitionId

    /** *
      * 获取数据(K,C)，丢弃partitionId
      * @return
      */
    override def next(): Product2[K, C] = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val elem = data.next()
      (elem._1._2, elem._2)
    }
  }
}
