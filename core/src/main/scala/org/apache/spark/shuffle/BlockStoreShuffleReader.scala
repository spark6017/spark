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

package org.apache.spark.shuffle

import org.apache.spark._
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockManager, ShuffleBlockFetcherIterator}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * Fetches and reads the partitions in range [startPartition, endPartition) from a shuffle by
 * requesting them from other nodes' block stores.
  *
  *
  * 读取Shuffle数据，因为Shuffle数据可能位于多个Node上，因此需要有数据传输的过程，这也是Shuffle影响性能的点，
 *
 * startPartition和endPartition通常只差1，表示只读取一个Partition的数据，参见
 * @see [[org.apache.spark.rdd.ShuffledRDD#compute]]#compute
 *
 */
private[spark] class BlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  /**
   *
   *  Shuffle Read
    *
    */
  override def read(): Iterator[Product2[K, C]] = {

    /**
     *blocksByAddress的类型是 Seq[(BlockManagerId, Seq[(BlockId, Long)])],
     * 每个元素是一个二元组，每个元组的第一个元素是BlockManagerId，第二个元素是元组的集合，
     *
     *  Shuffle Read 数据
     *
     *  问题：为什么返回类型是Seq[(BlockManagerId, Seq[(BlockId, Long)])]
     *  因为，对于一个Reduce task，它所需要的数据来源于多个map task，而这些map task是由多个多个executor执行的
     *  因此需要从多个executor要数据，而每个executor对应着一个BlockManagerId，也就是说一个reduce task会对应多个BlockManagerId
     *  对于一个BlockManager，它可以管理这多个Block，每个Block都有数据长度，也就是说BlockManager和Block是一对多的关系
     *
     */
    val blocksByAddress = mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition)


    /**
     * 构造ShuffleBlockFetcherIterator，ShuffleBlockFetcherIterator的主构造器会调用initialize方法
     *
     * ShuffleBlockFetcherIterator是一个可遍历的集合(继承自Iterator)，因此它有map方法；并且该集合的元素类型是(BlockId,InputStream)
     */
    val blockFetcherItr = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      blocksByAddress,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024)

    // Wrap the streams for compression based on configuration

    // blockManager.wrapForCompression方法返回一个InputStream，因此wrappedStreams是InputStream的集合
    val wrappedStreams = blockFetcherItr.map { case (blockId, inputStream) =>
      blockManager.wrapForCompression(blockId, inputStream)
    }

    /**
     * 获得序列化和反序列化实例
     */
    val ser = Serializer.getSerializer(dep.serializer)
    val serializerInstance = ser.newInstance()

    // Create a key/value iterator for each stream
    //反序列化stream，返回DeserializationStream
    val recordIter = wrappedStreams.flatMap { wrappedStream =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    val readMetrics = context.taskMetrics.registerTempShuffleReadMetrics()


    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](


      recordIter.map(record => {
        readMetrics.incRecordsRead(1)
        record
      }),



      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)


    /** *
      * 获取aggregatedIterator
      *
      * 对于ShuffleDependency，如果定义了mapSideCombine，那么一定要定义Aggregator；反之，如果定义了Aggregator，则可以要求不定义mapSideCombine
      *
      * 如果定义了aggregator
      *           ---如果定义了mapSideCombine，那么调用aggregator.combineCombinersByKey返回Iterator
      *           ---如果没有定义mapSideCombine，那么调用aggregator.combineValuesByKey返回Iterator
      *  如果没有定义aggregator
      *           ---直接原样返回Iterator
      *
      *  如果 定义了keyOrdering，比如sortByKey
      *          ---调用ExternalSorter插入数据
      */
    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]

        /** *
          * combineCombinersByKey使用ExternalAppendOnlyMap
          */
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]

        /**
         * combineValuesByKey也使用ExternalAppendOnlyMap
         */
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.

    //如果需要排序，则使用ExternalSorter，可能需要spill到磁盘，也就是说，写的过程可能需要spill，读的时候依然可能需要Spill
    //如果无需排序，那么直接返回aggregatedIter
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,
        // the ExternalSorter won't spill to disk.

        /** *
          * 此处创建ExternalSorter时，并没有将Aggregator传入，也就是说，此时的ExternalSorter是基于Aggregator为None实现的，但是传入了ordering
          */
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = Some(ser))
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        aggregatedIter
    }
  }
}
