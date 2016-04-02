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

package org.apache.spark.shuffle.sort

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.ExternalSorter

/** *
  * 问题：K,V,C三个类型表示什么意思？K,V表示RDD[K,V]，C呢?
  *
  * 因为这是SortShuffleWriter，那么Writer只需要mapId和shuffleId两方面的信息即可，通过这两方面的信息可以唯一确定一个
  * map out file，在通过DiskBlockManager写磁盘时，指定的reducerId统一为0(参见IndexShuffleBlockResolver的NOOP_REDUCE_ID)
  *
  * @param shuffleBlockResolver
  * @param handle ShuffleHandle记录着ShuffleId
  * @param mapId ShuffleMapTask的ID
  * @param context
  * @tparam K
  * @tparam V
  * @tparam C
  */
private[spark] class SortShuffleWriter[K, V, C](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, C],
    mapId: Int,
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  /**
   * dependency为什么使用handle传递，ShuffleHandle到底是干啥用的？
   */
  private val dep = handle.dependency

  private val blockManager = SparkEnv.get.blockManager

  private var sorter: ExternalSorter[K, V, _] = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = null

  private val writeMetrics = context.taskMetrics().registerShuffleWriteMetrics()

  /**
   * Write a bunch of records to this task's output
    *
   * 问题：该map stage的下一个stage有多少个分区(或者task)是如何判定的？
   *
    *
   * 调用ExternalSorter的insertAll方法写入数据注意点：
   *    1.  如果有map side combine，则如何将map端combine的数据写到磁盘
   *    2. 如果有排序，比如sortByKey，则如何将有序的数据写到磁盘 --- ExternalSorter.insertAll并没有进行排序处理？是的，排序的数据逻辑是在ShufflerReader中完成的
   *    3. 如果内存不够用，如何spill磁盘，最后如何做Merge？这个逻辑在ExternalSorter的writePartitionedFile方法中
   *
    *
    * @param records records是该map task处理后得到的RDD分区数据通过该map task对应的ShuffleWrite进行shuffle到磁盘中
    */
  override def write(records: Iterator[Product2[K, V]]): Unit = {

    /***
      * 使用ExternalSorter进行数据逻辑逻辑(sort、combine、spill)
      *
      * 创建ExternalSorter，
      *     1. 如果定义了mapSideCombine(隐含着Aggregator必须定义)，比如reduceByKey， 那么将aggregator传给ExternalSorter(对于reduceByKey，keyOrdering为None)
      *     2. 如果没有定义mapSideCombine，那么既不传Aggregator也不传keyOrdering给ExternalSorter,比如sortByKey就是没有定义mapSideCombine，但是定义了keyOrdering
      *     3. 对于groupByKey而言，定义了Aggregator，但是没有定义mapSideCombine，那么既不传Aggregator也不传keyOrdering给ExternalSorter,
      *
      *   问题： 如果没有定义ShuffleDependency没有定义mapSideCombine，那么构造ExternalSorter时，既不传入Aggregator也不传入keyOrdering，
      *   那么Sort Based Shuffle这个Sort如何体现
      *
      */
    sorter = if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      /** *
        * In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
        * care whether the keys get sorted in each partition; that will be done on the reduce side
        * if the operation being run is sortByKey
        */
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }

    /**
      * 插入数据
      */
    sorter.insertAll(records)


    /**
      * 数据写完后，整理文件以及MapStatus, 文件包括数据文件(data file)和索引文件(index file)
     *
     * MapStatus将在stop方法中返回
      *
      *
      */
    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).

    //根据shuffleId和mapId获取一个数据文件(这个文件是一个新文件，没有写入数据)
    //为什么使用IndexShuffleBlockResolver类去获取data file
    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)

    //根据output文件的绝对路径+UUID获取的一个临时文件
    val tmp = Utils.tempFileWith(output)

    /***
      * 构造ShuffleBlockId，output文件名和路径是与blockId的name相关的
      * blockId只有shuffleId，mapId，而reducerId的值为0，这是因为在Shuffle Write中，所有的reducer数据都写在一个文件中，
      * 因此Shuffle Writer不需要关心ReducerID，
      * 而Shuffle Reader时则需要ReducerID
      */
    val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)

    /**
     * 调用ExternalSorter的writePartitionedFile方法完成Shuffle数据写入磁盘的操作，所有这些Shuffle数据会写到要给数据文件中，
     * 写入时，会按照partitionID进行排序(优先按照分区ID排序，也有可能进行分区内按照Key排序)
     * 如果insertAll过程中有spill，那么需要归并排序返回每个分区的长度的集合
     *
     * PartitionedFile指的是一个Mapper output文件中记录了所有Reducer需要的数据，这个数据需要记录分段信息
     *
     * 注意：数据写入到临时文件tmp中
     */
    val partitionLengths = sorter.writePartitionedFile(blockId, tmp)

    /***
      * 每个Task都会执行IndexShuffleBlockResolver的writeIndexFileAndCommit方法，
      * 因此在IndexShuffleBlockResolver的writeIndexFileAndCommit方法中进行了数据文件和索引文件的写同步操作？
      * ----这个地方的理解是不对的，不同的map task写不同的是数据文件和索引文件，因此没有同步的问题，问题的关键是同一个map task，可能由于
      * 推测执行等策略，导致一个map任务由多个task执行，为了防止这种情况，对IndexShuffleBlockResolver进行了加锁，也就是说，所有的map task
      * 在写磁盘文件时进行了同步
      *
      *
      * 不同的task写入到磁盘文件和数据文件中，细节是什么？
      * map task 1先执行完写数据文件和磁盘，而后map task 3执行完写数据文件和磁盘，map task 2执行完而后又写数据文件和磁盘，
      * 对于这三个task以这样的顺序写完，它们如何更新索引文件和数据文件？
      * 答：这三个task会创建不同的索引文件和数据文件！
      */
    shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)

    /**
     * write方法的返回值是个Unit，这是只是给mapStatus赋值，并不返回；mapStatus返回是在stop方法中实现的，
      *
      * BlockManager的shuffleServerId是BlockManagerId
      *
      * 问题：ShuffleReader怎么根据MapStatus信息去Shuffle数据？
     *
     * MapStatus的第二个参数是partitionLengths，它是该任务写到磁盘上上的对应reduce task的字节数组，换句话说，
     * partitionLengths记录了该map task输出到每个reduce task的字节数
     *
     */
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
  }

  /**
   *
   * Close this writer, passing along whether the map completed
   *  返回MapStatus对象
   *
    * */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        return Option(mapStatus)
      } else {
        // The map task failed, so delete our output data.
        shuffleBlockResolver.removeDataByMap(dep.shuffleId, mapId)
        return None
      }
    } finally {
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        writeMetrics.incWriteTime(System.nanoTime - startTime)
        sorter = null
      }
    }
  }
}

private[spark] object SortShuffleWriter {

  /***
    * 如果有map端combine的话，就不能ByPass Merge Sort
    * @param conf
    * @param dep
    * @return
    */
  def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
    // We cannot bypass sorting if we need to do map-side aggregation.
    if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      false
    } else {

      /**
        * 如果没有客户端combine，那么需要判断Shuffle的分区数是否小于spark.shuffle.sort.bypassMergeThreshold配置的分区数
        */
      val bypassMergeThreshold: Int = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
      dep.partitioner.numPartitions <= bypassMergeThreshold
    }
  }
}
