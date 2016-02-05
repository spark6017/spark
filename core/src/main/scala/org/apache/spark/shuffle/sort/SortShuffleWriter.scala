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
   * 调用ExternalSorter的insertAll方法写入数据注意点：
   *    1.  如果有map side combine，则如何将map端combine的数据写到磁盘
   *    2. 如果有排序，比如sortByKey，则如何将有序的数据写到磁盘 --- ExternalSorter.insertAll并没有进行排序处理？是的，排序的数据逻辑是在ShufflerReader中完成的
   *    3. 如果内存不够用，如何spill磁盘，最后如何做Merge？这个逻辑在ExternalSorter的writePartitionedFile方法中
   *
   * */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    sorter = if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    sorter.insertAll(records)

    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    val tmp = Utils.tempFileWith(output)
    val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)

    /**
     * 将通过ExternalSorter.insertAll方法写入的数据全部写入到文件中，如果insertAll过程中有spill，那么需要考虑归并排序
     */
    val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
    shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)

    /**
     * write方法的返回值是个Unit，这是只是给mapStatus赋值，并不返回；mapStatus返回是在stop方法中实现的，
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
  def shouldBypassMergeSort(conf: SparkConf, dep: ShuffleDependency[_, _, _]): Boolean = {
    // We cannot bypass sorting if we need to do map-side aggregation.
    if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      false
    } else {
      val bypassMergeThreshold: Int = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
      dep.partitioner.numPartitions <= bypassMergeThreshold
    }
  }
}
