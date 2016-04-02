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

import java.io._

import com.google.common.io.ByteStreams

import org.apache.spark.{Logging, SparkConf, SparkEnv}
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage._
import org.apache.spark.util.Utils

/**
 * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
 *
 *  两个概念：logical和physical，“local" block和”physical" file location,这里的logical和physical表达什么含义？
 *
 *  IndexShuffleBlockResolver用于创建和维护logical block和physical file location之间的对应关系
 *
 * Data of shuffle blocks from the same map task are stored in a single consolidated data file.
 * The offsets of the data blocks in the data file are stored in a separate index file.
 *
 *
 * 同一个map task产生的shuffle 数据存储在一个文件中，这个文件中的数据聚合了各个Reducer需要的数据片段。
 * 每个Reducer数据在文件中的位置(或称为偏移量，offset)存放在index文件中
 *
 * We use the name of the shuffle data's shuffleBlockId with reduce ID set to 0 and add ".data"
 * as the filename postfix for data file, and ".index" as the filename postfix for index file.
 *
 * 数据文件和索引文件的命名规则：
 * 使用ShuffleBlockId(ReduceID从0开始计算)作为数据文件和索引文件的前缀，数据文件拼接.data,索引文件拼接.index
 * ShuffleBlockId的命名是String shuffleBlockIdName= "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId
 *
 * Note: Changes to the format in this file should be kept in sync with org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getSortBasedShuffleBlockData().
 *
  *
  * @param conf
  * @param _blockManager BlockManager用于管理数据文件和索引文件？
  */
private[spark] class IndexShuffleBlockResolver(
    conf: SparkConf,
    _blockManager: BlockManager = null)
  extends ShuffleBlockResolver
  with Logging {

  /** *
    * 如果没有指定BlockManager，那么通过SparkEnv获取BlockManager，对于每个Executor而言，BlockManager是唯一的
    * 也就是说，对于Executor(一个进程)而言，BlockManager是单例的
    */
  private lazy val blockManager = Option(_blockManager).getOrElse(SparkEnv.get.blockManager)

  /** *
    * 传输用的配置？
    */
  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")

  /** *
    * 根据shuffleId和mapId调用BlockManager的DiskBlockManager来获取一个文件，获取文件的参数是ShuffleDataBlockId
    * 这里没有指定reduceId，而是使用了值为0的NOOP_REDUCE_ID，
    * 也就是说，此处获取到的数据文件是(shuffleId,mapId,0)
    *
    *问题： 因为一个mapId产生一个唯一的文件，那么shuffleId和mapId可以唯一确定一个数据文件，为什么getFile方法需要一个reduceId？
    * 答案：在NOOP_REDUCE_ID的注释中对此事进行了说明
    *
    * @param shuffleId shuffleId
    * @param mapId mapId
    * @return  从DiskBlockManager中获取"shuffle_" + shuffleId + "_" + mapId + "_" + 0 + ".data"标识的文件，该文件位于哪台机器上？
    */
  def getDataFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  /** *
    * 和 data file一样，index file也可以由shuffleId和mapId唯一确定，因此此处给定的reduce id 为NOOP_REDUCE_ID(0)
    *
    * 这也就以为着，sort based shuffle在将数据文件和索引文件写入到磁盘时，指定的reduce id 为NOOP_REDUCE_ID
    * @param shuffleId
    * @param mapId
    * @return
    */
  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  /**
   * Remove data file and index file that contain the output data from one map task.
    * 删除一个MapTask写到磁盘上的数据文件和索引文件，
   * 这里所谓的ByMap，其实指的是ByMapTask
    * @param shuffleId
    * @param mapId
    */
  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    /** *
      * 获取然后删除数据文件
      */
    var file = getDataFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting data ${file.getPath()}")
      }
    }

    /** *
      * 获取并删除索引文件
      */
    file = getIndexFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting index ${file.getPath()}")
      }
    }
  }

  /**
   * Check whether the given index and data files match each other.
   * If so, return the partition lengths in the data file. Otherwise return null.
   *
   *   检查给定的索引文件和数据文件是否包含blocks个数据segment，
   *   如果匹配，那么返回每个segment的偏移量
   *
   *   这里只返回每个reducer的数据长度，并没有返回每个reducer在数据文件中的起始位置，
   *   目测是读取数据文件一次： 依次读取指定个数的字节，然后就可以把数据文件读完，从一个数组存放每个reducer所属的数据
    *
    * @param index
    * @param data
    * @param reducerNumber
    * @return
    */
  private def checkIndexAndDataFile(index: File, data: File, reducerNumber: Int): Array[Long] = {
    /** *
      *  the index file should have `reducerNumber + 1` longs as offset.
      *  index文件的长度应该是(reducerNumber+1)个字节？ 不是的，index.length返回的就是字节数
      *  index.length()=(reducerNumber+1）*8表示index文件写入了reducerNumber+1个long数字
      *  那么每个数字表达什么含义？
      */
    if (index.length() != (reducerNumber + 1) * 8) {
      return null
    }

    /** *
      * 返回的数组的长度是blocks，也就是说，reducerNumber指的是reducer的个数
      */
    val lengths = new Array[Long](reducerNumber)
    /** *
      * Read the length of  the data belonging to each reducer
      * 此处是读取index文件的逻辑，可以用于index文件的可视化(或者文本化)
      * index文件是通过DataOutputStream写入的二进制文件
      */
    val in = try {
      new DataInputStream(new BufferedInputStream(new FileInputStream(index)))
    } catch {
      case e: IOException =>
        return null
    }
    try {
      /**
       *  Convert the offsets into lengths of each block
       *  index文件的第一个long值是0
       */
      var offset = in.readLong()
      if (offset != 0L) {
        return null
      }

      /** *
        * index文件的值是递增的，算法是：
        * 1. 第N个值-第N-1个值就是第N个reducer的数据长度(N >=0)
        * 2. 第N个值=前N-1个reducer的总的数据长度+第N个reducer的数据长度(N>=1)，第0个值是0
        */
      var i = 0
      while (i < reducerNumber) {
        /** *
          * 读取index文件中的第(i+1)个long值，
          */
        val current = in.readLong()

        /** *
          * 第i个reducer的数据长度=
          */
        lengths(i) = current - offset
        offset = current
        i += 1
      }
    } catch {
      case e: IOException =>
        return null
    } finally {
      in.close()
    }

    /** *
      * the size of data file should match with index file
      * 数据文件的长度应该等于所有reducer的长度之和
      */
    if (data.length() == lengths.sum) {
      lengths
    } else {
      null
    }
  }

  /**
   *  问题：这里的Commit是个什么含义？何为Commit？
   *
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockData to figure out where each block
   * begins and ends.
   *
   *  写索引文件，这里的操作其实就是创建索引文件，索引文件保存(reducerNumber + 1)个long数字，
   *  因此索引文件的长度为(reducer + 1) * 8
   *
   * It will commit the data and index file as an atomic operation, use the existing ones, or
   * replace them with new ones.
   *
   * 问题1： 数据文件和索引文件进行原子提交，如何做到的？
   * 问题2： 对于一个索引文件，是否是一个任务一次性全部写完，还是说每个任务都会更新一次数据文件和索引文件？
   * 按照索引文件的结构，貌似不是的，因为索引文件中的offset是按照reducer id进行排序的
   *
   * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
    *
    * @param shuffleId shuffleId
    * @param mapId mapId
    * @param lengths 一个long类型的数组，每个元素记录了属于每个reducer的数据长度
    * @param dataTmp 这是什么？这个应该是已经写好的临时数据文件，也需要提交
    */
  def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Int,
      lengths: Array[Long],
      dataTmp: File): Unit = {

    /***
      *  代码运行到此处，索引文件并没有创建，因此这里的getIndexFile将首先索引文件
      */
    val indexFile = getIndexFile(shuffleId, mapId)

    /** *
      * 创建索引文件的临时文件，indexFile拼接一个UUID
      */
    val indexTmp = Utils.tempFileWith(indexFile)

    /** *
      *  打开临时文件准备写索引数据
      */
    val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexTmp)))

    /** *
      * tryWithSafeFinally是一个try {}finally{}构造块，第一个参数是try中执行的代码块，第二个参数是finally中执行的代码块
      */
    Utils.tryWithSafeFinally {
      /** *
        *  We take in lengths of each block, need to convert it to offsets.
        *  往临时索引文件中写入reducerNumber + 1个long值，写入的值是递增的
        */
      var offset = 0L
      out.writeLong(offset)
      for (length <- lengths) {
        offset += length
        out.writeLong(offset)
      }
    } {
      out.close()
    }

    /** *
      * 获取数据文件，因此参数是dataTmp，目测此处将新建数据文件
      */
    val dataFile = getDataFile(shuffleId, mapId)
    /** *
      * There is only one IndexShuffleBlockResolver per executor, this synchronization make sure the following check and rename are atomic.
      *
      * 同一个executor的shuffleId和mapId标识的数据文件和索引文件应该是executor进程独有的，因此此处加synchronized是能起到
      * 线程安全目的的，问题是是否有必要进行同步操作？
      *  要的，因为这个操作是任务级别的，也就是说writeIndexFileAndCommit可能被多个Task执行？
      */
    synchronized {
      /** *
        *  检查是否已经写成功，如果是的吧
        */
      val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
      if (existingLengths != null) {
        // Another attempt for the same task has already written our map outputs successfully,
        // so just use the existing partition lengths and delete our temporary map outputs.
        System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)

        /** *
          * 如果临时数据文件存在，则删除
          */
        if (dataTmp != null && dataTmp.exists()) {
          dataTmp.delete()
        }

        /** *
          * 删除刚新建的临时索引文件
          */
        indexTmp.delete()
      } else {
       /** *
          * This is the first successful attempt in writing the map outputs for this task, so override any existing index and data files with the ones we wrote.
         *  如果索引文件和数据文件不匹配(以为着有错)，那么将数据文件和索引文件删除
          */
        if (indexFile.exists()) {
          indexFile.delete()
        }
        if (dataFile.exists()) {
          dataFile.delete()
        }
        //因为索引文件和数据文件，刚才已经删除，因此此处可以重命名
        if (!indexTmp.renameTo(indexFile)) {
          throw new IOException("fail to rename file " + indexTmp + " to " + indexFile)
        }
        if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
          throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
        }
      }
    }
  }

  /** *
    * 根据指定的ShuffleBlockId(也就是对应一个Block)，返回ManagedBuffer(此处的ManagedBuffer是一个FileSegmentManagedBuffer)
    * ManagedBuffer是一个抽象类，本Resolver实现的FileSegmentManagedBuffer
    *
    *  The block is actually going to be a range of a single map output file for this map, so find out the consolidated file, then the offset within that from our index
    * 对于sort based shuffle而言，ShuffleBlockId对应的数据文件是一个，需要从索引文件中找到reduceId所在的offset，然后从数据文件中
    * 读取之
    * @param blockId 此处的blockId是一个ShuffleBlockId，而不是ShuffleDataBlockId或者ShuffleIndexBlockId,
    *                也就是说，ShuffleBlockId中的reduceId是真实的，需要根据该reduceId以及索引文件中获取数据
    *
    *
    * @return
    */
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {

    /** *
      * 获取索引文件
      */
    val indexFile = getIndexFile(blockId.shuffleId, blockId.mapId)

    /** *
      * 打开索引文件
      */
    val in = new DataInputStream(new FileInputStream(indexFile))
    try {
      /** *
        * 索引文件是reducer number + 8个long型数据，第一个long值是0
        * reduceId从0开始计算
        * 首先根据reduceId跳过reduceId*8个字节，
        * 如果reduceId为0，则无需跳过；如果reducerId = 1，那么跳过第一个long值，读取该long值A，再跳到下一个long值B，B-A就是改reduceId的数据长度
        */
      ByteStreams.skipFully(in, blockId.reduceId * 8)
      /**
       * offset记录该reducer的数据在数据文件中的起始位置
       */
      val offset = in.readLong()

      /** *
        * nextOffset记录该reducer的数据在数据文件中的结束位置
        */
      val nextOffset = in.readLong()

      /** *
        * 创建FileSegmentManagedBuffer，参数包括
        * 1. 数据在数据文件中的起始位置，数据长度
        * 2. 数据文件
        * 3. 用于Netty数据传输的配置对象transportConf
        */
      new FileSegmentManagedBuffer(
        transportConf,
        getDataFile(blockId.shuffleId, blockId.mapId),
        offset,
        nextOffset - offset)
    } finally {
      in.close()
    }
  }

  /** *
    * stop方法是空实现
    */
  override def stop(): Unit = {}
}

private[spark] object IndexShuffleBlockResolver {
  // No-op reduce ID used in interactions with disk store.
  // The disk store currently expects puts to relate to a (map, reduce) pair, but in the sort
  // shuffle outputs for several reduces are glommed into a single file.
  /** *
    * 对于sort based shuffle，同一个mapId的shuffle数据都在一个文件中，因此可以不需要reduceId信息定位一个数据文件
    * 因此，对于ShuffleDataBlockId，统一赋值为0
    *
    */
  val NOOP_REDUCE_ID = 0
}
