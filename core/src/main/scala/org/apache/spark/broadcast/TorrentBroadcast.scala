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

package org.apache.spark.broadcast

import java.io._
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.{Logging, SparkConf, SparkEnv, SparkException}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BroadcastBlockId, StorageLevel}
import org.apache.spark.util.{ByteBufferInputStream, Utils}
import org.apache.spark.util.io.ByteArrayChunkOutputStream

/**
 * A BitTorrent-like implementation of [[org.apache.spark.broadcast.Broadcast]].
 *
 * The mechanism is as follows:
 *
 * The driver divides the serialized object into small chunks and
 * stores those chunks in the BlockManager of the driver.
 *
 * On each executor, the executor first attempts to fetch the object from its BlockManager. If
 * it does not exist, it then uses remote fetches to fetch the small chunks from the driver and/or
 * other executors if available. Once it gets the chunks, it puts the chunks in its own
 * BlockManager, ready for other executors to fetch from.
 *
 * This prevents the driver from being the bottleneck in sending out multiple copies of the
 * broadcast data (one per executor).
 *
 * When initialized, TorrentBroadcast objects read SparkEnv.get.conf.
 *
 * @param obj object to broadcast
 * @param id A unique identifier for the broadcast variable.
 */
private[spark] class TorrentBroadcast[T: ClassTag](obj: T, id: Long)
  extends Broadcast[T](id) with Logging with Serializable {

  /**
   * Value of the broadcast object on executors. This is reconstructed by [[readBroadcastBlock]],
   * which builds this value by reading blocks from the driver and/or other executors.
   *
   * On the driver, if the value is required, it is read lazily from the block manager.
    *
    * 延迟计算_value变量，进行读取操作
    *
   */
  @transient private lazy val _value: T = readBroadcastBlock()

  /** The compression codec to use, or None if compression is disabled */
  @transient private var compressionCodec: Option[CompressionCodec] = _
  /** Size of each block. Default value is 4MB.  This value is only read by the broadcaster. */
  @transient private var blockSize: Int = _

  private def setConf(conf: SparkConf) {
    compressionCodec = if (conf.getBoolean("spark.broadcast.compress", true)) {
      Some(CompressionCodec.createCodec(conf))
    } else {
      None
    }
    // Note: use getSizeAsKb (not bytes) to maintain compatiblity if no units are provided
    blockSize = conf.getSizeAsKb("spark.broadcast.blockSize", "4m").toInt * 1024
  }
  setConf(SparkEnv.get.conf)

  private val broadcastId = BroadcastBlockId(id)

  /**
    *
    * Total number of blocks this broadcast variable contains.
    * numBlocks返回的是广播对象obj分解的block个数
    *
    * */
  private val numBlocks: Int = writeBlocks(obj)

  override protected def getValue() = {
    _value
  }

  /**
   * Divide the object into multiple blocks and put those blocks in the block manager.
   * @param value the object to divide
   * @return number of blocks this broadcast variable is divided into
   */
  private def writeBlocks(value: T): Int = {
    // Store a copy of the broadcast variable in the driver so that tasks run on the driver
    // do not create a duplicate copy of the broadcast variable's value.
    SparkEnv.get.blockManager.putSingle(broadcastId, value, StorageLevel.MEMORY_AND_DISK,
      tellMaster = false)



    val blocks =
      TorrentBroadcast.blockifyObject(value, blockSize, SparkEnv.get.serializer, compressionCodec)
    blocks.zipWithIndex.foreach { case (block, i) =>
      SparkEnv.get.blockManager.putBytes(
        BroadcastBlockId(id, "piece" + i),
        block,
        StorageLevel.MEMORY_AND_DISK_SER,
        tellMaster = true)
    }
    blocks.length
  }

  /***
    * Fetch torrent blocks from the driver and/or other executors
    *
    * @return Broadcast变量所有的block的集合
    */
  private def readBlocks(): Array[ByteBuffer] = {
    // Fetch chunks of data. Note that all these chunks are stored in the BlockManager and reported
    // to the driver, so other executors can pull these chunks from this executor as well.
    val blocks = new Array[ByteBuffer](numBlocks)
    val bm = SparkEnv.get.blockManager

    /***
      * 打散
      */
    for (pid <- Random.shuffle(Seq.range(0, numBlocks))) {
      /***
        * pieceId是BroadcastBlockId，
        */
      val pieceId = BroadcastBlockId(id, "piece" + pid)
      logDebug(s"Reading piece $pieceId of $broadcastId")
      // First try getLocalBytes because there is a chance that previous attempts to fetch the
      // broadcast blocks have already fetched some of the blocks. In that case, some blocks
      // would be available locally (on this executor).
      def getLocal: Option[ByteBuffer] = bm.getLocalBytes(pieceId)

      /***
        * 调用BlockManager的getRemoteBytes,返回的是一个Option[ByteBuffer]。如果读取到了Block，那么将它put到本Execuotr的BlockManager，并通知Master
        * @return
        */
      def getRemote: Option[ByteBuffer] = bm.getRemoteBytes(pieceId).map { block =>
        // If we found the block from remote executors/driver's BlockManager, put the block
        // in this executor's BlockManager.
        /***
          * 并且告诉Master本Executor读取到了一个block，以便其它Executor进行读取
          */
        SparkEnv.get.blockManager.putBytes(
          pieceId,
          block,
          StorageLevel.MEMORY_AND_DISK_SER,
          tellMaster = true)
        block
      }

      /***
        * 读取一个块：
        * 首先尝试从getLocal读取，如果getLocal读取不到，那么从Remote读取，如果Remote也读取不到，此时抛出异常
        */
      val block: ByteBuffer = getLocal.orElse(getRemote).getOrElse(
        throw new SparkException(s"Failed to get $pieceId of $broadcastId"))

      /***
        * 将pid对应的block存到blocks集合中
        */
      blocks(pid) = block
    }
    blocks
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors.
   */
  override protected def doUnpersist(blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver = false, blocking)
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors
   * and driver.
   */
  override protected def doDestroy(blocking: Boolean) {
    TorrentBroadcast.unpersist(id, removeFromDriver = true, blocking)
  }

  /** Used by the JVM when serializing this object. */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    assertValid()
    out.defaultWriteObject()
  }

  /***
    * 这是一个加锁操作，因此在一个Executor进程中，如果有两个Task读取Broadcast，只会有一个任务进行实际的读取操作，其它的Task只能等待
    *
    * 问题：这个方法是在Executor上执行还是Driver上执行？
    * 答：因为readBroadcastBlock方法是在调用Broadcast.value方法时触发的，因此是在Executor一侧执行的
    * @return
    */
  private def readBroadcastBlock(): T = Utils.tryOrIOException {
    TorrentBroadcast.synchronized {
      setConf(SparkEnv.get.conf)

      /***
        * 调用getLocal获取本地BlockManager的broadcastId对应的Block（封装在BlockResult对象中），如果有那么调用BlockResult的data变量获得Iterator
        * 然后调用Iterator的next方法获取到数据
        */
      SparkEnv.get.blockManager.getLocal(broadcastId).map(_.data.next()) match {
        /***
          * 如果本地BlockManager有，则直接返回。x指的是Broadcast对象，是所有的block组装到一起得到的Broadcast对象
          */
        case Some(x) =>
          x.asInstanceOf[T]

        /***
          * 如果没有，那么调用readBlocks方法获取
          * 问题：readBlocks方法返回的是blocks，就是说是一个block的集合，那么这个集合是什么含义？是指的是Broadcast变量所需要的所有的block集合
          */
        case None =>
          logInfo("Started reading broadcast variable " + id)
          val startTimeMs = System.currentTimeMillis()
          val blocks = readBlocks()
          logInfo("Reading broadcast variable " + id + " took" + Utils.getUsedTimeMs(startTimeMs))

          val obj = TorrentBroadcast.unBlockifyObject[T](
            blocks, SparkEnv.get.serializer, compressionCodec)
          // Store the merged copy in BlockManager so other tasks on this executor don't
          // need to re-fetch it.
          SparkEnv.get.blockManager.putSingle(
            broadcastId, obj, StorageLevel.MEMORY_AND_DISK, tellMaster = false)
          obj
      }
    }
  }

}


private object TorrentBroadcast extends Logging {

  def blockifyObject[T: ClassTag](
      obj: T,
      blockSize: Int,
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): Array[ByteBuffer] = {
    val bos = new ByteArrayChunkOutputStream(blockSize)
    val out: OutputStream = compressionCodec.map(c => c.compressedOutputStream(bos)).getOrElse(bos)
    val ser = serializer.newInstance()
    val serOut = ser.serializeStream(out)
    serOut.writeObject[T](obj).close()
    bos.toArrays.map(ByteBuffer.wrap)
  }

  def unBlockifyObject[T: ClassTag](
      blocks: Array[ByteBuffer],
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): T = {
    require(blocks.nonEmpty, "Cannot unblockify an empty array of blocks")
    val is = new SequenceInputStream(
      blocks.iterator.map(new ByteBufferInputStream(_)).asJavaEnumeration)
    val in: InputStream = compressionCodec.map(c => c.compressedInputStream(is)).getOrElse(is)
    val ser = serializer.newInstance()
    val serIn = ser.deserializeStream(in)
    val obj = serIn.readObject[T]()
    serIn.close()
    obj
  }

  /**
   * Remove all persisted blocks associated with this torrent broadcast on the executors.
   * If removeFromDriver is true, also remove these persisted blocks on the driver.
   */
  def unpersist(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    logDebug(s"Unpersisting TorrentBroadcast $id")
    SparkEnv.get.blockManager.master.removeBroadcast(id, removeFromDriver, blocking)
  }
}
