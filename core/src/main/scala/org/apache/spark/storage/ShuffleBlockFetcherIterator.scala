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

import java.io.InputStream
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.mutable.{ArrayBuffer, HashSet, Queue}
import scala.util.control.NonFatal

import org.apache.spark.{Logging, SparkException, TaskContext}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.{BlockFetchingListener, ShuffleClient}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.Utils

/**
 * An iterator that fetches multiple blocks. For local blocks, it fetches from the local block
 * manager. For remote blocks, it fetches them using the provided BlockTransferService.
 *
 * This creates an iterator of (BlockID, InputStream) tuples so the caller can handle blocks
 * in a pipelined fashion as they are received.
 *
 * 集合的元素类型是(BlockID,InputStream)，这在ShuffleBlockFetcherIterator继承的Iterator[(BlockId, InputStream)] 中可以看出来
 *
 * ShuffleBlockFetcherIterator是一个迭代器
 *
 *
 * The implementation throttles the remote fetches so they don't exceed maxBytesInFlight to avoid
 * using too much memory.
 *
 * @param context [[TaskContext]], used for metrics update
 * @param shuffleClient [[ShuffleClient]] for fetching remote blocks
 * @param blockManager [[BlockManager]] for reading local blocks
 * @param blocksByAddress list of blocks to fetch grouped by the [[BlockManagerId]].
 *                        For each block we also require the size (in bytes as a long field) in
 *                        order to throttle the memory usage.
 * @param maxBytesInFlight max size (in bytes) of remote blocks to fetch at any given point.， 每个Reducer Task每次最多传输多大的数据量?不是这个含义，是说所有的传输最大字节
 */
private[spark]
final class ShuffleBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: ShuffleClient,
    blockManager: BlockManager,
    blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])],
    maxBytesInFlight: Long)
  extends Iterator[(BlockId, InputStream)] with Logging {

  import ShuffleBlockFetcherIterator._

  /**
   * Total number of blocks to fetch. This can be smaller than the total number of blocks
   * in [[blocksByAddress]] because we filter out zero-sized blocks in [[initialize]].
   *
   * This should equal localBlocks.size + remoteBlocks.size.
   */
  private[this] var numBlocksToFetch = 0

  /**
   * The number of blocks proccessed by the caller. The iterator is exhausted when
   * [[numBlocksProcessed]] == [[numBlocksToFetch]].
   */
  private[this] var numBlocksProcessed = 0

  private[this] val startTime = System.currentTimeMillis

  /** Local blocks to fetch, excluding zero-sized blocks. */
  private[this] val localBlocks = new ArrayBuffer[BlockId]()

  /** Remote blocks to fetch, excluding zero-sized blocks. */
  private[this] val remoteBlocks = new HashSet[BlockId]()

  /**
   * A queue to hold our results. This turns the asynchronous model provided by
   * [[org.apache.spark.network.BlockTransferService]] into a synchronous model (iterator).
   */
  private[this] val results = new LinkedBlockingQueue[FetchResult]

  /**
   * Current [[FetchResult]] being processed. We track this so we can release the current buffer
   * in case of a runtime exception when processing the current buffer.
   */
  @volatile private[this] var currentResult: FetchResult = null

  /**
   * Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
   * the number of bytes in flight is limited to maxBytesInFlight.
   */
  private[this] val remoteFetchRequests = new Queue[FetchRequest]

  /** Current bytes in flight from our requests */
  private[this] var bytesInFlight = 0L

  private[this] val shuffleMetrics = context.taskMetrics().registerTempShuffleReadMetrics()

  /**
   * Whether the iterator is still active. If isZombie is true, the callback interface will no
   * longer place fetched blocks into [[results]].
   */
  @volatile private[this] var isZombie = false


  /**
   * 初始化
   */
  initialize()

  // Decrements the buffer reference count.
  // The currentResult is set to null to prevent releasing the buffer again on cleanup()
  private[storage] def releaseCurrentResultBuffer(): Unit = {
    // Release the current buffer if necessary
    currentResult match {
      case SuccessFetchResult(_, _, _, buf) => buf.release()
      case _ =>
    }
    currentResult = null
  }

  /**
   * Mark the iterator as zombie, and release all buffers that haven't been deserialized yet.
   */
  private[this] def cleanup() {
    isZombie = true
    releaseCurrentResultBuffer()
    // Release buffers in the results queue
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(_, _, _, buf) => buf.release()
        case _ =>
      }
    }
  }

  /**
   * 异步方式发起获取Shuffle数据的请求
   * @param fetchRequest
   */
  private[this] def sendRequest(fetchRequest: FetchRequest) {
    logDebug("Sending request for %d blocks (%s) from %s".format(
      fetchRequest.blocks.size, Utils.bytesToString(fetchRequest.size), fetchRequest.address.hostPort))
    bytesInFlight += fetchRequest.size

    // so we can look up the size of each blockID
    val sizeMap = fetchRequest.blocks.map { case (blockId, size) => (blockId.toString, size) }.toMap

    /** *
      * blockIds
      */
    val blockIds = fetchRequest.blocks.map(_._1.toString)

    /** *
      * req的address是BlockManagerId
      */
    val address = fetchRequest.address


    shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
      new BlockFetchingListener {
        override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
          // Only add the buffer to results queue if the iterator is not zombie,
          // i.e. cleanup() has not been called yet.
          if (!isZombie) {
            // Increment the ref count because we need to pass this to a different thread.
            // This needs to be released after use.
            buf.retain()
            results.put(new SuccessFetchResult(BlockId(blockId), address, sizeMap(blockId), buf))
            shuffleMetrics.incRemoteBytesRead(buf.size)
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          logTrace("Got remote block " + blockId + " after " + Utils.getUsedTimeMs(startTime))
        }

        override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
          logError(s"Failed to get block(s) from ${fetchRequest.address.host}:${fetchRequest.address.port}", e)
          results.put(new FailureFetchResult(BlockId(blockId), address, e))
        }
      }
    )
  }

  /**
   * 拆分Local和Remote Block
   * @return
   */
  private[this] def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
    // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
    // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
    // nodes, rather than blocking on reading output from one node.
    val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
    logDebug("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize)

    // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
    // at most maxBytesInFlight in order to limit the amount of data in flight.
    val remoteRequests = new ArrayBuffer[FetchRequest]

    // Tracks total number of blocks (including zero sized blocks)
    var totalBlocks = 0
    for ((address, blockInfos) <- blocksByAddress) {
      totalBlocks += blockInfos.size
      if (address.executorId == blockManager.blockManagerId.executorId) {
        // Filter out zero-sized blocks
        localBlocks ++= blockInfos.filter(_._2 != 0).map(_._1)
        numBlocksToFetch += localBlocks.size
      } else {
        val iterator = blockInfos.iterator
        var curRequestSize = 0L
        var curBlocks = new ArrayBuffer[(BlockId, Long)]
        while (iterator.hasNext) {
          val (blockId, size) = iterator.next()
          // Skip empty blocks
          if (size > 0) {
            curBlocks += ((blockId, size))
            remoteBlocks += blockId
            numBlocksToFetch += 1
            curRequestSize += size
          } else if (size < 0) {
            throw new BlockException(blockId, "Negative block size " + size)
          }
          if (curRequestSize >= targetRequestSize) {
            // Add this FetchRequest
            remoteRequests += new FetchRequest(address, curBlocks)
            curBlocks = new ArrayBuffer[(BlockId, Long)]
            logDebug(s"Creating fetch request of $curRequestSize at $address")
            curRequestSize = 0
          }
        }
        // Add in the final request
        if (curBlocks.nonEmpty) {
          remoteRequests += new FetchRequest(address, curBlocks)
        }
      }
    }
    logInfo(s"Getting $numBlocksToFetch non-empty blocks out of $totalBlocks blocks")
    remoteRequests
  }

  /**
   * Fetch the local blocks while we are fetching remote blocks. This is ok because
   * [[ManagedBuffer]]'s memory is allocated lazily when we create the input stream, so all we
   * track in-memory are the ManagedBuffer references themselves.
   */
  private[this] def fetchLocalBlocks() {

    //localBlocks是ArrayBuffer[BlockId]类型，即每个元素是一个BlockId。问题： 如何根据BlockId获得数据
    val iter = localBlocks.iterator
    while (iter.hasNext) {
      val blockId = iter.next()
      try {

        //buf是ManagedBuffer类型
        val buf = blockManager.getBlockData(blockId)
        shuffleMetrics.incLocalBlocksFetched(1)
        shuffleMetrics.incLocalBytesRead(buf.size)
        buf.retain()

        /** *
          * 将buf包装为SuccessFetchResult
          */
        results.put(new SuccessFetchResult(blockId, blockManager.blockManagerId, 0, buf))
      } catch {
        case e: Exception =>
          // If we see an exception, stop immediately.
          logError(s"Error occurred while fetching local blocks", e)
          results.put(new FailureFetchResult(blockId, blockManager.blockManagerId, e))
          return
      }
    }
  }

  /** *
    * 初始化是发起请求
    */
  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    context.addTaskCompletionListener(_ => cleanup())

    // Split local and remote blocks.

    /** *
      * 将local和remote的blocks分开，local blocks在本地读取即可；remote blocks需要远程抓取数据
      */
    val remoteRequests = splitLocalRemoteBlocks()


    // Add the remote requests into our queue in a random order
    //Remote Request
    remoteFetchRequests ++= Utils.randomize(remoteRequests)

    // Send out initial requests for blocks, up to our maxBytesInFlight
    //在 ShuffleBlockFetcherIterator的初始化方法中就发起Fetch数据的请求
    fetchUpToMaxBytes()

    val numFetches = remoteRequests.size - remoteFetchRequests.size
    logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))

    // Get Local Blocks
    fetchLocalBlocks()
    logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime))
  }

  /**
   * 判断是否还有可遍历的元素的逻辑
   * @return
   */
  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch

  /**
   * Fetches the next (BlockId, InputStream). If a task fails, the ManagedBuffers
   * underlying each InputStream will be freed by the cleanup() method registered with the
   * TaskCompletionListener. However, callers should close() these InputStreams
   * as soon as they are no longer needed, in order to release memory as early as possible.
   *
   * Throws a FetchFailedException if the next block could not be fetched.
   *
   *
   * InputStream是Block数据对应的输入流，可能是文件输入流或者network IO输入流
   */
  override def next(): (BlockId, InputStream) = {
    numBlocksProcessed += 1
    val startFetchWait = System.currentTimeMillis()

    /**
     * 阻塞等待results队列中有一个FetchResult,取出一个FetchRequest
     */
    currentResult = results.take()
    val result = currentResult
    val stopFetchWait = System.currentTimeMillis()
    shuffleMetrics.incFetchWaitTime(stopFetchWait - startFetchWait)

    result match {
      case SuccessFetchResult(_, _, size, _) => bytesInFlight -= size
      case _ =>
    }

    /** *
      * Send fetch requests up to maxBytesInFlight
      *
      * 请求得到结果后，继续发起请求
      */
    fetchUpToMaxBytes()

    result match {
      case FailureFetchResult(blockId, address, e) =>
        throwFetchFailedException(blockId, address, e)

      /**
       * 如果Fetch Result成功，那么就构造二元组，blockId和InputStream
       */
      case SuccessFetchResult(blockId, address, _, buf) =>
        try {
          (result.blockId, new BufferReleasingInputStream(buf.createInputStream(), this))
        } catch {
          case NonFatal(t) =>
            throwFetchFailedException(blockId, address, t)
        }
    }
  }

  /**
   * 循环发送请求，循环的条件是：
   * remoteFetchRequests队列中还有未fetch的数据(FetchRequest)
   *
   * bytesInFlight==0表示什么条件？如果bytesInFlight==0表示是第一个Request，因此无需判断是否maxBytesInFlight
   *
   *
   * remoteFetchRequests.front表示第一个元素,该元素类型是FetchRequest，它有一个size属性
   */
  private def fetchUpToMaxBytes(): Unit = {
    // Send fetch requests up to maxBytesInFlight
    while (remoteFetchRequests.nonEmpty &&
      (bytesInFlight == 0 || bytesInFlight + remoteFetchRequests.front.size <= maxBytesInFlight)) {

      /** *
        * 从队列中取出一个FetchRequest发送请求，同时更新bytesInFlight的值
        */
      sendRequest(remoteFetchRequests.dequeue())
    }
  }

  private def throwFetchFailedException(blockId: BlockId, address: BlockManagerId, e: Throwable) = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, e)
      case _ =>
        throw new SparkException(
          "Failed to get block " + blockId + ", which is not a shuffle block", e)
    }
  }
}

/**
 * Helper class that ensures a ManagedBuffer is released upon InputStream.close()
 */
private class BufferReleasingInputStream(
    private val delegate: InputStream,
    private val iterator: ShuffleBlockFetcherIterator)
  extends InputStream {
  private[this] var closed = false

  override def read(): Int = delegate.read()

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      iterator.releaseCurrentResultBuffer()
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long = delegate.skip(n)

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = delegate.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = delegate.read(b, off, len)

  override def reset(): Unit = delegate.reset()
}

private[storage]
object ShuffleBlockFetcherIterator {

  /**
   * A request to fetch blocks from a remote BlockManager.
   * @param address remote BlockManager to fetch from.
   * @param blocks Sequence of tuple, where the first element is the block id,
   *               and the second element is the estimated size, used to calculate bytesInFlight.
   */
  case class FetchRequest(address: BlockManagerId, blocks: Seq[(BlockId, Long)]) {
    val size = blocks.map(_._2).sum
  }

  /**
   * Result of a fetch from a remote block.
   */
  private[storage] sealed trait FetchResult {
    val blockId: BlockId
    val address: BlockManagerId
  }

  /**
   * Result of a fetch from a remote block successfully.
   * @param blockId block id
   * @param address BlockManager that the block was fetched from.
   * @param size estimated size of the block, used to calculate bytesInFlight.
   *             Note that this is NOT the exact bytes.
   * @param buf [[ManagedBuffer]] for the content.
   */
  private[storage] case class SuccessFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      size: Long,
      buf: ManagedBuffer)
    extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }

  /**
   * Result of a fetch from a remote block unsuccessfully.
   * @param blockId block id
   * @param address BlockManager that the block was attempted to be fetched from
   * @param e the failure exception
   */
  private[storage] case class FailureFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      e: Throwable)
    extends FetchResult
}
