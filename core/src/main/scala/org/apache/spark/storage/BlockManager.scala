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

import java.io._
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Random
import scala.util.control.NonFatal

import sun.nio.ch.DirectBuffer

import org.apache.spark._
import org.apache.spark.executor.{DataReadMethod, ShuffleWriteMetrics}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.memory.MemoryManager
import org.apache.spark.network._
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.ExternalShuffleClient
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.serializer.{Serializer, SerializerInstance}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.util._

private[spark] sealed trait BlockValues
private[spark] case class ByteBufferValues(buffer: ByteBuffer) extends BlockValues
private[spark] case class IteratorValues(iterator: Iterator[Any]) extends BlockValues
private[spark] case class ArrayValues(buffer: Array[Any]) extends BlockValues

/* Class for returning a fetched block and associated metrics. */
private[spark] class BlockResult(
    val data: Iterator[Any],
    val readMethod: DataReadMethod.Value,
    val bytes: Long)

/**
 * Manager running on every node (driver and executors) which provides interfaces for putting and
 * retrieving blocks both locally and remotely into various stores (memory, disk, and off-heap).
 *
 * Note that [[initialize()]] must be called before the BlockManager is usable.
  *
  * BlockManager构造方法持有BlockManagerMaster，在成员变量中持有一个slaveEndpoint（BlockManagerSlaveEndpoint）
  *
  *
  * @param executorId
  * @param rpcEnv
  * @param master
  * @param defaultSerializer
  * @param conf
  * @param memoryManager
  * @param mapOutputTracker
  * @param shuffleManager
  * @param blockTransferService
  * @param securityManager
  * @param numUsableCores
  */
private[spark] class BlockManager(
    executorId: String,
    rpcEnv: RpcEnv,
    val master: BlockManagerMaster,
    defaultSerializer: Serializer,
    val conf: SparkConf,
    memoryManager: MemoryManager,
    mapOutputTracker: MapOutputTracker,
    shuffleManager: ShuffleManager,
    blockTransferService: BlockTransferService,
    securityManager: SecurityManager,
    numUsableCores: Int)
  extends BlockDataManager with Logging {

  val diskBlockManager = new DiskBlockManager(this, conf)

  /***
    * BlockId与BlockInfo的对应关系，如果一个Block在内存放不下而存储级别是Memory_Only，那么将它删除
    */
  private val blockInfo = new ConcurrentHashMap[BlockId, BlockInfo]

  private val futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("block-manager-future", 128))

  // Actual storage of where blocks are kept
  /** *
    * 构造MemoryStore时，传入MemoryStore
    */
  private[spark] val memoryStore = new MemoryStore(this, memoryManager)
  private[spark] val diskStore = new DiskStore(this, diskBlockManager)

  /** *
    * 为MemoryManager设置MemoryStore，
    * 因为每个JVM进程只有一个BlockManager，而每个JVM也只有一个MemoryStorage，
    * 因此MemoryManager和BlockManager是1:1对应的
    */
  memoryManager.setMemoryStore(memoryStore)

  // Note: depending on the memory manager, `maxStorageMemory` may actually vary over time.
  // However, since we use this only for reporting and logging, what we actually want here is
  // the absolute maximum value that `maxStorageMemory` can ever possibly reach. We may need
  // to revisit whether reporting this value as the "max" is intuitive to the user.
  private val maxMemory = memoryManager.maxStorageMemory

  private[spark]
  val externalShuffleServiceEnabled = conf.getBoolean("spark.shuffle.service.enabled", false)

  // Port used by the external shuffle service. In Yarn mode, this may be already be
  // set through the Hadoop configuration as the server is launched in the Yarn NM.
  private val externalShuffleServicePort = {
    val tmpPort = Utils.getSparkOrYarnConfig(conf, "spark.shuffle.service.port", "7337").toInt
    if (tmpPort == 0) {
      // for testing, we set "spark.shuffle.service.port" to 0 in the yarn config, so yarn finds
      // an open port.  But we still need to tell our spark apps the right port to use.  So
      // only if the yarn config has the port set to 0, we prefer the value in the spark config
      conf.get("spark.shuffle.service.port").toInt
    } else {
      tmpPort
    }
  }

  var blockManagerId: BlockManagerId = _

  // Address of the server that serves this executor's shuffle files. This is either an external
  // service, or just our own Executor's BlockManager.
  private[spark] var shuffleServerId: BlockManagerId = _

  // Client to read other executors' shuffle files. This is either an external service, or just the
  // standard BlockTransferService to directly connect to other Executors.
  private[spark] val shuffleClient = if (externalShuffleServiceEnabled) {
    val transConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores)
    new ExternalShuffleClient(transConf, securityManager, securityManager.isAuthenticationEnabled(),
      securityManager.isSaslEncryptionEnabled())
  } else {
    blockTransferService
  }

  // Whether to compress broadcast variables that are stored
  private val compressBroadcast = conf.getBoolean("spark.broadcast.compress", true)
  //

  /** *
    *  Whether to compress shuffle output that are stored
    *  默认情况下是要压缩shuffle数据的
    */
  private val compressShuffle = conf.getBoolean("spark.shuffle.compress", true)
  // Whether to compress RDD partitions that are stored serialized
  private val compressRdds = conf.getBoolean("spark.rdd.compress", false)
  // Whether to compress shuffle output temporarily spilled to disk
  private val compressShuffleSpill = conf.getBoolean("spark.shuffle.spill.compress", true)

  /***
    * BlockManager中构造BlockManagerSlaveEndpoint，因为Driver和Executor都会创建BlockManager，因此
    * BlockManagerSlaveEndpoint在Driver和Executor上都会创建
    */
  private val slaveEndpoint = rpcEnv.setupEndpoint(
    "BlockManagerEndpoint" + BlockManager.ID_GENERATOR.next,
    new BlockManagerSlaveEndpoint(rpcEnv, this, mapOutputTracker))

  // Pending re-registration action being executed asynchronously or null if none is pending.
  // Accesses should synchronize on asyncReregisterLock.
  private var asyncReregisterTask: Future[Unit] = null
  private val asyncReregisterLock = new Object

  // Field related to peer block managers that are necessary for block replication
  @volatile private var cachedPeers: Seq[BlockManagerId] = _
  private val peerFetchLock = new Object
  private var lastPeerFetchTime = 0L

  /* The compression codec to use. Note that the "lazy" val is necessary because we want to delay
   * the initialization of the compression codec until it is first used. The reason is that a Spark
   * program could be using a user-defined codec in a third party jar, which is loaded in
   * Executor.updateDependencies. When the BlockManager is initialized, user level jars hasn't been
   * loaded yet. */
  private lazy val compressionCodec: CompressionCodec = CompressionCodec.createCodec(conf)

  /**
   * Initializes the BlockManager with the given appId. This is not performed in the constructor as
   * the appId may not be known at BlockManager instantiation time (in particular for the driver,
   * where it is only learned after registration with the TaskScheduler).
   *
   * This method initializes the BlockTransferService and ShuffleClient, registers with the
   * BlockManagerMaster, starts the BlockManagerWorker endpoint, and registers with a local shuffle
   * service if configured.
    *
    *
    * initialize方法不是在构造方法中创建的，而是创建完成后显式的调用
   */
  def initialize(appId: String): Unit = {
    blockTransferService.init(this)
    shuffleClient.init(appId)

    /***
      * 构造出BlockManager对应的BlockManagerId
      */
    blockManagerId = BlockManagerId(
      executorId, blockTransferService.hostName, blockTransferService.port)

    shuffleServerId = if (externalShuffleServiceEnabled) {
      logInfo(s"external shuffle service port = $externalShuffleServicePort")
      BlockManagerId(executorId, blockTransferService.hostName, externalShuffleServicePort)
    } else {
      blockManagerId
    }

    /***
      * 通过master注册BlockManager，master是BlockManagerMasterEndpoint或者BlockManagerMasterEndpoint的ref
      * driverEndpoint: NettyRpcEndpointRef(spark://BlockManagerMaster@10.12.167.42:50867)
      * isDriver true
      * master的类型是BlockManagerMaster,它持有blockManagerMasterEndpoint的ref
      */
    master.registerBlockManager(blockManagerId, maxMemory, slaveEndpoint)

    // Register Executors' configuration with the local shuffle service, if one should exist.
    if (externalShuffleServiceEnabled && !blockManagerId.isDriver) {
      registerWithExternalShuffleServer()
    }
  }

  private def registerWithExternalShuffleServer() {
    logInfo("Registering executor with local external shuffle service.")
    val shuffleConfig = new ExecutorShuffleInfo(
      diskBlockManager.localDirs.map(_.toString),
      diskBlockManager.subDirsPerLocalDir,
      shuffleManager.shortName)

    val MAX_ATTEMPTS = 3
    val SLEEP_TIME_SECS = 5

    for (i <- 1 to MAX_ATTEMPTS) {
      try {
        // Synchronous and will throw an exception if we cannot connect.
        shuffleClient.asInstanceOf[ExternalShuffleClient].registerWithShuffleServer(
          shuffleServerId.host, shuffleServerId.port, shuffleServerId.executorId, shuffleConfig)
        return
      } catch {
        case e: Exception if i < MAX_ATTEMPTS =>
          logError(s"Failed to connect to external shuffle server, will retry ${MAX_ATTEMPTS - i}"
            + s" more times after waiting $SLEEP_TIME_SECS seconds...", e)
          Thread.sleep(SLEEP_TIME_SECS * 1000)
      }
    }
  }

  /**
   * Report all blocks to the BlockManager again. This may be necessary if we are dropped
   * by the BlockManager and come back or if we become capable of recovering blocks on disk after
   * an executor crash.
   *
   * This function deliberately fails silently if the master returns false (indicating that
   * the slave needs to re-register). The error condition will be detected again by the next
   * heart beat attempt or new block registration and another try to re-register all blocks
   * will be made then.
   */
  private def reportAllBlocks(): Unit = {
    logInfo(s"Reporting ${blockInfo.size} blocks to the master.")
    for ((blockId, info) <- blockInfo.asScala) {
      val status = getCurrentBlockStatus(blockId, info)
      if (!tryToReportBlockStatus(blockId, info, status)) {
        logError(s"Failed to report $blockId to master; giving up.")
        return
      }
    }
  }

  /**
   * Re-register with the master and report all blocks to it. This will be called by the heart beat
   * thread if our heartbeat to the block manager indicates that we were not registered.
   *
   * Note that this method must be called without any BlockInfo locks held.
   */
  def reregister(): Unit = {
    // TODO: We might need to rate limit re-registering.
    logInfo("BlockManager re-registering with master")
    master.registerBlockManager(blockManagerId, maxMemory, slaveEndpoint)
    reportAllBlocks()
  }

  /**
   * Re-register with the master sometime soon.
   */
  private def asyncReregister(): Unit = {
    asyncReregisterLock.synchronized {
      if (asyncReregisterTask == null) {
        asyncReregisterTask = Future[Unit] {
          // This is a blocking action and should run in futureExecutionContext which is a cached
          // thread pool
          reregister()
          asyncReregisterLock.synchronized {
            asyncReregisterTask = null
          }
        }(futureExecutionContext)
      }
    }
  }

  /**
   * For testing. Wait for any pending asynchronous re-registration; otherwise, do nothing.
   */
  def waitForAsyncReregister(): Unit = {
    val task = asyncReregisterTask
    if (task != null) {
      Await.ready(task, Duration.Inf)
    }
  }

  /**
   * Interface to get local block data. Throws an exception if the block cannot be found or
   * cannot be read successfully.
   *
   *
   * 根据BlockId获取Block Data
   */
  override def getBlockData(blockId: BlockId): ManagedBuffer = {
    if (blockId.isShuffle) {
      shuffleManager.shuffleBlockResolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId])
    } else {
      val blockBytesOpt = doGetLocal(blockId, asBlockResult = false)
        .asInstanceOf[Option[ByteBuffer]]
      if (blockBytesOpt.isDefined) {
        val buffer = blockBytesOpt.get
        new NioManagedBuffer(buffer)
      } else {
        throw new BlockNotFoundException(blockId.toString)
      }
    }
  }

  /**
   * Put the block locally, using the given storage level.
   */
  override def putBlockData(blockId: BlockId, data: ManagedBuffer, level: StorageLevel): Unit = {
    putBytes(blockId, data.nioByteBuffer(), level)
  }

  /**
   * Get the BlockStatus for the block identified by the given ID, if it exists.
   * NOTE: This is mainly for testing, and it doesn't fetch information from external block store.
   */
  def getStatus(blockId: BlockId): Option[BlockStatus] = {
    blockInfo.asScala.get(blockId).map { info =>
      val memSize = if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
      val diskSize = if (diskStore.contains(blockId)) diskStore.getSize(blockId) else 0L
      BlockStatus(info.level, memSize = memSize, diskSize = diskSize)
    }
  }

  /**
   * Get the ids of existing blocks that match the given filter. Note that this will
   * query the blocks stored in the disk block manager (that the block manager
   * may not know of).
   */
  def getMatchingBlockIds(filter: BlockId => Boolean): Seq[BlockId] = {
    (blockInfo.asScala.keys ++ diskBlockManager.getAllBlocks()).filter(filter).toSeq
  }

  /**
   * Tell the master about the current storage status of a block. This will send a block update
   * message reflecting the current status, *not* the desired storage level in its block info.
   * For example, a block with MEMORY_AND_DISK set might have fallen out to be only on disk.
   *
   * droppedMemorySize exists to account for when the block is dropped from memory to disk (so
   * it is still valid). This ensures that update in master will compensate for the increase in
   * memory on slave.
    *
    * 向Driver汇报本Executor的BlockManager的状态
    *
    * @param blockId
    * @param info
    * @param status
    * @param droppedMemorySize
    */
  private def reportBlockStatus(
      blockId: BlockId,
      info: BlockInfo,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Unit = {
    val successReportBlockStatus = tryToReportBlockStatus(blockId, info, status, droppedMemorySize)

    /***
      * report失败，那么需要重新注册
      */
    val needReregister = !successReportBlockStatus

    /**
      * 如果需要重新，那么将汇报所有block的状态
      */
    if (needReregister) {
      logInfo(s"Got told to re-register updating block $blockId")
      // Re-registering will report our new block for free.
      asyncReregister()
    }
    logDebug(s"Told master about block $blockId")
  }

  /**
   * Actually send a UpdateBlockInfo message. Returns the master's response,
   * which will be true if the block was successfully recorded and false if
   * the slave needs to re-register.
    *
    * BlockManager调用blockManagerMaster将外界发布信息
   */
  private def tryToReportBlockStatus(
      blockId: BlockId,
      info: BlockInfo,
      status: BlockStatus,
      droppedMemorySize: Long = 0L): Boolean = {
    /***
      * 是否tellMaster也记录在了BlockInfo中了
      */
    if (info.tellMaster) {
      val storageLevel = status.storageLevel
      val inMemSize = Math.max(status.memSize, droppedMemorySize)
      val onDiskSize = status.diskSize

      /***
        * 调用master的updateBlockInfo方法,master是BlockManagerMaster类型
        */
      master.updateBlockInfo(blockManagerId, blockId, storageLevel, inMemSize, onDiskSize)
    } else {
      true
    }
  }

  /**
   * Return the updated storage status of the block with the given ID. More specifically, if
   * the block is dropped from memory and possibly added to disk, return the new storage level
   * and the updated in-memory and on-disk sizes.
    *
    * 更新BlockStatus，包括存储级别以及内存和磁盘的使用量
    *
    * @param blockId
    * @param info
    * @return
    */
  private def getCurrentBlockStatus(blockId: BlockId, info: BlockInfo): BlockStatus = {
    info.synchronized {

      /***
        * 1. 如果存储级别为memory only而block从内存擦除，如何更新？ StorageLevel为(false,false,false,1)
        * 2. 如果存储级别为block + memory，而block从内存擦除，如何更新？ StorageLevel为(true,false,false,原来的存储级别)
        *
        * 以上两种情况都会将blockId从memoryStore中删除（通过 memoryStore.remove(blockId)）
        */
      info.level match {
        case null =>
          BlockStatus(StorageLevel.NONE, memSize = 0L, diskSize = 0L)
        case level =>
          val inMem = level.useMemory && memoryStore.contains(blockId)
          val onDisk = level.useDisk && diskStore.contains(blockId) //diskStore.contains(blockId)是通过检查文件是否存在的方式
          val deserialized = if (inMem) level.deserialized else false //如果是内存，则判断level是否保持原始数据；其它情况都是使用序列化数据(比如存放到磁盘)
          val replication = if (inMem  || onDisk) level.replication else 1
          val storageLevel =
            StorageLevel(onDisk, inMem, deserialized, replication)
          val memSize = if (inMem) memoryStore.getSize(blockId) else 0L
          val diskSize = if (onDisk) diskStore.getSize(blockId) else 0L

          BlockStatus(storageLevel, memSize, diskSize)
      }
    }
  }

  /**
   * Get locations of an array of blocks.
   */
  private def getLocationBlockIds(blockIds: Array[BlockId]): Array[Seq[BlockManagerId]] = {
    val startTimeMs = System.currentTimeMillis
    val locations = master.getLocations(blockIds).toArray
    logDebug("Got multiple block location in %s".format(Utils.getUsedTimeMs(startTimeMs)))
    locations
  }

  /**
   * Get block from local block manager.
   */
  def getLocal(blockId: BlockId): Option[BlockResult] = {
    logDebug(s"Getting local block $blockId")
    doGetLocal(blockId, asBlockResult = true).asInstanceOf[Option[BlockResult]]
  }

  /**
   * Get block from the local block manager as serialized bytes.
   */
  def getLocalBytes(blockId: BlockId): Option[ByteBuffer] = {
    logDebug(s"Getting local block $blockId as bytes")
    // As an optimization for map output fetches, if the block is for a shuffle, return it
    // without acquiring a lock; the disk store never deletes (recent) items so this should work
    if (blockId.isShuffle) {
      val shuffleBlockResolver = shuffleManager.shuffleBlockResolver
      // TODO: This should gracefully handle case where local block is not available. Currently
      // downstream code will throw an exception.
      Option(
        shuffleBlockResolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId]).nioByteBuffer())
    } else {
      doGetLocal(blockId, asBlockResult = false).asInstanceOf[Option[ByteBuffer]]
    }
  }

  /***
    * 从本地Executor的BlockManager中获取blockId对应的数据
    * @param blockId
    * @param asBlockResult 返回值是BlockResult还是ByteBuffer，如果asBlockResult则表示返回值是BlockResult类型，否则是ByteBuffer类型
    * @return
    */
  private def doGetLocal(blockId: BlockId, asBlockResult: Boolean): Option[Any] = {

    /***
      * 如果info为null，表示数据不在本地executor的BlockManager
      */
    val info = blockInfo.get(blockId)
    if (info != null) {

      /**
        * 对info加锁，那么因为removeBlock也是对info枷锁，所以在加锁获取过程中，removeBlock就得等待
        */
      info.synchronized {
        // Double check to make sure the block is still there. There is a small chance that the
        // block has been removed by removeBlock (which also synchronizes on the blockInfo object).
        // Note that this only checks metadata tracking. If user intentionally deleted the block
        // on disk or from off heap storage without using removeBlock, this conditional check will
        // still pass but eventually we will get an exception because we can't find the block.
        if (blockInfo.asScala.get(blockId).isEmpty) {
          logWarning(s"Block $blockId had been removed")
          return None
        }

        // If another thread is writing the block, wait for it to become ready.
        if (!info.waitForReady()) {
          // If we get here, the block write failed.
          logWarning(s"Block $blockId was marked as failure.")
          return None
        }

        val level = info.level
        logDebug(s"Level for block $blockId is $level")

        /***
          * Block使用了内存存储
          */
        if (level.useMemory) {
          logDebug(s"Getting block $blockId from memory")
          /***
            * 从memoryStore中读取，获取RDD[T]的一个分区数据时设置了asBlockResult为true，因此通过memoryStore.getValues方法获取
            * 如果是获取二进制数据，那么使用memoryStore.getBytes获取
            */
          val result = if (asBlockResult) {
            memoryStore.getValues(blockId).map(new BlockResult(_, DataReadMethod.Memory, info.size))
          } else {
            memoryStore.getBytes(blockId)
          }
          result match {
            case Some(values) =>
              return result
            case None =>
              logDebug(s"Block $blockId not found in memory")
          }
        }

        /***
          * 如果内存没有，但是Block设置了磁盘存储级别，如果Block设置了Memory+Disk存储级别，那么代码走到这里表示内存没有放下这个Block而仅仅存放在磁盘上
          */
        if (level.useDisk) {

          /***
            * 从磁盘中获取
            */
          logDebug(s"Getting block $blockId from disk")
          val bytes: ByteBuffer = diskStore.getBytes(blockId) match {
            case Some(b) => b
            case None =>
              throw new BlockException(
                blockId, s"Block $blockId not found on disk, though it should be")
          }
          assert(0 == bytes.position())

          /***
            * 如果Block仅仅设置了磁盘级别，而没有设置内存级别，那么根据asBlockResult设置返回BlockResult还是ByteBuffer
            */
          if (!level.useMemory) {
            // If the block shouldn't be stored in memory, we can just return it
            if (asBlockResult) {
              return Some(new BlockResult(dataDeserialize(blockId, bytes), DataReadMethod.Disk,
                info.size))
            } else {
              return Some(bytes)
            }
          }

          /***
            * 如果是使用了内存存储级别，代码走到这个逻辑，表示Block是内存+磁盘的存储级别
            */
          else {
            // Otherwise, we also have to store something in the memory store
            /**
              * 如果Block数据是二进制形式(asBlockResult为false或者level.deserialized两个都可以进行判断)
              * 将二进制数据写入到内存中
              *
              */
            if (!level.deserialized || !asBlockResult) {
              /* We'll store the bytes in memory if the block's storage level includes
               * "memory serialized", or if it should be cached as objects in memory
               * but we only requested its serialized bytes. */
              memoryStore.putBytes(blockId, bytes.limit, () => {
                // https://issues.apache.org/jira/browse/SPARK-6076
                // If the file size is bigger than the free memory, OOM will happen. So if we cannot
                // put it into MemoryStore, copyForMemory should not be created. That's why this
                // action is put into a `() => ByteBuffer` and created lazily.
                val copyForMemory = ByteBuffer.allocate(bytes.limit)
                copyForMemory.put(bytes)
              })
              bytes.rewind()
            }

            /**
              * 如果!asBlockResult为真，一方面数据在前面写入内存；另一方面，不需要进行反序列化直接返回
              */
            if (!asBlockResult) {
              return Some(bytes)
            } else {

              /***
                * 如果asBlockResult为true，那么在level.deserialized为false时，数据已经写到内存，而level.deserialized为true时还没有写入
                * 发序列化
                */
              val values = dataDeserialize(blockId, bytes)

              /***
                * 存储的是原始数据，此时数据还没有写到缓存，通过putIterator写到内存
                */
              if (level.deserialized) {
                // Cache the values before returning them
                val putResult = memoryStore.putIterator(
                  blockId, values, level, returnValues = true, allowPersistToDisk = false)
                // The put may or may not have succeeded, depending on whether there was enough
                // space to unroll the block. Either way, the put here should return an iterator.
                putResult.data match {
                  case Left(it) =>
                    return Some(new BlockResult(it, DataReadMethod.Disk, info.size))
                  case _ =>
                    // This only happens if we dropped the values back to disk (which is never)
                    throw new SparkException("Memory store did not return an iterator!")
                }
              }

              /***
                * 存储的是二进制数据，此时数据在前面已经写到缓存，此时直接返回
                */
              else {
                return Some(new BlockResult(values, DataReadMethod.Disk, info.size))
              }
            }
          }
        }
      }
    } else {
      logDebug(s"Block $blockId not registered locally")
    }
    None
  }

  /***
    * Get block from remote block managers.
    *
    * 从远端获取数据，对于RDDBlockId这种存放Java对象的Block而言，应该调用这个方法
    * @param blockId
    * @return
    */
  def getRemote(blockId: BlockId): Option[BlockResult] = {
    logDebug(s"Getting remote block $blockId")
    doGetRemote(blockId, asBlockResult = true).asInstanceOf[Option[BlockResult]]
  }

  /**
   * Get block from remote block managers as serialized bytes.
    *
    * 从Remote的BlockManager获取一个Block(该Block的ID是blockId),获取结果是一个ByteBuffer
    *
    * @param blockId
    * @return
    */
  def getRemoteBytes(blockId: BlockId): Option[ByteBuffer] = {
    logDebug(s"Getting remote block $blockId as bytes")
    doGetRemote(blockId, asBlockResult = false).asInstanceOf[Option[ByteBuffer]]
  }

  /**
   * Return a list of locations for the given block, prioritizing the local machine since
   * multiple block managers can share the same host.
    *
    * 调用BlockManagerMaster的getLocations获取blockId对应的BlockManagerId集合
    *
    * @param blockId
    * @return  Seq[BlockManagerId]
    */
  private def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    val locations = master.getLocations(blockId)
    val locs = Random.shuffle(locations)

    /***
      * 按照BlockMangerId是否在本机进行分开，优先选择本机的含义是什么？一个BlockId的数据完全属于一个BlockManagerId，如果数据有备份
      * 那么会放在多个BlockManagerId中，
      * 也就是说，最后只会使用一个BlockManagerId(优先从本机读取）
      */
    val (preferredLocs, otherLocs) = locs.partition { loc => blockManagerId.host == loc.host }
    preferredLocs ++ otherLocs
  }

  /***
    * 从远端获取Block数据
    * 问题：
    * 1. 从远端获取过来的数据是否会存到本地， --- 否
    * 2. 因为远端的Block由于存储的策略，可能配置多份，那么具体从哪个machine获取，这个策略如何定义的？ ---只是优先从本机其它的blockmanager读取，其它情况下是随机选取一个机器进行读取
    * 3. 如果本地机器有多个executor，那么本地机器就有多个blockmanager，属于另一个executor的blockmanager是否也算是remote的？ 如果算是，如何优先从本机的blockmanager读取？
    *    --算是remote blockmanager，优先从本机blockmanager读取
    * @param blockId
    * @param asBlockResult 如果asBlockResult会false，则返回的是Option[ByteBuffer]
    * @return 如果从远程机器没有读取到，则返回None
    */
  private def doGetRemote(blockId: BlockId, asBlockResult: Boolean): Option[Any] = {
    require(blockId != null, "BlockId is null")

    /***
      * 根据BlockId获取数据存放的位置，locations是BlockManagerId的集合
      */
    val locations = getLocations(blockId)
    var numFetchFailures = 0

    /***
      * 遍历所有的BlockManagerId
      */
    for (loc <- locations) {
      logDebug(s"Getting remote block $blockId from $loc")
      val data = try {
        /***
          * 同步fetch数据，从指定的host，port上(host+port怎么知道要读取多少数据？)
          */
        val byteBuffer = blockTransferService.fetchBlockSync(
          loc.host, loc.port, loc.executorId, blockId.toString)
          byteBuffer.nioByteBuffer()
      } catch {
        case NonFatal(e) =>
          numFetchFailures += 1

          /***
            * 读取所有位置的数据都败了，表示数据没有读到
            */
          if (numFetchFailures == locations.size) {
            // An exception is thrown while fetching this block from all locations
            throw new BlockFetchException(s"Failed to fetch block from" +
              s" ${locations.size} locations. Most recent failure cause:", e)
          } else {
            // This location failed, so we retry fetch from a different one by returning null here
            /***
              * 从loc读取失败，下次循环从下一个loc读取，此处记录警告信息
              */
            logWarning(s"Failed to fetch remote block $blockId " +
              s"from $loc (failed attempt $numFetchFailures)", e)
            null
          }
      }

      /***
        * data ！= null表示从loc这个位置fetch到了数据
        */
      if (data != null) {

        /***
          * 如果要求返回BlockResult，则进行数据反序列化(因为数据是从远端读取过来的，读取到的数据必然是二进制数据）
          */
        if (asBlockResult) {
          return Some(new BlockResult(
            dataDeserialize(blockId, data),
            DataReadMethod.Network,
            data.limit()))
        } else {
          /***
            * 数据原样返回
            */
          return Some(data)
        }
      }
      logDebug(s"The value of block $blockId is null")
    }

    /***
      * 如果没有读取到，那么打印日志，返回None
      */
    logDebug(s"Block $blockId not found")
    None
  }

  /***
    * Get a block from the block manager (either local or remote).
    *
    * 首先尝试从本地读取，如果本地读取不到，则从远端读取
    * @param blockId
    * @return
    */
  def get(blockId: BlockId): Option[BlockResult] = {

    //从本地获取
    val local = getLocal(blockId)
    if (local.isDefined) {
      logInfo(s"Found block $blockId locally")
      return local
    }
    val remote = getRemote(blockId)
    if (remote.isDefined) {
      logInfo(s"Found block $blockId remotely")
      return remote
    }
    None
  }

  /**
   * @return true if the block was stored or false if the block was already stored or an
   *         error occurred.
   */
  def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      tellMaster: Boolean = true,
      effectiveStorageLevel: Option[StorageLevel] = None): Boolean = {
    require(values != null, "Values is null")
    doPut(blockId, IteratorValues(values), level, tellMaster, effectiveStorageLevel)
  }

  /**
   * A short circuited method to get a block writer that can write data directly to disk.
   * The Block will be appended to the File specified by filename. Callers should handle error
   * cases.
    * @param blockId
    * @param file
    * @param serializerInstance
    * @param bufferSize
    * @param writeMetrics
    * @return
    */
  def getDiskWriter(
      blockId: BlockId,
      file: File,
      serializerInstance: SerializerInstance,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetrics): DiskBlockObjectWriter = {

    /** *
      * 将输出流进行压缩，函数字面量
      */
    val compressStream: OutputStream => OutputStream = wrapForCompression(blockId, _)
    val syncWrites = conf.getBoolean("spark.shuffle.sync", false)

    /** *
      * 构造DiskBlockObjectWriter
      */
    new DiskBlockObjectWriter(file, serializerInstance, bufferSize, compressStream,
      syncWrites, writeMetrics, blockId)
  }

  /**
   * Put a new block of values to the block manager.
   *
   * @return true if the block was stored or false if the block was already stored or an
   *         error occurred.
   */
  def putArray(
      blockId: BlockId,
      values: Array[Any],
      level: StorageLevel,
      tellMaster: Boolean = true,
      effectiveStorageLevel: Option[StorageLevel] = None): Boolean = {
    require(values != null, "Values is null")
    doPut(blockId, ArrayValues(values), level, tellMaster, effectiveStorageLevel)
  }

  /**
   * Put a new block of serialized bytes to the block manager.
   *
   * 将二进制流写入BlockManager
   * 问题：
   * 1. 如果缓存到内存，而内存不够用，此时是什么策略？直接OOM还是剔除在内存中停留时间最长的RDD占用的Memory
   * 2. 当副本数大于1时，选择哪台机器上进行复制
   *
   * @return true if the block was stored or false if the block was already stored or an
   *         error occurred.
    *
    * @param blockId
    * @param bytes
    * @param level
    * @param tellMaster tellMaster为true表示什么意思？
    * @param effectiveStorageLevel 有效存储级别，什么是有效存储级别
    * @return
    */
  def putBytes(
      blockId: BlockId,
      bytes: ByteBuffer,
      level: StorageLevel,
      tellMaster: Boolean = true,
      effectiveStorageLevel: Option[StorageLevel] = None): Boolean = {
    require(bytes != null, "Bytes is null")

    /** *
      *
      */
    doPut(blockId, ByteBufferValues(bytes), level, tellMaster, effectiveStorageLevel)
  }

  /**
   * Put the given block according to the given level in one of the block stores, replicating
   * the values if necessary.
   *
   * The effective storage level refers to the level according to which the block will actually be
   * handled. This allows the caller to specify an alternate behavior of doPut while preserving
   * the original level specified by the user.
   *
   * 此处解释了什么是有效存储级别。有效存储级别指得是
   *
   * @return true if the block was stored or false if the block was already stored or an
   *         error occurred.
    *
    * @param blockId
    * @param data
    * @param level
    * @param tellMaster
    * @param effectiveStorageLevel
    * @return
    */
  private def doPut(
      blockId: BlockId,
      data: BlockValues,
      level: StorageLevel,
      tellMaster: Boolean = true,
      effectiveStorageLevel: Option[StorageLevel] = None): Boolean = {

    require(blockId != null, "BlockId is null")
    require(level != null && level.isValid, "StorageLevel is null or invalid")
    effectiveStorageLevel.foreach { level =>
      require(level != null && level.isValid, "Effective StorageLevel is null or invalid")
    }

    /* Remember the block's storage level so that we can correctly drop it to disk if it needs
     * to be dropped right after it got put into memory. Note, however, that other threads will
     * not be able to get() this block until we call markReady on its BlockInfo. */
    val putBlockInfo = {
      val tinfo = new BlockInfo(level, tellMaster)
      // Do atomically !
      val oldBlockOpt = Option(blockInfo.putIfAbsent(blockId, tinfo))
      if (oldBlockOpt.isDefined) {
        if (oldBlockOpt.get.waitForReady()) {
          logWarning(s"Block $blockId already exists on this machine; not re-adding it")
          return false
        }
        // TODO: So the block info exists - but previous attempt to load it (?) failed.
        // What do we do now ? Retry on it ?
        oldBlockOpt.get
      } else {
        tinfo
      }
    }

    val startTimeMs = System.currentTimeMillis

    /* If we're storing values and we need to replicate the data, we'll want access to the values,
     * but because our put will read the whole iterator, there will be no values left. For the
     * case where the put serializes data, we'll remember the bytes, above; but for the case where
     * it doesn't, such as deserialized storage, let's rely on the put returning an Iterator. */
    var valuesAfterPut: Iterator[Any] = null

    // Ditto for the bytes after the put
    var bytesAfterPut: ByteBuffer = null

    // Size of the block in bytes
    var size = 0L

    // The level we actually use to put the block
    val putLevel = effectiveStorageLevel.getOrElse(level)

    // If we're storing bytes, then initiate the replication before storing them locally.
    // This is faster as data is already serialized and ready to send.
    val replicationFuture = data match {
      case b: ByteBufferValues if putLevel.replication > 1 =>
        // Duplicate doesn't copy the bytes, but just creates a wrapper
        val bufferView = b.buffer.duplicate()
        Future {
          // This is a blocking action and should run in futureExecutionContext which is a cached
          // thread pool
          replicate(blockId, bufferView, putLevel)
        }(futureExecutionContext)
      case _ => null
    }

    var marked = false

    putBlockInfo.synchronized {
      logTrace("Put for block %s took %s to get into synchronized block"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))

      try {
        // returnValues - Whether to return the values put
        // blockStore - The type of storage to put these values into
        /**
          * blockStore可以是MemoryStore，也可以是DiskStore
          *
        */
        val (returnValues, blockStore: BlockStore) = {
          if (putLevel.useMemory) {
            // Put it in memory first, even if it also has useDisk set to true;
            // We will drop it to disk later if the memory store can't hold it.
            (true, memoryStore)
          } else if (putLevel.useDisk) {
            // Don't get back the bytes from put unless we replicate them
            (putLevel.replication > 1, diskStore)
          } else {
            assert(putLevel == StorageLevel.NONE)
            throw new BlockException(
              blockId, s"Attempted to put block $blockId without specifying storage level!")
          }
        }

        // Actually put the values
        val result = data match {
          case IteratorValues(iterator) =>
            blockStore.putIterator(blockId, iterator, putLevel, returnValues)
          case ArrayValues(array) =>
            blockStore.putArray(blockId, array, putLevel, returnValues)
          case ByteBufferValues(bytes) =>
            bytes.rewind()
            blockStore.putBytes(blockId, bytes, putLevel)
        }
        size = result.size
        result.data match {
          case Left (newIterator) if putLevel.useMemory => valuesAfterPut = newIterator
          case Right (newBytes) => bytesAfterPut = newBytes
          case _ =>
        }

        val putBlockStatus = getCurrentBlockStatus(blockId, putBlockInfo)
        if (putBlockStatus.storageLevel != StorageLevel.NONE) {
          // Now that the block is in either the memory, externalBlockStore, or disk store,
          // let other threads read it, and tell the master about it.
          marked = true

          putBlockInfo.markReady(size)

          /***
            * 通知Driver Block Manager,本Executor的BlockManager写入了一个Block，BlockId是blockId
            * 问题：什么时候可以不通知？
            *
            */
          if (tellMaster) {
            reportBlockStatus(blockId, putBlockInfo, putBlockStatus)
          }
          Option(TaskContext.get()).foreach { c =>
            c.taskMetrics().incUpdatedBlockStatuses(Seq((blockId, putBlockStatus)))
          }
        }
      } finally {
        // If we failed in putting the block to memory/disk, notify other possible readers
        // that it has failed, and then remove it from the block info map.
        if (!marked) {
          // Note that the remove must happen before markFailure otherwise another thread
          // could've inserted a new BlockInfo before we remove it.
          blockInfo.remove(blockId)
          putBlockInfo.markFailure()
          logWarning(s"Putting block $blockId failed")
        }
      }
    }
    logDebug("Put block %s locally took %s".format(blockId, Utils.getUsedTimeMs(startTimeMs)))

    // Either we're storing bytes and we asynchronously started replication, or we're storing
    // values and need to serialize and replicate them now:
    if (putLevel.replication > 1) {
      data match {
        case ByteBufferValues(bytes) =>
          if (replicationFuture != null) {
            Await.ready(replicationFuture, Duration.Inf)
          }
        case _ =>
          val remoteStartTime = System.currentTimeMillis
          // Serialize the block if not already done
          if (bytesAfterPut == null) {
            if (valuesAfterPut == null) {
              throw new SparkException(
                "Underlying put returned neither an Iterator nor bytes! This shouldn't happen.")
            }
            bytesAfterPut = dataSerialize(blockId, valuesAfterPut)
          }
          replicate(blockId, bytesAfterPut, putLevel)
          logDebug("Put block %s remotely took %s"
            .format(blockId, Utils.getUsedTimeMs(remoteStartTime)))
      }
    }

    BlockManager.dispose(bytesAfterPut)

    if (putLevel.replication > 1) {
      logDebug("Putting block %s with replication took %s"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))
    } else {
      logDebug("Putting block %s without replication took %s"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))
    }

    marked
  }

  /**
   * Get peer block managers in the system.
   */
  private def getPeers(forceFetch: Boolean): Seq[BlockManagerId] = {
    peerFetchLock.synchronized {
      val cachedPeersTtl = conf.getInt("spark.storage.cachedPeersTtl", 60 * 1000) // milliseconds
      val timeout = System.currentTimeMillis - lastPeerFetchTime > cachedPeersTtl
      if (cachedPeers == null || forceFetch || timeout) {
        cachedPeers = master.getPeers(blockManagerId).sortBy(_.hashCode)
        lastPeerFetchTime = System.currentTimeMillis
        logDebug("Fetched peers from master: " + cachedPeers.mkString("[", ",", "]"))
      }
      cachedPeers
    }
  }

  /**
   * Replicate block to another node. Not that this is a blocking call that returns after
   * the block has been replicated.
   */
  private def replicate(blockId: BlockId, data: ByteBuffer, level: StorageLevel): Unit = {
    val maxReplicationFailures = conf.getInt("spark.storage.maxReplicationFailures", 1)
    val numPeersToReplicateTo = level.replication - 1
    val peersForReplication = new ArrayBuffer[BlockManagerId]
    val peersReplicatedTo = new ArrayBuffer[BlockManagerId]
    val peersFailedToReplicateTo = new ArrayBuffer[BlockManagerId]
    val tLevel = StorageLevel(level.useDisk, level.useMemory, level.deserialized, 1)
    val startTime = System.currentTimeMillis
    val random = new Random(blockId.hashCode)

    var replicationFailed = false
    var failures = 0
    var done = false

    // Get cached list of peers
    peersForReplication ++= getPeers(forceFetch = false)

    // Get a random peer. Note that this selection of a peer is deterministic on the block id.
    // So assuming the list of peers does not change and no replication failures,
    // if there are multiple attempts in the same node to replicate the same block,
    // the same set of peers will be selected.
    def getRandomPeer(): Option[BlockManagerId] = {
      // If replication had failed, then force update the cached list of peers and remove the peers
      // that have been already used
      if (replicationFailed) {
        peersForReplication.clear()
        peersForReplication ++= getPeers(forceFetch = true)
        peersForReplication --= peersReplicatedTo
        peersForReplication --= peersFailedToReplicateTo
      }
      if (!peersForReplication.isEmpty) {
        Some(peersForReplication(random.nextInt(peersForReplication.size)))
      } else {
        None
      }
    }

    // One by one choose a random peer and try uploading the block to it
    // If replication fails (e.g., target peer is down), force the list of cached peers
    // to be re-fetched from driver and then pick another random peer for replication. Also
    // temporarily black list the peer for which replication failed.
    //
    // This selection of a peer and replication is continued in a loop until one of the
    // following 3 conditions is fulfilled:
    // (i) specified number of peers have been replicated to
    // (ii) too many failures in replicating to peers
    // (iii) no peer left to replicate to
    //
    while (!done) {
      getRandomPeer() match {
        case Some(peer) =>
          try {
            val onePeerStartTime = System.currentTimeMillis
            data.rewind()
            logTrace(s"Trying to replicate $blockId of ${data.limit()} bytes to $peer")
            blockTransferService.uploadBlockSync(
              peer.host, peer.port, peer.executorId, blockId, new NioManagedBuffer(data), tLevel)
            logTrace(s"Replicated $blockId of ${data.limit()} bytes to $peer in %s ms"
              .format(System.currentTimeMillis - onePeerStartTime))
            peersReplicatedTo += peer
            peersForReplication -= peer
            replicationFailed = false
            if (peersReplicatedTo.size == numPeersToReplicateTo) {
              done = true  // specified number of peers have been replicated to
            }
          } catch {
            case e: Exception =>
              logWarning(s"Failed to replicate $blockId to $peer, failure #$failures", e)
              failures += 1
              replicationFailed = true
              peersFailedToReplicateTo += peer
              if (failures > maxReplicationFailures) { // too many failures in replcating to peers
                done = true
              }
          }
        case None => // no peer left to replicate to
          done = true
      }
    }
    val timeTakeMs = (System.currentTimeMillis - startTime)
    logDebug(s"Replicating $blockId of ${data.limit()} bytes to " +
      s"${peersReplicatedTo.size} peer(s) took $timeTakeMs ms")
    if (peersReplicatedTo.size < numPeersToReplicateTo) {
      logWarning(s"Block $blockId replicated to only " +
        s"${peersReplicatedTo.size} peer(s) instead of $numPeersToReplicateTo peers")
    }
  }

  /**
   * Read a block consisting of a single object.
   */
  def getSingle(blockId: BlockId): Option[Any] = {
    get(blockId).map(_.data.next())
  }

  /**
   * Write a block consisting of a single object.
   *
   * @return true if the block was stored or false if the block was already stored or an
   *         error occurred.
   */
  def putSingle(
      blockId: BlockId,
      value: Any,
      level: StorageLevel,
      tellMaster: Boolean = true): Boolean = {
    putIterator(blockId, Iterator(value), level, tellMaster)
  }

  /**
   * Drop a block from memory, possibly putting it on disk if applicable. Called when the memory
   * store reaches its limit and needs to free up space.
   *
   * If `data` is not put on disk, it won't be created.
    *
    *
    * @param blockId
    * @param data 一个入参为空，返回值可以是Array[Any]或者ByteBuffer的Either，也就是说，对于两个不同的类型，可以使用Either表示
    */
  def dropFromMemory(
      blockId: BlockId,
      data: () => Either[Array[Any], ByteBuffer]): Unit = {

    logInfo(s"Dropping block $blockId from memory")

    /***
      * 从blockInfo中取出BlockInfo变量
      */
    val info = blockInfo.get(blockId)

    // If the block has not already been dropped
    if (info != null) {
      info.synchronized {
        // required ? As of now, this will be invoked only for blocks which are ready
        // But in case this changes in future, adding for consistency sake.
        if (!info.waitForReady()) {
          // If we get here, the block write failed.
          logWarning(s"Block $blockId was marked as failure. Nothing to drop")
          return
        } else if (blockInfo.asScala.get(blockId).isEmpty) {
          logWarning(s"Block $blockId was already dropped.")
          return
        }
        var blockIsUpdated = false
        val level = info.level

        // Drop to disk, if storage level requires
        /***
          * 如果该Block的持久化级别支持写到磁盘，而磁盘中尚无此Block，那么首先将该block落到磁盘
          */
        if (level.useDisk && !diskStore.contains(blockId)) {
          logInfo(s"Writing block $blockId to disk")

          /**
            * 调用data()方法返回要落地到磁盘的数据，data()返回的是一个Either，有两个值Left和Right，Left表示Array[Any],Right表示ByteBuffer
            */
          val dataToWriteToDisk = data()
          dataToWriteToDisk match {
            /**
              * 写Array[Any]
              */
            case Left(elements) =>
              diskStore.putArray(blockId, elements, level, returnValues = false)

            /***
              * 写ByteBuffer
              */
            case Right(bytes) =>
              diskStore.putBytes(blockId, bytes, level)
          }
          blockIsUpdated = true
        }

        // Actually drop from memory store
        // 为什么会出现memoryStore.contains(blockId)为真的情况，下面还调用了memoryStore.remove(blockId)进行了删除操作？因为这时从MemoryStore中擦除blockId
        // 所以memoryStore中应该包括它
        val droppedMemorySize =
          if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L

        /***
          * 从memoryStore中删除，如果成功，返回true；否则返回false
          */
        val blockIsRemoved = memoryStore.remove(blockId)
        if (blockIsRemoved) {
          blockIsUpdated = true
        } else {
          logWarning(s"Block $blockId could not be dropped from memory as it does not exist")
        }

        /***
          * 将数据drop掉后，需要更新该block的BlockStatus，
          * 代码执行到此处，数据从Memory中删除，如果Block支持写磁盘，那么此时数据也在磁盘上
          */
        val status = getCurrentBlockStatus(blockId, info)
        if (info.tellMaster) {
          ///通知master，block status已经更新
          reportBlockStatus(blockId, info, status, droppedMemorySize)
        }
        if (!level.useDisk) {
          // The block is completely gone from this node; forget it so we can put() it again later.
          blockInfo.remove(blockId)
        }
        if (blockIsUpdated) {
          Option(TaskContext.get()).foreach { c =>
            c.taskMetrics().incUpdatedBlockStatuses(Seq((blockId, status)))
          }
        }
      }
    }
  }

  /**
   * Remove all blocks belonging to the given RDD.
   * @return The number of blocks removed.
   */
  def removeRdd(rddId: Int): Int = {
    // TODO: Avoid a linear scan by creating another mapping of RDD.id to blocks.
    logInfo(s"Removing RDD $rddId")
    val blocksToRemove = blockInfo.asScala.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster = false) }
    blocksToRemove.size
  }

  /**
   * Remove all blocks belonging to the given broadcast.
   */
  def removeBroadcast(broadcastId: Long, tellMaster: Boolean): Int = {
    logDebug(s"Removing broadcast $broadcastId")
    val blocksToRemove = blockInfo.asScala.keys.collect {
      case bid @ BroadcastBlockId(`broadcastId`, _) => bid
    }
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster) }
    blocksToRemove.size
  }

  /**
   * Remove a block from both memory and disk.
   */
  def removeBlock(blockId: BlockId, tellMaster: Boolean = true): Unit = {
    logDebug(s"Removing block $blockId")
    val info = blockInfo.get(blockId)
    if (info != null) {
      info.synchronized {
        // Removals are idempotent in disk store and memory store. At worst, we get a warning.
        val removedFromMemory = memoryStore.remove(blockId)
        val removedFromDisk = diskStore.remove(blockId)
        if (!removedFromMemory && !removedFromDisk) {
          logWarning(s"Block $blockId could not be removed as it was not found in either " +
            "the disk, memory, or external block store")
        }
        blockInfo.remove(blockId)
        if (tellMaster && info.tellMaster) {
          val status = getCurrentBlockStatus(blockId, info)
          reportBlockStatus(blockId, info, status)
        }
      }
    } else {
      // The block has already been removed; do nothing.
      logWarning(s"Asked to remove block $blockId, which does not exist")
    }
  }

  private def shouldCompress(blockId: BlockId): Boolean = {
    blockId match {
      case _: ShuffleBlockId => compressShuffle
      case _: BroadcastBlockId => compressBroadcast
      case _: RDDBlockId => compressRdds
      case _: TempLocalBlockId => compressShuffleSpill
      case _: TempShuffleBlockId => compressShuffle
      case _ => false
    }
  }

  /**
   * Wrap an output stream for compression if block compression is enabled for its block type
   */
  def wrapForCompression(blockId: BlockId, s: OutputStream): OutputStream = {
    if (shouldCompress(blockId)) compressionCodec.compressedOutputStream(s) else s
  }

  /**
   * Wrap an input stream for compression if block compression is enabled for its block type
    * compressionCodec.compressedInputStream(InputStream)是对InputStream进行包装
    * 比如LZ4BlockInputStream内置了解压逻辑
   */
  def wrapForCompression(blockId: BlockId, s: InputStream): InputStream = {
    if (shouldCompress(blockId)) compressionCodec.compressedInputStream(s) else s
  }

  /**
    * 将数据序列化到输出流中
    *
    * Serializes into a stream.
    *
    *
    * */
  def dataSerializeStream(
      blockId: BlockId,
      outputStream: OutputStream,
      values: Iterator[Any]): Unit = {
    /***
      * 包装成BufferedOutputStream
      */
    val byteStream = new BufferedOutputStream(outputStream)

    /***
      * 返回SerializerInstance实例
      */
    val ser = defaultSerializer.newInstance()

    /***
      * 1. wrapForCompression,获得一个启用压缩的OutputStream，写入数据时可以压缩
      * 2.
      */
    val compressionOutputStream = wrapForCompression(blockId, byteStream)
    ser.serializeStream(compressionOutputStream).writeAll(values).close()
  }

  /** Serializes into a byte buffer. */
  def dataSerialize(blockId: BlockId, values: Iterator[Any]): ByteBuffer = {
    val byteStream = new ByteBufferOutputStream(4096)
    dataSerializeStream(blockId, byteStream, values)
    byteStream.toByteBuffer
  }

  /**
   * Deserializes a ByteBuffer into an iterator of values and disposes of it when the end of
   * the iterator is reached.
    * 对ByteBuffer中的数据进行反序列化
    *
   */
  def dataDeserialize(blockId: BlockId, bytes: ByteBuffer): Iterator[Any] = {
    bytes.rewind()
    dataDeserializeStream(blockId, new ByteBufferInputStream(bytes, true))
  }

  /**
   * Deserializes a InputStream into an iterator of values and disposes of it when the end of
   * the iterator is reached.
   */
  def dataDeserializeStream(blockId: BlockId, inputStream: InputStream): Iterator[Any] = {
    val stream = new BufferedInputStream(inputStream)
    defaultSerializer
      .newInstance()
      .deserializeStream(wrapForCompression(blockId, stream))
      .asIterator
  }

  def stop(): Unit = {
    blockTransferService.close()
    if (shuffleClient ne blockTransferService) {
      // Closing should be idempotent, but maybe not for the NioBlockTransferService.
      shuffleClient.close()
    }
    diskBlockManager.stop()
    rpcEnv.stop(slaveEndpoint)
    blockInfo.clear()
    memoryStore.clear()
    diskStore.clear()
    futureExecutionContext.shutdownNow()
    logInfo("BlockManager stopped")
  }
}


private[spark] object BlockManager extends Logging {
  private val ID_GENERATOR = new IdGenerator

  /**
   * Attempt to clean up a ByteBuffer if it is memory-mapped. This uses an *unsafe* Sun API that
   * might cause errors if one attempts to read from the unmapped buffer, but it's better than
   * waiting for the GC to find it because that could lead to huge numbers of open files. There's
   * unfortunately no standard API to do this.
   */
  def dispose(buffer: ByteBuffer): Unit = {
    if (buffer != null && buffer.isInstanceOf[MappedByteBuffer]) {
      logTrace(s"Unmapping $buffer")
      if (buffer.asInstanceOf[DirectBuffer].cleaner() != null) {
        buffer.asInstanceOf[DirectBuffer].cleaner().clean()
      }
    }
  }

  def blockIdsToHosts(
      blockIds: Array[BlockId],
      env: SparkEnv,
      blockManagerMaster: BlockManagerMaster = null): Map[BlockId, Seq[String]] = {

    // blockManagerMaster != null is used in tests
    assert(env != null || blockManagerMaster != null)
    val blockLocations: Seq[Seq[BlockManagerId]] = if (blockManagerMaster == null) {
      env.blockManager.getLocationBlockIds(blockIds)
    } else {
      blockManagerMaster.getLocations(blockIds)
    }

    val blockManagers = new HashMap[BlockId, Seq[String]]
    for (i <- 0 until blockIds.length) {
      blockManagers(blockIds(i)) = blockLocations(i).map(_.host)
    }
    blockManagers.toMap
  }
}
