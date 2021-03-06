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

import java.io.{File, IOException}
import java.util.UUID

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * Creates and maintains the logical mapping between logical blocks and physical on-disk
 * locations. By default, one block is mapped to one file with a name given by its BlockId.
 * However, it is also possible to have a block map to only a segment of a file, by calling
 * mapBlockToFileSegment().
 *
 * Block files are hashed among the directories listed in spark.local.dir (or in
 * SPARK_LOCAL_DIRS, if it's set).
 */
private[spark] class DiskBlockManager(blockManager: BlockManager, conf: SparkConf)
  extends Logging {

  /***
    * 子目录是64个二级目录
    */
  private[spark]
  val subDirNumberPerLocalDir = blockManager.conf.getInt("spark.diskStore.subDirectories", 64)


  var printFileLocation = true

  /* Create one local directory for each path mentioned in spark.local.dir; then, inside this
   * directory, create multiple subdirectories that we will hash files into, in order to avoid
   * having really large inodes at the top level.
   *
   *
   *
   * */
  private[spark] val localDirs: Array[File] = createLocalDirs(conf)
  if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }
  // The content of subDirs is immutable but the content of subDirs(i) is mutable. And the content
  // of subDirs(i) is protected by the lock of subDirs(i)
  /**
   * 每个local dir目录下的子目录集合，一共subDirNumberPerLocalDir个，默认64
   * subDirsUnderLocalDir是一个二维数组，第一维表示由下标标识的local dir，第二维表示该local dir下的子目录
   */
  private val subDirsUnderLocalDir= Array.fill(localDirs.length)(new Array[File](subDirNumberPerLocalDir))

  private val shutdownHook = addShutdownHook()

  // This method should be kept in sync with
  // org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getFile().
  /** *
    * Looks up a file by hashing it into one of our local subdirectories.
    * 根据文件名获取文件，Spark对文件名进行hash，以决定该filename存放在哪个子目录下
    *
    * @param filename 磁盘上真实存在的文件名，它位于哪个目录下，有它的hash值进行控制
    * @return
    */
  def getFile(filename: String): File = {
    // Figure out which local directory it hashes to, and which subdirectory in that
    /**
      * 1. 根据文件名计算hash值
      * 2. 在localDirs目录集合中获得属于那个目录(通过localDirs[dirId])
      * 3. 算法是(hash / localDirs.length) % subDirsPerLocalDir
      */
    val hash = Utils.nonNegativeHash(filename)

    /** *
      * dirId表示该文件位于local dirs的那个目录下
      */
    val dirId = hash % localDirs.length

    /** *
      * subDirId表示该文件位于local dir的那个子目录下
      */
    val subDirId = (hash / localDirs.length) % subDirNumberPerLocalDir


    /** *
      * subDirs表示dirId标识的local dir下的子目录集合，它是Array[File]
      */
    val subDirs =  subDirsUnderLocalDir(dirId)

    /** *
      *  Create the subdirectory if it doesn't already exist
      *  subDir表示该文件的绝对路径，如果该路径不存在，那么首先创建
      *  subDir由dirId和subDirId共同决定
      */
    val subDir =subDirs.synchronized {
      /** *
        * 通过dirId和subDirId从subDirsUnderLocalDir获取出该数据文件所处的子目录
        * old是File类型，它是一个目录
        */
      val oldDir = subDirsUnderLocalDir(dirId)(subDirId)
      if (oldDir != null) {
        oldDir
      } else {
        /** *
          * 如果还不存在，那么首先构造出该newDir，创建规则是父目录是local dir目录 ，目录名是"%02x".format(subDirId)
          * 如果subDirId是18，那么"%02x".format(subDirId)为12
          * 如果subDirId是51，那么"%02x".format(subDirId)为36
          * 如果subDirId是512， 那么"%02x".format(subDirId)为1000,截断得到00
          */
        val newDir = new File(localDirs(dirId), "%02x".format(subDirId))

        //创建目录
        if (!newDir.exists() && !newDir.mkdir()) {
          throw new IOException(s"Failed to create local dir in $newDir.")
        }

        /** *
          * 将newDir保存到subDirsUnderLocalDir二维数组中
          */
        subDirsUnderLocalDir(dirId)(subDirId) = newDir
        newDir
      }
    }

    /** *
      * 返回数据文件
      */
    new File(subDir, filename)
  }

  /***
    * 根据BlockId(的名称)获得文件
    *  这个文件是存放在Executor所在的磁盘上，受Executor所对应的BlockManager管理
    *
    * @param blockId
    * @return
    */
  def getFile(blockId: BlockId): File = {
    getFile(blockId.name)
  }

  /** Check if disk block manager has a block. */
  def containsBlock(blockId: BlockId): Boolean = {
    getFile(blockId.name).exists()
  }

  /** List all the files currently stored on disk by the disk manager. */
  def getAllFiles(): Seq[File] = {
    // Get all the files inside the array of array of directories
    subDirsUnderLocalDir.flatMap { dir =>
      dir.synchronized {
        // Copy the content of dir because it may be modified in other threads
        dir.clone()
      }
    }.filter(_ != null).flatMap { dir =>
      val files = dir.listFiles()
      if (files != null) files else Seq.empty
    }
  }

  /** List all the blocks currently stored on disk by the disk manager. */
  def getAllBlocks(): Seq[BlockId] = {
    getAllFiles().map(f => BlockId(f.getName))
  }

  /** Produces a unique block id and File suitable for storing local intermediate results. */
  def createTempLocalBlock(): (TempLocalBlockId, File) = {
    var blockId = new TempLocalBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = new TempLocalBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /** Produces a unique block id and File suitable for storing shuffled intermediate results. */
  def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
    var blockId = new TempShuffleBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = new TempShuffleBlockId(UUID.randomUUID())
    }
    val blockFile = getFile(blockId)
    if (printFileLocation) {
         printFileLocation = false
//        println("=================================================================")
//        println("---------->" + blockFile.getAbsolutePath)
//        println("=================================================================")
    }
    (blockId, blockFile)
  }

  /**
   * Create local directories for storing block data. These directories are
   * located inside configured local directories and won't
   * be deleted on JVM exit when using the external shuffle service.
   */
  private def createLocalDirs(conf: SparkConf): Array[File] = {
    /***
      * 获取配置的local directories
      */
    val localdirs = Utils.getConfiguredLocalDirs(conf)
    localdirs.flatMap { rootDir =>
      try {
        /***
          * 创建目录，
          */
        val localDir = Utils.createDirectory(rootDir, "blockmgr")
        logInfo(s"Created local directory at $localDir")
        Some(localDir)
      } catch {
        case e: IOException =>
          logError(s"Failed to create local dir in $rootDir. Ignoring this directory.", e)
          None
      }
    }
  }

  private def addShutdownHook(): AnyRef = {
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY + 1) { () =>
      logInfo("Shutdown hook called")
      DiskBlockManager.this.doStop()
    }
  }

  /** Cleanup local dirs and stop shuffle sender. */
  private[spark] def stop() {
    // Remove the shutdown hook.  It causes memory leaks if we leave it around.
    try {
      ShutdownHookManager.removeShutdownHook(shutdownHook)
    } catch {
      case e: Exception =>
        logError(s"Exception while removing shutdown hook.", e)
    }
    doStop()
  }

  private def doStop(): Unit = {
    // Only perform cleanup if an external service is not serving our shuffle files.
    // Also blockManagerId could be null if block manager is not initialized properly.
    if (!blockManager.externalShuffleServiceEnabled ||
      (blockManager.blockManagerId != null && blockManager.blockManagerId.isDriver)) {
      localDirs.foreach { localDir =>
        if (localDir.isDirectory() && localDir.exists()) {
          try {
            if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(localDir)) {
              Utils.deleteRecursively(localDir)
            }
          } catch {
            case e: Exception =>
              logError(s"Exception while deleting local spark dir: $localDir", e)
          }
        }
      }
    }
  }
}
