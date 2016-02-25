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

package org.apache.spark

import java.io.File
import java.net.Socket

import scala.collection.mutable
import scala.util.Properties

import com.google.common.collect.MapMaker

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.PythonWorkerFactory
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.memory.{MemoryManager, StaticMemoryManager, UnifiedMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.rpc.{RpcAddress, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.{LiveListenerBus, OutputCommitCoordinator}
import org.apache.spark.scheduler.OutputCommitCoordinator.OutputCommitCoordinatorEndpoint
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage._
import org.apache.spark.util.{RpcUtils, Utils}

/**
 * :: DeveloperApi ::
 * Holds all the runtime environment objects for a running Spark instance (either master or worker),
 * including the serializer, RpcEnv, block manager, map output tracker, etc. Currently
 * Spark code finds the SparkEnv through a global variable, so all the threads can access the same
 * SparkEnv. It can be accessed by SparkEnv.get (e.g. after creating a SparkContext).
 *
 * NOTE: This is not intended for external use. This is exposed for Shark and may be made private
 *       in a future release.
 */
@DeveloperApi
class SparkEnv (
    val executorId: String,
    private[spark] val rpcEnv: RpcEnv,
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val cacheManager: CacheManager,
    val mapOutputTracker: MapOutputTracker,
    val shuffleManager: ShuffleManager,
    val broadcastManager: BroadcastManager,
    val blockTransferService: BlockTransferService,
    val blockManager: BlockManager,
    val securityManager: SecurityManager,
    val sparkFilesDir: String,
    val metricsSystem: MetricsSystem,
    val memoryManager: MemoryManager,
    val outputCommitCoordinator: OutputCommitCoordinator,
    val conf: SparkConf) extends Logging {

  private[spark] var isStopped = false
  private val pythonWorkers = mutable.HashMap[(String, Map[String, String]), PythonWorkerFactory]()

  // A general, soft-reference map for metadata needed during HadoopRDD split computation
  // (e.g., HadoopFileRDD uses this to cache JobConfs and InputFormats).
  private[spark] val hadoopJobMetadata = new MapMaker().softValues().makeMap[String, Any]()

  private var driverTmpDirToDelete: Option[String] = None

  private[spark] def stop() {

    if (!isStopped) {
      isStopped = true
      pythonWorkers.values.foreach(_.stop())
      mapOutputTracker.stop()
      shuffleManager.stop()
      broadcastManager.stop()
      blockManager.stop()
      blockManager.master.stop()
      metricsSystem.stop()
      outputCommitCoordinator.stop()
      rpcEnv.shutdown()

      // Note that blockTransferService is stopped by BlockManager since it is started by it.

      // If we only stop sc, but the driver process still run as a services then we need to delete
      // the tmp dir, if not, it will create too many tmp dirs.
      // We only need to delete the tmp dir create by driver, because sparkFilesDir is point to the
      // current working dir in executor which we do not need to delete.
      driverTmpDirToDelete match {
        case Some(path) => {
          try {
            Utils.deleteRecursively(new File(path))
          } catch {
            case e: Exception =>
              logWarning(s"Exception while deleting Spark temp dir: $path", e)
          }
        }
        case None => // We just need to delete tmp dir created by driver, so do nothing on executor
      }
    }
  }

  private[spark]
  def createPythonWorker(pythonExec: String, envVars: Map[String, String]): java.net.Socket = {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(pythonExec, envVars)).create()
    }
  }

  private[spark]
  def destroyPythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.stopWorker(worker))
    }
  }

  private[spark]
  def releasePythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
    synchronized {
      val key = (pythonExec, envVars)
      pythonWorkers.get(key).foreach(_.releaseWorker(worker))
    }
  }
}

object SparkEnv extends Logging {
  @volatile private var env: SparkEnv = _

  private[spark] val driverSystemName = "sparkDriver"
  private[spark] val executorSystemName = "sparkExecutor"

  def set(e: SparkEnv) {
    env = e
  }

  /**
   * Returns the SparkEnv.
   */
  def get: SparkEnv = {
    env
  }

  /**
   * Returns the ThreadLocal SparkEnv.
   */
  @deprecated("Use SparkEnv.get instead", "1.2.0")
  def getThreadLocal: SparkEnv = {
    env
  }

  /**
   * Create a SparkEnv for the driver.
    *
    * 创建Driver端的SparkEnv
    *
    * @param conf
    * @param isLocal 表示是本地运行模式，比如--master local
    * @param listenerBus
    * @param numCores Driver使用的内核数，通过--driver-cores指定
    * @param mockOutputCommitCoordinator 对于Executor的SparkEnv没有传递这个参数，因此去默认值None
    * @return
    */
  private[spark] def createDriverEnv(
      conf: SparkConf,
      isLocal: Boolean,
      listenerBus: LiveListenerBus,
      numCores: Int,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
    assert(conf.contains("spark.driver.host"), "spark.driver.host is not set on the driver!")
    assert(conf.contains("spark.driver.port"), "spark.driver.port is not set on the driver!")
    val hostname = conf.get("spark.driver.host")
    val port = conf.get("spark.driver.port").toInt
    create(
      conf,
      SparkContext.DRIVER_IDENTIFIER, /**Driver的executorId是driver**/
      hostname,
      port,
      isDriver = true,
      isLocal = isLocal,
      numUsableCores = numCores,
      listenerBus = listenerBus,
      mockOutputCommitCoordinator = mockOutputCommitCoordinator
    )
  }

  /**
   * Create a SparkEnv for an executor.
   * In coarse-grained mode, the executor provides an RpcEnv that is already instantiated.
    *
    * 创建Executor的SparkEnv
   */
  private[spark] def createExecutorEnv(
      conf: SparkConf,
      executorId: String,
      hostname: String,
      port: Int,
      numCores: Int,
      isLocal: Boolean): SparkEnv = {

    /**
      * 创建Executor端的SparkEnv
      */
    val env = create(
      conf,
      executorId,
      hostname,
      port,
      isDriver = false,
      isLocal = isLocal,
      numUsableCores = numCores
    )

    /**
      * 设置类似于ThreadLocal变量
      */
    SparkEnv.set(env)
    env
  }

  /**
   * Helper method to create a SparkEnv for a driver or an executor.
    *
    * @param conf
    * @param executorId 可能是Driver的ID，也可能是Executor的ID
    * @param hostname
    * @param port
    * @param isDriver 是否创建Driver的SparkEnv
    * @param isLocal 是否运行于本地模式，比如--master local
    * @param numUsableCores
    * @param listenerBus
    * @param mockOutputCommitCoordinator
    * @return
    */
  private def create(
      conf: SparkConf,
      executorId: String,
      hostname: String,
      port: Int,
      isDriver: Boolean,
      isLocal: Boolean,
      numUsableCores: Int,
      listenerBus: LiveListenerBus = null,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {

    // Listener bus is only used on the driver
    if (isDriver) {
      assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
    }

    val securityManager = new SecurityManager(conf)

    val systemName = if (isDriver) driverSystemName else executorSystemName

    /***
      * clientMode是什么意思？如果是Driver就不是client模式，如果是Executor就是client模式
      */
    val rpcEnv = RpcEnv.create(systemName, hostname, port, conf, securityManager,
      clientMode = !isDriver)

    // Figure out which port RpcEnv actually bound to in case the original port is 0 or occupied.
    // In the non-driver case, the RPC env's address may be null since it may not be listening
    // for incoming connections.
    //从这个地方看出来，Driver运行于哪个端口是调用RpcEnv.create得到的
    if (isDriver) {
      conf.set("spark.driver.port", rpcEnv.address.port.toString)
    } else if (rpcEnv.address != null) {
      conf.set("spark.executor.port", rpcEnv.address.port.toString)
    }

    // Create an instance of the class with the given name, possibly initializing it with our conf
    def instantiateClass[T](className: String): T = {
      val cls = Utils.classForName(className)
      // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
      // SparkConf, then one taking no arguments
      try {
        cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
          .newInstance(conf, new java.lang.Boolean(isDriver))
          .asInstanceOf[T]
      } catch {
        case _: NoSuchMethodException =>
          try {
            cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
          } catch {
            case _: NoSuchMethodException =>
              cls.getConstructor().newInstance().asInstanceOf[T]
          }
      }
    }

    // Create an instance of the class named by the given SparkConf property, or defaultClassName
    // if the property is not set, possibly initializing it with our conf
    def instantiateClassFromConf[T](propertyName: String, defaultClassName: String): T = {
      instantiateClass[T](conf.get(propertyName, defaultClassName))
    }

    /**
      * 通过反射创建JavaSerializer（以SparkConf为参数）
      */
    val serializer = instantiateClassFromConf[Serializer](
      "spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    logDebug(s"Using serializer: ${serializer.getClass}")

    /***
      * 初始化闭包序列化类，默认也是JavaSerializer（以SparkConf为参数）
      */
    val closureSerializer = instantiateClassFromConf[Serializer](
      "spark.closure.serializer", "org.apache.spark.serializer.JavaSerializer")

    /**
      * 返回一个RpcEndpointRef实例，表示是EndpointRef
      *
      * 分区Driver和Executor
      * 如果是Driver，那么给全局的rpcEnv调用setupEndpoint
      * 如果是Executor，那么给全局的rpcEnv调用setupEndpointRef
      *
      * @param name
      * @param endpointCreator 返回类型
      * @return 返回RpcEndpointRef实例
      */
    def registerOrLookupEndpoint(
        name: String, endpointCreator: => RpcEndpoint):
      RpcEndpointRef = {
      /**
       * rpcEnv.setupEndpoint创建一个EndPoint(Endpoint有点类似于创建一个server)
       */
      if (isDriver) {
        logInfo("Registering " + name)

        /** *
          * 注册一个Endpoint，其名称是name，返回该Endpoint对应的EndpointRef
          */
        rpcEnv.setupEndpoint(name, endpointCreator)
      } else {
        val driverHost: String = conf.get("spark.driver.host", "localhost")
        val driverPort: Int = conf.getInt("spark.driver.port", 7077)
        Utils.checkHost(driverHost, "Expected hostname")

        /** *
          * rpcEnv.setupEndpointRef有点类似于创建一个跟Endpoint(类似于Server)通信的client
          *
          * 每个RpcEndpoint是由RpcAddress和name表征的，RpcAddress由Host和Port组成，
          * 所以说RpcEndpoint类似于Server
          */
        rpcEnv.setupEndpointRef(RpcAddress(driverHost, driverPort), name)
      }
    }

    /**
      * 如果是Driver，那么就创建MapOutputTrackerMaster,否则就是MapOutputTrackerWorker
      */
    val mapOutputTracker = if (isDriver) {
      new MapOutputTrackerMaster(conf)
    } else {
      new MapOutputTrackerWorker(conf)
    }

    // Have to assign trackerEndpoint after initialization as MapOutputTrackerEndpoint
    // requires the MapOutputTracker itself
    /**
      * register或者lookup是什么含义？
      * 首先masterEndPoint是RpcEndpoint类型
      * 如果是Driver是什么情况？如果是Executor又是什么情况？
      */
    val masterEndPoint = new MapOutputTrackerMasterEndpoint(rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf)

    /** *
      * trakerEndpoint是MapOutputTracker的一个可修改的成员变量
      */
    mapOutputTracker.trackerEndpoint = registerOrLookupEndpoint(MapOutputTracker.ENDPOINT_NAME,masterEndPoint)

    // Let the user specify short names for shuffle managers
    /***
      * ShuffleManager
      */
    val shortShuffleMgrNames = Map(
      "hash" -> "org.apache.spark.shuffle.hash.HashShuffleManager",
      "sort" -> "org.apache.spark.shuffle.sort.SortShuffleManager",
      "tungsten-sort" -> "org.apache.spark.shuffle.sort.SortShuffleManager")
    val shuffleMgrName = conf.get("spark.shuffle.manager", "sort")
    val shuffleMgrClass = shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase, shuffleMgrName)
    val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)

    /***
      * MemoryManager
      */
    val useLegacyMemoryManager = conf.getBoolean("spark.memory.useLegacyMode", false)
    val memoryManager: MemoryManager =
      if (useLegacyMemoryManager) {
        new StaticMemoryManager(conf, numUsableCores)
      } else {
        UnifiedMemoryManager(conf, numUsableCores)
      }

    /***
      * NettyBlockTransferService
      */
    val blockTransferService = new NettyBlockTransferService(conf, securityManager, numUsableCores)

    /**
      * 不管是Driver还是Executor，都会创建BlockManagerMasterEndpoint实例,
      * 但是对于Driver，endpoint指向BlockManagerMasterEndpoint本尊，而对于Executor，endpoint指向BlockManagerMasterEndpoint引用
     *
     *   BlockManagerMasterEndpoint只是构造出来，并没有进行注册，在registerOrLookupEndpoint方法中，因为
     *   driver先执行BlockManagerMasterEndpoint注册，然后Executor在通过lookup获得BlockManagerMasterEndpoint
     *   的ref
     *
     */
    val bmmEndpoint = new BlockManagerMasterEndpoint(rpcEnv, isLocal, conf, listenerBus)

    /** *
      * endpoint是bmmEndpoint的ref，而BlockManagerMaster.DRIVER_ENDPOINT_NAME是BlockManagerMasterEndpoint的
      * 名称
      */
    val endpoint = registerOrLookupEndpoint(BlockManagerMaster.DRIVER_ENDPOINT_NAME, bmmEndpoint)

    /***
      * 不管是Executor还是Driver，都会创建BlockManagerMaster实例,这个实例创建完了会设置到BlockManager中
      *
      * 也就是说，在Driver上BlockManagerMaster是真实存在的，在Executor上，BlockManagerMaster只是包含指向Driver BlockManager的endpoint ref
      *
      * endpoint可能是BlockManagerMasterEndpoint本尊也可能是BlockManagerMasterEndpoint的引用
      *
      * BlockManagerMaster在Driver和Executor上都存在，
      *   1. 在Driver上，endpoint指向BlockManagerMasterEndpoint本尊ref
      *   2. 在Executor上，endpoint是BlockManagerMasterEndpoint的ref
      * 也就是说，作为一个有IP有端口的服务，BlockManagerMasterEndpoint运行在driver端
      *
      *
      * BlockManagerMaster是通过给它持有的endpoint发送消息的方式来实现Spark操作BlockManager，也就是说，
      * 用户不会直接给BlockManagerMasterEndpoint发送消息，而是调用BlockManagerMaster提供的API，然后由
      * 这些API实现Endpoint发送消息
      *
      */
    val blockManagerMaster = new BlockManagerMaster(endpoint, conf, isDriver)

    // NB: blockManager is not valid until initialize() is called later.

    /**
      *  不管是Executor还是Driver， 都会创建BlockManager对象，
      *
      *  每个BlockManager会关联一个BlockManagerMaster，BlockManagerMaster持有Driver的Endpoint或者Endpoint Ref
      *  BlockManager持有BlockManagerMaster以及BlockManagerSlaveEndpoint,BlockManagerMaster持有BlockManagerMasterEndpoint
      */
    val blockManager = new BlockManager(executorId, rpcEnv, blockManagerMaster,
      serializer, conf, memoryManager, mapOutputTracker, shuffleManager,
      blockTransferService, securityManager, numUsableCores)


    /**
      * 不管是Executor还是Driver，都会创建BroadcastManager对象
      */
    val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)

    /**
      * 不管是Executor还是Driver，都会创建CacheManager对象
      */
    val cacheManager = new CacheManager(blockManager)

    val metricsSystem = if (isDriver) {
      // Don't start metrics system right now for Driver.
      // We need to wait for the task scheduler to give us an app ID.
      // Then we can start the metrics system.
      MetricsSystem.createMetricsSystem("driver", conf, securityManager)
    } else {
      // We need to set the executor ID before the MetricsSystem is created because sources and
      // sinks specified in the metrics configuration file will want to incorporate this executor's
      // ID into the metrics they report.
      conf.set("spark.executor.id", executorId)
      val ms = MetricsSystem.createMetricsSystem("executor", conf, securityManager)
      ms.start()
      ms
    }

    // Set the sparkFiles directory, used when downloading dependencies.  In local mode,
    // this is a temporary directory; in distributed mode, this is the executor's current working
    // directory.
    val sparkFilesDir: String = if (isDriver) {
      val path = Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
      path
    } else {
      "."
    }

    /**
      * 不管是Executor还是Driver，都有OutputCommitCoordinator对象
      */
    val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
      new OutputCommitCoordinator(conf, isDriver)
    }
    val outputCommitCoordinatorRef = registerOrLookupEndpoint("OutputCommitCoordinator",
      new OutputCommitCoordinatorEndpoint(rpcEnv, outputCommitCoordinator))
    outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)

    val envInstance = new SparkEnv(
      executorId,
      rpcEnv,
      serializer,
      closureSerializer,
      cacheManager,
      mapOutputTracker,
      shuffleManager,
      broadcastManager,
      blockTransferService,
      blockManager,
      securityManager,
      sparkFilesDir,
      metricsSystem,
      memoryManager,
      outputCommitCoordinator,
      conf)

    // Add a reference to tmp dir created by driver, we will delete this tmp dir when stop() is
    // called, and we only need to do it for driver. Because driver may run as a service, and if we
    // don't delete this tmp dir when sc is stopped, then will create too many tmp dirs.
    if (isDriver) {
      envInstance.driverTmpDirToDelete = Some(sparkFilesDir)
    }

    envInstance
  }

  /**
   * Return a map representation of jvm information, Spark properties, system properties, and
   * class paths. Map keys define the category, and map values represent the corresponding
   * attributes as a sequence of KV pairs. This is used mainly for SparkListenerEnvironmentUpdate.
   */
  private[spark]
  def environmentDetails(
      conf: SparkConf,
      schedulingMode: String,
      addedJars: Seq[String],
      addedFiles: Seq[String]): Map[String, Seq[(String, String)]] = {

    import Properties._
    val jvmInformation = Seq(
      ("Java Version", s"$javaVersion ($javaVendor)"),
      ("Java Home", javaHome),
      ("Scala Version", versionString)
    ).sorted

    // Spark properties
    // This includes the scheduling mode whether or not it is configured (used by SparkUI)
    val schedulerMode =
      if (!conf.contains("spark.scheduler.mode")) {
        Seq(("spark.scheduler.mode", schedulingMode))
      } else {
        Seq[(String, String)]()
      }
    val sparkProperties = (conf.getAll ++ schedulerMode).sorted

    // System properties that are not java classpaths
    val systemProperties = Utils.getSystemProperties.toSeq
    val otherProperties = systemProperties.filter { case (k, _) =>
      k != "java.class.path" && !k.startsWith("spark.")
    }.sorted

    // Class paths including all added jars and files
    val classPathEntries = javaClassPath
      .split(File.pathSeparator)
      .filterNot(_.isEmpty)
      .map((_, "System Classpath"))
    val addedJarsAndFiles = (addedJars ++ addedFiles).map((_, "Added By User"))
    val classPaths = (addedJarsAndFiles ++ classPathEntries).sorted

    Map[String, Seq[(String, String)]](
      "JVM Information" -> jvmInformation,
      "Spark Properties" -> sparkProperties,
      "System Properties" -> otherProperties,
      "Classpath Entries" -> classPaths)
  }
}
