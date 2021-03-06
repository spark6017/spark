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

package org.apache.spark.deploy.yarn

import java.io.{File, IOException}
import java.lang.reflect.InvocationTargetException
import java.net.{Socket, URL}
import java.util.concurrent.atomic.AtomicReference

import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.rpc._
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, YarnSchedulerBackend}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util._

/**
 * Common application master functionality for Spark on Yarn.
  *
  * application	master starts	the	driver program	before	allocating	resources	for executors
 *
 * 在Yarn Cluster模式下，Application Master首先将Driver启动起来，然后再向ResourceManager申请Container？
 * 这个逻辑是什么
  *
  * @param args
  * @param client
  */
private[spark] class ApplicationMaster(
    args: ApplicationMasterArguments,
    client: YarnRMClient)
  extends Logging {

  // Load the properties file with the Spark configuration and set entries as system properties,
  // so that user code run inside the AM also has access to them.

  /***
    * 首先把传递给ApplicationMaster的配置文件逐一解析出来放到系统环境变量中
    * 然后ApplicationMaster创建默认的SparkConf时，会读取系统变量以spark.开头的配置加到SparkConf中
    * 比如用户jar，是放在spark.yarn.user.jar这个系统属性中的
    */
  if (args.propertiesFile != null) {
    Utils.getPropertiesFromFile(args.propertiesFile).foreach { case (k, v) =>
      sys.props(k) = v
    }
  }

  // TODO: Currently, task to container is computed once (TaskSetManager) - which need not be
  // optimal as more containers are available. Might need to handle this better.

  /** *
    * 这是创建不带参数的SparkConf
    * 创建SparkConf时，会加载system properties上的以spark.开头的属性。
    * 这个做法是在YARN CLIENT模式下，ApplicationMaster得不到SparkContext对象，ApplicationMaster只能将启动ApplicationMaster时将传递给它的Spark配置属性
    * 写到System Properties中，然后通过new SparkConf进行加载
    */
  private val sparkConf = new SparkConf()
  private val yarnConf: YarnConfiguration = SparkHadoopUtil.get.newConfiguration(sparkConf)
    .asInstanceOf[YarnConfiguration]
  private val isClusterMode = args.userClass != null

  //
  //

  /***
    *Default to twice the number of executors (twice the maximum number of executors if dynamic allocation is enabled), with a minimum of 3.
    *
    * Executor失败的最大次数是executor个数的两倍
    * 如果Spark Application启动了executor个数的动态分配，那么取spark.dynamicAllocation.maxExecutors*2
    * 如果Spark Application使用executor个数静态分配，那么取spark.executor.instances * 2
    *
    */
  private val maxNumExecutorFailures = {
    val defaultKey =
      if (Utils.isDynamicAllocationEnabled(sparkConf)) {
        "spark.dynamicAllocation.maxExecutors"
      } else {
        "spark.executor.instances"
      }
    val effectiveNumExecutors = sparkConf.getInt(defaultKey, 0)
    val defaultMaxNumExecutorFailures = math.max(3, 2 * effectiveNumExecutors)

    sparkConf.getInt("spark.yarn.max.executor.failures", defaultMaxNumExecutorFailures)
  }

  @volatile private var exitCode = 0
  @volatile private var unregistered = false
  @volatile private var finished = false
  @volatile private var finalStatus = getDefaultFinalStatus
  @volatile private var finalMsg: String = ""

  /** *
    * 在Yarn-Cluster模式下，执行用户通过SparkSubmit提交的应用，主类是--class指定的类名
    */
  @volatile private var userClassThread: Thread = _

  @volatile private var reporterThread: Thread = _

  /** *
    * allocator什么时候初始化？
    */
  @volatile private var allocator: YarnAllocator = _

  // Lock for controlling the allocator (heartbeat) thread.
  private val allocatorLock = new Object()

  // Steady state heartbeat interval. We want to be reasonably responsive without causing too many
  // requests to RM.
  private val heartbeatInterval = {
    // Ensure that progress is sent before YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS elapses.

    //expiry：过期，逾期，默认是120000毫秒，即2分钟
    val expiryInterval = yarnConf.getInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 120000)

    //心跳时间的默认值是3s，这个心跳时间决定了在YARN模式下，Executor挂了之后，多久才能重启
    math.max(0, math.min(expiryInterval / 2,
      sparkConf.getTimeAsMs("spark.yarn.scheduler.heartbeat.interval-ms", "3s")))
  }

  /** *
    *  Initial wait interval before allocator poll, to allow for quicker ramp up when executors are being requested.
    *  初始调度的时间间隔
    */
  private val initialAllocationInterval = math.min(heartbeatInterval,
    sparkConf.getTimeAsMs("spark.yarn.scheduler.initial-allocation.interval", "200ms"))

  // Next wait interval before allocator poll.
  private var nextAllocationInterval = initialAllocationInterval

  // Fields used in client mode.
  private var rpcEnv: RpcEnv = null
  private var amEndpoint: RpcEndpointRef = _

  /** *
    * Fields used in cluster mode.
    * AtomicReference是原子引用，它是如何工作的？
    * AtomicReference何时更新值的？
    */
  private val sparkContextRef = new AtomicReference[SparkContext](null)

  private var delegationTokenRenewerOption: Option[AMDelegationTokenRenewer] = None

  /** *
    * ApplicationMaster class定义了getAttemptId方法
    * 通过YarnRMClient的getAttemptId方法获取ApplicationAttemptId对象
    * @return
    */
  def getAttemptId(): ApplicationAttemptId = {
    client.getAttemptId()
  }

  /** *
    * ApplicationMaster进程的主体代码，它会等待用户application运行结束(在runDriver中实现)
    * @return
    */
  final def run(): Int = {
    try {
      val appAttemptId = client.getAttemptId()

      /** *
        * 如果是Yarn Cluster Mode
        */
      if (isClusterMode) {
        /** *
          *  Set the web ui port to be ephemeral for yarn so we don't conflict with other spark processes running on the same box
          *
          * 端口设置为0，表示什么意思？
          */
        System.setProperty("spark.ui.port", "0")

        /** *
          *  Set the master property to match the requested mode.
          *  设置spark master为yarn-cluster
          */
        System.setProperty("spark.master", "yarn-cluster")

        // Propagate the application ID so that YarnClusterSchedulerBackend can pick it up.
        //将spark.yarn.app.id设置到System环境变量中, SparkConf通过读取System Properties将它放到SparkConf中
        System.setProperty("spark.yarn.app.id", appAttemptId.getApplicationId().toString())

        // Propagate the attempt if, so that in case of event logging,
        // different attempt's logs gets created in different directory
        System.setProperty("spark.yarn.app.attemptId", appAttemptId.getAttemptId().toString())
      }

      logInfo("ApplicationAttemptId: " + appAttemptId)

      val fs = FileSystem.get(yarnConf)

      /** *
        *  This shutdown hook should run *after* the SparkContext is shut down.
        *  优先级越高，priority的值越大？是的
        */
      val priority = ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY - 1


      /** *
        * Application shut down时，
        */
      ShutdownHookManager.addShutdownHook(priority) { () =>
        val maxAppAttempts = client.getMaxRegAttempts(sparkConf, yarnConf)
        val isLastAttempt = client.getAttemptId().getAttemptId() >= maxAppAttempts

        /** *
          * 如果还没有运行结束
          */
        if (!finished) {
          // This happens when the user application calls System.exit(). We have the choice
          // of either failing or succeeding at this point. We report success to avoid
          // retrying applications that have succeeded (System.exit(0)), which means that
          // applications that explicitly exit with a non-zero status will also show up as
          // succeeded in the RM UI.
          finish(finalStatus,
            ApplicationMaster.EXIT_SUCCESS,
            "Shutdown hook called before final status was reported.")
        }

        /** *
          * 如果还没有执行注销操作，那么需要注销
          */
        if (!unregistered) {
          // we only want to unregister if we don't want the RM to retry
          if (finalStatus == FinalApplicationStatus.SUCCEEDED || isLastAttempt) {
            unregister(finalStatus, finalMsg)
            cleanupStagingDir(fs)
          }
        }
      }

      // Call this to force generation of secret so it gets populated into the
      // Hadoop UGI. This has to happen before the startUserApplication which does a
      // doAs in order for the credentials to be passed on to the executor containers.
      val securityMgr = new SecurityManager(sparkConf)

      // If the credentials file config is present, we must periodically renew tokens. So create
      // a new AMDelegationTokenRenewer
      if (sparkConf.contains("spark.yarn.credentials.file")) {
        delegationTokenRenewerOption = Some(new AMDelegationTokenRenewer(sparkConf, yarnConf))
        // If a principal and keytab have been set, use that to create new credentials for executors
        // periodically
        delegationTokenRenewerOption.foreach(_.scheduleLoginFromKeytab())
      }

      /**
        * 如果是Cluster Mode，那么在ApplicationMaster中启动Driver，
       * runDriver方法做的事情绝不仅仅是启动Driver，所有事情都要在这个方法中做，包括
       * 1. 等待用户application运行完成
       * 2. 启动汇报线程，监控资源使用、健康情况
        */
      if (isClusterMode) {
        runDriver(securityMgr)
      } else {
        /**
          * 如果是Client模式，那么调用runExecutorLauncher
          */
        runExecutorLauncher(securityMgr)
      }
    } catch {
      case e: Exception =>
        // catch everything else if not specifically handled
        logError("Uncaught exception: ", e)
        finish(FinalApplicationStatus.FAILED,
          ApplicationMaster.EXIT_UNCAUGHT_EXCEPTION,
          "Uncaught exception: " + e)
    }
    exitCode
  }

  /**
   * Set the default final application status for client mode to UNDEFINED to handle
   * if YARN HA restarts the application so that it properly retries. Set the final
   * status to SUCCEEDED in cluster mode to handle if the user calls System.exit
   * from the application code.
   */
  final def getDefaultFinalStatus(): FinalApplicationStatus = {
    if (isClusterMode) {
      FinalApplicationStatus.SUCCEEDED
    } else {
      FinalApplicationStatus.UNDEFINED
    }
  }

  /**
   * unregister is used to completely unregister the application from the ResourceManager.
   * This means the ResourceManager will not retry the application attempt on your behalf if
   * a failure occurred.
    *
    * @param status
    * @param diagnostics
    */
  final def unregister(status: FinalApplicationStatus, diagnostics: String = null): Unit = {
    synchronized {
      if (!unregistered) {
        logInfo(s"Unregistering ApplicationMaster with $status" +
          Option(diagnostics).map(msg => s" (diag message: $msg)").getOrElse(""))
        unregistered = true
        client.unregister(status, Option(diagnostics).getOrElse(""))
      }
    }
  }

  /** *
    * 结束一个Application
    * @param status
    * @param code
    * @param msg
    */
  final def finish(status: FinalApplicationStatus, code: Int, msg: String = null): Unit = {
    synchronized {
      /** *
        * Application还没有运行完，
        */
      if (!finished) {

        /** *
          * 检查JVM是否处于Shutting down状态
          */
        val inShutdown = ShutdownHookManager.inShutdown()
        logInfo(s"Final app status: $status, exitCode: $code" +
          Option(msg).map(msg => s", (reason: $msg)").getOrElse(""))
        exitCode = code
        finalStatus = status
        finalMsg = msg
        finished = true
        if (!inShutdown && Thread.currentThread() != reporterThread && reporterThread != null) {
          logDebug("shutting down reporter thread")
          reporterThread.interrupt()
        }

        /** *
          * 在成员变量的userThread的run方法中，调用了finish方法，那么
          * userThread的run方法执行这个finish方法时，Thread.currentThread()就是userClassThread，那么就不能调用interrupt方法
          */
        if (!inShutdown && Thread.currentThread() != userClassThread && userClassThread != null) {
          logDebug("shutting down user thread")
          userClassThread.interrupt()
        }
        if (!inShutdown) delegationTokenRenewerOption.foreach(_.stop())
      }
    }
  }

  /**
    * 更新sparkContextRef变量，使得ApplicationMaster持有一个SparkContext对象
    * @param sc
    */
  private def sparkContextInitialized(sc: SparkContext) = {
    /** *
      * 既然sparkContextRef使用了加锁机制，那就没必要使用CAS原语进行更新
      */
    sparkContextRef.synchronized {
      sparkContextRef.compareAndSet(null, sc)
      sparkContextRef.notifyAll()
    }
  }

  private def sparkContextStopped(sc: SparkContext) = {
    sparkContextRef.compareAndSet(sc, null)
  }

  /**
    * 启动完ApplicationMaster和Driver后，ApplicationMaster就可以申请计算资源了
    *
    * 假如我在spark-submit上指定了申请了10个executor，这个信息，ApplicationMaster是如何得知的？
    * @param _rpcEnv
    * @param driverRef
    * @param applicationTrackingUrl
    * @param securityMgr
    */
  private def registerAM(
      _rpcEnv: RpcEnv,
      driverRef: RpcEndpointRef,
      applicationTrackingUrl: String,
      securityMgr: SecurityManager) = {
    val sc = sparkContextRef.get()

    val appId = client.getAttemptId().getApplicationId().toString()
    val attemptId = client.getAttemptId().getAttemptId().toString()
    /** *
      * 历史服务器的地址，如果没有配置spark.yarn.historyServer.address，则返回null
      *
      * historyAddress的地址构成规则： http://localhost:19888/history/{appId}/{attemptId}
      */
    val historyAddress =
      sparkConf.getOption("spark.yarn.historyServer.address")
        .map { text => SparkHadoopUtil.get.substituteHadoopVariables(text, yarnConf) }
        .map { address => s"${address}${HistoryServer.UI_PATH_PREFIX}/${appId}/${attemptId}" }
        .getOrElse("")

    /**
      * 获得用户的SparkConf
     * 如果是YARN CLUSTER模式，那么sc不为null，直接通过SparkContext获得SparkConf实例
     * 如果是YARN CLIENT模式，那么sc为null，那么就通过不带参数的new SparkConf加载配置
      */
    val _sparkConf = if (sc != null) sc.getConf else sparkConf
    val driverUrl = RpcEndpointAddress(
      _sparkConf.get("spark.driver.host"),
      _sparkConf.get("spark.driver.port").toInt,
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString


    /**
      *ApplicationMaster进程启动后，ApplicationMaster进程将自身注册给ResourceManager，返回一个YarnAllocator对象。
     *
     * 注意：YarnAllocator是Spark Yarn提供的API，而不是Hadoop YARN提供的API
     *
     * 问题：
      * 1. 具体需要向ResourceManager申请多少个Container是在什么地方控制的？
     *     答案： 这个信息写在_sparkConf对象中
     * 2. ApplicationMaster申请到了Container并启动后，Driver是如何如何得知的？(Executor进程会主动向Driver注册)
     * 3. 如果有Executor挂了，ApplicationMaster如何重新申请？Executor作为一个Container，它挂了，NodeManager能够知道，NodeManager
     * 会向ApplicationMaster以及ResourceManager汇报？
     *
     * 这个信息是在reporterThread的任务体中处理的
      */
    allocator = client.register(driverUrl,
      driverRef,
      yarnConf,
      _sparkConf,
      applicationTrackingUrl,
      historyAddress,
      securityMgr)

    /**
      * 调用YarnAllocator的allocateResource进行资源分配
     * 问题：如果Executor/Container一次性没有全部分配，或者分配到的Container在application运行中有挂，如何容错？
      */
    allocator.allocateResources()

    /** *
      * 启动汇报线程，这个线程很关键
      * Executor是否已经挂了，在这个汇报线程中检测
      */
    reporterThread = launchReporterThread()
  }

  /**
   * Create an [[RpcEndpoint]] that communicates with the driver.
   *
   * In cluster mode, the AM and the driver belong to same process
   * so the AMEndpoint need not monitor lifecycle of the driver.
   *
   * @return A reference to the driver's RPC endpoint.
   */
  private def runAMEndpoint(
      host: String,
      port: String,
      isClusterMode: Boolean): RpcEndpointRef = {
    val driverEndpoint = rpcEnv.setupEndpointRef(
      RpcAddress(host, port.toInt),
      YarnSchedulerBackend.ENDPOINT_NAME)
    amEndpoint =
      rpcEnv.setupEndpoint("YarnAM", new AMEndpoint(rpcEnv, driverEndpoint, isClusterMode))
    driverEndpoint
  }

  /**
    * 在startUserApplication方法中执行用户程序中的main然后等待SparkContext初始化成功
    *
    * 因为Driver和ApplicationMaster运行在同一个Java进程中，因此对于设置到system properties中的spark属性，Driver的用户代码通过new SparkConf创建SparkConf也可以读取到
    *
    * @param securityMgr
    */
  private def runDriver(securityMgr: SecurityManager): Unit = {
    addAmIpFilter()

    /** *
      * 启动用户线程，该线程保存在userClassThread中
      * 该用户线程userClassThread用于启动SparkSubmit的主类，也就是执行Application的逻辑
      * 问题：Application启动起来，ApplicationMaster如何帮助Driver申请资源？
      */
    userClassThread = startUserApplication()

    // This a bit hacky, but we need to wait until the spark.driver.port property has
    // been set by the Thread executing the user class.

    /***
      *  等待Driver的SparkContext初始化完成
      *
      * 问题：如何判断Driver（或者SparkContext)已经初始化完成。
      * 在前面startUserApplication方法中已经开始执行用户Class的main方法，此处同步等待SparkContext初始化完成
      * 方法是：socket连接到Driver的host和port
      */
    val sc = waitForSparkContextInitialized()

    // If there is no SparkContext at this point, just fail the app.
    if (sc == null) {
      finish(FinalApplicationStatus.FAILED,
        ApplicationMaster.EXIT_SC_NOT_INITED,
        "Timed out waiting for SparkContext.")
    } else {
      rpcEnv = sc.env.rpcEnv
      val driverRef = runAMEndpoint(
        sc.getConf.get("spark.drivr.host"), //SparkContext会把host和port记录在SparkConf中
        sc.getConf.get("spark.driver.port"),
        isClusterMode = true)

      /**
        * 注册AM?在ApplicationMaster中运行registerAM
        */
      val applicationTrackingUrl = sc.ui.map(_.appUIAddress).getOrElse("")


      /** *
        * 将自身注册到ResourceManager中，携带着要申请的资源
        */
      registerAM(rpcEnv, driverRef, applicationTrackingUrl, securityMgr)

      /** *
        * 将用户应用线程合并，因为userClassThread是要等待执行完成
        * 因此此处也将等待用户作业执行完成
        */
      userClassThread.join()
    }
  }

  private def runExecutorLauncher(securityMgr: SecurityManager): Unit = {
    val port = sparkConf.getInt("spark.yarn.am.port", 0)
    rpcEnv = RpcEnv.create("sparkYarnAM", Utils.localHostName, port, sparkConf, securityMgr,
      clientMode = true)
    val driverRef = waitForSparkDriver()
    addAmIpFilter()
    registerAM(rpcEnv, driverRef, sparkConf.get("spark.driver.appUIAddress", ""), securityMgr)

    // In client mode the actor will stop the reporter thread.
    reporterThread.join()
  }

  /***
    * ApplicationMaster启动汇报进程
    * 可以用于Executor失败时重新启动？
    * @return
    */
  private def launchReporterThread(): Thread = {
    // The number of failures in a row until Reporter thread give up
    val reporterMaxFailures = sparkConf.getInt("spark.yarn.scheduler.reporterThread.maxFailures", 5)

    val t = new Thread {
      override def run() {
        var failureCount = 0
        while (!finished) {
          try {
            //如果失败数大于executor数，那么作业将停止？比如./spark-shell --master yarn-client --num-executors 2，那么杀死两次就会完蛋？不对！
            //可以杀一个重启一个，重启后再杀，杀了在重启，作业依然存活
            if (allocator.getNumExecutorsFailed >= maxNumExecutorFailures) {
              finish(FinalApplicationStatus.FAILED,
                ApplicationMaster.EXIT_MAX_EXECUTOR_FAILURES,
                s"Max number of executor failures ($maxNumExecutorFailures) reached")
            } else {
              logDebug("Sending progress")
              allocator.allocateResources()
            }
            failureCount = 0
          } catch {
            case i: InterruptedException =>
            case e: Throwable => {
              failureCount += 1
              // this exception was introduced in hadoop 2.4 and this code would not compile
              // with earlier versions if we refer it directly.
              if ("org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException" ==
                e.getClass().getName()) {
                logError("Exception from Reporter thread.", e)
                finish(FinalApplicationStatus.FAILED, ApplicationMaster.EXIT_REPORTER_FAILURE,
                  e.getMessage)
              } else if (!NonFatal(e) || failureCount >= reporterMaxFailures) {
                finish(FinalApplicationStatus.FAILED,
                  ApplicationMaster.EXIT_REPORTER_FAILURE, "Exception was thrown " +
                    s"$failureCount time(s) from Reporter thread.")
              } else {
                logWarning(s"Reporter thread fails $failureCount time(s) in a row.", e)
              }
            }
          }
          try {
            val numPendingAllocate = allocator.getPendingAllocate.size
            allocatorLock.synchronized {
              val sleepInterval =
                if (numPendingAllocate > 0 || allocator.getNumPendingLossReasonRequests > 0) {
                  val currentAllocationInterval =
                    math.min(heartbeatInterval, nextAllocationInterval)

                  /**
                   * 更新nextAllocationInterval
                   */
                  nextAllocationInterval = currentAllocationInterval * 2 // avoid overflow
                  currentAllocationInterval
                } else {
                  nextAllocationInterval = initialAllocationInterval
                  heartbeatInterval
                }
              logDebug(s"Number of pending allocations is $numPendingAllocate. " +
                       s"Sleeping for $sleepInterval.")
              allocatorLock.wait(sleepInterval)
            }
          } catch {
            case e: InterruptedException =>
          }
        }
      }
    }
    // setting to daemon status, though this is usually not a good idea.
    t.setDaemon(true)
    t.setName("Reporter")
    t.start()
    logInfo(s"Started progress reporter thread with (heartbeat : $heartbeatInterval, " +
            s"initial allocation : $initialAllocationInterval) intervals")
    t
  }

  /**
   * Clean up the staging directory.
   */
  private def cleanupStagingDir(fs: FileSystem) {
    var stagingDirPath: Path = null
    try {
      val preserveFiles = sparkConf.getBoolean("spark.yarn.preserve.staging.files", false)
      if (!preserveFiles) {
        stagingDirPath = new Path(System.getenv("SPARK_YARN_STAGING_DIR"))
        if (stagingDirPath == null) {
          logError("Staging directory is null")
          return
        }
        logInfo("Deleting staging directory " + stagingDirPath)
        fs.delete(stagingDirPath, true)
      }
    } catch {
      case ioe: IOException =>
        logError("Failed to cleanup staging dir " + stagingDirPath, ioe)
    }
  }

  private def waitForSparkContextInitialized(): SparkContext = {
    logInfo("Waiting for spark context initialization")
    sparkContextRef.synchronized {
      val totalWaitTime = sparkConf.getTimeAsMs("spark.yarn.am.waitTime", "100s")
      val deadline = System.currentTimeMillis() + totalWaitTime

      /**
        * 每隔10秒钟醒一次
        */
      while (sparkContextRef.get() == null && System.currentTimeMillis < deadline && !finished) {
        logInfo("Waiting for spark context initialization ... ")
        sparkContextRef.wait(10000L)
      }

      val sparkContext = sparkContextRef.get()
      if (sparkContext == null) {
        logError(("SparkContext did not initialize after waiting for %d ms. Please check earlier"
          + " log output for errors. Failing the application.").format(totalWaitTime))
      }
      sparkContext
    }
  }

  private def waitForSparkDriver(): RpcEndpointRef = {
    logInfo("Waiting for Spark driver to be reachable.")
    var driverUp = false
    val hostport = args.userArgs(0)
    val (driverHost, driverPort) = Utils.parseHostPort(hostport)

    // Spark driver should already be up since it launched us, but we don't want to
    // wait forever, so wait 100 seconds max to match the cluster mode setting.
    val totalWaitTimeMs = sparkConf.getTimeAsMs("spark.yarn.am.waitTime", "100s")
    val deadline = System.currentTimeMillis + totalWaitTimeMs

    while (!driverUp && !finished && System.currentTimeMillis < deadline) {
      try {
        val socket = new Socket(driverHost, driverPort)
        socket.close()
        logInfo("Driver now available: %s:%s".format(driverHost, driverPort))
        driverUp = true
      } catch {
        case e: Exception =>
          logError("Failed to connect to driver at %s:%s, retrying ...".
            format(driverHost, driverPort))
          Thread.sleep(100L)
      }
    }

    if (!driverUp) {
      throw new SparkException("Failed to connect to driver!")
    }

    sparkConf.set("spark.driver.host", driverHost)
    sparkConf.set("spark.driver.port", driverPort.toString)

    runAMEndpoint(driverHost, driverPort.toString, isClusterMode = false)
  }

  /** Add the Yarn IP filter that is required for properly securing the UI. */
  private def addAmIpFilter() = {
    val proxyBase = System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV)
    val amFilter = "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter"
    val params = client.getAmIpFilterParams(yarnConf, proxyBase)
    if (isClusterMode) {
      System.setProperty("spark.ui.filters", amFilter)
      params.foreach { case (k, v) => System.setProperty(s"spark.$amFilter.param.$k", v) }
    } else {
      amEndpoint.send(AddWebUIFilter(amFilter, params.toMap, proxyBase))
    }
  }

  /**
   * Start the user class, which contains the spark driver, in a separate Thread.
   * If the main routine exits cleanly or exits with System.exit(N) for any N
   * we assume it was successful, for all other cases we assume failure.
   *
   * Returns the user thread that was started.
    *
    *
    * 执行用户class的main方法
   */
  private def startUserApplication(): Thread = {
    logInfo("Starting the user application in a separate Thread")

    /**
      * 从SparkConf中得到用户jar的路径，是一个HDFS路径
      */
    val classpath = Client.getUserClasspath(sparkConf)
    val urls = classpath.map { entry =>
      new URL("file:" + new File(entry.getPath()).getAbsolutePath())
    }

    /**
      * 将urls作为classpath的一部分启动ClassLoader
      */
    val userClassLoader =
      if (Client.isUserClassPathFirst(sparkConf, isDriver = true)) {
        new ChildFirstURLClassLoader(urls, Utils.getContextOrSparkClassLoader)
      } else {
        new MutableURLClassLoader(urls, Utils.getContextOrSparkClassLoader)
      }

    var userArgs = args.userArgs
    if (args.primaryPyFile != null && args.primaryPyFile.endsWith(".py")) {
      // When running pyspark, the app is run using PythonRunner. The second argument is the list
      // of files to add to PYTHONPATH, which Client.scala already handles, so it's empty.
      userArgs = Seq(args.primaryPyFile, "") ++ userArgs
    }
    if (args.primaryRFile != null && args.primaryRFile.endsWith(".R")) {
      // TODO(davies): add R dependencies here
    }
    /** *
      * 使用userClassLoader加载Class
      */
    val mainMethod = userClassLoader.loadClass(args.userClass)
      .getMethod("main", classOf[Array[String]])

    /** *
      * 以单独的线程执行用户提交的main class，即以单独从线程执行用户提交的Application的逻辑
      */
    val userThread = new Thread {
      override def run() {
        try {

          /**
            * 启动Driver，调用用户提交的mainClass的main方法，该main方法执行完返回表示用户作业已经执行完
            */
          mainMethod.invoke(null, userArgs.toArray)

          /**
            * finish方法是干啥的？
           * 因为用户class的main方法已经执行完，因此整个Application都执行完了，开始设置任务状态
            */
          finish(FinalApplicationStatus.SUCCEEDED, ApplicationMaster.EXIT_SUCCESS)

          /** *
            * 打印日志提示用户类已经执行完成
            */
          logDebug("Done running users class")
        } catch {
          case e: InvocationTargetException =>
            e.getCause match {
              case _: InterruptedException =>
                // Reporter thread can interrupt to stop user class
              case SparkUserAppException(exitCode) =>
                val msg = s"User application exited with status $exitCode"
                logError(msg)
                finish(FinalApplicationStatus.FAILED, exitCode, msg)
              case cause: Throwable =>
                logError("User class threw exception: " + cause, cause)
                finish(FinalApplicationStatus.FAILED,
                  ApplicationMaster.EXIT_EXCEPTION_USER_CLASS,
                  "User class threw exception: " + cause)
            }
        }
      }
    }

    /** *
      * 为线程指定ClassLoader
      */
    userThread.setContextClassLoader(userClassLoader)
    userThread.setName("Driver")

    /** *
      * 启动用户线程
      */
    userThread.start()
    userThread
  }

  /** *
    * 重置Allocator Interval
    */
  private def resetAllocatorInterval(): Unit = allocatorLock.synchronized {
    nextAllocationInterval = initialAllocationInterval
    allocatorLock.notifyAll()
  }

  /**
   * An [[RpcEndpoint]] that communicates with the driver's scheduler backend.
   * 与SchedulerBackend进行通信的ApplicationMaster实例
   */
  private class AMEndpoint(
      override val rpcEnv: RpcEnv, driver: RpcEndpointRef, isClusterMode: Boolean)
    extends RpcEndpoint with Logging {

    override def onStart(): Unit = {
      driver.send(RegisterClusterManager(self))
    }

    override def receive: PartialFunction[Any, Unit] = {
      case x: AddWebUIFilter =>
        logInfo(s"Add WebUI Filter. $x")
        driver.send(x)
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      /** *
        * ApplicationMaster收到RequestExecutors消息
        * 问题：这个消息是谁发送的？
        * 这个消息带有三个参数：
        * 1. requestedTotal，申请的executor总数
        * 2. 具有数据本地性的任务个数
        * 3. 具有数据本地性的host和在它之上运行的task个数的Map
        */
      case RequestExecutors(requestedTotal, localityAwareTasks, preferredHostToLocalTaskCountMap) =>
        Option(allocator) match {
          case Some(a) =>
            if (a.requestTotalExecutorsWithPreferredLocalities(requestedTotal,
              localityAwareTasks, preferredHostToLocalTaskCountMap)) {
              resetAllocatorInterval()
            }
            context.reply(true)

          case None =>
            logWarning("Container allocator is not ready to request executors yet.")
            context.reply(false)
        }

      case KillExecutors(executorIds) =>
        logInfo(s"Driver requested to kill executor(s) ${executorIds.mkString(", ")}.")
        Option(allocator) match {
          case Some(a) => executorIds.foreach(a.killExecutor)
          case None => logWarning("Container allocator is not ready to kill executors yet.")
        }
        context.reply(true)

      case GetExecutorLossReason(eid) =>
        Option(allocator) match {
          case Some(a) =>
            a.enqueueGetLossReasonRequest(eid, context)
            resetAllocatorInterval()
          case None =>
            logWarning("Container allocator is not ready to find executor loss reasons yet.")
        }
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      // In cluster mode, do not rely on the disassociated event to exit
      // This avoids potentially reporting incorrect exit codes if the driver fails
      if (!isClusterMode) {
        logInfo(s"Driver terminated or disconnected! Shutting down. $remoteAddress")
        finish(FinalApplicationStatus.SUCCEEDED, ApplicationMaster.EXIT_SUCCESS)
      }
    }
  }

}

/**
 *  在yarn-client模式下，启动ExecutorLauncher进程
 * 在yarn-cluster模式下，启动ApplicationMaster进程
 */
object ApplicationMaster extends Logging {

  // exit codes for different causes, no reason behind the values
  private val EXIT_SUCCESS = 0
  private val EXIT_UNCAUGHT_EXCEPTION = 10
  private val EXIT_MAX_EXECUTOR_FAILURES = 11
  private val EXIT_REPORTER_FAILURE = 12
  private val EXIT_SC_NOT_INITED = 13
  private val EXIT_SECURITY = 14
  private val EXIT_EXCEPTION_USER_CLASS = 15

  /**
    * master是ApplicationMaster class的对象
   * 也就是说，ApplicationMaster object持有一个ApplicationMaster class的对象
    */
  private var master: ApplicationMaster = _

  /**
    * ApplicationMaster进程的命令行参数包装在ApplicationMasterArguments类中
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    SignalLogger.register(log)
    val amArgs = new ApplicationMasterArguments(args)

    /** *
      * runAsSparkUser的参数是一个函数字面量，该函数的参数是空，函数体是创建ApplicationMaster并执行ApplicationMaster的run方法
      */
    SparkHadoopUtil.get.runAsSparkUser { () =>
      val am2rmClient = new YarnRMClient(amArgs)

      /** *
        * 初始化master对象
        */
      master = new ApplicationMaster(amArgs, am2rmClient)
      /** *
        * 执行ApplicationMaster的run方法，知道运行结束
        */
      val exitCode = master.run()
      System.exit(exitCode)
    }
  }

  /** *
    * SparkContext初始化完成
    * @param sc
    */
  private[spark] def sparkContextInitialized(sc: SparkContext): Unit = {
    master.sparkContextInitialized(sc)
  }

  private[spark] def sparkContextStopped(sc: SparkContext): Boolean = {
    master.sparkContextStopped(sc)
  }

  /** *
    * ApplicationMaster object定义了getAttemptId方法，返回ApplicationAttemptId
    * ApplicationAttemptId类包含了ApplicationId属性和attemptId属性
    *
    * @return
    */
  private[spark] def getAttemptId(): ApplicationAttemptId = {
    master.getAttemptId
  }

}

/**
 * This object does not provide any special functionality. It exists so that it's easy to tell
 * apart the client-mode AM from the cluster-mode AM when using tools such as ps or jps.
 *
 * 在yarn-client模式下，启动ExecutorLauncher进程
 * 在yarn-cluster模式下，启动ApplicationMaster进程
 */
object ExecutorLauncher {

  /**
   * 执行ExecutorLauncher进程
   * args参数列表：
   * --arg: Driver的IP和Port
   * --executor-memory 1024m
   * --executor-cores 1
   * --properties-file 存放在Hadoop目录上的配置文件，这个文件的内容是Spark写入的
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    ApplicationMaster.main(args)
  }

}
