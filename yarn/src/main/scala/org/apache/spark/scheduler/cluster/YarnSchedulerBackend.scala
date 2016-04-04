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

package org.apache.spark.scheduler.cluster

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, ApplicationId}

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rpc._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.ui.JettyUtils
import org.apache.spark.util.{RpcUtils, ThreadUtils}

/**
 * Abstract Yarn scheduler backend that contains common logic between the client and cluster Yarn scheduler backends.
 *
 * YarnSchedulerBackend是YarnClusterSchedulerBackend和YarnClientSchedulerBackend的共同父类
 */

/** *
  * YarnSchedulerBackend实现了CoarseGrainedSchedulerBackend，
  * 为什么查看CoarseGrainedSchedulerBackend的实现类时，没有显示YarnSchedulerBackend
  * @param scheduler
  * @param sc
  */
private[spark] abstract class YarnSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  /** *
    * 如果没有指定spark.scheduler.minRegisteredResourcesRatio，那么
    * minRegisteredRatio指定为0.8
    */
  if (conf.getOption("spark.scheduler.minRegisteredResourcesRatio").isEmpty) {
    minRegisteredRatio = 0.8
  }

  protected var totalExpectedExecutors = 0

  private val yarnSchedulerEndpoint = new YarnSchedulerEndpoint(rpcEnv)

  private val yarnSchedulerEndpointRef = rpcEnv.setupEndpoint(
    YarnSchedulerBackend.ENDPOINT_NAME, yarnSchedulerEndpoint)

  private implicit val askTimeout = RpcUtils.askRpcTimeout(sc.conf)

  /** Application ID. */
  protected var appId: Option[ApplicationId] = None

  /** Attempt ID. This is unset for client-mode schedulers */
  private var attemptId: Option[ApplicationAttemptId] = None

  /** Scheduler extension services. */
  private val services: SchedulerExtensionServices = new SchedulerExtensionServices()

  // Flag to specify whether this schedulerBackend should be reset.
  private var shouldResetOnAmRegister = false

  /**
   * Bind to YARN. This *must* be done before calling [[start()]].
   *
   * 在调用start方法前需要调用这个方法，设置appId和attemptId
   *
   * @param appId YARN application ID
   * @param attemptId Optional YARN attempt ID
   */
  protected def bindToYarn(appId: ApplicationId, attemptId: Option[ApplicationAttemptId]): Unit = {
    this.appId = Some(appId)
    this.attemptId = attemptId
  }

  override def start() {
    require(appId.isDefined, "application ID unset")
    val binding = SchedulerExtensionServiceBinding(sc, appId.get, attemptId)
    services.start(binding)
    super.start()
  }

  override def stop(): Unit = {
    try {
      super.stop()
    } finally {
      services.stop()
    }
  }

  /**
   * Get the attempt ID for this run, if the cluster manager supports multiple
   * attempts. Applications run in client mode will not have attempt IDs.
   *
   * @return The application attempt id, if available.
   */
  override def applicationAttemptId(): Option[String] = {
    attemptId.map(_.toString)
  }

  /**
   * Get an application ID associated with the job.
   * This returns the string value of [[appId]] if set, otherwise
   * the locally-generated ID from the superclass.
   * @return The application ID
   */
  override def applicationId(): String = {
    appId.map(_.toString).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }
  }

  /**
   * Request executors from the ApplicationMaster by specifying the total number desired.
   * This includes executors already pending or running.
   */
  override def doRequestTotalExecutors(requestedTotal: Int): Boolean = {
    yarnSchedulerEndpointRef.askWithRetry[Boolean](
      RequestExecutors(requestedTotal, localityAwareTasks, hostToLocalTaskCount))
  }

  /**
   * Request that the ApplicationMaster kill the specified executors.
   */
  override def doKillExecutors(executorIds: Seq[String]): Boolean = {
    yarnSchedulerEndpointRef.askWithRetry[Boolean](KillExecutors(executorIds))
  }

  /** *
    * 是否已经分配了足够的资源以使YarnSchedulerBackend进入就绪状态。
    * 它决定YarnSchedulerBackend是否进入就绪状态
    * @return
    */
  override def sufficientResourcesRegistered(): Boolean = {

    /** *
      * 已经注册的executor的个数>=总的executor个数*最小比例
      * 问题：
      * 在YARN CLUSTER模式下，
      * 1. 已经注册的Executor个数如何判定？
      * 在 CoarseGrainedSchedulerBackend的RegisterExecutor消息中将totalRegisteredExecutors做了增1操作，在RemoveExecutor消息
      * 中将totalRegisteredExecutors做了减1操作
      *
      * 2. totalExpectedExecutors个数如何判定
      * totalExpectedExecutors定义在YarnSchedulerBackend中，在YarnSchedulerBackend中并没有对它进行修改，
      * totalExpectedExecutors的修改是在它的子类(比如YarnClusterSchedulerBackend)中执行的
      * 在YarnClusterSchedulerBackend中，totalExpectedExecutors的处理逻辑是对于静态分配则取用户指定的--executor-number值，如果是动态分配，则取初始值
      *
      * 3. minRegisteredRatio 最小比例如何判定
      * 在YarnSchedulerBackend中，minRegisteredRatio指定为0.8
      *
      * 结论：也就是说对于Yarn模式，注册的Executor个数要到达executor总个数*0.8, YarnSchedulerBackend才能进入就绪状态
      * 所谓的YarnSchedulerBackend进入就绪状态是指进行任务分配？
      */
    totalRegisteredExecutors.get() >= totalExpectedExecutors * minRegisteredRatio
  }

  /**
   * Add filters to the SparkUI.
   */
  private def addWebUIFilter(
      filterName: String,
      filterParams: Map[String, String],
      proxyBase: String): Unit = {
    if (proxyBase != null && proxyBase.nonEmpty) {
      System.setProperty("spark.ui.proxyBase", proxyBase)
    }

    val hasFilter =
      filterName != null && filterName.nonEmpty &&
      filterParams != null && filterParams.nonEmpty
    if (hasFilter) {
      logInfo(s"Add WebUI Filter. $filterName, $filterParams, $proxyBase")
      conf.set("spark.ui.filters", filterName)
      filterParams.foreach { case (k, v) => conf.set(s"spark.$filterName.param.$k", v) }
      scheduler.sc.ui.foreach { ui => JettyUtils.addFilters(ui.getHandlers, conf) }
    }
  }

  override def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    new YarnDriverEndpoint(rpcEnv, properties)
  }

  /**
   * Reset the state of SchedulerBackend to the initial state. This is happened when AM is failed
   * and re-registered itself to driver after a failure. The stale state in driver should be
   * cleaned.
   */
  override protected def reset(): Unit = {
    super.reset()
    sc.executorAllocationManager.foreach(_.reset())
  }

  /**
   * Override the DriverEndpoint to add extra logic for the case when an executor is disconnected.
   * This endpoint communicates with the executors and queries the AM for an executor's exit
   * status when the executor is disconnected.
   */
  private class YarnDriverEndpoint(rpcEnv: RpcEnv, sparkProperties: Seq[(String, String)])
      extends DriverEndpoint(rpcEnv, sparkProperties) {

    /**
     * When onDisconnected is received at the driver endpoint, the superclass DriverEndpoint
     * handles it by assuming the Executor was lost for a bad reason and removes the executor
     * immediately.
     *
     * In YARN's case however it is crucial to talk to the application master and ask why the
     * executor had exited. If the executor exited for some reason unrelated to the running tasks
     * (e.g., preemption), according to the application master, then we pass that information down
     * to the TaskSetManager to inform the TaskSetManager that tasks on that lost executor should
     * not count towards a job failure.
     */
    override def onDisconnected(rpcAddress: RpcAddress): Unit = {
      addressToExecutorId.get(rpcAddress).foreach { executorId =>
        if (disableExecutor(executorId)) {
          yarnSchedulerEndpoint.handleExecutorDisconnectedFromDriver(executorId, rpcAddress)
        }
      }
    }
  }

  /**
   * An [[RpcEndpoint]] that communicates with the ApplicationMaster.
   */
  private class YarnSchedulerEndpoint(override val rpcEnv: RpcEnv)
    extends ThreadSafeRpcEndpoint with Logging {
    private var amEndpoint: Option[RpcEndpointRef] = None

    private val askAmThreadPool =
      ThreadUtils.newDaemonCachedThreadPool("yarn-scheduler-ask-am-thread-pool")
    implicit val askAmExecutor = ExecutionContext.fromExecutor(askAmThreadPool)

    private[YarnSchedulerBackend] def handleExecutorDisconnectedFromDriver(
        executorId: String,
        executorRpcAddress: RpcAddress): Unit = {
      amEndpoint match {
        case Some(am) =>
          val lossReasonRequest = GetExecutorLossReason(executorId)
          val future = am.ask[ExecutorLossReason](lossReasonRequest, askTimeout)
          future onSuccess {
            case reason: ExecutorLossReason => {
              driverEndpoint.askWithRetry[Boolean](RemoveExecutor(executorId, reason))
            }
          }
          future onFailure {
            case NonFatal(e) => {
              logWarning(s"Attempted to get executor loss reason" +
                s" for executor id ${executorId} at RPC address ${executorRpcAddress}," +
                s" but got no response. Marking as slave lost.", e)
              driverEndpoint.askWithRetry[Boolean](RemoveExecutor(executorId, SlaveLost()))
            }
            case t => throw t
          }
        case None =>
          logWarning("Attempted to check for an executor loss reason" +
            " before the AM has registered!")
          driverEndpoint.askWithRetry[Boolean](
            RemoveExecutor(executorId, SlaveLost("AM is not yet registered.")))
      }
    }

    override def receive: PartialFunction[Any, Unit] = {
      case RegisterClusterManager(am) =>
        logInfo(s"ApplicationMaster registered as $am")
        amEndpoint = Option(am)
        if (!shouldResetOnAmRegister) {
          shouldResetOnAmRegister = true
        } else {
          // AM is already registered before, this potentially means that AM failed and
          // a new one registered after the failure. This will only happen in yarn-client mode.
          reset()
        }

      case AddWebUIFilter(filterName, filterParams, proxyBase) =>
        addWebUIFilter(filterName, filterParams, proxyBase)

      case RemoveExecutor(executorId, reason) =>
        logWarning(reason.toString)
        removeExecutor(executorId, reason)
    }


    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case r: RequestExecutors =>
        amEndpoint match {
          case Some(am) =>
            Future {
              context.reply(am.askWithRetry[Boolean](r))
            } onFailure {
              case NonFatal(e) =>
                logError(s"Sending $r to AM was unsuccessful", e)
                context.sendFailure(e)
            }
          case None =>
            logWarning("Attempted to request executors before the AM has registered!")
            context.reply(false)
        }

      case k: KillExecutors =>
        amEndpoint match {
          case Some(am) =>
            Future {
              context.reply(am.askWithRetry[Boolean](k))
            } onFailure {
              case NonFatal(e) =>
                logError(s"Sending $k to AM was unsuccessful", e)
                context.sendFailure(e)
            }
          case None =>
            logWarning("Attempted to kill executors before the AM has registered!")
            context.reply(false)
        }
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      if (amEndpoint.exists(_.address == remoteAddress)) {
        logWarning(s"ApplicationMaster has disassociated: $remoteAddress")
        amEndpoint = None
      }
    }

    override def onStop(): Unit = {
      askAmThreadPool.shutdownNow()
    }
  }
}

private[spark] object YarnSchedulerBackend {
  val ENDPOINT_NAME = "YarnScheduler"
}
