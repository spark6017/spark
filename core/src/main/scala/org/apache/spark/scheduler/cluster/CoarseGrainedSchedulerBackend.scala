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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import org.apache.spark.{ExecutorAllocationClient, Logging, SparkEnv, SparkException, TaskState}
import org.apache.spark.rpc._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend.ENDPOINT_NAME
import org.apache.spark.util.{RpcUtils, SerializableBuffer, ThreadUtils, Utils}

/**
 * A scheduler backend that waits for coarse-grained executors to connect.
 * This backend holds onto each executor for the duration of the Spark job rather than relinquishing
 * executors whenever a task is done and asking the scheduler to launch a new executor for
 * each new task. Executors may be launched in a variety of ways, such as Mesos tasks for the
 * coarse-grained Mesos mode or standalone processes for Spark's standalone deploy mode
 * (spark.deploy.*).
 */
private[spark]
class CoarseGrainedSchedulerBackend(scheduler: TaskSchedulerImpl, val rpcEnv: RpcEnv)
  extends ExecutorAllocationClient with SchedulerBackend with Logging
{
  /** *
    *  Use an atomic variable to track total number of cores in the cluster for simplicity and speed
    *  该Application申请的内核总数
    */
  var totalCoreCount = new AtomicInteger(0)

  /** *
    * Total number of executors that are currently registered
    * 已经注册的Executor的数目
    */
  var totalRegisteredExecutors = new AtomicInteger(0)
  val conf = scheduler.sc.conf
  private val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)

  /** *
    * Submit tasks only after (registered resources / total expected resources)  is equal to larger than this value, that is double between 0 and 1.
    *
    * 如果不配置spark.scheduler.minRegisteredResourcesRatio这个参数，那么默认是0，
    * 这个值在YarnSchedulerBackend中进行了重写
    */
  var minRegisteredRatio =
    math.min(1, conf.getDouble("spark.scheduler.minRegisteredResourcesRatio", 0))

  /** *
    * Submit tasks after maxRegisteredWaitingTime milliseconds  if minRegisteredRatio has not yet been reached
    * 如果注册资源/申请资源小于minRegisteredRatio，那么最多等待30s
    */
  val maxRegisteredWaitingTimeMs =
    conf.getTimeAsMs("spark.scheduler.maxRegisteredResourcesWaitingTime", "30s")

  /** *
    * SchedulerBackend的创建时间
    */
  val createTime = System.currentTimeMillis()

  private val executorDataMap = new HashMap[String, ExecutorData]

  // Number of executors requested from the cluster manager that have not registered yet
  private var numPendingExecutors = 0

  private val listenerBus = scheduler.sc.listenerBus

  // Executors we have requested the cluster manager to kill that have not died yet; maps
  // the executor ID to whether it was explicitly killed by the driver (and thus shouldn't
  // be considered an app-related failure).
  private val executorsPendingToRemove = new HashMap[String, Boolean]

  // A map to store hostname with its possible task number running on it
  protected var preferredHostToLocalTaskCountMap: Map[String, Int] = Map.empty

  // The number of pending tasks which is locality required
  protected var localityAwareTasks = 0

  // Executors that have been lost, but for which we don't yet know the real exit reason.
  protected val executorsPendingLossReason = new HashSet[String]

  /**
   * Driver进程对应的RPC通信Endpoint，
    * CoarseGrainedSchedulerBackend会给Driver发送ReviveOffers消息
   *
   * 问题：CoarseGrainedSchedulerBackend有个一个isReady方法，那么DriverEndpoint是在isReady为true之前构造还是在
   * isReady为true之后构造？
   * 答：因为DriverEndpoint是在 CoarseGrainedSchedulerBackend的start方法中创建的，因此构造DriverEndpoint的时序与SchedulerBackend是否isReady无关
   *
   * @param rpcEnv
   * @param sparkProperties
   */
  class DriverEndpoint(override val rpcEnv: RpcEnv, sparkProperties: Seq[(String, String)])
    extends ThreadSafeRpcEndpoint with Logging {

    // If this DriverEndpoint is changed to support multiple threads,
    // then this may need to be changed so that we don't share the serializer
    // instance across threads
    private val ser = SparkEnv.get.closureSerializer.newInstance()

    override protected def log = CoarseGrainedSchedulerBackend.this.log

    protected val addressToExecutorId = new HashMap[RpcAddress, String]

    private val reviveThread =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-revive-thread")

    override def onStart() {
      // Periodically revive offers to allow delay scheduling to work

      /**
        * 每隔1秒中发起1次ReviveOffers，目的是给延时调度的任务执行的机会
        * 延时调度的任务等待一定的时间才被调度
        */
      val reviveIntervalMs = conf.getTimeAsMs("spark.scheduler.revive.interval", "1s")

      reviveThread.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
          Option(self).foreach(_.send(ReviveOffers))
        }
      }, 0, reviveIntervalMs, TimeUnit.MILLISECONDS)
    }

    override def receive: PartialFunction[Any, Unit] = {

      /***
        * Driver进程收到StatusUpdate消息
        * Driver接收ExecutorBackend发送过来的任务状态更新消息，如果任务执行完则归还executor使用的core，为什么不归还内存
        */
      case StatusUpdate(executorId, taskId, state, data) =>

        /**
          * Driver调用TaskScheduler的statusUpdate方法, 告诉TaskScheduler，任务状态已更新
          * 在scheduler的statusUpdate方法中，调用入成功队列、入失败队列以及调用taskId对应的TaskSetManager进行失败重试流程
          */
      scheduler.statusUpdate(taskId, state, data.value)

        /**
          * 任务执行完，释放资源
          */
        if (TaskState.isFinished(state)) {
          executorDataMap.get(executorId) match { /**此处需要将executorId从executorDataMap中删除么？**/
            case Some(executorInfo) =>
              executorInfo.freeCores += scheduler.CPUS_PER_TASK

              /***
                * makeOffers以executorId作为参数，有一个资源给TaskScheduler，然后调用TaskSetManager选择一个合适的任务出来
                */
              makeOffers(executorId)
            case None =>
              // Ignoring the update since we don't know about the executor.
              logWarning(s"Ignored task status update ($taskId state $state) " +
                s"from unknown executor with ID $executorId")
          }
        }

      /**
       * Driver Endpoint收到CoarseGrainedSchedulerBackend的ReviveOffers消息，调用Driver的makeOffers方法
       */
      case ReviveOffers =>
        makeOffers()

      case KillTask(taskId, executorId, interruptThread) =>
        executorDataMap.get(executorId) match {
          case Some(executorInfo) =>
            executorInfo.executorEndpoint.send(KillTask(taskId, executorId, interruptThread))
          case None =>
            // Ignoring the task kill since the executor is not registered.
            logWarning(s"Attempted to kill task $taskId for unknown executor $executorId.")
        }
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

      /***
        * Driver收到RegisterExecutor消息才明确的知道，现在有Executor进程启动就绪可以分配任务了
        * 问题：是谁给Driver发送了RegisterExecutor消息
        * 答案：是CoarseGrainedExecutorBackend进程启动时，执行onStart方法时，向Driver发起RegisterExecutor的消息
        */
      case RegisterExecutor(executorId, executorRef, cores, logUrls) =>
        if (executorDataMap.contains(executorId)) {
          context.reply(RegisterExecutorFailed("Duplicate executor ID: " + executorId))
        } else {
          // If the executor's rpc env is not listening for incoming connections, `hostPort`
          // will be null, and the client connection should be used to contact the executor.
          val executorAddress = if (executorRef.address != null) {
              executorRef.address
            } else {
              context.senderAddress
            }
          logInfo(s"Registered executor $executorRef ($executorAddress) with ID $executorId")
          addressToExecutorId(executorAddress) = executorId

          /***
            * totalCoreCount用于计算默认并行度，有多个core，并行度就是多少
            * 也就是说，这个并行度跟application使用的内核数是相同的
            */
          totalCoreCount.addAndGet(cores)
          totalRegisteredExecutors.addAndGet(1)

          /***
            * 将executor信息封装成ExecutorData进行维护
            */
          val data = new ExecutorData(executorRef, executorRef.address, executorAddress.host,
            cores, cores, logUrls)
          // This must be synchronized because variables mutated
          // in this block are read when requesting executors
          CoarseGrainedSchedulerBackend.this.synchronized {
            executorDataMap.put(executorId, data)

            /***
              * numPendingExecutors记录了持有的executor数目与申请的executor数目之间的差值
              */
            if (numPendingExecutors > 0) {
              numPendingExecutors -= 1
              logDebug(s"Decremented number of pending executors ($numPendingExecutors left)")
            }
          }
          // Note: some tests expect the reply to come after we put the executor in the map
          context.reply(RegisteredExecutor(executorAddress.host))
          listenerBus.post(
            SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, data))

          /***
            * 有新executor加入了，可以为它分配任务了
            */
          makeOffers()
        }

      case StopDriver =>
        context.reply(true)
        stop()

      case StopExecutors =>
        logInfo("Asking each executor to shut down")
        for ((_, executorData) <- executorDataMap) {
          executorData.executorEndpoint.send(StopExecutor)
        }
        context.reply(true)

      case RemoveExecutor(executorId, reason) =>
        removeExecutor(executorId, reason)
        context.reply(true)

      /**
       * 应答CoarseGrainedExecutorBackend的请求，返回Spark的配置信息
       */
      case RetrieveSparkProps =>
        context.reply(sparkProperties)
    }

    // Make fake resource offers on all executors
    private def makeOffers() {
      // Filter out executors under killing

      /**
       * 过滤出Alive的Executor，executorDataMap是HashMap，Key是executorId，Value是ExecutorData
       */
      val activeExecutors = executorDataMap.filterKeys(executorIsAlive)

      /**
       * HashMap的map方法得到的每个元素是一个(K,V),
       * 针对每个Executor,构造WorkerOffer，包括executor所在的Host以及executor空闲的Core
       * 也就是说，每个Executor同时可能执行多个非同一个Stage的任务？
       */
      val workOffers = activeExecutors.map { case (id, executorData) =>
        new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
      }.toSeq

      /**
       * 调用TaskScheduler的resourceOffers方法，这个方法有点名不副实，
       * 这里的逻辑是，给TaskScheduler一些可用的计算资源，让TaskScheduler返回一些可以执行的任务(这些可执行的任务是要考虑数据本地性的)
        *
        * 给定TaskScheduler一些可用资源返回一些可运行的任务，TaskScheduler吧这个职责给谁了？
        *   1. 首先TaskScheduler需要到Pool池中把待调度的任务取出来过一遍?取出来的是TaskSetManager
        *   2. Pool中的每个任务属于一个TaskSetManager，因此，这个应该是TaskSetManager的职责
       */
      val tasks = scheduler.resourceOffers(workOffers)

      /**
       * Driver通过给Executor发送LaunchTask指令启动任务，指令中包含序列化的任务
       */
      launchTasks(tasks)
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      addressToExecutorId
        .get(remoteAddress)
        .foreach(removeExecutor(_, SlaveLost("Remote RPC client disassociated. Likely due to " +
          "containers exceeding thresholds, or network issues. Check driver logs for WARN " +
          "messages.")))
    }

    /**
      * Make fake resource offers on just one executor
      * 当一个executor执行完任务，就可以针对这个executor重新分配任务了
      * @param executorId
      */
    private def makeOffers(executorId: String) {
      // Filter out executors under killing
      if (executorIsAlive(executorId)) {
        val executorData = executorDataMap(executorId)
        val workOffers = Seq(
          new WorkerOffer(executorId, executorData.executorHost, executorData.freeCores))
        launchTasks(scheduler.resourceOffers(workOffers))
      }
    }

    private def executorIsAlive(executorId: String): Boolean = synchronized {
      !executorsPendingToRemove.contains(executorId) &&
        !executorsPendingLossReason.contains(executorId)
    }

    // Launch tasks returned by a set of resource offers
    private def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
      for (task <- tasks.flatten) {
        val serializedTask = ser.serialize(task)
        if (serializedTask.limit >= maxRpcMessageSize) {
          scheduler.taskIdToTaskSetManager.get(task.taskId).foreach { taskSetMgr =>
            try {
              var msg = "Serialized task %s:%d was %d bytes, which exceeds max allowed: " +
                "spark.rpc.message.maxSize (%d bytes). Consider increasing " +
                "spark.rpc.message.maxSize or using broadcast variables for large values."
              msg = msg.format(task.taskId, task.index, serializedTask.limit, maxRpcMessageSize)
              taskSetMgr.abort(msg)
            } catch {
              case e: Exception => logError("Exception in error callback", e)
            }
          }
        }
        else {
          val executorData = executorDataMap(task.executorId)
          executorData.freeCores -= scheduler.CPUS_PER_TASK

          /**
           * 向Executor发送LaunchTask请求
           */
          executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
        }
      }
    }

    // Remove a disconnected slave from the cluster
    def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
      executorDataMap.get(executorId) match {
        case Some(executorInfo) =>
          // This must be synchronized because variables mutated
          // in this block are read when requesting executors
          val killed = CoarseGrainedSchedulerBackend.this.synchronized {
            addressToExecutorId -= executorInfo.executorAddress
            executorDataMap -= executorId
            executorsPendingLossReason -= executorId
            executorsPendingToRemove.remove(executorId).getOrElse(false)
          }
          totalCoreCount.addAndGet(-executorInfo.totalCores)
          totalRegisteredExecutors.addAndGet(-1)
          scheduler.executorLost(executorId, if (killed) ExecutorKilled else reason)
          listenerBus.post(
            SparkListenerExecutorRemoved(System.currentTimeMillis(), executorId, reason.toString))
        case None => logInfo(s"Asked to remove non-existent executor $executorId")
      }
    }

    /**
     * Stop making resource offers for the given executor. The executor is marked as lost with
     * the loss reason still pending.
     *
     * @return Whether executor should be disabled
      *
      * @param executorId 要disable的executor的ID
      * @return
      */
    protected def disableExecutor(executorId: String): Boolean = {
      val shouldDisable = CoarseGrainedSchedulerBackend.this.synchronized {
        if (executorIsAlive(executorId)) {
          executorsPendingLossReason += executorId
          true
        } else {
          // Returns true for explicitly killed executors, we also need to get pending loss reasons;
          // For others return false.
          executorsPendingToRemove.contains(executorId)
        }
      }

      if (shouldDisable) {
        logInfo(s"Disabling executor $executorId.")
        scheduler.executorLost(executorId, LossReasonPending)
      }

      shouldDisable
    }

    override def onStop() {
      reviveThread.shutdownNow()
    }
  }

  var driverEndpoint: RpcEndpointRef = null
  val taskIdsOnSlave = new HashMap[String, HashSet[String]]

  /** *
    * CoarseGrainedSchedulerBackend初始化完成就会构造DriverEndpoint
    */
  override def start() {
    val properties = new ArrayBuffer[(String, String)]
    for ((key, value) <- scheduler.sc.conf.getAll) {
      if (key.startsWith("spark.")) {
        properties += ((key, value))
      }
    }

    // TODO (prashant) send conf instead of properties
    driverEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME, createDriverEndpoint(properties))
  }

  /** *
    * 创建DriverEndPoint
    * @param properties
    * @return
    */
  protected def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    new DriverEndpoint(rpcEnv, properties)
  }

  def stopExecutors() {
    try {
      if (driverEndpoint != null) {
        logInfo("Shutting down all executors")
        driverEndpoint.askWithRetry[Boolean](StopExecutors)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error asking standalone scheduler to shut down executors", e)
    }
  }

  override def stop() {
    stopExecutors()
    try {
      if (driverEndpoint != null) {
        driverEndpoint.askWithRetry[Boolean](StopDriver)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error stopping standalone scheduler's driver endpoint", e)
    }
  }

  /**
   * Reset the state of CoarseGrainedSchedulerBackend to the initial state. Currently it will only
   * be called in the yarn-client mode when AM re-registers after a failure, also dynamic
   * allocation is enabled.
   * */
  protected def reset(): Unit = synchronized {
    if (Utils.isDynamicAllocationEnabled(conf)) {
      numPendingExecutors = 0
      executorsPendingToRemove.clear()

      // Remove all the lingering executors that should be removed but not yet. The reason might be
      // because (1) disconnected event is not yet received; (2) executors die silently.
      executorDataMap.toMap.foreach { case (eid, _) =>
        driverEndpoint.askWithRetry[Boolean](
          RemoveExecutor(eid, SlaveLost("Stale executor after cluster manager re-registered.")))
      }
    }
  }

  /**
   * CoarseGrainedSchedulerBackend的reviveOffers的实现是给Driver发送ReviveOffers消息
   */
  override def reviveOffers() {
    driverEndpoint.send(ReviveOffers)
  }

  override def killTask(taskId: Long, executorId: String, interruptThread: Boolean) {
    driverEndpoint.send(KillTask(taskId, executorId, interruptThread))
  }

  /***
    * 在CoarseGrainedSchedulerBackend，如果配置了spark.default.parallelism，则默认取这个值；
    *
    * 如果没配置这个值，那么并行度最小为2
    * @return
    */
  override def defaultParallelism(): Int = {
    conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
  }

  // Called by subclasses when notified of a lost worker
  def removeExecutor(executorId: String, reason: ExecutorLossReason) {
    try {
      driverEndpoint.askWithRetry[Boolean](RemoveExecutor(executorId, reason))
    } catch {
      case e: Exception =>
        throw new SparkException("Error notifying standalone scheduler's driver endpoint", e)
    }
  }

  /** *
    * 默认是true
    * 问题：谁它进行了重写？
    * @return
    */
  def sufficientResourcesRegistered(): Boolean = true

  /** *
    * SchedulerBackend是否是就绪状态
    * 1.  已经注册了足够的资源
    * 2. 等待的时间到达
    * @return
    */
  override def isReady(): Boolean = {
    /** *
      * 如果已经Application已经申请到了足够的资源(所谓的足够资源是最小比例的资源)
      */
    if (sufficientResourcesRegistered) {
      logInfo("SchedulerBackend is ready for scheduling beginning after " +
        s"reached minRegisteredResourcesRatio: $minRegisteredRatio")
      return true
    }

    /** *
      * 如果能容忍的最小资源也没申请到，那么就等待一定的时间，
      * 超过这个等待时间，SchedulerBackend就进入就绪状态
      */
    if ((System.currentTimeMillis() - createTime) >= maxRegisteredWaitingTimeMs) {
      logInfo("SchedulerBackend is ready for scheduling beginning after waiting " +
        s"maxRegisteredResourcesWaitingTime: $maxRegisteredWaitingTimeMs(ms)")
      return true
    }
    false
  }

  /**
   * Return the number of executors currently registered with this backend.
   */
  def numExistingExecutors: Int = executorDataMap.size

  /**
   * Request an additional number of executors from the cluster manager.
   * @return whether the request is acknowledged.
   */
  final override def requestExecutors(numAdditionalExecutors: Int): Boolean = synchronized {
    if (numAdditionalExecutors < 0) {
      throw new IllegalArgumentException(
        "Attempted to request a negative number of additional executor(s) " +
        s"$numAdditionalExecutors from the cluster manager. Please specify a positive number!")
    }
    logInfo(s"Requesting $numAdditionalExecutors additional executor(s) from the cluster manager")
    logDebug(s"Number of pending executors is now $numPendingExecutors")

    numPendingExecutors += numAdditionalExecutors
    // Account for executors pending to be added or removed
    val newTotal = numExistingExecutors + numPendingExecutors - executorsPendingToRemove.size
    doRequestTotalExecutors(newTotal)
  }

  /**
   * Update the cluster manager on our scheduling needs. Three bits of information are included
   * to help it make decisions.
   * @param numExecutors The total number of executors we'd like to have. The cluster manager
   *                     shouldn't kill any running executor to reach this number, but,
   *                     if all existing executors were to die, this is the number of executors
   *                     we'd want to be allocated.
   * @param localityAwareTasks The number of tasks in all active stages that have a locality
   *                           preferences. This includes running, pending, and completed tasks.
   * @param hostToLocalTaskCount A map of hosts to the number of tasks from all active stages
   *                             that would like to like to run on that host.
   *                             This includes running, pending, and completed tasks.
   * @return whether the request is acknowledged by the cluster manager.
   */
  final override def requestTotalExecutors(
      numExecutors: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int]
    ): Boolean = synchronized {
    if (numExecutors < 0) {
      throw new IllegalArgumentException(
        "Attempted to request a negative number of executor(s) " +
          s"$numExecutors from the cluster manager. Please specify a positive number!")
    }

    this.localityAwareTasks = localityAwareTasks
    this.preferredHostToLocalTaskCountMap = hostToLocalTaskCount

    numPendingExecutors =
      math.max(numExecutors - numExistingExecutors + executorsPendingToRemove.size, 0)
    doRequestTotalExecutors(numExecutors)
  }

  /**
   * Request executors from the cluster manager by specifying the total number desired,
   * including existing pending and running executors.
   *
   * The semantics here guarantee that we do not over-allocate executors for this application,
   * since a later request overrides the value of any prior request. The alternative interface
   * of requesting a delta of executors risks double counting new executors when there are
   * insufficient resources to satisfy the first request. We make the assumption here that the
   * cluster manager will eventually fulfill all requests when resources free up.
   *
   * @return whether the request is acknowledged.
   */
  protected def doRequestTotalExecutors(requestedTotal: Int): Boolean = false

  /**
   * Request that the cluster manager kill the specified executors.
   * @return whether the kill request is acknowledged. If list to kill is empty, it will return
   *         false.
   */
  final override def killExecutors(executorIds: Seq[String]): Boolean = synchronized {
    killExecutors(executorIds, replace = false, force = false)
  }

  /**
   * Request that the cluster manager kill the specified executors.
   *
   * When asking the executor to be replaced, the executor loss is considered a failure, and
   * killed tasks that are running on the executor will count towards the failure limits. If no
   * replacement is being requested, then the tasks will not count towards the limit.
   *
   * @param executorIds identifiers of executors to kill
   * @param replace whether to replace the killed executors with new ones
   * @param force whether to force kill busy executors
   * @return whether the kill request is acknowledged. If list to kill is empty, it will return
   *         false.
   */
  final def killExecutors(
      executorIds: Seq[String],
      replace: Boolean,
      force: Boolean): Boolean = synchronized {
    logInfo(s"Requesting to kill executor(s) ${executorIds.mkString(", ")}")
    val (knownExecutors, unknownExecutors) = executorIds.partition(executorDataMap.contains)
    unknownExecutors.foreach { id =>
      logWarning(s"Executor to kill $id does not exist!")
    }

    // If an executor is already pending to be removed, do not kill it again (SPARK-9795)
    // If this executor is busy, do not kill it unless we are told to force kill it (SPARK-9552)
    val executorsToKill = knownExecutors
      .filter { id => !executorsPendingToRemove.contains(id) }
      .filter { id => force || !scheduler.isExecutorBusy(id) }
    executorsToKill.foreach { id => executorsPendingToRemove(id) = !replace }

    // If we do not wish to replace the executors we kill, sync the target number of executors
    // with the cluster manager to avoid allocating new ones. When computing the new target,
    // take into account executors that are pending to be added or removed.
    if (!replace) {
      doRequestTotalExecutors(
        numExistingExecutors + numPendingExecutors - executorsPendingToRemove.size)
    } else {
      numPendingExecutors += knownExecutors.size
    }

    !executorsToKill.isEmpty && doKillExecutors(executorsToKill)
  }

  /**
   * Kill the given list of executors through the cluster manager.
   * @return whether the kill request is acknowledged.
   */
  protected def doKillExecutors(executorIds: Seq[String]): Boolean = false

}

private[spark] object CoarseGrainedSchedulerBackend {
  val ENDPOINT_NAME = "CoarseGrainedScheduler"
}
