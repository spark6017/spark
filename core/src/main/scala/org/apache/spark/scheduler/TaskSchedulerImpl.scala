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

package org.apache.spark.scheduler

import java.nio.ByteBuffer
import java.util.{Timer, TimerTask}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.language.postfixOps
import scala.util.Random

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.TaskLocality.TaskLocality
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Schedules tasks for multiple types of clusters by acting through a SchedulerBackend.
 * It can also work with a local setup by using a LocalBackend and setting isLocal to true.
 * It handles common logic, like determining a scheduling order across jobs, waking up to launch
 * speculative tasks, etc.
 *
 * Clients should first call initialize() and start(), then submit task sets through the
 * runTasks method.
 *
 * THREADING: SchedulerBackends and task-submitting clients can call this class from multiple
 * threads, so it needs locks in public API methods to maintain its state. In addition, some
 * SchedulerBackends synchronize on themselves when they want to send events here, and then
 * acquire a lock on us, so we need to make sure that we don't try to lock the backend while
 * we are holding a lock on ourselves.
  *
  * TaskSchedulerImpl的构造参数有一个maxTaskFailures，负责失败重试？
  *
  * TaskSchedulerImpl持有一个SchedulerBackend，负责将
  *
 */
private[spark] class TaskSchedulerImpl(
    val sc: SparkContext,
    val maxTaskFailures: Int,
    isLocal: Boolean = false)
  extends TaskScheduler with Logging
{
  /**
    * 只有一个SparkContext参数的构造函数，失败重试次数默认是4，由spark.task.maxFailures决定
    * @param sc
    * @return
    */
  def this(sc: SparkContext) = this(sc, sc.conf.getInt("spark.task.maxFailures", 4))

  val conf = sc.conf

  // How often to check for speculative tasks
  val SPECULATION_INTERVAL_MS = conf.getTimeAsMs("spark.speculation.interval", "100ms")

  private val speculationScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("task-scheduler-speculation")

  // Threshold above which we warn user initial TaskSet may be starved
  val STARVATION_TIMEOUT_MS = conf.getTimeAsMs("spark.starvation.timeout", "15s")

  // CPUs to request per task
  val CPUS_PER_TASK = conf.getInt("spark.task.cpus", 1)

  // TaskSetManagers are not thread safe, so any access to one should be synchronized
  // on this class.
  private val taskSetsByStageIdAndAttempt = new HashMap[Int, HashMap[Int, TaskSetManager]]

  private[scheduler] val taskIdToTaskSetManager = new HashMap[Long, TaskSetManager]
  val taskIdToExecutorId = new HashMap[Long, String]

  @volatile private var hasReceivedTask = false
  @volatile private var hasLaunchedTask = false
  private val starvationTimer = new Timer(true)

  // Incrementing task IDs
  val nextTaskId = new AtomicLong(0)

  // Number of tasks running on each executor
  private val executorIdToTaskCount = new HashMap[String, Int]

  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn is used to decide when we can attain data locality on a given host
  protected val executorsByHost = new HashMap[String, HashSet[String]]

  protected val hostsByRack = new HashMap[String, HashSet[String]]

  protected val executorIdToHost = new HashMap[String, String]

  // Listener object to pass upcalls into
  var dagScheduler: DAGScheduler = null


  /**
    * SchedulerBackend，最重要的功能是reviveOffers
    */
  var backend: SchedulerBackend = null

  val mapOutputTracker = SparkEnv.get.mapOutputTracker

  /**
    * 将TaskSet封装为TaskSetManager，然后添加到任务队列中
    */
  var schedulableBuilder: SchedulableBuilder = null

  /**
    * 调度实体，两种调度实体，Pool和TaskSetManager
    */
  var rootPool: Pool = null


  // default scheduler is FIFO
  private val schedulingModeConf = conf.get("spark.scheduler.mode", "FIFO")
  val schedulingMode: SchedulingMode = try {
    SchedulingMode.withName(schedulingModeConf.toUpperCase)
  } catch {
    case e: java.util.NoSuchElementException =>
      throw new SparkException(s"Unrecognized spark.scheduler.mode: $schedulingModeConf")
  }

  // This is a var so that we can reset it for testing purposes.
  private[spark] var taskResultGetter = new TaskResultGetter(sc.env, this)

  override def setDAGScheduler(dagScheduler: DAGScheduler) {
    this.dagScheduler = dagScheduler
  }

  /**
    * TaskScheduler的初始化方法，包括：
    * 1. 初始化SchedulerBackend
    * 2. 初始化Pool
    * 3. 初始化调度模式
    * @param backend
    */
  def initialize(backend: SchedulerBackend) {
    this.backend = backend
    // temporarily set rootPool name to empty
    //创建Pool对象，指定调度模式
    rootPool = new Pool("", schedulingMode, 0, 0)
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool, conf)
      }
    }
    schedulableBuilder.buildPools()
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()

  override def start() {
    backend.start()

    if (!isLocal && conf.getBoolean("spark.speculation", false)) {
      logInfo("Starting speculative execution thread")
      speculationScheduler.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = Utils.tryOrStopSparkContext(sc) {
          checkSpeculatableTasks()
        }
      }, SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)
    }
  }

  /**
   * postStartHook在什么地方调用的？在SparkContext的主构造函数中
   * 等待Backend进行就绪状态
   *
   */
  override def postStartHook() {
    waitBackendReady()
  }

  /**
   * TaskScheduler将TaskSet加入到调度队列中
   * @param taskSet
   */
  override def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")

    /**
     * 为TaskSet创建TaskSetManager
     */
    this.synchronized {

      /**
        * TaskSetManager也有maxTaskFailures信息，TaskSchedulerImpl的maxTaskFailures只在创建TaskSetManager用到了
        * 也就是说，失败重试是在TaskSetManager中管理的。
        *
        * 另外TaskSetManager通过持有TaskSchedulerImpl实例，通过访问TaskSchedulerImpl持有的DAGScheduler实例，从而调用DAGScheduler的方法来通知任务状态
        */
      val manager = createTaskSetManager(taskSet, maxTaskFailures)
      val stage = taskSet.stageId
      val stageTaskSets =
        taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
      stageTaskSets(taskSet.stageAttemptId) = manager
      val conflictingTaskSet = stageTaskSets.exists { case (_, ts) =>
        ts.taskSet != taskSet && !ts.isZombie
      }
      if (conflictingTaskSet) {
        throw new IllegalStateException(s"more than one active taskSet for stage $stage:" +
          s" ${stageTaskSets.toSeq.map{_._2.taskSet.id}.mkString(",")}")
      }

      /**
       * 将TaskSetManager加入到调度队列中，
       * 问题：在什么时候从调度队列中读取任务执行，任务的本地性如何决定的？
       */
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

      if (!isLocal && !hasReceivedTask) {
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run() {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient resources")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
      }
      hasReceivedTask = true
    }

    /**
     * SchedulerBackend.reviveOffers是干啥的？SchedulerBackend作为TaskScheduler的对外接口，负责与外界进行通信
     * 包括任务的调度，killTask等之类的交互性工作
     */
    backend.reviveOffers()
  }

  // Label as private[scheduler] to allow tests to swap in different task set managers if necessary
  private[scheduler] def createTaskSetManager(
      taskSet: TaskSet,
      maxTaskFailures: Int): TaskSetManager = {
    new TaskSetManager(this, taskSet, maxTaskFailures)
  }

  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = synchronized {
    logInfo("Cancelling stage " + stageId)
    taskSetsByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, tsm) =>
        // There are two possible cases here:
        // 1. The task set manager has been created and some tasks have been scheduled.
        //    In this case, send a kill signal to the executors to kill the task and then abort
        //    the stage.
        // 2. The task set manager has been created but no tasks has been scheduled. In this case,
        //    simply abort the stage.
        tsm.runningTasksSet.foreach { tid =>
          val execId = taskIdToExecutorId(tid)
          backend.killTask(tid, execId, interruptThread)
        }
        tsm.abort("Stage %s cancelled".format(stageId))
        logInfo("Stage %d was cancelled".format(stageId))
      }
    }
  }

  /**
   * Called to indicate that all task attempts (including speculated tasks) associated with the
   * given TaskSetManager have completed, so state associated with the TaskSetManager should be
   * cleaned up.
   */
  def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
    taskSetsByStageIdAndAttempt.get(manager.taskSet.stageId).foreach { taskSetsForStage =>
      taskSetsForStage -= manager.taskSet.stageAttemptId
      if (taskSetsForStage.isEmpty) {
        taskSetsByStageIdAndAttempt -= manager.taskSet.stageId
      }
    }
    manager.parent.removeSchedulable(manager)
    logInfo("Removed TaskSet %s, whose tasks have all completed, from pool %s"
      .format(manager.taskSet.id, manager.parent.name))
  }

  private def resourceOfferSingleTaskSetManager(
      taskSetManager: TaskSetManager,
      maxLocality: TaskLocality,
      shuffledOffers: Seq[WorkerOffer],
      availableCpus: Array[Int],
      tasks: Seq[ArrayBuffer[TaskDescription]]) : Boolean = {
    var launchedTask = false
    for (i <- 0 until shuffledOffers.size) {
      val execId = shuffledOffers(i).executorId
      val host = shuffledOffers(i).host
      if (availableCpus(i) >= CPUS_PER_TASK) {
        try {

          /**
           * TaskSetManager的resourceOffer方法是提供一个计算资源，返回一个Option[TaskDescription]，这里使用for循环
           */
          for (task <- taskSetManager.resourceOffer(execId, host, maxLocality)) {
            tasks(i) += task
            val tid = task.taskId
            taskIdToTaskSetManager(tid) = taskSetManager
            taskIdToExecutorId(tid) = execId
            executorIdToTaskCount(execId) += 1
            executorsByHost(host) += execId
            availableCpus(i) -= CPUS_PER_TASK
            assert(availableCpus(i) >= 0)
            launchedTask = true
          }
        } catch {
          case e: TaskNotSerializableException =>
            logError(s"Resource offer failed, task set ${taskSetManager.name} was not serializable")
            // Do not offer resources for this task, but don't throw an error to allow other
            // task sets to be submitted.
            return launchedTask
        }
      }
    }
    return launchedTask
  }

  /**
   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   *
   * Driver拿出所有可用的资源，TaskScheduler根据这些资源进行任务分配，TaskScheduler调度时会考虑数据本地性
   *
   *  NOTE：这个是所有的TaskScheduler实现类通用的方法，包括本地模式以及CoarseGrained模式
   *
   */
  def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    // Mark each slave as alive and remember its hostname
    // Also track if new executor is added
    var newExecAvail = false
    for (o <- offers) {

      /***
        * executorId与exectorHost的对应关系
        */
      executorIdToHost(o.executorId) = o.host

      /***
        * executorId与它指定的任务数之间的对应关系
        */
      executorIdToTaskCount.getOrElseUpdate(o.executorId, 0)

      /***
        * 如果之前维持的executorsByHost不包含o.host，那么表示该Executor是新加入的,并且是在新机器上加入的，既然是在新机器上加入的，那么
        * 就需要重新计算数据本地性
        */
      if (!executorsByHost.contains(o.host)) {
        executorsByHost(o.host) = new HashSet[String]()

        /***
          * 给TaskScheduler提供的executor属于一个新加入的host，调用DAGScheduler的executorAdded方法，同时将newExecAvail置为true，接下来需要重新计算数据本地性
          */
        executorAdded(o.executorId, o.host)
        newExecAvail = true
      }
      for (rack <- getRackForHost(o.host)) {
        hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
      }
    }

    // Randomly shuffle offers to avoid always placing tasks on the same set of workers.
    val shuffledOffers = Random.shuffle(offers)

    /**
      * Build a list of tasks to assign to each worker.
      * shuffledOffers是WorkerOffer类型的集合，map操作是将每个WorkerOffer转换为一个TaskDescription的ArrayBuffer集合，这个ArrayBuffer的长度是o.cores
      */
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))

    /**
      * 每个WorkerOffer拥有的cores
      */
    val availableCpus = shuffledOffers.map(o => o.cores).toArray

    /**
     * 此处是从调度队列中取出在队列中等待调度的TaskSetManager，
      * sortedTaskSets是元素类型为TaskSetManager的ArrayBuffer
      *
      * 问题：rootPool是什么？rootPool是Pool类型的资源池
      *
     */
    val sortedTaskSetManagers = rootPool.getSortedTaskSetQueue

    for (taskSetManager <- sortedTaskSetManagers) {
      logInfo("parentName: %s, name: %s, runningTasks: %s".format(
        taskSetManager.parent.name, taskSetManager.name, taskSetManager.runningTasks))

      /**
        * newExecAvail为true表示有属于新host加入，那么调用taskSetManager.executorAdded()时会重新计算本地性
        */
      if (newExecAvail) {
        taskSetManager.executorAdded()
      }
    }

    // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
    var launchedTask = false

    /**两次循环，对每个taskSet，编译所有的localityLevels*/
    for (taskSetManager <- sortedTaskSetManagers; maxLocality <- taskSetManager.myLocalityLevels) {
      do {
        launchedTask = resourceOfferSingleTaskSetManager(
            taskSetManager, maxLocality, shuffledOffers, availableCpus, tasks)
      } while (launchedTask)
    }

    if (tasks.size > 0) {
      hasLaunchedTask = true
    }
    return tasks
  }

  /**
    * TaskScheduler有statusUpdate方法，statusUpdate方法是如何触发调用的？
   *
   * 应该是Executor通知Driver，然后Driver调用TaskScheduler的statusUpdate方法？在SchedulerBackend(LocalBackend, CoarseGrainedSchedulerBackend)中调用的
   *
   * 根据tid获得关联的TaskSetManager
    * @param tid
    * @param state
    * @param serializedData
    */
  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {

    /**
     * failedExecutor表示Executor Lost？
     */
    var failedExecutor: Option[String] = None
    synchronized {
      try {

        /** *
          * 如果是Executor Lost
          */
        if (state == TaskState.LOST && taskIdToExecutorId.contains(tid)) {
          // We lost this entire executor, so remember that it's gone
          val execId = taskIdToExecutorId(tid)

          if (executorIdToTaskCount.contains(execId)) {
            removeExecutor(execId,
              SlaveLost(s"Task $tid was lost, so marking the executor as lost as well."))
            failedExecutor = Some(execId)
          }
        }
        taskIdToTaskSetManager.get(tid) match {
          case Some(taskSet) =>

            /**
             *
              * 如果运行完成，那么就将<tid,TaskSetManager>从taskIdToTaskSetManager中移除，
              * 原因是，taskIdToTaskSetManager这个Map的Key是taskId，不同的任务的taskId是不同的
              */
          if (TaskState.isFinished(state)) {
              taskIdToTaskSetManager.remove(tid)
              taskIdToExecutorId.remove(tid).foreach { execId =>
                if (executorIdToTaskCount.contains(execId)) {
                  executorIdToTaskCount(execId) -= 1
                }
              }
            }

            /**
              * 如果任务执行成功，则加入到运行成功队列中
              */
            if (state == TaskState.FINISHED) {
              taskSet.removeRunningTask(tid)
              taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
            }


            /**
              * 如果是FAILED,KILLED或者LOST，则添加到失败的任务队列
              */
            else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
              taskSet.removeRunningTask(tid)
              taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
            }
          case None =>
            logError(
              ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
                "likely the result of receiving duplicate task finished status updates)")
                .format(state, tid))
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the DAGScheduler without holding a lock on this, since that can deadlock
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get)
      backend.reviveOffers()
    }
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  override def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulableInfo])],
      blockManagerId: BlockManagerId): Boolean = {
    // (taskId, stageId, stageAttemptId, accumUpdates)
    val accumUpdatesWithTaskIds: Array[(Long, Int, Int, Seq[AccumulableInfo])] = synchronized {
      accumUpdates.flatMap { case (id, updates) =>
        taskIdToTaskSetManager.get(id).map { taskSetMgr =>
          (id, taskSetMgr.stageId, taskSetMgr.taskSet.stageAttemptId, updates)
        }
      }
    }
    dagScheduler.executorHeartbeatReceived(execId, accumUpdatesWithTaskIds, blockManagerId)
  }

  def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long): Unit = synchronized {
    taskSetManager.handleTaskGettingResult(tid)
  }

  /**
    * TaskScheduler将处理任务成功执行后的处理逻辑交给TaskSetManager
    * 问题：一个TaskScheduler应该包含多个TaskSetManager才对
    * @param taskSetManager
    * @param tid
    * @param taskResult
    */
  def handleSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskResult: DirectTaskResult[_]): Unit = synchronized {
    taskSetManager.handleSuccessfulTask(tid, taskResult)
  }

  /**
    * TaskScheduler的handleFailedTask方法
   *
   *  TaskScheduler的handleFailedTask方法是如何触发调用的？
   *
    *  - 首先调用TaskSetManager的handleFailedTask方法
    *  - 然后调用SchedulerBackend的reviveOffers方法
    *
    * @param taskSetManager
    * @param tid
    * @param taskState
    * @param reason
    */
  def handleFailedTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskState: TaskState,
      reason: TaskEndReason): Unit = synchronized {

    /**
      *
      */
    taskSetManager.handleFailedTask(tid, taskState, reason)
    if (!taskSetManager.isZombie && taskState != TaskState.KILLED) {
      // Need to revive offers again now that the task set manager state has been updated to
      // reflect failed tasks that need to be re-run.
      backend.reviveOffers()
    }
  }

  def error(message: String) {
    synchronized {
      if (taskSetsByStageIdAndAttempt.nonEmpty) {
        // Have each task set throw a SparkException with the error
        for {
          attempts <- taskSetsByStageIdAndAttempt.values
          manager <- attempts.values
        } {
          try {
            manager.abort(message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        // No task sets are active but we still got an error. Just exit since this
        // must mean the error is during registration.
        // It might be good to do something smarter here in the future.
        throw new SparkException(s"Exiting due to error from cluster scheduler: $message")
      }
    }
  }

  override def stop() {
    speculationScheduler.shutdown()
    if (backend != null) {
      backend.stop()
    }
    if (taskResultGetter != null) {
      taskResultGetter.stop()
    }
    starvationTimer.cancel()
  }

  /***
    * 调用SchedulerBackend获取默认并行度
    * @return
    */
  override def defaultParallelism(): Int = backend.defaultParallelism()

  // Check for speculatable tasks in all our active jobs.
  def checkSpeculatableTasks() {
    var shouldRevive = false
    synchronized {
      shouldRevive = rootPool.checkSpeculatableTasks()
    }
    if (shouldRevive) {
      backend.reviveOffers()
    }
  }

  override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {
    var failedExecutor: Option[String] = None

    synchronized {
      if (executorIdToTaskCount.contains(executorId)) {
        val hostPort = executorIdToHost(executorId)
        logExecutorLoss(executorId, hostPort, reason)
        removeExecutor(executorId, reason)
        failedExecutor = Some(executorId)
      } else {
        executorIdToHost.get(executorId) match {
          case Some(hostPort) =>
            // If the host mapping still exists, it means we don't know the loss reason for the
            // executor. So call removeExecutor() to update tasks running on that executor when
            // the real loss reason is finally known.
            logExecutorLoss(executorId, hostPort, reason)
            removeExecutor(executorId, reason)

          case None =>
            // We may get multiple executorLost() calls with different loss reasons. For example,
            // one may be triggered by a dropped connection from the slave while another may be a
            // report of executor termination from Mesos. We produce log messages for both so we
            // eventually report the termination reason.
            logError(s"Lost an executor $executorId (already removed): $reason")
        }
      }
    }
    // Call dagScheduler.executorLost without holding the lock on this to prevent deadlock
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get)
      backend.reviveOffers()
    }
  }

  private def logExecutorLoss(
      executorId: String,
      hostPort: String,
      reason: ExecutorLossReason): Unit = reason match {
    case LossReasonPending =>
      logDebug(s"Executor $executorId on $hostPort lost, but reason not yet known.")
    case ExecutorKilled =>
      logInfo(s"Executor $executorId on $hostPort killed by driver.")
    case _ =>
      logError(s"Lost executor $executorId on $hostPort: $reason")
  }

  /**
   * Remove an executor from all our data structures and mark it as lost. If the executor's loss
   * reason is not yet known, do not yet remove its association with its host nor update the status
   * of any running tasks, since the loss reason defines whether we'll fail those tasks.
   *
   * 在TaskScheduler中处理Executor丢失的逻辑？
   */
  private def removeExecutor(executorId: String, reason: ExecutorLossReason) {
    executorIdToTaskCount -= executorId

    val host = executorIdToHost(executorId)
    val execs = executorsByHost.getOrElse(host, new HashSet)
    execs -= executorId
    if (execs.isEmpty) {
      executorsByHost -= host
      for (rack <- getRackForHost(host); hosts <- hostsByRack.get(rack)) {
        hosts -= host
        if (hosts.isEmpty) {
          hostsByRack -= rack
        }
      }
    }

    if (reason != LossReasonPending) {
      executorIdToHost -= executorId
      rootPool.executorLost(executorId, host, reason)
    }
  }

  /***
    * 调用DAGScheduler的executorAdded方法(DAGScheduler发给自身发出ExecutorAdded消息)
    * @param execId
    * @param host
    */
  def executorAdded(execId: String, host: String) {
    dagScheduler.executorAdded(execId, host)
  }

  def getExecutorsAliveOnHost(host: String): Option[Set[String]] = synchronized {
    executorsByHost.get(host).map(_.toSet)
  }

  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    executorsByHost.contains(host)
  }

  def hasHostAliveOnRack(rack: String): Boolean = synchronized {
    hostsByRack.contains(rack)
  }

  def isExecutorAlive(execId: String): Boolean = synchronized {
    executorIdToTaskCount.contains(execId)
  }

  def isExecutorBusy(execId: String): Boolean = synchronized {
    executorIdToTaskCount.getOrElse(execId, -1) > 0
  }

  // By default, rack is unknown
  def getRackForHost(value: String): Option[String] = None

  /** *
    * 等待backend进入就绪状态
    *
    * 问题：waitBackendReady方法是在什么地方调用的？
    */
  private def waitBackendReady(): Unit = {
    /** *
      * 如果SchedulerBackend已经就绪则立即返回
      */
    if (backend.isReady) {
      return
    }

    /** *
      * 如果Backend尚未就绪，则循环等待，每次等待100ms
      */
    while (!backend.isReady) {
      synchronized {
        this.wait(100)
      }
    }
  }

  /** *
    *  TaskScheduler调用它关联的SchedulerBackend的applicationId方法
    * @return An application ID
    */
  override def applicationId(): String = backend.applicationId()

  override def applicationAttemptId(): Option[String] = backend.applicationAttemptId()

  private[scheduler] def taskSetManagerForAttempt(
      stageId: Int,
      stageAttemptId: Int): Option[TaskSetManager] = {
    for {
      attempts <- taskSetsByStageIdAndAttempt.get(stageId)
      manager <- attempts.get(stageAttemptId)
    } yield {
      manager
    }
  }

}


private[spark] object TaskSchedulerImpl {
  /**
   * Used to balance containers across hosts.
   *
   * Accepts a map of hosts to resource offers for that host, and returns a prioritized list of
   * resource offers representing the order in which the offers should be used.  The resource
   * offers are ordered such that we'll allocate one container on each host before allocating a
   * second container on any host, and so on, in order to reduce the damage if a host fails.
   *
   * For example, given <h1, [o1, o2, o3]>, <h2, [o4]>, <h1, [o5, o6]>, returns
   * [o1, o5, o4, 02, o6, o3]
   */
  def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      (left, right) => map(left).size > map(right).size
    )

    val retval = new ArrayBuffer[T](keyList.size * 2)
    var index = 0
    var found = true

    while (found) {
      found = false
      for (key <- keyList) {
        val containerList: ArrayBuffer[T] = map.get(key).getOrElse(null)
        assert(containerList != null)
        // Get the index'th entry for this host - if present
        if (index < containerList.size){
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }

}
