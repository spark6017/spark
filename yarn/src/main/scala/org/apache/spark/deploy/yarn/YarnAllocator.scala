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

import java.util.Collections
import java.util.concurrent._
import java.util.regex.Pattern

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil._
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef}
import org.apache.spark.scheduler.{ExecutorExited, ExecutorLossReason}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RemoveExecutor
import org.apache.spark.util.ThreadUtils

/**
 * YarnAllocator is charged with requesting containers from the YARN ResourceManager and deciding
 * what to do with containers when YARN fulfills these requests.
 *
 * YarnAllocator负责事情：
 * 1. 向YARN ResourceManager申请资源(就是Container).
 * 2. 当YARN ResourceManager分配了这些Container，YarnAllocator决定如何使用这些Container(数据本地性考虑?)
 *
 * This class makes use of YARN's AMRMClient APIs. We interact with the AMRMClient in three ways:
 * * Making our resource needs known, which updates local bookkeeping about containers requested.
 * * Calling "allocate", which syncs our local container requests with the RM, and returns any
 *   containers that YARN has granted to us.  This also functions as a heartbeat.
 * * Processing the containers granted to us to possibly launch executors inside of them.
 *
 * The public methods of this class are thread-safe.  All methods that mutate state are
 * synchronized.
 *
 *  YarnAllocator定义了非常多的属性，需要将这些属性捋一遍
  *
  * @param driverUrl
  * @param driverRef
  * @param conf
  * @param sparkConf
  * @param amClient
  * @param appAttemptId
  * @param args
  * @param securityMgr
  */
private[yarn] class YarnAllocator(
    driverUrl: String,
    driverRef: RpcEndpointRef,
    conf: Configuration,
    sparkConf: SparkConf,
    amClient: AMRMClient[ContainerRequest],
    appAttemptId: ApplicationAttemptId,
    args: ApplicationMasterArguments,
    securityMgr: SecurityManager)
  extends Logging {

  import YarnAllocator._

  // RackResolver logs an INFO message whenever it resolves a rack, which is way too often.
  if (Logger.getLogger(classOf[RackResolver]).getLevel == null) {
    Logger.getLogger(classOf[RackResolver]).setLevel(Level.WARN)
  }

  /** *
    * ContainerId有ApplicationAttemptId和getId方法
    * allocatedHostToContainersMap记录着一个NodeManager启动了几个Container, 一个Host可以启动多个Container
    */
  val allocatedHostToContainersMap = new HashMap[String, collection.mutable.Set[ContainerId]]

  /** *
    * Container与Host之间的对应关系，一个Container只能属于一个Host
    */
  val allocatedContainerToHostMap = new HashMap[ContainerId, String]

  /** *
    *  Containers that we no longer care about. We've either already told the RM to release them or
    *  will on the next heartbeat. Containers get removed from this map after the RM tells us they've completed.
    * 准备释放的Container列表，我们需要通知RM我们不再需要Container，RM可以删除掉
    * 当RM通知我们这些Container已经被删除掉了，需要更新这个数据结构
    *
    * 问题：releasedContainers是一个Set，它是从Map的key转换得到的。
    * 问题：Map的结构是ContainerId->Boolean，这个Boolean值指的是什么？
    */
  private val releasedContainers = Collections.newSetFromMap[ContainerId](
    new ConcurrentHashMap[ContainerId, java.lang.Boolean])

  /** *
    * 正在运行的Executor的数目，是否就等于正在运行的Container的数目
    * 如何更新这个变量
    */
  @volatile private var numExecutorsRunning = 0

  /** *
    * Used to generate a unique ID per executor
    * executorId生成器，根据executor个数生成executorId，也就是说executorId是有序的
    */
  private var executorIdCounter = 0

  /**
   * 失败的executor的个数，是否就等于失败的Container的个数？是的
   * 问题：如何更新这个变量
   */
  @volatile private var numExecutorsFailed = 0

  /**
    * 获得要申请的初始Executor数目，
   * 对于静态分配，初始值就是用户指定的executor个数
   * 对于动态分配，初始值是根据配置项获得的，之后可以进行动态申请和释放
    */
  @volatile private var targetNumExecutors =
    YarnSparkHadoopUtil.getInitialTargetExecutorNumber(sparkConf)

  // Executor loss reason requests that are pending - maps from executor ID for inquiry to a
  // list of requesters that should be responded to once we find out why the given executor
  // was lost.
  private val pendingLossReasonRequests = new HashMap[String, mutable.Buffer[RpcCallContext]]

  /** *
    * Maintain loss reasons for already released executors,
    * it will be added when executor loss  reason is got from AM-RM call, and be removed after querying this loss reason.
    *
    * 维护着已经释放的executor与它executor丢失原因之间的对应关系
    *
    */
  private val releasedExecutorLossReasons = new HashMap[String, ExecutorLossReason]

  /** *
    * Keep track of which container is running which executor to remove the executors later
    * executorId与container之间的一一对应关系，Container有一个ContainerId属性
    */
  private[yarn] val executorIdToContainer = new HashMap[String, Container]

  /** *
    * 未预期的Container释放的个数
    */
  private var numUnexpectedContainerRelease = 0L

  /** *
    * containerId与ExecutorId之间的映射关系，一个Container对应一个Executor，反之亦然，这是一一对应关系
    */
  private val containerIdToExecutorId = new HashMap[ContainerId, String]

  /** *
    * Executor memory in MB.
    * 每个executor使用的内存量，默认1024M
    */
  protected val executorMemory = args.executorMemory


  /** *
    * Additional memory overhead.
    * 每个executor在YARN上占用的overhead内存数量，默认384M
    */
  protected val memoryOverhead: Int = sparkConf.getInt("spark.yarn.executor.memoryOverhead",
    math.max((MEMORY_OVERHEAD_FACTOR * executorMemory).toInt, MEMORY_OVERHEAD_MIN))

  /** *
    * Number of cores per executor.
    * 每个executor的内核数
    */
  protected val executorCores = args.executorCores

  /***
    * Resource capability requested for each executors
    *
    * 申请的内存量是指定的executor memory + memory overhead，申请的内核数executorCores
    *
    *
    * 这是一个静态的变量，也就是说，每个Container申请的内存量+核数是固定的
    *
    */
  private[yarn] val resource = Resource.newInstance(executorMemory + memoryOverhead, executorCores)

  /** *
    * 启动Container的线程的最大个数，默认25个
    */
  private val launcherPool = ThreadUtils.newDaemonCachedThreadPool(
    "ContainerLauncher",
    sparkConf.getInt("spark.yarn.containerLauncherMaxThreads", 25))

  /** *
    * 这个配置项是干什么的？
    */
  private val launchContainers = sparkConf.getBoolean("spark.yarn.launchContainers", true)

  /** *
    * Node Label表达式，这个Yarn的新功能，如何使用？返回一个Option
    */
  private val labelExpression = sparkConf.getOption("spark.yarn.executor.nodeLabelExpression")

  // ContainerRequest constructor that can take a node label expression. We grab it through
  // reflection because it's only available in later versions of YARN.

  /** *
    * 为什么调用Option的flatMap方法，这有什么道道吗？
    */
  private val nodeLabelConstructor = labelExpression.flatMap { expr =>
    try {

      /** *
        * 通过返回获取ContainerRequest的构造器，该构造方法的参数列表是
        * Resource、Array[String], Array[String], Priority、Boolean、String
        *
        * ContainerRequest(capability : Resource, nodes : Array[String], racks : Array[String], priority : Priority, relaxLocality : Boolean)
        */
      Some(classOf[ContainerRequest].getConstructor(classOf[Resource],
        classOf[Array[String]], classOf[Array[String]], classOf[Priority], classOf[Boolean],
        classOf[String]))
    } catch {
      case e: NoSuchMethodException => {
        logWarning(s"Node label expression $expr will be ignored because YARN version on" +
          " classpath does not support it.")
        None
      }
    }
  }

  /** *
    *  A map to store preferred hostname and possible task numbers running on it.
    *
    *
    */
  private var preferredHostToLocalTaskCountMap: Map[String, Int] = Map.empty

  /** *
    * Number of tasks that have locality preferences in active stages
    *
    * 具有数据本地性的任务个数
    */
  private var numLocalityAwareTasks: Int = 0

  /** *
    *  A container placement strategy based on pending tasks' locality preference
    *
    */
  private[yarn] val containerPlacementStrategy =
    new LocalityPreferredContainerPlacementStrategy(sparkConf, conf, resource)

  def getNumExecutorsRunning: Int = numExecutorsRunning

  /** *
    * numExecutorFailed如何计算？
    * @return
    */
  def getNumExecutorsFailed: Int = numExecutorsFailed

  /** *
    * A sequence of pending container requests that have not yet been fulfilled.
    *
    * 正在等待分配的Container Request集合，它是分配到任意Host
    * @return
    */
  def getPendingAllocate: Seq[ContainerRequest] = getPendingAtLocation(ANY_HOST)

  /**
   * A sequence of pending container requests at the given location that have not yet been
   * fulfilled.
   *
   *  正在等待分配到指定位置的ContainerRequest的数目
    *
    * @param location
    * @return
    */
  private def getPendingAtLocation(location: String): Seq[ContainerRequest] = {
    /** *
      * matchingRequests的返回值是Java的List集合
      * 每个元素是Java的Collection集合，实际类型是List<Collection<ContainerRequest>>
      */
    val matchingRequests = amClient.getMatchingRequests(RM_REQUEST_PRIORITY, location, resource)

    /** *
      * 首先将Java List转换为Scala的集合
      * 然后_.asScala是把Java Collection类型的元素转换为Scala集合，
      * 通过flatMap将二维的集合转换为一维的集合
      * 通过toSeq转换为Seq对象
      */
    matchingRequests.asScala.flatMap(_.asScala).toSeq
  }

  /**
   * Request as many executors from the ResourceManager as needed to reach the desired total. If
   * the requested total is smaller than the current number of running executors, no executors will
   * be killed.
   * @param requestedTotal total number of containers requested
   * @param localityAwareTasks number of locality aware tasks to be used as container placement hint
   * @param preferredHostToLocalTaskCountMap a map of preferred hostname to possible task counts to be used as
   *                             container placement hint.
   * @return Whether the new requested total is different than the old value.
   */
  def requestTotalExecutorsWithPreferredLocalities(
      requestedTotal: Int,
      localityAwareTasks: Int,
      preferredHostToLocalTaskCountMap: Map[String, Int]): Boolean = synchronized {
    this.numLocalityAwareTasks = localityAwareTasks
    this.preferredHostToLocalTaskCountMap = preferredHostToLocalTaskCountMap

    if (requestedTotal != targetNumExecutors) {
      logInfo(s"Driver requested a total number of $requestedTotal executor(s).")
      targetNumExecutors = requestedTotal
      true
    } else {
      false
    }
  }

  /**
   * Request that the ResourceManager release the container running the specified executor.
   */
  def killExecutor(executorId: String): Unit = synchronized {
    if (executorIdToContainer.contains(executorId)) {
      val container = executorIdToContainer.get(executorId).get
      internalReleaseContainer(container)
      numExecutorsRunning -= 1
    } else {
      logWarning(s"Attempted to kill unknown executor $executorId!")
    }
  }

  /**
   * Request resources such that, if YARN gives us all we ask for, we'll have a number of containers
   * equal to maxExecutors.
   *
   * Deal with any containers YARN has granted to us by possibly launching executors in them.
   *
   * This must be synchronized because variables read in this method are mutated by other methods.
   *
   *  0. 更新资源申请请求
   *  1. 请求RM分配资源，
   *  2.  使用RM分配的Container(在Container中启动Executor)
   */
  def allocateResources(): Unit = synchronized {
    updateResourceRequests()

    val progressIndicator = 0.1f
    // Poll the ResourceManager. This doubles as a heartbeat if there are no pending container
    // requests.
    val allocateResponse = amClient.allocate(progressIndicator)

    val allocatedContainers = allocateResponse.getAllocatedContainers()

    if (allocatedContainers.size > 0) {
      logDebug("Allocated containers: %d. Current executor count: %d. Cluster resources: %s."
        .format(
          allocatedContainers.size,
          numExecutorsRunning,
          allocateResponse.getAvailableResources))

      /***
        *
        */
      handleAllocatedContainers(allocatedContainers.asScala)
    }

    val completedContainers = allocateResponse.getCompletedContainersStatuses()
    if (completedContainers.size > 0) {
      logDebug("Completed %d containers".format(completedContainers.size))
      processCompletedContainers(completedContainers.asScala)
      logDebug("Finished processing %d completed containers. Current running executor count: %d."
        .format(completedContainers.size, numExecutorsRunning))
    }
  }

  /**
   * Update the set of container requests that we will sync with the RM based on the number of
   * executors we have currently running and our target number of executors.
   *
   * 在请求RM申请资源前，首先整理我们已经分配到的、尚未分配到的、RM分配的但是无用的Container
   *
   */
  def updateResourceRequests(): Unit = {

    /** *
      * 已经发出申请请求，但是YARN尚未实际的分配的ContainerRequest集合
      */
    val pendingAllocate = getPendingAllocate

    /** *
      * 返回正在申请中的Container的数目
      */
    val numPendingAllocate = pendingAllocate.size

    /** *
      * 还需要申请的数目 = Application当前需要的Executor的数目 - 已经向RM发出申请尚未答复的Container数目 - 已经申请到且正在运行的Container数目
      * 可能大于0，可能等于0，可能小于0
      */
    val missing = targetNumExecutors - numPendingAllocate - numExecutorsRunning


    /***
      * 如果missing 大于 0 表示Application申请的Container资源还没有完全满足，需要再missing个executor container
      * 问题：如果已经申请到的Container已经运行完关闭了，
      */
    if (missing > 0) {
      logInfo(s"Will request $missing executor containers, each with ${resource.getVirtualCores} " +
        s"cores and ${resource.getMemory} MB memory including $memoryOverhead MB overhead")

      // Split the pending container request into three groups: locality matched list, locality
      // unmatched list and non-locality list. Take the locality matched container request into
      // consideration of container placement, treat as allocated containers.
      // For locality unmatched and locality free container requests, cancel these container
      // requests, since required locality preference has been changed, recalculating using
      // container placement strategy.
      /** *
        * pendingAllocate是要申请资源的ContainerRequest集合
        */
      val (localityMatched, localityUnMatched, localityFree) = splitPendingAllocationsByLocality(
        preferredHostToLocalTaskCountMap, pendingAllocate)

      /** *
        *  Remove the outdated container request and recalculate the requested container number
        *  将数据本地性不友好的ContainerRequest删除掉
        */
      localityUnMatched.foreach(amClient.removeContainerRequest)
      localityFree.foreach(amClient.removeContainerRequest)


      /** *
        * 需要申请的Container数量
        */
      val updatedNumContainer = missing + localityUnMatched.size + localityFree.size

      val containerLocalityPreferences = containerPlacementStrategy.localityOfRequestedContainers(
        updatedNumContainer, numLocalityAwareTasks, preferredHostToLocalTaskCountMap,
          allocatedHostToContainersMap, localityMatched)

      for (locality <- containerLocalityPreferences) {

        /***
          * 创建Container Request
          */
        val request = createContainerRequest(resource, locality.nodes, locality.racks)

        /** *
          * 调用AMRMClient的addContainerRequest方法添加ContainerRequest
          */
        amClient.addContainerRequest(request)
        val nodes = request.getNodes
        val hostStr = if (nodes == null || nodes.isEmpty) "Any" else nodes.asScala.last
        logInfo(s"Container request (host: $hostStr, capability: $resource)")
      }
    }

    /***
      * missing可能小于0，表示申请的大于实际需要的
      */
    else if (missing < 0) {
      val numToCancel = math.min(numPendingAllocate, -missing)
      logInfo(s"Canceling requests for $numToCancel executor containers")

      val matchingRequests = amClient.getMatchingRequests(RM_REQUEST_PRIORITY, ANY_HOST, resource)
      if (!matchingRequests.isEmpty) {
        matchingRequests.iterator().next().asScala
          .take(numToCancel).foreach(amClient.removeContainerRequest)
      } else {
        logWarning("Expected to find pending requests, but found none.")
      }
    }
  }

  /**
   * Creates a container request, handling the reflection required to use YARN features that were
   * added in recent versions.
   *
    * 创建ContainerRequest，需要的参数：resource、nodes、racks，优先级固定为1
    *
   *  @param resource
    * @param nodes
    * @param racks
    * @return
    */
  private def createContainerRequest(
      resource: Resource,
      nodes: Array[String],
      racks: Array[String]): ContainerRequest = {
    /** *
      * 如果定义了NodeLabelExpression，那么按照NodeLabel模式进行创建
      * 如果没有定义NodeLabelExpression，那么创建默认的ContainerRequest
      *
      */
    nodeLabelConstructor.map { constructor =>
      constructor.newInstance(resource, nodes, racks, RM_REQUEST_PRIORITY, true: java.lang.Boolean, /**显示指定为Java的Boolean**/
        labelExpression.orNull)
    }.getOrElse(new ContainerRequest(resource, nodes, racks, RM_REQUEST_PRIORITY))
  }

  /**
   * Handle containers granted by the RM by launching executors on them.
   *
   * Due to the way the YARN allocation protocol works, certain healthy race conditions can result
   * in YARN granting containers that we no longer need. In this case, we release them.
   *
   * Visible for testing.
   */
  def handleAllocatedContainers(allocatedContainers: Seq[Container]): Unit = {
    val containersToUse = new ArrayBuffer[Container](allocatedContainers.size)

    // Match incoming requests by host
    val remainingAfterHostMatches = new ArrayBuffer[Container]
    for (allocatedContainer <- allocatedContainers) {
      matchContainerToRequest(allocatedContainer, allocatedContainer.getNodeId.getHost,
        containersToUse, remainingAfterHostMatches)
    }

    // Match remaining by rack
    val remainingAfterRackMatches = new ArrayBuffer[Container]
    for (allocatedContainer <- remainingAfterHostMatches) {
      val rack = RackResolver.resolve(conf, allocatedContainer.getNodeId.getHost).getNetworkLocation
      matchContainerToRequest(allocatedContainer, rack, containersToUse,
        remainingAfterRackMatches)
    }

    // Assign remaining that are neither node-local nor rack-local
    val remainingAfterOffRackMatches = new ArrayBuffer[Container]
    for (allocatedContainer <- remainingAfterRackMatches) {
      matchContainerToRequest(allocatedContainer, ANY_HOST, containersToUse,
        remainingAfterOffRackMatches)
    }

    if (!remainingAfterOffRackMatches.isEmpty) {
      logDebug(s"Releasing ${remainingAfterOffRackMatches.size} unneeded containers that were " +
        s"allocated to us")
      for (container <- remainingAfterOffRackMatches) {
        internalReleaseContainer(container)
      }
    }

    /** *
      * 在分配到的Container上运行Executor
      */
    runAllocatedContainers(containersToUse)

    logInfo("Received %d containers from YARN, launching executors on %d of them."
      .format(allocatedContainers.size, containersToUse.size))
  }

  /**
   * Looks for requests for the given location that match the given container allocation. If it
   * finds one, removes the request so that it won't be submitted again. Places the container into
   * containersToUse or remaining.
   *
   * @param allocatedContainer container that was given to us by YARN
   * @param location resource name, either a node, rack, or *
   * @param containersToUse list of containers that will be used
   * @param remaining list of containers that will not be used
   */
  private def matchContainerToRequest(
      allocatedContainer: Container,
      location: String,
      containersToUse: ArrayBuffer[Container],
      remaining: ArrayBuffer[Container]): Unit = {
    // SPARK-6050: certain Yarn configurations return a virtual core count that doesn't match the
    // request; for example, capacity scheduler + DefaultResourceCalculator. So match on requested
    // memory, but use the asked vcore count for matching, effectively disabling matching on vcore
    // count.
    val matchingResource = Resource.newInstance(allocatedContainer.getResource.getMemory,
          resource.getVirtualCores)
    val matchingRequests = amClient.getMatchingRequests(allocatedContainer.getPriority, location,
      matchingResource)

    // Match the allocation to a request
    if (!matchingRequests.isEmpty) {
      val containerRequest = matchingRequests.get(0).iterator.next
      amClient.removeContainerRequest(containerRequest)
      containersToUse += allocatedContainer
    } else {
      remaining += allocatedContainer
    }
  }

  /**
   * Launches executors in the allocated containers.
   */
  private def runAllocatedContainers(containersToUse: ArrayBuffer[Container]): Unit = {
    for (container <- containersToUse) {
      numExecutorsRunning += 1
      assert(numExecutorsRunning <= targetNumExecutors)
      val executorHostname = container.getNodeId.getHost
      val containerId = container.getId
      executorIdCounter += 1
      val executorId = executorIdCounter.toString

      assert(container.getResource.getMemory >= resource.getMemory)

      logInfo("Launching container %s for on host %s".format(containerId, executorHostname))
      executorIdToContainer(executorId) = container
      containerIdToExecutorId(container.getId) = executorId

      val containerSet = allocatedHostToContainersMap.getOrElseUpdate(executorHostname,
        new HashSet[ContainerId])

      containerSet += containerId
      allocatedContainerToHostMap.put(containerId, executorHostname)

      val executorRunnable = new ExecutorRunnable(
        container,
        conf,
        sparkConf,
        driverUrl,
        executorId,
        executorHostname,
        executorMemory,
        executorCores,
        appAttemptId.getApplicationId.toString,
        securityMgr)
      if (launchContainers) {
        logInfo("Launching ExecutorRunnable. driverUrl: %s,  executorHostname: %s".format(
          driverUrl, executorHostname))
        launcherPool.execute(executorRunnable)
      }
    }
  }

  /** *
    * 处理运行结束的Container
    * @param completedContainers
    */
  private[yarn] def processCompletedContainers(completedContainers: Seq[ContainerStatus]): Unit = {
    /** *
      * 遍历completedContainers集合中的每个ContainerStatus
      */
    for (completedContainer <- completedContainers) {
      //获得containerId
      val containerId = completedContainer.getContainerId
      //从releasedContainers集合中删除(问题：何时加入到这个集合中的)
      //删除成功返回true，否则返回false
      val alreadyReleased = releasedContainers.remove(containerId)
      val hostOpt = allocatedContainerToHostMap.get(containerId)
      val onHostStr = hostOpt.map(host => s" on host: $host").getOrElse("")
      val exitReason = if (!alreadyReleased) {
        // Decrement the number of executors running. The next iteration of
        // the ApplicationMaster's reporting thread will take care of allocating.
        numExecutorsRunning -= 1
        logInfo("Completed container %s%s (state: %s, exit status: %s)".format(
          containerId,
          onHostStr,
          completedContainer.getState,
          completedContainer.getExitStatus))
        // Hadoop 2.2.X added a ContainerExitStatus we should switch to use
        // there are some exit status' we shouldn't necessarily count against us, but for
        // now I think its ok as none of the containers are expected to exit.
        val exitStatus = completedContainer.getExitStatus
        val (exitCausedByApp, containerExitReason) = exitStatus match {
          case ContainerExitStatus.SUCCESS =>
            (false, s"Executor for container $containerId exited because of a YARN event (e.g., " +
              "pre-emption) and not because of an error in the running job.")
          case ContainerExitStatus.PREEMPTED =>
            // Preemption is not the fault of the running tasks, since YARN preempts containers
            // merely to do resource sharing, and tasks that fail due to preempted executors could
            // just as easily finish on any other executor. See SPARK-8167.
            (false, s"Container ${containerId}${onHostStr} was preempted.")
          // Should probably still count memory exceeded exit codes towards task failures
          case VMEM_EXCEEDED_EXIT_CODE =>
            (true, memLimitExceededLogMessage(
              completedContainer.getDiagnostics,
              VMEM_EXCEEDED_PATTERN))
          case PMEM_EXCEEDED_EXIT_CODE =>
            (true, memLimitExceededLogMessage(
              completedContainer.getDiagnostics,
              PMEM_EXCEEDED_PATTERN))

          /***
            * 1. bin/spark-shell --master yarn-client  --executor-memory 300M 会触发这个case _逻辑
            * 2. bin/spark-shell --master yarn-client --num-executors 2 找到ExecutorBackend的进程ID，杀死它也会触发这个逻辑
            *
            */
          case _ =>
            numExecutorsFailed += 1
            (true, "Container marked as failed: " + containerId + onHostStr +
              ". Exit status: " + completedContainer.getExitStatus +
              ". Diagnostics: " + completedContainer.getDiagnostics)

        }
        if (exitCausedByApp) {
          logWarning(containerExitReason)
        } else {
          logInfo(containerExitReason)
        }
        ExecutorExited(exitStatus, exitCausedByApp, containerExitReason)
      } else {
        // If we have already released this container, then it must mean
        // that the driver has explicitly requested it to be killed
        ExecutorExited(completedContainer.getExitStatus, exitCausedByApp = false,
          s"Container $containerId exited from explicit termination request.")
      }

      for {
        host <- hostOpt
        containerSet <- allocatedHostToContainersMap.get(host)
      } {
        containerSet.remove(containerId)
        if (containerSet.isEmpty) {
          allocatedHostToContainersMap.remove(host)
        } else {
          allocatedHostToContainersMap.update(host, containerSet)
        }

        allocatedContainerToHostMap.remove(containerId)
      }

      containerIdToExecutorId.remove(containerId).foreach { eid =>
        executorIdToContainer.remove(eid)
        pendingLossReasonRequests.remove(eid) match {
          case Some(pendingRequests) =>
            // Notify application of executor loss reasons so it can decide whether it should abort
            pendingRequests.foreach(_.reply(exitReason))

          case None =>
            // We cannot find executor for pending reasons. This is because completed container
            // is processed before querying pending result. We should store it for later query.
            // This is usually happened when explicitly killing a container, the result will be
            // returned in one AM-RM communication. So query RPC will be later than this completed
            // container process.
            releasedExecutorLossReasons.put(eid, exitReason)
        }
        if (!alreadyReleased) {
          // The executor could have gone away (like no route to host, node failure, etc)
          // Notify backend about the failure of the executor
          numUnexpectedContainerRelease += 1
          driverRef.send(RemoveExecutor(eid, exitReason))
        }
      }
    }
  }

  /**
   * Register that some RpcCallContext has asked the AM why the executor was lost. Note that
   * we can only find the loss reason to send back in the next call to allocateResources().
   */
  private[yarn] def enqueueGetLossReasonRequest(
      eid: String,
      context: RpcCallContext): Unit = synchronized {
    if (executorIdToContainer.contains(eid)) {
      pendingLossReasonRequests
        .getOrElseUpdate(eid, new ArrayBuffer[RpcCallContext]) += context
    } else if (releasedExecutorLossReasons.contains(eid)) {
      // Executor is already released explicitly before getting the loss reason, so directly send
      // the pre-stored lost reason
      context.reply(releasedExecutorLossReasons.remove(eid).get)
    } else {
      logWarning(s"Tried to get the loss reason for non-existent executor $eid")
      context.sendFailure(
        new SparkException(s"Fail to find loss reason for non-existent executor $eid"))
    }
  }

  private def internalReleaseContainer(container: Container): Unit = {
    releasedContainers.add(container.getId())
    amClient.releaseAssignedContainer(container.getId())
  }

  private[yarn] def getNumUnexpectedContainerRelease = numUnexpectedContainerRelease

  private[yarn] def getNumPendingLossReasonRequests: Int = synchronized {
    pendingLossReasonRequests.size
  }

  /**
   * Split the pending container requests into 3 groups based on current localities of pending
   * tasks.
   * @param hostToLocalTaskCount a map of preferred hostname to possible task counts to be used as
   *                             container placement hint.
   * @param pendingAllocations A sequence of pending allocation container request.
   * @return A tuple of 3 sequences, first is a sequence of locality matched container
   *         requests, second is a sequence of locality unmatched container requests, and third is a
   *         sequence of locality free container requests.
   */
  private def splitPendingAllocationsByLocality(
      hostToLocalTaskCount: Map[String, Int],
      pendingAllocations: Seq[ContainerRequest]
    ): (Seq[ContainerRequest], Seq[ContainerRequest], Seq[ContainerRequest]) = {


    val localityMatched = ArrayBuffer[ContainerRequest]()
    val localityUnMatched = ArrayBuffer[ContainerRequest]()
    val localityFree = ArrayBuffer[ContainerRequest]()
    /** *
      * preferered hosts
      */
    val preferredHosts = hostToLocalTaskCount.keySet

    /** *
      * pendingAllocations是ApplicationMaster要向Yarn ResourceManager申请的ContainerRequest集合
      */
    pendingAllocations.foreach { cr =>

      /**
       * 如果ContainerRequest没有指定Nodes信息，那么localityFree集合加上该ContainerRequest
       */
      val nodes = cr.getNodes
      if (nodes == null) {
        localityFree += cr
      }

      /** *
        * 如果ContainerRequest的nodes和prefererredHosts有交集，那么localityMatched集合加上该ContainerRequest
        */
      else if (nodes.asScala.toSet.intersect(preferredHosts).nonEmpty) {
        localityMatched += cr
      }

      /** *
        * 如果ContainerRequest的nodes信息和preferredHosts集合没有交集，那么localityUnMatched集合加上该ContainerRequest
        */
      else {
        localityUnMatched += cr
      }
    }

    (localityMatched.toSeq, localityUnMatched.toSeq, localityFree.toSeq)
  }

}

private object YarnAllocator {
  val MEM_REGEX = "[0-9.]+ [KMG]B"
  val PMEM_EXCEEDED_PATTERN =
    Pattern.compile(s"$MEM_REGEX of $MEM_REGEX physical memory used")
  val VMEM_EXCEEDED_PATTERN =
    Pattern.compile(s"$MEM_REGEX of $MEM_REGEX virtual memory used")
  val VMEM_EXCEEDED_EXIT_CODE = -103
  val PMEM_EXCEEDED_EXIT_CODE = -104

  def memLimitExceededLogMessage(diagnostics: String, pattern: Pattern): String = {
    val matcher = pattern.matcher(diagnostics)
    val diag = if (matcher.find()) " " + matcher.group() + "." else ""
    ("Container killed by YARN for exceeding memory limits." + diag
      + " Consider boosting spark.yarn.executor.memoryOverhead.")
  }
}
