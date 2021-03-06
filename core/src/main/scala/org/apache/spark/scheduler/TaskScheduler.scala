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

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.BlockManagerId

/**
 * Low-level task scheduler interface, currently implemented exclusively by
 * [[org.apache.spark.scheduler.TaskSchedulerImpl]].
 * This interface allows plugging in different task schedulers. Each TaskScheduler schedules tasks
 * for a single SparkContext. These schedulers get sets of tasks submitted to them from the
 * DAGScheduler for each stage, and are responsible for sending the tasks to the cluster, running
 * them, retrying if there are failures, and mitigating stragglers. They return events to the
 * DAGScheduler.
  * TaskScheduler是low level的调度
  * TaskScheduler持有Pool对象，Pool是TaskSetManager，将一个Job的并行Stage放到Pool进行调度
  *
 */
private[spark] trait TaskScheduler {

  /** *
    * 默认的Spark Application ID，这是Standalone模式下的ApplicationID?不是的，TaskScheduler的唯一实现类TaskSchedulerImpl对它进行了
    * 重写，TaskSchedulerImpl将它交给TaskScheduler对应的SchedulerBackend进行实现
    * Local模式和YARN模式下，SparkApplicationID需要重写
    */
  private val appId = "spark-application-" + System.currentTimeMillis

  def rootPool: Pool

  def schedulingMode: SchedulingMode

  def start(): Unit

  /** *
    * Invoked after system has successfully initialized (typically in spark context).
    * Yarn uses this to bootstrap allocation of resources based on preferred locations, wait for slave registrations, etc.
    * 在Yarn模式下，在这个方法中等待，已分配的executor个数占到申请的executor个数一定的比例
    */
  def postStartHook() { }

  // Disconnect from the cluster.
  def stop(): Unit

  // Submit a sequence of tasks to run.
  def submitTasks(taskSet: TaskSet): Unit

  // Cancel a stage.
  def cancelTasks(stageId: Int, interruptThread: Boolean)

  // Set the DAG scheduler for upcalls. This is guaranteed to be set before submitTasks is called.
  def setDAGScheduler(dagScheduler: DAGScheduler): Unit

  // Get the default level of parallelism to use in the cluster, as a hint for sizing jobs.
  def defaultParallelism(): Int

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulableInfo])],
      blockManagerId: BlockManagerId): Boolean

  /**
   * Get an application ID associated with the job.
   *
   * @return An application ID
   */
  def applicationId(): String = appId

  /**
   * Process a lost executor
   */
  def executorLost(executorId: String, reason: ExecutorLossReason): Unit

  /**
   * Get an application's attempt ID associated with the job.
   *
   * @return An application's Attempt ID
   */
  def applicationAttemptId(): Option[String]

}
