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

package org.apache.spark.memory

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.spark.Logging

/**
 * Implements policies and bookkeeping for sharing a adjustable-sized pool of memory between tasks.
 *
 * ExecutionMemoryPool作为Task运行过程中共享的内存资源池，需要解决多个Task并行计算时共享内存的问题。目标之一是
 * 保证每个任务能获得一定数量的内存，而不能让先执行的内存先占用大量内存，后面执行的任务只能使用少量内存(spill to disk repeatedly)
 *
 * Tries to ensure that each task gets a reasonable share of memory, instead of some task ramping up
 * to a large amount first and then causing others to spill to disk repeatedly.
 *
 * If there are N tasks, it ensures that each task can acquire at least 1 / 2N of the memory
 * before it has to spill, and at most 1 / N. Because N varies dynamically, we keep track of the
 * set of active tasks and redo the calculations of 1 / 2N and 1 / N in waiting tasks whenever this
 * set changes. This is all done by synchronizing access to mutable state and using wait() and
 * notifyAll() to signal changes to callers. Prior to Spark 1.6, this arbitration of memory across
 * tasks was performed by the ShuffleMemoryManager.
 *
 * 每个分配的内存数是1/(2*N)~1/N
 *
 * @param lock a [[MemoryManager]] instance to synchronize on
 * @param poolName a human-readable name for this pool, for use in log messages
 */
private[memory] class ExecutionMemoryPool(
    lock: Object,
    poolName: String
  ) extends MemoryPool(lock) with Logging {

  /**
   * Map from taskAttemptId -> memory consumption in bytes
   * 每个Task使用的内存量(它这里称为memory consumption而不是memory used)
   */
  @GuardedBy("lock")
  private val memoryForTask = new mutable.HashMap[Long, Long]()

  /** *
    * ExecutionMemoryPool已经使用的内存量
    * @return
    */
  override def memoryUsed: Long = lock.synchronized {
    memoryForTask.values.sum
  }

  /**
   * Returns the memory consumption, in bytes, for the given task.
   *
   * 返回指定的Task使用的内存量，如果taskAttemptId没有记录在memoryForTask这个map中，则返回0
    *
    * @param taskAttemptId
    * @return
    */
  def getMemoryUsageForTask(taskAttemptId: Long): Long = lock.synchronized {
    memoryForTask.getOrElse(taskAttemptId, 0L)
  }

  /**
   *
   *  为Task分配内存，尝试为Task分配numBytes字节的内存，极端情况下，可能给Task分配了0字节
   *  它的实现上有加锁等待的机制？等待其它Task(线程)释放内存，那么应该有等待超时的机制
   *
   * Try to acquire up to `numBytes` of memory for the given task and return the number of bytes
   * obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   *
   * @param numBytes number of bytes to acquire numBytes表示要申请的内存量
   *
   * @param taskAttemptId the task attempt acquiring memory 申请的Task的AttemptID
   *
   * @param maybeGrowPool a callback that potentially grows the size of this pool. It takes in
   *                      one parameter (Long) that represents the desired amount of memory by
   *                      which this pool should be expanded.
   *
   *                      潜在的(maybe的含义)进行ExecutionMemoryPool扩容，这是一个类型为Long=>Unit的函数，入参是Long，返回值为Unit的函数
   *                      入参表示ExecutionMemoryPool扩容到多大还是说是在原来基础上扩容多少(应该是后者)
   *
   *                      maybeGrowPool带有默认值，默认值是不进行扩容，函数字面量(x:Int)=>Unit表示什么也不干
   *
   *
   * @param computeMaxPoolSize a callback that returns the maximum allowable size of this pool
   *                           at this given moment. This is not a field because the max pool
   *                           size is variable in certain cases. For instance, in unified
   *                           memory management, the execution pool can be expanded by evicting
   *                           cached blocks, thereby shrinking the storage pool.
   *
   *                           computeMaxPoolSize函数用于计算pool size的最大值，它是一个入参为空，返回值为Long的函数，它带有默认实现()=>poolSize,
   *                           poolSize是调用ExecutionMemoryPool的poolSize方法，返回当前pool的size（这个pool size包含了使用了和未使用的总量）
   *
   *                           computeMaxPoolSize用于计算在当时环境下的最大size，这是一个动态的值
   *
   *
   * @return the number of bytes granted to the task.
   */
  private[memory] def acquireMemory(
      numBytes: Long,
      taskAttemptId: Long,
      maybeGrowPool: Long => Unit = (additionalSpaceNeeded: Long) => Unit,
      computeMaxPoolSize: () => Long = () => poolSize): Long = lock.synchronized {
    assert(numBytes > 0, s"invalid number of bytes requested: $numBytes")

    // TODO: clean up this clunky method signature

    // Add this task to the taskMemory map just so we can keep an accurate count of the number
    // of active tasks, to let other tasks ramp down their memory in calls to `acquireMemory`

      /** *
        * 如果taskAttemptId还未记录在memoryForTask map中,表示该task是第一次申请内存，将它记录下来，同时记录该task申请的内存量为0
        */
    if (!memoryForTask.contains(taskAttemptId)) {
      memoryForTask(taskAttemptId) = 0L
      // This will later cause waiting tasks to wake up and check numTasks again
      lock.notifyAll()
    }

    // Keep looping until we're either sure that we don't want to grant this request (because this
    // task would have more than 1 / numActiveTasks of the memory) or we have enough free
    // memory to give it (we always let each task get at least 1 / (2 * numActiveTasks)).
    // TODO: simplify this to limit each task to its own slot
    /** *
      * 循环进行进行分配(1/2N~1/N)
      */
    while (true) {

      /** *
        * 当前活跃的Task个数，所谓活跃的Task个数指的是申请了内存的Task。
        */
      val numActiveTasks = memoryForTask.keys.size

      /** *
        *当前Task已经申请的内存数
        */
      val curMem = memoryForTask(taskAttemptId)

      // In every iteration of this loop, we should first try to reclaim any borrowed execution
      // space from storage. This is necessary because of the potential race condition where new
      // storage blocks may steal the free execution memory that this task was waiting for.

      /** *
        *  扩容的大小是numBytes - memoryFree，如果numBytes - memoryFree > 0表示execution不够，需要申请。
        *  对于默认的maybeGrowPool实现而言，不会进行扩容
        */
      maybeGrowPool(numBytes - memoryFree)

      // Maximum size the pool would have changed after potentially growing the pool.
      // This is used to compute the upper bound of how much memory each task can occupy. This
      // must take into account potential free memory as well as the amount this pool currently
      // occupies. Otherwise, we may run into SPARK-12155 where, in unified memory management,
      // we did not take into account space that could have been freed by evicting cached blocks.

      /** *
        * 申请完内存后，pool size可能已经发生变化，因此需要重新计算
        */
      val maxPoolSize = computeMaxPoolSize()

      /** *
        *  每个Task的最小和最大内存分别是(maxPoolSize / N, poolSize / (2*N))
        *
        *  问题：如果借用了内存，为什么poolSize不把借来的内存加到poolSize上？这个得看
        */
      val maxMemoryPerTask = maxPoolSize / numActiveTasks
      val minMemoryPerTask = poolSize / (2 * numActiveTasks)

      /** *
        * How much we can grant this task; keep its share within 0 <= X <= 1 / numActiveTasks
        * 可分配的最大内存，位于0和1/N之间
        * 算法;
        * 1. 需要分配的最大值XYZ是 maxMemoryPerTask - curMem（每个任务能分配的最大值减去已经分配的内存值）
        * 2. 求numBytes和XYZ之间的较小者，问题：如果XYZ比numBytes小，那么什么时候能把task需要的numBytes分配完？
        */
      val maxToGrant = math.min(numBytes, math.max(0, maxMemoryPerTask - curMem))

      /** *
        * Only give it as much memory as is free, which might be none if it reached 1 / numTasks
        * 可分配的实际内存是最大分配数和可用数的较小者
        */
      val toGrant = math.min(maxToGrant, memoryFree)

      // We want to let each task get at least 1 / (2 * numActiveTasks) before blocking;
      // if we can't give it this much now, wait for other tasks to free up memory
      // (this happens if older tasks allocated lots of memory before N grew)

      /** *
        * 如果分配的值仍然小于numBytes并且分配的值小于minMemoryPerTask，那么等待分配
        * 否则进行分配，
        *
        * 也就是说，一个Task可能永远分配不到numBytes？是的，分配不到那块只能由Task进行spill
        */
      if (toGrant < numBytes && curMem + toGrant < minMemoryPerTask) {
        logInfo(s"TID $taskAttemptId waiting for at least 1/2N of $poolName pool to be free")
        lock.wait()
      } else {
        memoryForTask(taskAttemptId) += toGrant
        return toGrant
      }
    }

    //默认值是0，但是代码逻辑到达不了
    0L  // Never reached
  }

  /**
   * Release `numBytes` of memory acquired by the given task.
   *
   * 释放由taskAttemptId占用的numBytes字节的内存，通过通知等待于acquireMemory方法的线程，已经有可用的内存可用
    *
    * @param numBytes
    * @param taskAttemptId
    */
  def releaseMemory(numBytes: Long, taskAttemptId: Long): Unit = lock.synchronized {
    val curMem = memoryForTask.getOrElse(taskAttemptId, 0L)
    /** *
      * 首先计算要释放的内存量，释放的内存是curMem和numBytes较小值
      *
      * 如果task占用2M(curMem为2M)，而numBytes为1M，那么本次调用只会释放1M，也就是说针对同一个Task允许多次调用releaseMemory将占用的
      * 内存释放完
      */
    var memoryToFree = if (curMem < numBytes) {
      logWarning(
        s"Internal error: release called on $numBytes bytes but task only has $curMem bytes " +
          s"of memory from the $poolName pool")
      curMem
    } else {
      numBytes
    }

    /** *
      * 将task使用的内存量减掉memoryToFree，如果所有的内存已经释放，那么将taskAttemptId从memoryForTask这个map中删除
      */
    if (memoryForTask.contains(taskAttemptId)) {
      memoryForTask(taskAttemptId) -= memoryToFree
      if (memoryForTask(taskAttemptId) <= 0) {
        memoryForTask.remove(taskAttemptId)
      }
    }
    lock.notifyAll() // Notify waiters in acquireMemory() that memory has been freed
  }

  /**
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
   * 首先获取出task使用的内存量，然后调用releaseMemory进行内存释放
   *
    * @param taskAttemptId
   * @return the number of bytes freed. 任务使用的内存
    */
  def releaseAllMemoryForTask(taskAttemptId: Long): Long = lock.synchronized {
    val numBytesToFree = getMemoryUsageForTask(taskAttemptId)
    releaseMemory(numBytesToFree, taskAttemptId)
    numBytesToFree
  }

}
