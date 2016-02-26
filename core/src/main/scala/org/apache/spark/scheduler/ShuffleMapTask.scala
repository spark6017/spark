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

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter

/**
 * A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
 * specified in the ShuffleDependency).
 *
 * See [[org.apache.spark.scheduler.Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param taskBinary broadcast version of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 * @param _initialAccums initial set of accumulators to be used in this task for tracking
 *                       internal metrics. Other accumulators will be registered later when
 *                       they are deserialized on the executors.
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation],
    _initialAccums: Seq[Accumulator[_]])
  extends Task[MapStatus](stageId, stageAttemptId, partition.index, _initialAccums)
  with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) {
    this(0, 0, null, new Partition { override def index: Int = 0 }, null, null)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  /**
    * ShuffleMapTask执行结果是一个MapStatus,
    *
    * MapStatus包含两方面信息：
    *   1. BlockManagerId(BlockManager的唯一标识，BlockManagerId包括host、port、executorId)
    *   2. 数据大小(估算值)
    *
    * 问题：ShuffleMapTask的任务序列化数据(taskBinary.value)，得到的是RDD和ShuffleDependency，问题是ShuffleDependency使如何创建出来的？
    *
    *
    * @param context
    * @return
    */
  override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    val deserializeStartTime = System.currentTimeMillis()
    val ser = SparkEnv.get.closureSerializer.newInstance()

    /**
     * 对taskBinary进行反序列化，反序列化的得到RDD和ShuffleDependency
      * rdd是RDD类型，dep是ShuffleDependency类型
     */
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)


    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime

    metrics = Some(context.taskMetrics)
    var writer: ShuffleWriter[Any, Any] = null
    try {

      /***
        * 将RDD中的数据Shuffle到磁盘
        */

      //1. 获取ShuffleManager，默认是SortShuffleManager
      val manager = SparkEnv.get.shuffleManager

      //2. 获取ShuffleWriter，ShuffleWriter是跟Partition相关的(由partitionId确定）,而partitionId是跟ShuffleMapTask相关的，即ShuffleMapTask处理的partition的ID

      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)

      //3. 执行写操作，因为只有(K,V)类型的数据才能够Shuffle，因此此处的rdd.iterator(partition)是Product2[Any,Any]类型的二元组
      //writer参数中的迭代器对象是去RDD的第partition个分区的数据

      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])

      //4. 数据写完后调用stop方法，并且传入succes=true
      val result = writer.stop(success = true)
      result.get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {

            //捕获所有异常，调用stop方法，并且传入success=false, 问题：已经写入的数据如何回滚？
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
