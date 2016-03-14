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

package org.apache.spark.sql.execution

import java.util.Random

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.hash.HashShuffleManager
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.util.MutablePair

/**
 * Performs a shuffle that will result in the desired `newPartitioning`.
 *  Exchange物理计划的目的是插入Shuffle，比如Sort、Join等SQL操作通常是涉及有Shuffle操作的
 *
  *
  * Exchange物理计划，它有三个参数: newpartitioning、child spark plan以及可选的ExchangeCoordinator
  *
  * 问题：为什么要Exchange物理计划，它的目的是什么？使用了Exchange物理计划，产生什么样的影响或者结果。在原来的物理计划
  *
  * @param newPartitioning 这个partitioning是Exchange这个物理计划的输出Partitioning
  * @param child
  * @param coordinator
  */
case class Exchange(
    var newPartitioning: Partitioning,
    child: SparkPlan,
    @transient coordinator: Option[ExchangeCoordinator]) extends UnaryNode {

  /***
    * Exchange物理计划显示的节点名称
    * @return
    */
  override def nodeName: String = {

    /** *
      *
      */
    val extraInfo = coordinator match {
      case Some(exchangeCoordinator) if exchangeCoordinator.isEstimated =>
        s"(coordinator id: ${System.identityHashCode(coordinator)})"
      case Some(exchangeCoordinator) if !exchangeCoordinator.isEstimated =>
        s"(coordinator id: ${System.identityHashCode(coordinator)})"
      case None => ""
    }

    val simpleNodeName = "Exchange"
    s"$simpleNodeName$extraInfo"
  }

  /** *
    * 该物理计划输出的数据的分区算法
    * @return
    */
  override def outputPartitioning: Partitioning = newPartitioning

  /**
   * 该物理计划输出的属性，Exchange物理计划输出的是孩子物理计划的输出属性。所以对于原来child的parent而言，它的child的输出属性不变
   * @return
   */
  override def output: Seq[Attribute] = child.output

  /**
   * Determines whether records must be defensively copied before being sent to the shuffle.
   * Several of Spark's shuffle components will buffer deserialized Java objects in memory. The
   * shuffle code assumes that objects are immutable and hence does not perform its own defensive
   * copying. In Spark SQL, however, operators' iterators return the same mutable `Row` object. In
   * order to properly shuffle the output of these operators, we need to perform our own copying
   * prior to sending records to the shuffle. This copying is expensive, so we try to avoid it
   * whenever possible. This method encapsulates the logic for choosing when to copy.
   *
   * In the long run, we might want to push this logic into core's shuffle APIs so that we don't
   * have to rely on knowledge of core internals here in SQL.
   *
   * See SPARK-2967, SPARK-4479, and SPARK-7375 for more discussion of this issue.
   *
   * @param partitioner the partitioner for the shuffle
   * @param serializer the serializer that will be used to write rows
   * @return true if rows should be copied before being shuffled, false otherwise
   */
  private def needToCopyObjectsBeforeShuffle(
      partitioner: Partitioner,
      serializer: Serializer): Boolean = {
    // Note: even though we only use the partitioner's `numPartitions` field, we require it to be
    // passed instead of directly passing the number of partitions in order to guard against
    // corner-cases where a partitioner constructed with `numPartitions` partitions may output
    // fewer partitions (like RangePartitioner, for example).
    val conf = child.sqlContext.sparkContext.conf
    val shuffleManager = SparkEnv.get.shuffleManager
    val sortBasedShuffleOn = shuffleManager.isInstanceOf[SortShuffleManager]
    val bypassMergeThreshold = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
    if (sortBasedShuffleOn) {
      val bypassIsSupported = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]
      if (bypassIsSupported && partitioner.numPartitions <= bypassMergeThreshold) {
        // If we're using the original SortShuffleManager and the number of output partitions is
        // sufficiently small, then Spark will fall back to the hash-based shuffle write path, which
        // doesn't buffer deserialized records.
        // Note that we'll have to remove this case if we fix SPARK-6026 and remove this bypass.
        false
      } else if (serializer.supportsRelocationOfSerializedObjects) {
        // SPARK-4550 and  SPARK-7081 extended sort-based shuffle to serialize individual records
        // prior to sorting them. This optimization is only applied in cases where shuffle
        // dependency does not specify an aggregator or ordering and the record serializer has
        // certain properties. If this optimization is enabled, we can safely avoid the copy.
        //
        // Exchange never configures its ShuffledRDDs with aggregators or key orderings, so we only
        // need to check whether the optimization is enabled and supported by our serializer.
        false
      } else {
        // Spark's SortShuffleManager uses `ExternalSorter` to buffer records in memory, so we must
        // copy.
        true
      }
    } else if (shuffleManager.isInstanceOf[HashShuffleManager]) {
      // We're using hash-based shuffle, so we don't need to copy.
      false
    } else {
      // Catch-all case to safely handle any future ShuffleManager implementations.
      true
    }
  }

  private val serializer: Serializer = new UnsafeRowSerializer(child.output.size)

  override protected def doPrepare(): Unit = {
    // If an ExchangeCoordinator is needed, we register this Exchange operator
    // to the coordinator when we do prepare. It is important to make sure
    // we register this operator right before the execution instead of register it
    // in the constructor because it is possible that we create new instances of
    // Exchange operators when we transform the physical plan
    // (then the ExchangeCoordinator will hold references of unneeded Exchanges).
    // So, we should only call registerExchange just before we start to execute
    // the plan.
    coordinator match {
      case Some(exchangeCoordinator) => exchangeCoordinator.registerExchange(this)
      case None =>
    }
  }

  /**
   * Returns a [[ShuffleDependency]] that will partition rows of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
    *
    * 对孩子物理计划输出的记录进行分区，分区算法使用Exchange的构造参数newPartitioning. ShuffleDependency的分区是Shuffle的输入
    *
    *
    * @return 返回ShuffleDependency
    */
  private[sql] def prepareShuffleDependency(): ShuffleDependency[Int, InternalRow, InternalRow] = {

    //执行孩子物理计划得到RDD，这里应该不是提交Job，而是RDD的转换
    val rdd = child.execute()

    //根据Exchange的newPartitioning获得ShuffleDependency的Partitioner
    val part: Partitioner = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) => new HashPartitioner(numPartitions)

      /***
        * 如果是HashPartitioning，那么在此实现Partitioner，以[K,V]的K作为partitionId
        */
      case HashPartitioning(_, n) =>
        new Partitioner {
          override def numPartitions: Int = n
          // For HashPartitioning, the partitioning key is already a valid partition ID, as we use
          // `HashPartitioning.partitionIdExpression` to produce partitioning key.
          override def getPartition(key: Any): Int = key.asInstanceOf[Int]
        }


      /***
        * 如果是RangePartitioning，那么使用RangePartitioner
        */
      case RangePartitioning(sortingExpressions, numPartitions) =>
        // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
        // partition bounds. To get accurate samples, we need to copy the mutable keys.
        val rddForSampling = rdd.mapPartitionsInternal { iter =>
          val mutablePair = new MutablePair[InternalRow, Null]()
          iter.map(row => mutablePair.update(row.copy(), null))
        }
        // We need to use an interpreted ordering here because generated orderings cannot be
        // serialized and this ordering needs to be created on the driver in order to be passed into
        // Spark core code.
        implicit val ordering = new InterpretedOrdering(sortingExpressions, child.output)
        new RangePartitioner(numPartitions, rddForSampling, ascending = true)

      /***
        * 如果是SinglePartition,那么在此实现Partitioner，只有一个分区
        */
      case SinglePartition =>
        new Partitioner {
          override def numPartitions: Int = 1
          override def getPartition(key: Any): Int = 0
        }
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
      // TODO: Handle BroadcastPartitioning.
    }


    /***
      *
      * @return 函数类型为InternalRow=>Any的函数
      */
    def getPartitionKeyExtractor(): InternalRow => Any = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) =>
        // Distributes elements evenly across output partitions, starting from a random partition.
        var position = new Random(TaskContext.get().partitionId()).nextInt(numPartitions)
        (row: InternalRow) => {
          // The HashPartitioner will handle the `mod` by the number of partitions
          position += 1
          position
        }
      case h: HashPartitioning =>
        val projection = UnsafeProjection.create(h.partitionIdExpression :: Nil, child.output)
        //返回的函数是row=>projection(row).getInt(0)
        row => projection(row).getInt(0)
      case RangePartitioning(_, _) | SinglePartition => identity
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
    }

    /** *
      * 获得一个RDD[K,V]用于Shuffle，它的Key是Int类型，Value是InternalRow类型
      */
    val rddWithPartitionIds: RDD[Product2[Int, InternalRow]] = {
      if (needToCopyObjectsBeforeShuffle(part, serializer)) {
        rdd.mapPartitionsInternal { iter =>
          val getPartitionKey = getPartitionKeyExtractor()
          iter.map { row => (part.getPartition(getPartitionKey(row)), row.copy()) }
        }
      } else {
        rdd.mapPartitionsInternal { iter =>
          val getPartitionKey = getPartitionKeyExtractor()
          val mutablePair = new MutablePair[Int, InternalRow]()
          iter.map { row => mutablePair.update(part.getPartition(getPartitionKey(row)), row) }
        }
      }
    }

    // Now, we manually create a ShuffleDependency. Because pairs in rddWithPartitionIds
    // are in the form of (partitionId, row) and every partitionId is in the expected range
    // [0, part.numPartitions - 1]. The partitioner of this is a PartitionIdPassthrough.

    /** *
      *  rddWithPartitionIds是RDD
      *  PartitionIdPassthrough是Partitioner
      *  Some(serializer)是序列化器
      */
    val dependency =
      new ShuffleDependency[Int, InternalRow, InternalRow](
        rddWithPartitionIds,
        new PartitionIdPassthrough(part.numPartitions),
        Some(serializer))

    dependency
  }

  /**
   * Returns a [[ShuffledRowRDD]] that represents the post-shuffle dataset.
   * This [[ShuffledRowRDD]] is created based on a given [[ShuffleDependency]] and an optional
   * partition start indices array. If this optional array is defined, the returned
   * [[ShuffledRowRDD]] will fetch pre-shuffle partitions based on indices of this array.
   *
   *
   * @param shuffleDependency
   * @param specifiedPartitionStartIndices
   * @return  ShuffledRowRDD，它是RDD[InternalRow]的子类
   */
  private[sql] def preparePostShuffleRDD(
      shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow],
      specifiedPartitionStartIndices: Option[Array[Int]] = None): ShuffledRowRDD = {
    // If an array of partition start indices is provided, we need to use this array
    // to create the ShuffledRowRDD. Also, we need to update newPartitioning to
    // update the number of post-shuffle partitions.
    specifiedPartitionStartIndices.foreach { indices =>
      assert(newPartitioning.isInstanceOf[HashPartitioning])
      newPartitioning = UnknownPartitioning(indices.length)  // 为什么给newPartitioning赋值为UnknownPartitioning?
    }
    new ShuffledRowRDD(shuffleDependency, specifiedPartitionStartIndices)
  }

  /** *
    * Exchange的doExecute方法返回的RDD类型是RDD[InternalRow]
    * @return
    */
  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    coordinator match {

      /** *
        * 如果有Exchange协处理器，那么调用Exchange协处理器的postShuffleRDD计算ShuffledRowRDD
        */
      case Some(exchangeCoordinator) =>
        val shuffleRDD = exchangeCoordinator.postShuffleRDD(this)
        assert(shuffleRDD.partitions.length == newPartitioning.numPartitions)
        shuffleRDD

      /**
       * 如果没有Exchange协处理器，那么首先获取ShuffleDependency，然后再获得ShuffleRDD
       */
      case None =>
        //创建ShuffleDependency
        val shuffleDependency = prepareShuffleDependency()
        preparePostShuffleRDD(shuffleDependency)
    }
  }
}

/** *
  * Exchange伴生对象，调用apply方法创建Exchange类的实例
  * Exchange类是SparkPlan的子类，也有doExecute方法
  *
  */
object Exchange {

  /** *
    * 生成的Exchange物理计划的outputPartitioning
    * @param newPartitioning
    * @param child 原来物理计划树上的child物理计划将称为Exchange物理计划的child，而原child物理计划的parent将成为Exchange物理计划的parent
    *
    *              parent
    *              /
    *          /
    *      child
    *
    *变化为：
    *                parent
    *                   /
    *                /
    *           Exchange
    *             /
    *         /
    *   child
    *
    *
    * @return
    */
  def apply(newPartitioning: Partitioning, child: SparkPlan): Exchange = {
    Exchange(newPartitioning, child, coordinator = None: Option[ExchangeCoordinator])
  }
}

/**
 * Ensures that the [[org.apache.spark.sql.catalyst.plans.physical.Partitioning Partitioning]]
 * of input data meets the
 * [[org.apache.spark.sql.catalyst.plans.physical.Distribution Distribution]] requirements for
 * each operator by inserting [[Exchange]] Operators where required.  Also ensure that the
 * input partition ordering requirements are met.
  *
 *  EnsureRequirements是一物理计划的转换策略，在物理计划执行前就会试试转换
 *
  * @param sqlContext
  */
private[sql] case class EnsureRequirements(sqlContext: SQLContext) extends Rule[SparkPlan] {

  /***
    * 默认的pre-shuffle的分区数
    * @return
    */
  private def defaultNumPreShufflePartitions: Int = sqlContext.conf.numShufflePartitions

  /***
    * target是什么含义？
    * post-shuffle指的是什么？
    * post-shuffle input size指的是什么？ 默认64M
    *
    *
    * @return
    */
  private def targetPostShuffleInputSize: Long = sqlContext.conf.targetPostShuffleInputSize

  /***
    * 是否其中自适应性Execution
    * @return
    */
  private def adaptiveExecutionEnabled: Boolean = sqlContext.conf.adaptiveExecutionEnabled

  /***
    * Post Shuffle的最小分区数,默认-1
    * @return
    */
  private def minNumPostShufflePartitions: Option[Int] = {
    val minNumPostShufflePartitions = sqlContext.conf.minNumPostShufflePartitions
    if (minNumPostShufflePartitions > 0) Some(minNumPostShufflePartitions) else None
  }

  /**
   * Given a required distribution, returns a partitioning that satisfies that distribution.
   *
    * 给定一个Distribution，返回一个能够满足Distribution的Partitioning
    * 创建一个能够满足distribution的partitioning
   *
   * @param requiredDistribution
   * @param numPartitions
   * @return
   */
  private def createPartitioning(
      requiredDistribution: Distribution,
      numPartitions: Int): Partitioning = {
    requiredDistribution match {
      case AllTuples => SinglePartition
      case ClusteredDistribution(clustering) => HashPartitioning(clustering, numPartitions)
      case OrderedDistribution(ordering) => RangePartitioning(ordering, numPartitions)
      case dist => sys.error(s"Do not know how to satisfy distribution $dist")
    }
  }

  /**
   * Adds [[ExchangeCoordinator]] to [[Exchange]]s if adaptive query execution is enabled
   * and partitioning schemes of these [[Exchange]]s support [[ExchangeCoordinator]].
    *
    * @param children
    * @param requiredChildDistributions
    * @return
    */
  private def withExchangeCoordinator(
      children: Seq[SparkPlan],
      requiredChildDistributions: Seq[Distribution]): Seq[SparkPlan] = {

    /***
      * 判断是否需要Exchange协处理器
      */
    val supportsCoordinator =

    /***
      * 如果children中有Exchange物理计划
      */
      if (children.exists(_.isInstanceOf[Exchange])) {
        // Right now, ExchangeCoordinator only support HashPartitionings.
        children.forall {
          case e @ Exchange(hash: HashPartitioning, _, _) => true
          case child =>
            child.outputPartitioning match {
              case hash: HashPartitioning => true
              case collection: PartitioningCollection =>
                collection.partitionings.forall(_.isInstanceOf[HashPartitioning])
              case _ => false
            }
        }
      }

      /***
        * 如果children中没有Exchange物理计划，那么children的个数要大于1并且每个child都要求是ClusteredDistribution
        */
      else {
        // In this case, although we do not have Exchange operators, we may still need to
        // shuffle data when we have more than one children because data generated by
        // these children may not be partitioned in the same way.
        // Please see the comment in withCoordinator for more details.
        val supportsDistribution =
          requiredChildDistributions.forall(_.isInstanceOf[ClusteredDistribution])
        children.length > 1 && supportsDistribution
      }

    val withCoordinator =
      if (adaptiveExecutionEnabled && supportsCoordinator) {
        val coordinator = new ExchangeCoordinator(children.length, targetPostShuffleInputSize, minNumPostShufflePartitions)
        children.zip(requiredChildDistributions).map {
          case (e: Exchange, _) =>
            // This child is an Exchange, we need to add the coordinator.
            e.copy(coordinator = Some(coordinator))
          case (child, distribution) =>
            // If this child is not an Exchange, we need to add an Exchange for now.
            // Ideally, we can try to avoid this Exchange. However, when we reach here,
            // there are at least two children operators (because if there is a single child
            // and we can avoid Exchange, supportsCoordinator will be false and we
            // will not reach here.). Although we can make two children have the same number of
            // post-shuffle partitions. Their numbers of pre-shuffle partitions may be different.
            // For example, let's say we have the following plan
            //         Join
            //         /  \
            //       Agg  Exchange
            //       /      \
            //    Exchange  t2
            //      /
            //     t1
            // In this case, because a post-shuffle partition can include multiple pre-shuffle
            // partitions, a HashPartitioning will not be strictly partitioned by the hashcodes
            // after shuffle. So, even we can use the child Exchange operator of the Join to
            // have a number of post-shuffle partitions that matches the number of partitions of
            // Agg, we cannot say these two children are partitioned in the same way.
            // Here is another case
            //         Join
            //         /  \
            //       Agg1  Agg2
            //       /      \
            //   Exchange1  Exchange2
            //       /       \
            //      t1       t2
            // In this case, two Aggs shuffle data with the same column of the join condition.
            // After we use ExchangeCoordinator, these two Aggs may not be partitioned in the same
            // way. Let's say that Agg1 and Agg2 both have 5 pre-shuffle partitions and 2
            // post-shuffle partitions. It is possible that Agg1 fetches those pre-shuffle
            // partitions by using a partitionStartIndices [0, 3]. However, Agg2 may fetch its
            // pre-shuffle partitions by using another partitionStartIndices [0, 4].
            // So, Agg1 and Agg2 are actually not co-partitioned.
            //
            // It will be great to introduce a new Partitioning to represent the post-shuffle
            // partitions when one post-shuffle partition includes multiple pre-shuffle partitions.
            val targetPartitioning =
              createPartitioning(distribution, defaultNumPreShufflePartitions)
            assert(targetPartitioning.isInstanceOf[HashPartitioning])
            Exchange(targetPartitioning, child, Some(coordinator))
        }
      } else {
        // If we do not need ExchangeCoordinator, the original children are returned.
        children
      }

    withCoordinator
  }

  /**
   * 确保当前的物理计划对子物理计划的数据分布和排序要求得到满足
   * @param operator 当前的物理计划(算子)
   * @return
   */
  private def ensureDistributionAndOrdering(operator: SparkPlan): SparkPlan = {
    /** *
      * 当前物理计划要求子物理计划的数据分布方式。每个物理计划都有一个Distribution属性
      *
      */
    val requiredChildDistributions: Seq[Distribution] = operator.requiredChildDistribution

    /** *
      * 当前物理计划要求子物理计划的数据排序性，每个物理计划都有一个Seq[SortOrder]集合，根据一个或者多个列进行排序
      */
    val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering

    /**
     * 子物理计划
     */
    var children: Seq[SparkPlan] = operator.children

    /** *
      * 每个子物理计划都需要数据分布和数据排序性
      */
    assert(requiredChildDistributions.length == children.length)
    assert(requiredChildOrderings.length == children.length)

    // Ensure that the operator's children satisfy their output distribution requirements:

    /**
     * children是物理计划的集合，children.zip(requiredChildDistributions)是一个二元组集合，每个元素是child物理计划以及该物理计划
     * 对该child的数据分布的要求
     */
    children = children.zip(requiredChildDistributions).map {
      case (child, distribution) =>

        /** *
          * 如果child物理计划的数据分区策略(outputPartitioning)不能满足该物理计划对该child物理计划的数据分布(requiredChildDistribution)的要求，那么需要添加Exchange
          */
        if (child.outputPartitioning.satisfies(distribution)) {
          child
        } else {

          /** *
            * 如果child物理计划的数据分布不满足该物理计划对该child物理计划的数据分布的要求
            */
          Exchange(createPartitioning(distribution, defaultNumPreShufflePartitions), child)
        }
    }

    // If the operator has multiple children and specifies child output distributions (e.g. join),
    // then the children's output partitionings must be compatible:


    /** *
      * children个数大于1，比如JOIN
      *
      * if条件判定为true的条件是：
      * a. 孩子物理计划的个数大于1， 例如Join
      * b. 本物理计划需要的子物理计划的数据分布不仅仅是UnspecifiedDistribution(即，在要求的孩子物理计划的数据分布中，至少有一个不是UnspecifiedDistribution)，JOIN应该也有
      * c.
      */
    if (children.length > 1
        && requiredChildDistributions.toSet != Set(UnspecifiedDistribution)
        && !Partitioning.allCompatible(children.map(_.outputPartitioning))) {

      // First check if the existing partitions of the children all match. This means they are
      // partitioned by the same partitioning into the same number of partitions. In that case,
      // don't try to make them match `defaultPartitions`, just use the existing partitioning.
      val maxChildrenNumPartitions = children.map(_.outputPartitioning.numPartitions).max

      val useExistingPartitioning = children.zip(requiredChildDistributions).forall {
        case (child, distribution) => {
          child.outputPartitioning.guarantees(
            createPartitioning(distribution, maxChildrenNumPartitions))
        }
      }

      children = if (useExistingPartitioning) {
        // We do not need to shuffle any child's output.
        children
      } else {
        // We need to shuffle at least one child's output.
        // Now, we will determine the number of partitions that will be used by created
        // partitioning schemes.
        val numPartitions = {
          // Let's see if we need to shuffle all child's outputs when we use
          // maxChildrenNumPartitions.
          val shufflesAllChildren = children.zip(requiredChildDistributions).forall {
            case (child, distribution) => {
              !child.outputPartitioning.guarantees(
                createPartitioning(distribution, maxChildrenNumPartitions))
            }
          }
          // If we need to shuffle all children, we use defaultNumPreShufflePartitions as the
          // number of partitions. Otherwise, we use maxChildrenNumPartitions.
          if (shufflesAllChildren) defaultNumPreShufflePartitions else maxChildrenNumPartitions
        }

        children.zip(requiredChildDistributions).map {
          case (child, distribution) => {
            val targetPartitioning =
              createPartitioning(distribution, numPartitions)
            if (child.outputPartitioning.guarantees(targetPartitioning)) {
              child
            } else {
              child match {
                // If child is an exchange, we replace it with
                // a new one having targetPartitioning.
                case Exchange(_, c, _) => Exchange(targetPartitioning, c)
                case _ => Exchange(targetPartitioning, child)
              }
            }
          }
        }
      }
    }

    // Now, we need to add ExchangeCoordinator if necessary.
    // Actually, it is not a good idea to add ExchangeCoordinators while we are adding Exchanges.
    // However, with the way that we plan the query, we do not have a place where we have a
    // global picture of all shuffle dependencies of a post-shuffle stage. So, we add coordinator
    // at here for now.
    // Once we finish https://issues.apache.org/jira/browse/SPARK-10665,
    // we can first add Exchanges and then add coordinator once we have a DAG of query fragments.
    //在这里添加Exchange Coordinator，代码运行到此处，children集合已经包含了Exchange物理计划
    children = withExchangeCoordinator(children, requiredChildDistributions)

    // Now that we've performed any necessary shuffles, add sorts to guarantee output orderings:

    //添加Sort物理计划，注意不是添加Exchange物理计划，实际上，添加了Sort物理计划会再添加Exchange物理计划
    children = children.zip(requiredChildOrderings).map { case (child, requiredOrdering) =>
      if (requiredOrdering.nonEmpty) {
        // If child.outputOrdering is [a, b] and requiredOrdering is [a], we do not need to sort.
        //从child.outputOrdering取出requiredOrdering元素个数的SortOrder元素
        if (requiredOrdering != child.outputOrdering.take(requiredOrdering.length)) {

          //分区内排序，而不是全局排序
          Sort(requiredOrdering, global = false, child = child)
        } else {
          child
        }
      } else {
        child
      }
    }

    operator.withNewChildren(children)
  }

  /** *
    * 如果是Exchange物理计划，那么
    * @param plan
    * @return
    */
  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case operator @ Exchange(partitioning, child, _) =>
      child.children match {
        case Exchange(childPartitioning, baseChild, _)::Nil =>
          if (childPartitioning.guarantees(partitioning)) child else operator
        case _ => operator
      }
    case operator: SparkPlan => ensureDistributionAndOrdering(operator)
  }
}
