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

package org.apache.spark.rdd

import java.util.Random

import scala.collection.{mutable, Map}
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.{classTag, ClassTag}

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.hadoop.io.{BytesWritable, NullWritable, Text}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.TextOutputFormat

import org.apache.spark._
import org.apache.spark.Partitioner._
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.partial.BoundedDouble
import org.apache.spark.partial.CountEvaluator
import org.apache.spark.partial.GroupedCountEvaluator
import org.apache.spark.partial.PartialResult
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{BoundedPriorityQueue, Utils}
import org.apache.spark.util.collection.OpenHashMap
import org.apache.spark.util.random.{BernoulliCellSampler, BernoulliSampler, PoissonSampler,
  SamplingUtils}

/**
 * A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable,
 * partitioned collection of elements that can be operated on in parallel. This class contains the
 * basic operations available on all RDDs, such as `map`, `filter`, and `persist`. In addition,
 * [[org.apache.spark.rdd.PairRDDFunctions]] contains operations available only on RDDs of key-value
 * pairs, such as `groupByKey` and `join`;
 * [[org.apache.spark.rdd.DoubleRDDFunctions]] contains operations available only on RDDs of
 * Doubles; and
 * [[org.apache.spark.rdd.SequenceFileRDDFunctions]] contains operations available on RDDs that
 * can be saved as SequenceFiles.
 * All operations are automatically available on any RDD of the right type (e.g. RDD[(Int, Int)]
 * through implicit.
 *
 * Internally, each RDD is characterized by five main properties:
 *
 *  - A list of partitions
 *  - A function for computing each split
 *  - A list of dependencies on other RDDs
 *  - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
 *  - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
 *    an HDFS file)
 *
 * All of the scheduling and execution in Spark is done based on these methods, allowing each RDD
 * to implement its own way of computing itself. Indeed, users can implement custom RDDs (e.g. for
 * reading data from a new storage system) by overriding these functions. Please refer to the
 * [[http://www.cs.berkeley.edu/~matei/papers/2012/nsdi_spark.pdf Spark paper]] for more details
 * on RDD internals.
 */
abstract class RDD[T: ClassTag](
    @transient private var _sc: SparkContext,
    @transient private var deps: Seq[Dependency[_]]
  ) extends Serializable with Logging {

  if (classOf[RDD[_]].isAssignableFrom(elementClassTag.runtimeClass)) {
    // This is a warning instead of an exception in order to avoid breaking user programs that
    // might have defined nested RDDs without running jobs with them.
    logWarning("Spark does not support nested RDDs (see SPARK-5063)")
  }

  private def sc: SparkContext = {
    if (_sc == null) {
      throw new SparkException(
        "RDD transformations and actions can only be invoked by the driver, not inside of other " +
        "transformations; for example, rdd1.map(x => rdd2.values.count() * x) is invalid because " +
        "the values transformation and count action cannot be performed inside of the rdd1.map " +
        "transformation. For more information, see SPARK-5063.")
    }
    _sc
  }

  /** Construct an RDD with just a one-to-one dependency on one parent */
  def this(@transient oneParent: RDD[_]) =
    this(oneParent.context, List(new OneToOneDependency(oneParent)))

  private[spark] def conf = sc.conf
  // =======================================================================
  // Methods that should be implemented by subclasses of RDD
  // =======================================================================

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
    * compute方法会在iterator方法中调用，iterator是模板方法，它调用的compute方法由RDD的子类实现
   */
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[T]

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   *
   *
   * The partitions in this array must satisfy the following property:
   *   `rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
   */
  protected def getPartitions: Array[Partition]

  /**
   * Implemented by subclasses to return how this RDD depends on parent RDDs. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getDependencies: Seq[Dependency[_]] = deps

  /**
   * Optionally overridden by subclasses to specify placement preferences.
   */
  protected def getPreferredLocations(split: Partition): Seq[String] = Nil

  /** Optionally overridden by subclasses to specify how they are partitioned. */
  @transient val partitioner: Option[Partitioner] = None

  // =======================================================================
  // Methods and fields available on all RDDs
  // =======================================================================

  /** The SparkContext that created this RDD. */
  def sparkContext: SparkContext = sc

  /** A unique ID for this RDD (within its SparkContext). */
  val id: Int = sc.newRddId()

  /** A friendly name for this RDD */
  @transient var name: String = null

  /** Assign a name to this RDD */
  def setName(_name: String): this.type = {
    name = _name
    this
  }

  /**
   * Mark this RDD for persisting using the specified level.
    *
    * 如果RDD已经缓存过，那么如果改变RDD的缓存级别，那么会报错，除非设置overwrite了
    *
    * 只是标记该RDD需要缓存，在action执行时，如果缓存中没有，则首先计算，然后再存到缓存，之后再有action读取这个RDD的时候，就会从缓存中读取
    * rdd.cache >> rdd.count(计算并写缓存)
    *           >> rdd.foreach(略过计算直接读缓存)
    *
    *
    * 在RDD的iterator方法中compute或者read cache
    *
   *
   * @param newLevel the target storage level
   * @param allowOverride whether to override any existing level with the new one
   */
  private def persist(newLevel: StorageLevel, allowOverride: Boolean): this.type = {
    // TODO: Handle changes of StorageLevel
    if (storageLevel != StorageLevel.NONE && newLevel != storageLevel && !allowOverride) {
      throw new UnsupportedOperationException(
        "Cannot change storage level of an RDD after it was already assigned a level")
    }
    // If this is the first time this RDD is marked for persisting, register it
    // with the SparkContext for cleanups and accounting. Do this only once.
    if (storageLevel == StorageLevel.NONE) {
      sc.cleaner.foreach(_.registerRDDForCleanup(this))

      /***
        * 如果原来的存储级别是NONE，表示未曾缓存过，调用SparkContext的persistRDD方法，记录下这个RDD要进行缓存，并没有记录这个RDD的存储级别
        */
      sc.persistRDD(this)
    }
    storageLevel = newLevel
    this
  }

  /**
   * Set this RDD's storage level to persist its values across operations after the first time
   * it is computed. This can only be used to assign a new storage level if the RDD does not
   * have a storage level set yet. Local checkpointing is an exception.
    *
    *
    * 1. 首先判断是否是isLocallyCheckpointed？何为LocallyCheckepointed?
    * 2.
    *
    *
    * 问题：如果一个RDD已经persist过，比如先persist到内存，后persist到内存+磁盘，这个处理过程如何？
   */
  def persist(newLevel: StorageLevel): this.type = {
    if (isLocallyCheckpointed) {
      // This means the user previously called localCheckpoint(), which should have already
      // marked this RDD for persisting. Here we should override the old storage level with
      // one that is explicitly requested by the user (after adapting it to use disk).
      persist(LocalRDDCheckpointData.transformStorageLevel(newLevel), allowOverride = true)
    } else {
      persist(newLevel, allowOverride = false)
    }
  }

  /**
    * Persist this RDD with the default storage level (`MEMORY_ONLY`).
    *
    * 存储级别为MEMORY_ONLY
    * */
  def persist(): this.type = {
    persist(StorageLevel.MEMORY_ONLY)
  }

  /**
    * Persist this RDD with the default storage level (`MEMORY_ONLY`).
    *
    * 将数据缓存到内存（不涉及序列化操作，也就是MEMORY_ONLY)
    *
    */
  def cache(): this.type = persist()

  /**
   * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
   *
   * @param blocking Whether to block until all blocks are deleted.
   * @return This RDD.
   */
  def unpersist(blocking: Boolean = true): this.type = {
    logInfo("Removing RDD " + id + " from persistence list")
    sc.unpersistRDD(id, blocking)
    storageLevel = StorageLevel.NONE
    this
  }

  /** Get the RDD's current storage level, or StorageLevel.NONE if none is set. */
  def getStorageLevel: StorageLevel = storageLevel

  // Our dependencies and partitions will be gotten by calling subclass's methods below, and will
  // be overwritten when we're checkpointed
  private var dependencies_ : Seq[Dependency[_]] = null
  @transient private var partitions_ : Array[Partition] = null

  /** An Option holding our checkpoint RDD, if we are checkpointed */
  private def checkpointRDD: Option[CheckpointRDD[T]] = checkpointData.flatMap(_.checkpointRDD)

  /**
   * Get the list of dependencies of this RDD, taking into account whether the
   * RDD is checkpointed or not.
   *
   *  RDD的五大特性之一是RDD之间存在依赖关系，这个依赖关系是通过dependencies方法计算的，
   *  RDD之间的依赖关系是通过Dependency描述的。
   *
   *  此处返回的是一个Dependency集合，意思是一个RDD可以能依赖于多个RDD，RDD和RDD之间的依赖关系只有一个Dependency
   *
   *  问题：dependencies方法是在什么地方调用到的？
    *
    * @return
    */
  final def dependencies: Seq[Dependency[_]] = {

    //如果checkpointRDD有数据，那么构造OneToOneDependency,
    //如果checkpointRDD没有数据，那么调用getDependencies获取RDD的依赖
    //因此，如果RDD做了checkpoint，那么它的parent只有一个是CheckpointRDD
    checkpointRDD.map(r => List(new OneToOneDependency(r))).getOrElse {
      if (dependencies_ == null) {
        dependencies_ = getDependencies
      }
      dependencies_
    }
  }

  /**
   * Get the array of partitions of this RDD, taking into account whether the
   * RDD is checkpointed or not.
   */
  final def partitions: Array[Partition] = {
    checkpointRDD.map(_.partitions).getOrElse {
      if (partitions_ == null) {
        partitions_ = getPartitions
        partitions_.zipWithIndex.foreach { case (partition, index) =>
          require(partition.index == index,
            s"partitions($index).partition == ${partition.index}, but it should equal $index")
        }
      }
      partitions_
    }
  }

  /**
    * Returns the number of partitions of this RDD.
    */
  @Since("1.6.0")
  final def getNumPartitions: Int = partitions.length

  /**
   * Get the preferred locations of a partition, taking into account whether the
   * RDD is checkpointed.
   */
  final def preferredLocations(split: Partition): Seq[String] = {
    checkpointRDD.map(_.getPreferredLocations(split)).getOrElse {
      getPreferredLocations(split)
    }
  }

  /**
   * Internal method to this RDD; will read from cache if applicable, or otherwise compute it.
   * This should ''not'' be called by users directly, but is available for implementors of custom
   * subclasses of RDD.
    *
    * 1. 如果该RDD标记为已经Cache，那么从CacheManager中读取
    * 2. 如果没有标记为Cache，那么先计算或者从Checkpoint中读取
    *
    * 从这个逻辑中可以看出，如果一个RDD既被cache又做了checkpoint，那么只会从cache中读取
   */
  final def iterator(split: Partition, context: TaskContext): Iterator[T] = {

    /***
      * 如果存储级别不是NONE，表示RDD被标记为Cache(可能存放在BlockManager的memory block中，也可能存放在BlockManager的disk block中。
      * 如果已经实际缓存过那么读取缓存的数据；如果没有真正的cache过那么首先计算该RDD的数据再进行cache)
      */
    if (storageLevel != StorageLevel.NONE) {
      SparkEnv.get.cacheManager.getOrCompute(this, split, context, storageLevel)
    }

    /***
      * 如果没有cache，那么读取Checkpoint
      */
    else {
      computeOrReadCheckpoint(split, context)
    }
  }

  /**
   * Return the ancestors of the given RDD that are related to it only through a sequence of
   * narrow dependencies. This traverses the given RDD's dependency tree using DFS, but maintains
   * no ordering on the RDDs returned.
   */
  private[spark] def getNarrowAncestors: Seq[RDD[_]] = {
    val ancestors = new mutable.HashSet[RDD[_]]

    def visit(rdd: RDD[_]) {
      val narrowDependencies = rdd.dependencies.filter(_.isInstanceOf[NarrowDependency[_]])
      val narrowParents = narrowDependencies.map(_.rdd)
      val narrowParentsNotVisited = narrowParents.filterNot(ancestors.contains)
      narrowParentsNotVisited.foreach { parent =>
        ancestors.add(parent)
        visit(parent)
      }
    }

    visit(this)

    // In case there is a cycle, do not include the root itself
    ancestors.filterNot(_ == this).toSeq
  }

  /**
   * Compute an RDD partition or read it from a checkpoint if the RDD is checkpointing.
   */
  private[spark] def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] =
  {
    /***
      * 如果已经checkpoint，那么调用firstParent的iterator方法
      * isCheckpointedAndMaterialized方法返回RDD的isCheckpointed函数的值，isCheckpointed函数用于检查该RDD关联的check point data已经checkpointed
      */
    if (isCheckpointedAndMaterialized) {

      //如果该RDD已经checkpoint，那么直接查找firstParent的iterator方法,firstParent就是ReliableCheckpointRDD
      firstParent[T].iterator(split, context)
    } else {
      /***
        * 否则进行计算
        */
      compute(split, context)
    }
  }

  /**
   * Execute a block of code in a scope such that all new RDDs created in this body will
   * be part of the same scope. For more detail, see {{org.apache.spark.rdd.RDDOperationScope}}.
   *
   * Note: Return statements are NOT allowed in the given body.
   */
  private[spark] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](sc)(body)

  // Transformations (return a new RDD)

  /**
   * Return a new RDD by applying a function to all elements of this RDD.
    * 构造MapPartitionsRDD时， preservesPartitioning使用的是false，为什么分区算法不保留？
    * 比如rdd1两个分区，按照奇偶进行分区，那么
   *  rdd2 = rdd1.map(_ * 2),那么rdd2的所有元素都是偶数，如果分区算法保持，那么rdd2的所有数据都会集中到
   *  同一个分区(偶数)
   *
   *  map返回的是RDD的类型是MapPartitionsRDD，
   *  Task任务函数是(context, pid, iter) => iter.map(cleanF)，分别表示TaskContext、partitionId以及分区数据集合对应的Iteratorss
   */
  def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))
  }

  /**
   *  Return a new RDD by first applying a function to all elements of this
   *  RDD, and then flattening the results.
   *
   *  Scala的flatMap是一个首先将集合中的每个元素转换为集合(此时的结果为二维集合)，然后将每个元素集合中的所有元素
   *  展开得到一维集合
   *  比如
   *  val lines = Seq("Hello World","How Are You");
   *  lines.flatMap(_.split(" "));
   *  最后得到：
   *
   *  words= Seq("Hello","World","How","Are","You")
   *
   *
   * @param f 将RDD中的每个元素(元素类型为T)转换为一个集合，集合的元素的类型是U
   * @tparam U
   * @return
   */
  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.flatMap(cleanF))
  }

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   */
  def filter(f: T => Boolean): RDD[T] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[T, T](
      this,
      (context, pid, iter) => iter.filter(cleanF),
      preservesPartitioning = true)
  }

  /**
   * Return a new RDD containing the distinct elements in this RDD.
    * distinct的实现思路是word count(单词计数)
    * @param numPartitions
    * @param ord 这个参数是干啥的？
    * @return
    */
  def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    val pairRDD = map(x => (x, null))


    //reduceByKey的参数是(v,v)=>v,
    //也就是说此处针对相同Key的两个Value(都是null),返回一个null
    val reduce = pairRDD.reduceByKey((v1, v2) => v1, numPartitions)

    //reduce是(K,null)集合，因此通过_._1取出key

    reduce.map(_._1)
  }

  /**
   * Return a new RDD containing the distinct elements in this RDD.
    *
    * RDD去重
    * 对于RDD[Person]，根据什么去重？
    *
    * @return
    */
  def distinct(): RDD[T] = withScope {
    distinct(partitions.length)
  }

  /**
   * Return a new RDD that has exactly numPartitions partitions.
   *
   * Can increase or decrease the level of parallelism in this RDD. Internally, this uses
   * a shuffle to redistribute data.
   *
   * If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
   * which can avoid performing a shuffle.
   */
  def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    coalesce(numPartitions, shuffle = true)
  }

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
   *
   * This results in a narrow dependency, e.g. if you go from 1000 partitions
   * to 100 partitions, there will not be a shuffle, instead each of the 100
   * new partitions will claim 10 of the current partitions.
   *
   * However, if you're doing a drastic coalesce, e.g. to numPartitions = 1,
   * this may result in your computation taking place on fewer nodes than
   * you like (e.g. one node in the case of numPartitions = 1). To avoid this,
   * you can pass shuffle = true. This will add a shuffle step, but means the
   * current upstream partitions will be executed in parallel (per whatever
   * the current partitioning is).
   *
   * Note: With shuffle = true, you can actually coalesce to a larger number
   * of partitions. This is useful if you have a small number of partitions,
   * say 100, potentially with a few partitions being abnormally large. Calling
   * coalesce(1000, shuffle = true) will result in 1000 partitions with the
   * data distributed using a hash partitioner.
   */
  def coalesce(numPartitions: Int, shuffle: Boolean = false)(implicit ord: Ordering[T] = null)
      : RDD[T] = withScope {
    if (shuffle) {
      /** Distributes elements evenly across output partitions, starting from a random partition. */
      val distributePartition = (index: Int, items: Iterator[T]) => {
        var position = (new Random(index)).nextInt(numPartitions)
        items.map { t =>
          // Note that the hash code of the key will just be the key itself. The HashPartitioner
          // will mod it with the number of total partitions.
          position = position + 1
          (position, t)
        }
      } : Iterator[(Int, T)]

      // include a shuffle step so that our upstream tasks are still distributed
      /***
        * Key是Partition的ID
        */
      new CoalescedRDD(
        new ShuffledRDD[Int, T, T](mapPartitionsWithIndex(distributePartition),
        new HashPartitioner(numPartitions)),
        numPartitions).values
    } else {
      new CoalescedRDD(this, numPartitions)
    }
  }

  /**
   * Return a sampled subset of this RDD.
   *
   * @param withReplacement can elements be sampled multiple times (replaced when sampled out)
   * @param fraction expected size of the sample as a fraction of this RDD's size
   *  without replacement: probability that each element is chosen; fraction must be [0, 1]
   *  with replacement: expected number of times each element is chosen; fraction must be >= 0
   * @param seed seed for the random number generator
   */
  def sample(
      withReplacement: Boolean,
      fraction: Double,
      seed: Long = Utils.random.nextLong): RDD[T] = withScope {
    require(fraction >= 0.0, "Negative fraction value: " + fraction)
    if (withReplacement) {
      new PartitionwiseSampledRDD[T, T](this, new PoissonSampler[T](fraction), true, seed)
    } else {
      new PartitionwiseSampledRDD[T, T](this, new BernoulliSampler[T](fraction), true, seed)
    }
  }

  /**
   * Randomly splits this RDD with the provided weights.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1
   * @param seed random seed
   *
   * @return split RDDs in an array
   */
  def randomSplit(
      weights: Array[Double],
      seed: Long = Utils.random.nextLong): Array[RDD[T]] = withScope {
    val sum = weights.sum
    val normalizedCumWeights = weights.map(_ / sum).scanLeft(0.0d)(_ + _)
    normalizedCumWeights.sliding(2).map { x =>
      randomSampleWithRange(x(0), x(1), seed)
    }.toArray
  }

  /**
   * Internal method exposed for Random Splits in DataFrames. Samples an RDD given a probability
   * range.
   * @param lb lower bound to use for the Bernoulli sampler
   * @param ub upper bound to use for the Bernoulli sampler
   * @param seed the seed for the Random number generator
   * @return A random sub-sample of the RDD without replacement.
   */
  private[spark] def randomSampleWithRange(lb: Double, ub: Double, seed: Long): RDD[T] = {
    this.mapPartitionsWithIndex( { (index, partition) =>
      val sampler = new BernoulliCellSampler[T](lb, ub)
      sampler.setSeed(seed + index)
      sampler.sample(partition)
    }, preservesPartitioning = true)
  }

  /**
   * Return a fixed-size sampled subset of this RDD in an array
    *
    * RDD的采样API
   *
   * @param withReplacement whether sampling is done with replacement
   * @param num size of the returned sample
   * @param seed seed for the random number generator
   * @return sample of specified size in an array
   */
  def takeSample(
      withReplacement: Boolean,
      num: Int,
      seed: Long = Utils.random.nextLong): Array[T] = withScope {
    val numStDev = 10.0

    require(num >= 0, "Negative number of elements requested")
    require(num <= (Int.MaxValue - (numStDev * math.sqrt(Int.MaxValue)).toInt),
      "Cannot support a sample size > Int.MaxValue - " +
      s"$numStDev * math.sqrt(Int.MaxValue)")

    if (num == 0) {
      new Array[T](0)
    } else {
      val initialCount = this.count()
      if (initialCount == 0) {
        new Array[T](0)
      } else {
        val rand = new Random(seed)
        if (!withReplacement && num >= initialCount) {
          Utils.randomizeInPlace(this.collect(), rand)
        } else {
          val fraction = SamplingUtils.computeFractionForSampleSize(num, initialCount,
            withReplacement)
          var samples = this.sample(withReplacement, fraction, rand.nextInt()).collect()

          // If the first sample didn't turn out large enough, keep trying to take samples;
          // this shouldn't happen often because we use a big multiplier for the initial size
          var numIters = 0
          while (samples.length < num) {
            logWarning(s"Needed to re-sample due to insufficient sample size. Repeat #$numIters")
            samples = this.sample(withReplacement, fraction, rand.nextInt()).collect()
            numIters += 1
          }
          Utils.randomizeInPlace(samples, rand).take(num)
        }
      }
    }
  }

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   */
  def union(other: RDD[T]): RDD[T] = withScope {
    if (partitioner.isDefined && other.partitioner == partitioner) {
      new PartitionerAwareUnionRDD(sc, Array(this, other))
    } else {
      new UnionRDD(sc, Array(this, other))
    }
  }

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   */
  def ++(other: RDD[T]): RDD[T] = withScope {
    this.union(other)
  }

  /**
   * Return this RDD sorted by the given key function.
   */
  def sortBy[K](
      f: (T) => K,
      ascending: Boolean = true,
      numPartitions: Int = this.partitions.length)
      (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = withScope {
    this.keyBy[K](f)
        .sortByKey(ascending, numPartitions)
        .values
  }

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.
   *
   * Note that this method performs a shuffle internally.
   */
  def intersection(other: RDD[T]): RDD[T] = withScope {
    this.map(v => (v, null)).cogroup(other.map(v => (v, null)))
        .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty }
        .keys
  }

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.
   *
   * Note that this method performs a shuffle internally.
   *
   * @param partitioner Partitioner to use for the resulting RDD
   */
  def intersection(
      other: RDD[T],
      partitioner: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    this.map(v => (v, null)).cogroup(other.map(v => (v, null)), partitioner)
        .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty }
        .keys
  }

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.  Performs a hash partition across the cluster
   *
   * Note that this method performs a shuffle internally.
   *
   * @param numPartitions How many partitions to use in the resulting RDD
   */
  def intersection(other: RDD[T], numPartitions: Int): RDD[T] = withScope {
    intersection(other, new HashPartitioner(numPartitions))
  }

  /**
   * Return an RDD created by coalescing all elements within each partition into an array.
   */
  def glom(): RDD[Array[T]] = withScope {
    new MapPartitionsRDD[Array[T], T](this, (context, pid, iter) => Iterator(iter.toArray))
  }

  /**
   * Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of
   * elements (a, b) where a is in `this` and b is in `other`.
   */
  def cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)] = withScope {
    new CartesianRDD(sc, this, other)
  }

  /**
   * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = withScope {
    groupBy[K](f, defaultPartitioner(this))
  }

  /**
   * Return an RDD of grouped elements. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  def groupBy[K](
      f: T => K,
      numPartitions: Int)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = withScope {
    groupBy(f, new HashPartitioner(numPartitions))
  }

  /**
   * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  def groupBy[K](f: T => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = null)
      : RDD[(K, Iterable[T])] = withScope {
    val cleanF = sc.clean(f)
    this.map(t => (cleanF(t), t)).groupByKey(p)
  }

  /**
   * Return an RDD created by piping elements to a forked external process.
   */
  def pipe(command: String): RDD[String] = withScope {
    new PipedRDD(this, command)
  }

  /**
   * Return an RDD created by piping elements to a forked external process.
   */
  def pipe(command: String, env: Map[String, String]): RDD[String] = withScope {
    new PipedRDD(this, command, env)
  }

  /**
   * Return an RDD created by piping elements to a forked external process.
   * The print behavior can be customized by providing two functions.
   *
   * @param command command to run in forked process.
   * @param env environment variables to set.
   * @param printPipeContext Before piping elements, this function is called as an opportunity
   *                         to pipe context data. Print line function (like out.println) will be
   *                         passed as printPipeContext's parameter.
   * @param printRDDElement Use this function to customize how to pipe elements. This function
   *                        will be called with each RDD element as the 1st parameter, and the
   *                        print line function (like out.println()) as the 2nd parameter.
   *                        An example of pipe the RDD data of groupBy() in a streaming way,
   *                        instead of constructing a huge String to concat all the elements:
   *                        def printRDDElement(record:(String, Seq[String]), f:String=&gt;Unit) =
   *                          for (e &lt;- record._2){f(e)}
   * @param separateWorkingDir Use separate working directories for each task.
   * @return the result RDD
   */
  def pipe(
      command: Seq[String],
      env: Map[String, String] = Map(),
      printPipeContext: (String => Unit) => Unit = null,
      printRDDElement: (T, String => Unit) => Unit = null,
      separateWorkingDir: Boolean = false): RDD[String] = withScope {
    new PipedRDD(this, command, env,
      if (printPipeContext ne null) sc.clean(printPipeContext) else null,
      if (printRDDElement ne null) sc.clean(printRDDElement) else null,
      separateWorkingDir)
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD.
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
    *
   *  mapPartitions操作是在对整个分区数据集合(Iterator)应用函数f，结果仍然是一个数据集合(Iterator)
   *
   *  从Iterator转换到另一个Iterator，mapPartitions直接
   *
   *
    * @param f
    * @param preservesPartitioning
    * @tparam U
    * @return
    */
  def mapPartitions[U: ClassTag](
      f: Iterator[T] => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = withScope {
    val cleanedF = sc.clean(f)
    new MapPartitionsRDD(
      this,
      (context: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(iter),
      preservesPartitioning)
  }

  /**
   * [performance] Spark's internal mapPartitions method which skips closure cleaning. It is a
   * performance API to be used carefully only if we are sure that the RDD elements are
   * serializable and don't require closure cleaning.
   *
   * @param preservesPartitioning indicates whether the input function preserves the partitioner,
   * which should be `false` unless this is a pair RDD and the input function doesn't modify
   * the keys.
   */
  private[spark] def mapPartitionsInternal[U: ClassTag](
      f: Iterator[T] => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = withScope {
    new MapPartitionsRDD(
      this,
      (context: TaskContext, index: Int, iter: Iterator[T]) => f(iter),
      preservesPartitioning)
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD, while tracking the index
   * of the original partition.
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   */
  def mapPartitionsWithIndex[U: ClassTag](
      f: (Int, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = withScope {
    val cleanedF = sc.clean(f)
    new MapPartitionsRDD(
      this,
      (context: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(index, iter),
      preservesPartitioning)
  }

  /**
   * Zips this RDD with another one, returning key-value pairs with the first element in each RDD,
   * second element in each RDD, etc. Assumes that the two RDDs have the *same number of
   * partitions* and the *same number of elements in each partition* (e.g. one was made through
   * a map on the other).
   */
  def zip[U: ClassTag](other: RDD[U]): RDD[(T, U)] = withScope {
    zipPartitions(other, preservesPartitioning = false) { (thisIter, otherIter) =>
      new Iterator[(T, U)] {
        def hasNext: Boolean = (thisIter.hasNext, otherIter.hasNext) match {
          case (true, true) => true
          case (false, false) => false
          case _ => throw new SparkException("Can only zip RDDs with " +
            "same number of elements in each partition")
        }
        def next(): (T, U) = (thisIter.next(), otherIter.next())
      }
    }
  }

  /**
   * Zip this RDD's partitions with one (or more) RDD(s) and return a new RDD by
   * applying a function to the zipped partitions. Assumes that all the RDDs have the
   * *same number of partitions*, but does *not* require them to have the same number
   * of elements in each partition.
   */
  def zipPartitions[B: ClassTag, V: ClassTag]
      (rdd2: RDD[B], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] = withScope {
    new ZippedPartitionsRDD2(sc, sc.clean(f), this, rdd2, preservesPartitioning)
  }

  def zipPartitions[B: ClassTag, V: ClassTag]
      (rdd2: RDD[B])
      (f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] = withScope {
    zipPartitions(rdd2, preservesPartitioning = false)(f)
  }

  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] = withScope {
    new ZippedPartitionsRDD3(sc, sc.clean(f), this, rdd2, rdd3, preservesPartitioning)
  }

  def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C])
      (f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] = withScope {
    zipPartitions(rdd2, rdd3, preservesPartitioning = false)(f)
  }

  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] = withScope {
    new ZippedPartitionsRDD4(sc, sc.clean(f), this, rdd2, rdd3, rdd4, preservesPartitioning)
  }

  def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D])
      (f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] = withScope {
    zipPartitions(rdd2, rdd3, rdd4, preservesPartitioning = false)(f)
  }


  // Actions (launch a job to return a value to the user program)

  /**
   * Applies a function f to all elements of this RDD.
   * 对RDD的所有元素应用函数f，函数f的类型是T=>Unit,foreach这个action的
   * 返回值是Unit,也就是说，foreach的功能是产生副作用的
   *
   * foreach是在executor上对每个分区的数据进行遍历处理，而不是将数据返回到Driver再处理
   * @param f
   */
  def foreach(f: T => Unit): Unit = withScope {
    val cleanF = sc.clean(f)

    //runJob的第二个参数processPartitionData函数的类型是(iter: Iterator[T]) => iter.foreach(cleanF)
    val processPartitionDataFunc = (iter: Iterator[T]) => iter.foreach(cleanF)

    //jobResult的类型是Array[Unit]，也就是说每个分区返回的分区计算结果是Unit
    val jobResult = sc.runJob(this, processPartitionDataFunc)
    jobResult
  }

  /**
   * Applies a function f to each partition of this RDD.
   *
   *  foreachPartition也是一个action，它接受一个函数，该函数类型是Iterator[T] => Unit
   *  也就是说，f函数的入参是整个分区的数据集合，然后对该集合应用f得到一个Unit结果
   *  跟foreach类似，foreachPartition也是一个带有副作用的action，他的结果在worker端
   *
   *  foreachPartition的效率通常要高于foreach
    *
    * @param f
    */
  def foreachPartition(f: Iterator[T] => Unit): Unit = withScope {
    val cleanF = sc.clean(f)
    sc.runJob(this, (iter: Iterator[T]) => cleanF(iter))
  }

  /**
   * Return an array that contains all of the elements in this RDD.
    *
    * 该方法调用的是返回类型为Array[U]的SparkContext.runJob方法.
    * 对于collect,返回的Array[U]中的每个元素是一个集合，
    * 所以调用Array.concat(results: _*)将每个元素对应的集合进行连接，类似于flatMap
    *
    *
    * @return 返回RDD中所有分区的数据
    */
  def collect(): Array[T] = withScope {
    /***
      * 分区数据处理函数是把分区数据(Iterator[T]类型)转换为数组
      */
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
  }

  /**
   * Return an iterator that contains all of the elements in this RDD.
   *
   * The iterator will consume as much memory as the largest partition in this RDD.
   *
   * Note: this results in multiple Spark jobs, and if the input RDD is the result
   * of a wide transformation (e.g. join with different partitioners), to avoid
   * recomputing the input RDD should be cached first.
   */
  def toLocalIterator: Iterator[T] = withScope {
    def collectPartition(p: Int): Array[T] = {
      sc.runJob(this, (iter: Iterator[T]) => iter.toArray, Seq(p)).head
    }
    (0 until partitions.length).iterator.flatMap(i => collectPartition(i))
  }

  /**
   * Return an RDD that contains all matching values by applying `f`.
   */
  def collect[U: ClassTag](f: PartialFunction[T, U]): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    filter(cleanF.isDefinedAt).map(cleanF)
  }

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   *
   * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
   * RDD will be &lt;= us.
   */
  def subtract(other: RDD[T]): RDD[T] = withScope {
    subtract(other, partitioner.getOrElse(new HashPartitioner(partitions.length)))
  }

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(other: RDD[T], numPartitions: Int): RDD[T] = withScope {
    subtract(other, new HashPartitioner(numPartitions))
  }

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(
      other: RDD[T],
      p: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
    if (partitioner == Some(p)) {
      // Our partitioner knows how to handle T (which, since we have a partitioner, is
      // really (K, V)) so make a new Partitioner that will de-tuple our fake tuples
      val p2 = new Partitioner() {
        override def numPartitions: Int = p.numPartitions
        override def getPartition(k: Any): Int = p.getPartition(k.asInstanceOf[(Any, _)]._1)
      }
      // Unfortunately, since we're making a new p2, we'll get ShuffleDependencies
      // anyway, and when calling .keys, will not have a partitioner set, even though
      // the SubtractedRDD will, thanks to p2's de-tupled partitioning, already be
      // partitioned by the right/real keys (e.g. p).
      this.map(x => (x, null)).subtractByKey(other.map((_, null)), p2).keys
    } else {
      this.map(x => (x, null)).subtractByKey(other.map((_, null)), p).keys
    }
  }

  /**
   * Reduces the elements of this RDD using the specified commutative and
   * associative binary operator.
    *
    * reduce是一个Action，它的参数是一个高阶函数，函数类型是(T,T)=>T，
    * 也就是说，reduce操作不改变数据的类型
    *
    * @param f
    * @return
    */
  def reduce(f: (T, T) => T): T = withScope {
    val cleanF = sc.clean(f)

    /**
      * reducePartition是分区内的函数，它是一个函数，函数类型是Iterator[T] => Option[T]
      * 入参是分区数据形成的Iterator,返回结果是对分区数据集合Iterator进行reduce的结果
      */
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        //问题：Iterator的reduceLeft是什么操作？跟reduceRight什么分别
        Some(iter.reduceLeft(cleanF))
      } else {
        None
      }
    }

    /***
      * jobResult记录的是reduce这个job的最终结果(不是分区内的中间结果),结果是一个Option[T]
      *
      * 此处使用闭包的方式更新jobResult
      */
    var jobResult: Option[T] = None

    /***
      * mergeResult是一个函数，它是SparkContext的第三个参数，
      * 参数的入参是(index:Int, taskResult: Option[T])
      * index参数指的是partition id, taskResult是处理index这个partition的task的结果
      *
      */
    val mergeResult = (index: Int, taskResult: Option[T]) => {

      /***
        * 如果taskResult有值(以为着index分区有数据)
        */
      if (taskResult.isDefined) {

        /***
          * 如果jobResult为None,表示index　partition对应的任务是一个完成的，将jobResult首先
          * 赋值为taskResult
          * 如果jobResult不为None,则表示index partition对应的任务不是第一个完成的，所以需要跟之前完成
          * 的任务结果进行比较(所有之前的计算结果只保留唯一值存放在jobResult中)，这个比较逻辑就是使用ｆ函数实现的
          */
        jobResult = jobResult match {
          case Some(value) => Some(f(value, taskResult.get))
          case None => taskResult
        }
      }
    }

    //SparkContext的runJob是否都是返回值为Unit的？不是的，结果可能是Array[T]，数据记录了每个个分区的
    sc.runJob(this, reducePartition, mergeResult)
    // Get the final result out of our Option, or throw an exception if the RDD was empty
    jobResult.getOrElse(throw new UnsupportedOperationException("empty collection"))
  }

  /**
   * Reduces the elements of this RDD in a multi-level tree pattern.
   *
   * @param depth suggested depth of the tree (default: 2)
   * @see [[org.apache.spark.rdd.RDD#reduce]]
   */
  def treeReduce(f: (T, T) => T, depth: Int = 2): T = withScope {
    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")
    val cleanF = context.clean(f)
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(cleanF))
      } else {
        None
      }
    }
    val partiallyReduced = mapPartitions(it => Iterator(reducePartition(it)))
    val op: (Option[T], Option[T]) => Option[T] = (c, x) => {
      if (c.isDefined && x.isDefined) {
        Some(cleanF(c.get, x.get))
      } else if (c.isDefined) {
        c
      } else if (x.isDefined) {
        x
      } else {
        None
      }
    }
    partiallyReduced.treeAggregate(Option.empty[T])(op, op, depth)
      .getOrElse(throw new UnsupportedOperationException("empty collection"))
  }

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using a
   * given associative and commutative function and a neutral "zero value". The function
   * op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object
   * allocation; however, it should not modify t2.
   *
   * This behaves somewhat differently from fold operations implemented for non-distributed
   * collections in functional languages like Scala. This fold operation may be applied to
   * partitions individually, and then fold those results into the final result, rather than
   * apply the fold to each element sequentially in some defined ordering. For functions
   * that are not commutative, the result may differ from that of a fold applied to a
   * non-distributed collection.
   *
   * @param zeroValue the initial value for the accumulated result of each partition for the `op`
   *                  operator, and also the initial value for the combine results from different
   *                  partitions for the `op` operator - this will typically be the neutral
   *                  element (e.g. `Nil` for list concatenation or `0` for summation)
   * @param op an operator used to both accumulate results within a partition and combine results
   *                  from different partitions
   */
  def fold(zeroValue: T)(op: (T, T) => T): T = withScope {
    // Clone the zero value since we will also be serializing it as part of tasks
    var jobResult = Utils.clone(zeroValue, sc.env.closureSerializer.newInstance())
    val cleanOp = sc.clean(op)
    val foldPartition = (iter: Iterator[T]) => iter.fold(zeroValue)(cleanOp)
    val mergeResult = (index: Int, taskResult: T) => jobResult = op(jobResult, taskResult)
    sc.runJob(this, foldPartition, mergeResult)
    jobResult
  }

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using
   * given combine functions and a neutral "zero value". This function can return a different result
   * type, U, than the type of this RDD, T. Thus, we need one operation for merging a T into an U
   * and one operation for merging two U's, as in scala.TraversableOnce. Both of these functions are
   * allowed to modify and return their first argument instead of creating a new U to avoid memory
   * allocation.
   *
   * @param zeroValue the initial value for the accumulated result of each partition for the
   *                  `seqOp` operator, and also the initial value for the combine results from
   *                  different partitions for the `combOp` operator - this will typically be the
   *                  neutral element (e.g. `Nil` for list concatenation or `0` for summation)
   * @param seqOp an operator used to accumulate results within a partition
   * @param combOp an associative operator used to combine results from different partitions
   */
  def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U = withScope {
    // Clone the zero value since we will also be serializing it as part of tasks
    var jobResult = Utils.clone(zeroValue, sc.env.serializer.newInstance())
    val cleanSeqOp = sc.clean(seqOp)
    val cleanCombOp = sc.clean(combOp)
    val aggregatePartition = (it: Iterator[T]) => it.aggregate(zeroValue)(cleanSeqOp, cleanCombOp)
    val mergeResult = (index: Int, taskResult: U) => jobResult = combOp(jobResult, taskResult)
    sc.runJob(this, aggregatePartition, mergeResult)
    jobResult
  }

  /**
   * Aggregates the elements of this RDD in a multi-level tree pattern.
   *
   * @param depth suggested depth of the tree (default: 2)
   * @see [[org.apache.spark.rdd.RDD#aggregate]]
   */
  def treeAggregate[U: ClassTag](zeroValue: U)(
      seqOp: (U, T) => U,
      combOp: (U, U) => U,
      depth: Int = 2): U = withScope {
    require(depth >= 1, s"Depth must be greater than or equal to 1 but got $depth.")
    if (partitions.length == 0) {
      Utils.clone(zeroValue, context.env.closureSerializer.newInstance())
    } else {
      val cleanSeqOp = context.clean(seqOp)
      val cleanCombOp = context.clean(combOp)
      val aggregatePartition =
        (it: Iterator[T]) => it.aggregate(zeroValue)(cleanSeqOp, cleanCombOp)
      var partiallyAggregated = mapPartitions(it => Iterator(aggregatePartition(it)))
      var numPartitions = partiallyAggregated.partitions.length
      val scale = math.max(math.ceil(math.pow(numPartitions, 1.0 / depth)).toInt, 2)
      // If creating an extra level doesn't help reduce
      // the wall-clock time, we stop tree aggregation.

      // Don't trigger TreeAggregation when it doesn't save wall-clock time
      while (numPartitions > scale + math.ceil(numPartitions.toDouble / scale)) {
        numPartitions /= scale
        val curNumPartitions = numPartitions
        partiallyAggregated = partiallyAggregated.mapPartitionsWithIndex {
          (i, iter) => iter.map((i % curNumPartitions, _))
        }.reduceByKey(new HashPartitioner(curNumPartitions), cleanCombOp).values
      }
      partiallyAggregated.reduce(cleanCombOp)
    }
  }

  /**
   * Return the number of elements in the RDD.
    * 如果Job执行失败，那么SparkContext将抛出异常，此时异常抛给count，由于count没有捕获异常，因此运行用户代码的JVM将异常退出，
    * 由于SparkContext注册了JVM推出时调用SparkContext的stop方法，因此将启动Application结束的流程，最终调用AppClient的stop方法时，给Master发送removeApplication的消息
    *
    *
    * 如sc.textFile(...).flatMap(...).reduceByKey(_+_).count,那么传给SparkContext是最后一个RDD(调用count方法的rdd)
    *
    *
    *
    *
   */
  def count(): Long = {
    /***
      * runJob返回一个数组，表示每个分区的计算数据(分区的数据是调用Utils.getIteratorSize进行计算的)，调用数组的sum，对所有的数据进行求和
      *
      * Utils.getIteratorSize _是分区内计算函数，它接收类型为Iterator，返回Iterator集合的元素个数
      *
      * 问题：Utils.getIteratorSize后面的下划线为什么是必须的？因为Utils.getIteratorSize是一个方法，而runJob的第二个参数是一个函数，
      * 这是是将方法转换为函数的做法
      *
      *
      */
//    val arr = sc.runJob(this, Utils.getIteratorSize _)
    val arr = sc.runJob(this, iteratorSize _);
    arr.sum
  }

  def iteratorSize[T](iter: Iterator[T]) :Long = {
    Utils.getIteratorSize(iter)
  }



  /**
   * Approximate version of count() that returns a potentially incomplete result
   * within a timeout, even if not all tasks have finished.
   */
  def countApprox(
      timeout: Long,
      confidence: Double = 0.95): PartialResult[BoundedDouble] = withScope {
    val countElements: (TaskContext, Iterator[T]) => Long = { (ctx, iter) =>
      var result = 0L
      while (iter.hasNext) {
        result += 1L
        iter.next()
      }
      result
    }
    val evaluator = new CountEvaluator(partitions.length, confidence)
    sc.runApproximateJob(this, countElements, evaluator, timeout)
  }

  /**
   * Return the count of each unique value in this RDD as a local map of (value, count) pairs.
   *
   * Note that this method should only be used if the resulting map is expected to be small, as
   * the whole thing is loaded into the driver's memory.
   * To handle very large results, consider using rdd.map(x =&gt; (x, 1L)).reduceByKey(_ + _), which
   * returns an RDD[T, Long] instead of a map.
   */
  def countByValue()(implicit ord: Ordering[T] = null): Map[T, Long] = withScope {
    map(value => (value, null)).countByKey()
  }

  /**
   * Approximate version of countByValue().
   */
  def countByValueApprox(timeout: Long, confidence: Double = 0.95)
      (implicit ord: Ordering[T] = null)
      : PartialResult[Map[T, BoundedDouble]] = withScope {
    if (elementClassTag.runtimeClass.isArray) {
      throw new SparkException("countByValueApprox() does not support arrays")
    }
    val countPartition: (TaskContext, Iterator[T]) => OpenHashMap[T, Long] = { (ctx, iter) =>
      val map = new OpenHashMap[T, Long]
      iter.foreach {
        t => map.changeValue(t, 1L, _ + 1L)
      }
      map
    }
    val evaluator = new GroupedCountEvaluator[T](partitions.length, confidence)
    sc.runApproximateJob(this, countPartition, evaluator, timeout)
  }

  /**
   * Return approximate number of distinct elements in the RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * The relative accuracy is approximately `1.054 / sqrt(2^p)`. Setting a nonzero `sp &gt; p`
   * would trigger sparse representation of registers, which may reduce the memory consumption
   * and increase accuracy when the cardinality is small.
   *
   * @param p The precision value for the normal set.
   *          `p` must be a value between 4 and `sp` if `sp` is not zero (32 max).
   * @param sp The precision value for the sparse set, between 0 and 32.
   *           If `sp` equals 0, the sparse representation is skipped.
   */
  def countApproxDistinct(p: Int, sp: Int): Long = withScope {
    require(p >= 4, s"p ($p) must be >= 4")
    require(sp <= 32, s"sp ($sp) must be <= 32")
    require(sp == 0 || p <= sp, s"p ($p) cannot be greater than sp ($sp)")
    val zeroCounter = new HyperLogLogPlus(p, sp)
    aggregate(zeroCounter)(
      (hll: HyperLogLogPlus, v: T) => {
        hll.offer(v)
        hll
      },
      (h1: HyperLogLogPlus, h2: HyperLogLogPlus) => {
        h1.addAll(h2)
        h1
      }).cardinality()
  }

  /**
   * Return approximate number of distinct elements in the RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   */
  def countApproxDistinct(relativeSD: Double = 0.05): Long = withScope {
    require(relativeSD > 0.000017, s"accuracy ($relativeSD) must be greater than 0.000017")
    val p = math.ceil(2.0 * math.log(1.054 / relativeSD) / math.log(2)).toInt
    countApproxDistinct(if (p < 4) 4 else p, 0)
  }

  /**
   * Zips this RDD with its element indices. The ordering is first based on the partition index
   * and then the ordering of items within each partition. So the first item in the first
   * partition gets index 0, and the last item in the last partition receives the largest index.
   *
   * This is similar to Scala's zipWithIndex but it uses Long instead of Int as the index type.
   * This method needs to trigger a spark job when this RDD contains more than one partitions.
   *
   * Note that some RDDs, such as those returned by groupBy(), do not guarantee order of
   * elements in a partition. The index assigned to each element is therefore not guaranteed,
   * and may even change if the RDD is reevaluated. If a fixed ordering is required to guarantee
   * the same index assignments, you should sort the RDD with sortByKey() or save it to a file.
   */
  def zipWithIndex(): RDD[(T, Long)] = withScope {
    new ZippedWithIndexRDD(this)
  }

  /**
   * Zips this RDD with generated unique Long ids. Items in the kth partition will get ids k, n+k,
   * 2*n+k, ..., where n is the number of partitions. So there may exist gaps, but this method
   * won't trigger a spark job, which is different from [[org.apache.spark.rdd.RDD#zipWithIndex]].
   *
   * Note that some RDDs, such as those returned by groupBy(), do not guarantee order of
   * elements in a partition. The unique ID assigned to each element is therefore not guaranteed,
   * and may even change if the RDD is reevaluated. If a fixed ordering is required to guarantee
   * the same index assignments, you should sort the RDD with sortByKey() or save it to a file.
   */
  def zipWithUniqueId(): RDD[(T, Long)] = withScope {
    val n = this.partitions.length.toLong
    this.mapPartitionsWithIndex { case (k, iter) =>
      iter.zipWithIndex.map { case (item, i) =>
        (item, i * n + k)
      }
    }
  }

  /**
   * Take the first num elements of the RDD. It works by first scanning one partition, and use the
   * results from that partition to estimate the number of additional partitions needed to satisfy
   * the limit.
   *
   * @note due to complications in the internal implementation, this method will raise
   * an exception if called on an RDD of `Nothing` or `Null`.
    *
    * 从RDD中取num个元素，算法策略是：
    *
    * 1. 首先扫描一个分区，如果分区中的数据足够，那么就用它作为结果
    * 2. 如果一个分区中的分区数据不够，那么num/分区数得到大约需要几个分区的数据(这是基于分区间数据比较均匀的情况)
    *
    * 极端情况是个所有的分区数据都不够
    *
    * @param num
    * @return
    */
  def take(num: Int): Array[T] = withScope {
    if (num == 0) {
      new Array[T](0)
    } else {
      //buf用于存放num个结果数据，是一个ArrayBuffer，可以动态添加数据
      val buf = new ArrayBuffer[T]

      //totalParts表示RDD的总分区个数
      val totalParts = this.partitions.length

      //partsScanned表示已经扫描的记录数
      var partsScanned = 0

      /***
        * 循环条件：
        * 1. 取出的元素个数少于num
        * 2. 扫描的分区数小于总分区数
        */
      while (buf.size < num && partsScanned < totalParts) {
        // The number of partitions to try in this iteration. It is ok for this number to be
        // greater than totalParts because we actually cap it at totalParts in runJob.

        var numPartsToTry = 1L

        //第一遍遍历时，扫描的分区数0，因此不执行该if条件判断
        if (partsScanned > 0) {
          // If we didn't find any rows after the previous iteration, quadruple and retry.
          // Otherwise, interpolate the number of partitions we need to try, but overestimate
          // it by 50%. We also cap the estimation in the end.
          if (buf.size == 0) {
            numPartsToTry = partsScanned * 4
          } else {
            // the left side of max is >=1 whenever partsScanned >= 2
            numPartsToTry = Math.max((1.5 * num * partsScanned / buf.size).toInt - partsScanned, 1)
            numPartsToTry = Math.min(numPartsToTry, partsScanned * 4)
          }
        }

        //需要读取的数据量，放到left中
        val left = num - buf.size

        //当numPartsToToTry=1,partsScanned=0是，那么p是Seq(0)
        val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
        val res = sc.runJob(this, (it: Iterator[T]) => it.take(left).toArray, p)

        res.foreach(buf ++= _.take(num - buf.size))
        partsScanned += p.size
      }

      buf.toArray
    }
  }

  /**
   * Return the first element in this RDD.
   * first操作调用的是top(1)操作
   */
  def first(): T = withScope {
    take(1) match {
      case Array(t) => t
      case _ => throw new UnsupportedOperationException("empty collection")
    }
  }

  /**
   * Returns the top k (largest) elements from this RDD as defined by the specified
   * implicit Ordering[T] and maintains the ordering. This does the opposite of
   * [[takeOrdered]]. For example:
   * {{{
   *   sc.parallelize(Seq(10, 4, 2, 12, 3)).top(1)
   *   // returns Array(12)
   *
   *   sc.parallelize(Seq(2, 3, 4, 5, 6)).top(2)
   *   // returns Array(6, 5)
   * }}}
   *
   * @param num k, the number of top elements to return
   * @param ord the implicit ordering for T
   * @return an array of top elements
   */
  def top(num: Int)(implicit ord: Ordering[T]): Array[T] = withScope {
    takeOrdered(num)(ord.reverse)
  }

  /**
   * Returns the first k (smallest) elements from this RDD as defined by the specified
   * implicit Ordering[T] and maintains the ordering. This does the opposite of [[top]].
   * For example:
   * {{{
   *   sc.parallelize(Seq(10, 4, 2, 12, 3)).takeOrdered(1)
   *   // returns Array(2)
   *
   *   sc.parallelize(Seq(2, 3, 4, 5, 6)).takeOrdered(2)
   *   // returns Array(2, 3)
   * }}}
   *
   * @param num k, the number of elements to return
   * @param ord the implicit ordering for T
   * @return an array of top elements
   */
  def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T] = withScope {
    if (num == 0) {
      Array.empty
    } else {
      val mapRDDs = mapPartitions { items =>
        // Priority keeps the largest elements, so let's reverse the ordering.
        val queue = new BoundedPriorityQueue[T](num)(ord.reverse)
        queue ++= util.collection.Utils.takeOrdered(items, num)(ord)

        /***
          * queue构造得到的Iterator
          */
        Iterator.single(queue)
      }

      /***
        * 如果没有元素，返回空数组
        */
      if (mapRDDs.partitions.length == 0) {
        Array.empty
      }

      /***
        * mapRDD是n个BoundedPriorityQueue，每个队列是num个元素，
        * 通过reduce操作将n*num个元素合并成top num个元素，然后调用队列的toArray方法返回队列的元素(返回的队列的元素是不保证排序的，因此需要执行sorted操作)
        *
        */
      else {
        val x = mapRDDs.reduce { (queue1, queue2) =>
          queue1 ++= queue2
          queue1
        }
        x.toArray.sorted(ord)
      }
    }
  }

  /**
   * Returns the max of this RDD as defined by the implicit Ordering[T].
   * @return the maximum element of the RDD
   * */
  def max()(implicit ord: Ordering[T]): T = withScope {
    /**
      * 调用reduce action，　reduce　action的参数是Ordering.max
      */
    this.reduce(ord.max)
  }

  /**
   * Returns the min of this RDD as defined by the implicit Ordering[T].
   * @return the minimum element of the RDD
   * */
  def min()(implicit ord: Ordering[T]): T = withScope {
    this.reduce(ord.min)
  }

  /**
   * @note due to complications in the internal implementation, this method will raise an
   * exception if called on an RDD of `Nothing` or `Null`. This may be come up in practice
   * because, for example, the type of `parallelize(Seq())` is `RDD[Nothing]`.
   * (`parallelize(Seq())` should be avoided anyway in favor of `parallelize(Seq[T]())`.)
   * @return true if and only if the RDD contains no elements at all. Note that an RDD
   *         may be empty even when it has at least 1 partition.
   */
  def isEmpty(): Boolean = withScope {
    partitions.length == 0 || take(1).length == 0
  }

  /**
   * Save this RDD as a text file, using string representations of elements.
   */
  def saveAsTextFile(path: String): Unit = withScope {
    // https://issues.apache.org/jira/browse/SPARK-2075
    //
    // NullWritable is a `Comparable` in Hadoop 1.+, so the compiler cannot find an implicit
    // Ordering for it and will use the default `null`. However, it's a `Comparable[NullWritable]`
    // in Hadoop 2.+, so the compiler will call the implicit `Ordering.ordered` method to create an
    // Ordering for `NullWritable`. That's why the compiler will generate different anonymous
    // classes for `saveAsTextFile` in Hadoop 1.+ and Hadoop 2.+.
    //
    // Therefore, here we provide an explicit Ordering `null` to make sure the compiler generate
    // same bytecodes for `saveAsTextFile`.
    val nullWritableClassTag = implicitly[ClassTag[NullWritable]]
    val textClassTag = implicitly[ClassTag[Text]]
    val r = this.mapPartitions { iter =>
      val text = new Text()
      iter.map { x =>
        text.set(x.toString)
        (NullWritable.get(), text)
      }
    }
    RDD.rddToPairRDDFunctions(r)(nullWritableClassTag, textClassTag, null)
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
  }

  /**
   * Save this RDD as a compressed text file, using string representations of elements.
   */
  def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Unit = withScope {
    // https://issues.apache.org/jira/browse/SPARK-2075
    val nullWritableClassTag = implicitly[ClassTag[NullWritable]]
    val textClassTag = implicitly[ClassTag[Text]]
    val r = this.mapPartitions { iter =>
      val text = new Text()
      iter.map { x =>
        text.set(x.toString)
        (NullWritable.get(), text)
      }
    }
    RDD.rddToPairRDDFunctions(r)(nullWritableClassTag, textClassTag, null)
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path, codec)
  }

  /**
   * Save this RDD as a SequenceFile of serialized objects.
   *
   *
   * saveAsObjectFile是将RDD中的元素(对象)进行序列化保存到文件中
   * 这个文件的格式是SequenceFile,SequenceFile的格式是K,V类型，K是NullWritable，V是BytesWritable, BytesWritable包装了对象序列化的字节
   * saveAsSequenceFile依然是RDD的方法，不过这个RDD的K，V的K和V必须都是继承自Writable接口的
   *
   * 这个是定义在SequenceFileRDDFunction中的
   *
   *
   * Utils.serialize方法是使用的Java内置的序列化方法
   *
   *
   * saveAsObjectFile方法对应SparkContext的objectFile方法
   *
   */
  def saveAsObjectFile(path: String): Unit = withScope {
    this.mapPartitions(iter => iter.grouped(10).map(_.toArray))
      .map(x => (NullWritable.get(), new BytesWritable(Utils.serialize(x))))
      .saveAsSequenceFile(path)
  }

  /**
   * Creates tuples of the elements in this RDD by applying `f`.
    *
    * 将RDD[T]转换为RDD[K,T]，通过函数f将T类型的元素转换为K类型的元素
    *
    *
    * @param f
    * @tparam K
    * @return
    */
  def keyBy[K](f: T => K): RDD[(K, T)] = withScope {
    val cleanedF = sc.clean(f)
    map(x => (cleanedF(x), x))
  }

  /** A private method for tests, to look at the contents of each partition */
  private[spark] def collectPartitions(): Array[Array[T]] = withScope {
    sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
  }

  /**
   * Mark this RDD for checkpointing. It will be saved to a file inside the checkpoint
   * directory set with `SparkContext#setCheckpointDir` and all references to its parent
   * RDDs will be removed. This function must be called before any job has been
   * executed on this RDD. It is strongly recommended that this RDD is persisted in
   * memory, otherwise saving it on a file will require recomputation.
    *
    * 将该RDD的checkpoint状态置为ReliableRDDCheckpointData.当action触发RDD的compute方法执行时，会检查每个RDD所关联的checkpointData变量是否已经定义
    * 如果没有定义，那么表示该RDD没有调用checkpoint
    *
   */
  def checkpoint(): Unit = RDDCheckpointData.synchronized {
    // NOTE: we use a global lock here due to complexities downstream with ensuring
    // children RDD partitions point to the correct parent partitions. In the future
    // we should revisit this consideration.
    //调用RDD的checkpoint必须通过SparkContext设置Checkpoint Directory
    if (context.checkpointDir.isEmpty) {
      throw new SparkException("Checkpoint directory has not been set in the SparkContext")
    }

    //对一个RDD可以调用多次checkpoint，只有第一次有效
    //创建checkpointData实例，它是ReliableRDDCheckpointData类型的对象，localcheckpoint方法创建的是LocalRDDCheckpointData
    //ReliableRDDCheckpointData有一个成员变量cpState,构造时的初始值是Initialized
    else if (checkpointData.isEmpty) {
      checkpointData = Some(new ReliableRDDCheckpointData(this))
    }
  }

  /**
   * Mark this RDD for local checkpointing using Spark's existing caching layer.
   *
   * This method is for users who wish to truncate RDD lineages while skipping the expensive
   * step of replicating the materialized data in a reliable distributed file system. This is
   * useful for RDDs with long lineages that need to be truncated periodically (e.g. GraphX).
   *
   * Local checkpointing sacrifices fault-tolerance for performance. In particular, checkpointed
   * data is written to ephemeral local storage in the executors instead of to a reliable,
   * fault-tolerant storage. The effect is that if an executor fails during the computation,
   * the checkpointed data may no longer be accessible, causing an irrecoverable job failure.
   *
   * This is NOT safe to use with dynamic allocation, which removes executors along
   * with their cached blocks. If you must use both features, you are advised to set
   * `spark.dynamicAllocation.cachedExecutorIdleTimeout` to a high value.
   *
   * The checkpoint directory set through `SparkContext#setCheckpointDir` is not used.
    *
    *
   */
  def localCheckpoint(): this.type = RDDCheckpointData.synchronized {
    if (conf.getBoolean("spark.dynamicAllocation.enabled", false) &&
        conf.contains("spark.dynamicAllocation.cachedExecutorIdleTimeout")) {
      logWarning("Local checkpointing is NOT safe to use with dynamic allocation, " +
        "which removes executors along with their cached blocks. If you must use both " +
        "features, you are advised to set `spark.dynamicAllocation.cachedExecutorIdleTimeout` " +
        "to a high value. E.g. If you plan to use the RDD for 1 hour, set the timeout to " +
        "at least 1 hour.")
    }

    // Note: At this point we do not actually know whether the user will call persist() on
    // this RDD later, so we must explicitly call it here ourselves to ensure the cached
    // blocks are registered for cleanup later in the SparkContext.
    //
    // If, however, the user has already called persist() on this RDD, then we must adapt
    // the storage level he/she specified to one that is appropriate for local checkpointing
    // (i.e. uses disk) to guarantee correctness.

    if (storageLevel == StorageLevel.NONE) {
      persist(LocalRDDCheckpointData.DEFAULT_STORAGE_LEVEL)
    } else {
      persist(LocalRDDCheckpointData.transformStorageLevel(storageLevel), allowOverride = true)
    }

    // If this RDD is already checkpointed and materialized, its lineage is already truncated.
    // We must not override our `checkpointData` in this case because it is needed to recover
    // the checkpointed data. If it is overridden, next time materializing on this RDD will
    // cause error.
    if (isCheckpointedAndMaterialized) {
      logWarning("Not marking RDD for local checkpoint because it was already " +
        "checkpointed and materialized")
    } else {
      // Lineage is not truncated yet, so just override any existing checkpoint data with ours
      checkpointData match {
        case Some(_: ReliableRDDCheckpointData[_]) => logWarning(
          "RDD was already marked for reliable checkpointing: overriding with local checkpoint.")
        case _ =>
      }
      checkpointData = Some(new LocalRDDCheckpointData(this))
    }
    this
  }

  /**
   * Return whether this RDD is checkpointed and materialized, either reliably or locally.
    *
    * 检查RDD是否已经完成checkpoint了，状态为isCheckpointed为true
    * checkpointData是一个Option，它有exists方法，表示Option是否有数据，并且数据满足某个条件
   */
  def isCheckpointed: Boolean = checkpointData.exists(_.isCheckpointed)

  /**
   * Return whether this RDD is checkpointed and materialized, either reliably or locally.
   * This is introduced as an alias for `isCheckpointed` to clarify the semantics of the
   * return value. Exposed for testing.
   */
  private[spark] def isCheckpointedAndMaterialized: Boolean = isCheckpointed

  /**
   * Return whether this RDD is marked for local checkpointing.
   * Exposed for testing.
   */
  private[rdd] def isLocallyCheckpointed: Boolean = {
    checkpointData match {
      case Some(_: LocalRDDCheckpointData[T]) => true
      case _ => false
    }
  }

  /**
   * Gets the name of the directory to which this RDD was checkpointed.
   * This is not defined if the RDD is checkpointed locally.
   */
  def getCheckpointFile: Option[String] = {
    checkpointData match {
      case Some(reliable: ReliableRDDCheckpointData[T]) => reliable.getCheckpointDir
      case _ => None
    }
  }

  // =======================================================================
  // Other internal methods and fields
  // =======================================================================

  private var storageLevel: StorageLevel = StorageLevel.NONE

  /** User code that created this RDD (e.g. `textFile`, `parallelize`). */
  @transient private[spark] val creationSite = sc.getCallSite()

  /**
   * The scope associated with the operation that created this RDD.
   *
   * This is more flexible than the call site and can be defined hierarchically. For more
   * detail, see the documentation of {{RDDOperationScope}}. This scope is not defined if the
   * user instantiates this RDD himself without using any Spark operations.
   */
  @transient private[spark] val scope: Option[RDDOperationScope] = {
    Option(sc.getLocalProperty(SparkContext.RDD_SCOPE_KEY)).map(RDDOperationScope.fromJson)
  }

  private[spark] def getCreationSite: String = Option(creationSite).map(_.shortForm).getOrElse("")

  private[spark] def elementClassTag: ClassTag[T] = classTag[T]

  private[spark] var checkpointData: Option[RDDCheckpointData[T]] = None

  // Whether to checkpoint all ancestor RDDs that are marked for checkpointing. By default,
  // we stop as soon as we find the first such RDD, an optimization that allows us to write
  // less data but is not safe for all workloads. E.g. in streaming we may checkpoint both
  // an RDD and its parent in every batch, in which case the parent may never be checkpointed
  // and its lineage never truncated, leading to OOMs in the long run (SPARK-6847).
  private val checkpointAllMarkedAncestors =
    Option(sc.getLocalProperty(RDD.CHECKPOINT_ALL_MARKED_ANCESTORS))
      .map(_.toBoolean).getOrElse(false)

  /** Returns the first parent RDD
    * @tparam U
    * @return
    */
  protected[spark] def firstParent[U: ClassTag]: RDD[U] = {
    dependencies.head.rdd.asInstanceOf[RDD[U]]
  }

  /** Returns the jth parent RDD: e.g. rdd.parent[T](0) is equivalent to rdd.firstParent[T] */
  protected[spark] def parent[U: ClassTag](j: Int) = {
    dependencies(j).rdd.asInstanceOf[RDD[U]]
  }

  /** The [[org.apache.spark.SparkContext]] that this RDD was created on. */
  def context: SparkContext = sc

  /**
   * Private API for changing an RDD's ClassTag.
   * Used for internal Java-Scala API compatibility.
   */
  private[spark] def retag(cls: Class[T]): RDD[T] = {
    val classTag: ClassTag[T] = ClassTag.apply(cls)
    this.retag(classTag)
  }

  /**
   * Private API for changing an RDD's ClassTag.
   * Used for internal Java-Scala API compatibility.
   */
  private[spark] def retag(implicit classTag: ClassTag[T]): RDD[T] = {
    this.mapPartitions(identity, preservesPartitioning = true)(classTag)
  }

  // Avoid handling doCheckpoint multiple times to prevent excessive recursion
  @transient private var doCheckpointCalled = false

  /**
   * Performs the checkpointing of this RDD by saving this. It is called after a job using this RDD
   * has completed (therefore the RDD has been materialized and potentially stored in memory).
   * doCheckpoint() is called recursively on the parent RDDs.
    *
    * 对RDD进行checkpoint操作，checkpoint的结果是RDD Lineage被打断
   */
  private[spark] def doCheckpoint(): Unit = {
    RDDOperationScope.withScope(sc, "checkpoint", allowNesting = false, ignoreParent = true) {
      if (!doCheckpointCalled) {
        doCheckpointCalled = true
        if (checkpointData.isDefined) {
          if (checkpointAllMarkedAncestors) {
            // TODO We can collect all the RDDs that needs to be checkpointed, and then checkpoint
            // them in parallel.
            // Checkpoint parents first because our lineage will be truncated after we checkpoint ourselves
            dependencies.foreach(_.rdd.doCheckpoint())
          }
          checkpointData.get.checkpoint()
        } else {
          dependencies.foreach(_.rdd.doCheckpoint())
        }
      }
    }
  }

  /**
   * Changes the dependencies of this RDD from its original parents to a new RDD (`newRDD`)
   * created from the checkpoint file, and forget its old dependencies and partitions.
   */
  private[spark] def markCheckpointed(): Unit = {
    clearDependencies()
    partitions_ = null
    deps = null    // Forget the constructor argument for dependencies too
  }

  /**
   * Clears the dependencies of this RDD. This method must ensure that all references
   * to the original parent RDDs is removed to enable the parent RDDs to be garbage
   * collected. Subclasses of RDD may override this method for implementing their own cleaning
   * logic. See [[org.apache.spark.rdd.UnionRDD]] for an example.
   */
  protected def clearDependencies() {
    dependencies_ = null
  }

  /** A description of this RDD and its recursive dependencies for debugging. */
  def toDebugString: String = {
    // Get a debug description of an rdd without its children
    def debugSelf(rdd: RDD[_]): Seq[String] = {
      import Utils.bytesToString

      val persistence = if (storageLevel != StorageLevel.NONE) storageLevel.description else ""
      val storageInfo = rdd.context.getRDDStorageInfo(_.id == rdd.id).map(info =>
        "    CachedPartitions: %d; MemorySize: %s; ExternalBlockStoreSize: %s; DiskSize: %s".format(
          info.numCachedPartitions, bytesToString(info.memSize),
          bytesToString(info.externalBlockStoreSize), bytesToString(info.diskSize)))

      s"$rdd [$persistence]" +: storageInfo
    }

    // Apply a different rule to the last child
    def debugChildren(rdd: RDD[_], prefix: String): Seq[String] = {
      val len = rdd.dependencies.length
      len match {
        case 0 => Seq.empty
        case 1 =>
          val d = rdd.dependencies.head
          debugString(d.rdd, prefix, d.isInstanceOf[ShuffleDependency[_, _, _]], true)
        case _ =>
          val frontDeps = rdd.dependencies.take(len - 1)
          val frontDepStrings = frontDeps.flatMap(
            d => debugString(d.rdd, prefix, d.isInstanceOf[ShuffleDependency[_, _, _]]))

          val lastDep = rdd.dependencies.last
          val lastDepStrings =
            debugString(lastDep.rdd, prefix, lastDep.isInstanceOf[ShuffleDependency[_, _, _]], true)

          (frontDepStrings ++ lastDepStrings)
      }
    }
    // The first RDD in the dependency stack has no parents, so no need for a +-
    def firstDebugString(rdd: RDD[_]): Seq[String] = {
      val partitionStr = "(" + rdd.partitions.length + ")"
      val leftOffset = (partitionStr.length - 1) / 2
      val nextPrefix = (" " * leftOffset) + "|" + (" " * (partitionStr.length - leftOffset))

      debugSelf(rdd).zipWithIndex.map{
        case (desc: String, 0) => s"$partitionStr $desc"
        case (desc: String, _) => s"$nextPrefix $desc"
      } ++ debugChildren(rdd, nextPrefix)
    }
    def shuffleDebugString(rdd: RDD[_], prefix: String = "", isLastChild: Boolean): Seq[String] = {
      val partitionStr = "(" + rdd.partitions.length + ")"
      val leftOffset = (partitionStr.length - 1) / 2
      val thisPrefix = prefix.replaceAll("\\|\\s+$", "")
      val nextPrefix = (
        thisPrefix
        + (if (isLastChild) "  " else "| ")
        + (" " * leftOffset) + "|" + (" " * (partitionStr.length - leftOffset)))

      debugSelf(rdd).zipWithIndex.map{
        case (desc: String, 0) => s"$thisPrefix+-$partitionStr $desc"
        case (desc: String, _) => s"$nextPrefix$desc"
      } ++ debugChildren(rdd, nextPrefix)
    }
    def debugString(
        rdd: RDD[_],
        prefix: String = "",
        isShuffle: Boolean = true,
        isLastChild: Boolean = false): Seq[String] = {
      if (isShuffle) {
        shuffleDebugString(rdd, prefix, isLastChild)
      } else {
        debugSelf(rdd).map(prefix + _) ++ debugChildren(rdd, prefix)
      }
    }
    firstDebugString(this).mkString("\n")
  }

  override def toString: String = "%s%s[%d] at %s".format(
    Option(name).map(_ + " ").getOrElse(""), getClass.getSimpleName, id, getCreationSite)

  def toJavaRDD() : JavaRDD[T] = {
    new JavaRDD(this)(elementClassTag)
  }
}


/**
 * Defines implicit functions that provide extra functionalities on RDDs of specific types.
 *
 * For example, [[RDD.rddToPairRDDFunctions]] converts an RDD into a [[PairRDDFunctions]] for
 * key-value-pair RDDs, and enabling extra functionalities such as [[PairRDDFunctions.reduceByKey]].
 */
object RDD {

  private[spark] val CHECKPOINT_ALL_MARKED_ANCESTORS =
    "spark.checkpoint.checkpointAllMarkedAncestors"

  // The following implicit functions were in SparkContext before 1.3 and users had to
  // `import SparkContext._` to enable them. Now we move them here to make the compiler find
  // them automatically. However, we still keep the old functions in SparkContext for backward
  // compatibility and forward to the following functions directly.

  implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    new PairRDDFunctions(rdd)
  }

  implicit def rddToAsyncRDDActions[T: ClassTag](rdd: RDD[T]): AsyncRDDActions[T] = {
    new AsyncRDDActions(rdd)
  }

  implicit def rddToSequenceFileRDDFunctions[K, V](rdd: RDD[(K, V)])
      (implicit kt: ClassTag[K], vt: ClassTag[V],
                keyWritableFactory: WritableFactory[K],
                valueWritableFactory: WritableFactory[V])
    : SequenceFileRDDFunctions[K, V] = {
    implicit val keyConverter = keyWritableFactory.convert
    implicit val valueConverter = valueWritableFactory.convert
    new SequenceFileRDDFunctions(rdd,
      keyWritableFactory.writableClass(kt), valueWritableFactory.writableClass(vt))
  }

  implicit def rddToOrderedRDDFunctions[K : Ordering : ClassTag, V: ClassTag](rdd: RDD[(K, V)])
    : OrderedRDDFunctions[K, V, (K, V)] = {
    new OrderedRDDFunctions[K, V, (K, V)](rdd)
  }

  implicit def doubleRDDToDoubleRDDFunctions(rdd: RDD[Double]): DoubleRDDFunctions = {
    new DoubleRDDFunctions(rdd)
  }

  implicit def numericRDDToDoubleRDDFunctions[T](rdd: RDD[T])(implicit num: Numeric[T])
    : DoubleRDDFunctions = {
    new DoubleRDDFunctions(rdd.map(x => num.toDouble(x)))
  }
}
