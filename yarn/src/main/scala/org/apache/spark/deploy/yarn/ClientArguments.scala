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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil._
import org.apache.spark.util.{IntParam, MemoryParam, Utils}

/** *
  * TODO: Add code and support for ensuring that yarn resource 'tasks' are location aware !
  *
  * ClientArguments支持哪些参数(的解析)
  * @param args
  * @param sparkConf
  */
private[spark] class ClientArguments(args: Array[String], sparkConf: SparkConf) {
  var addJars: String = null
  var files: String = null
  var archives: String = null
  var userJar: String = null
  var userClass: String = null
  var pyFiles: Seq[String] = Nil
  var primaryPyFile: String = null
  var primaryRFile: String = null

  /** *
    * userArgs是一个ArrayBuffer，也就是说，命令行上可以出现多个--arg参数
    */
  var userArgs: ArrayBuffer[String] = new ArrayBuffer[String]()

  /**
   * 默认的executor memory是1024M
   */
  var executorMemory = 1024 // MB

  /** *
    * 默认每个Executor使用1个Core
    */
  var executorCores = 1

  /** *
    * 默认使用的Executor数目是2
    */
  var numExecutors = DEFAULT_NUMBER_EXECUTORS

  /** *
    * 任务提交的队列
    */
  var amQueue = sparkConf.get("spark.yarn.queue", "default")

  /**
   * YARN Cluster模式下，AM默认的内存是512M，如果指定了driver-memory则覆盖这个这个值。
   * amMemory是个var，在代码逻辑中有对它进行修改的处理逻辑
   */
  var amMemory: Int = 512 // MB
  var amCores: Int = 1
  var appName: String = "Spark"
  var priority = 0
  var principal: String = null
  var keytab: String = null

  /** *
    *  判断是否是YARN Cluster模式的方法是判断userClass是否为null，对于Yarn Cluster模式那么userClass不为空；
    * @return
    */
  def isClusterMode: Boolean = userClass != null

  private var driverMemory: Int = Utils.DEFAULT_DRIVER_MEM_MB // MB
  private var driverCores: Int = 1

  /** *
    * driver memory在yarn上的overhead
    */
  private val driverMemOverheadKey = "spark.yarn.driver.memoryOverhead"
  private val amMemKey = "spark.yarn.am.memory"

  /** *
    *  application master在yarn上的overhead
    */
  private val amMemOverheadKey = "spark.yarn.am.memoryOverhead"


  private val driverCoresKey = "spark.driver.cores"
  private val amCoresKey = "spark.yarn.am.cores"

  /**
   * 是否启用Executor动态分配功能
   */
  private val isDynamicAllocationEnabled = Utils.isDynamicAllocationEnabled(sparkConf)


  /** *
    * 在主构造器中调用parseArgs, loadEnvironmentArgs以及validateArgs
    */
  parseArgs(args.toList)
  loadEnvironmentArgs()
  validateArgs()

  // Additional memory to allocate to containers
  //如果是YARN-Cluster，那么使用的是driverMemOverheadKey；如果是YARN-Client，那么使用的是amMemOverheadKey
  val amMemoryOverheadConf = if (isClusterMode) driverMemOverheadKey else amMemOverheadKey

  /** *
    * AM的内存Overhead的计算算法：
    *
    * 1. 首先取overhead配置项的Key，在Yarn-Cluster模式下是spark.yarn.driver.memoryOverhead， 在Yarn-Client模式下
    * 是spark.yarn.am.memoryOverhead，如果定义了这个配置就取它的值。
    *
    * 如果没有定义，那么取MEMORY_OVERHEAD_MIN和(MEMORY_OVERHEAD_FACTOR * amMemory).toInt的最大值
    *
    * MEMORY_OVERHEAD_MIN是常量384M，也就是说，默认情况下，amMemoryOverhead不少于384M，而最大上限是ApplicationMaster的内存值*0.1
    * 比如，如果Application Master的内存指定了8G，那么ApplicationManager的overhead 内存就是800M
    *
    */
  val amMemoryOverhead = sparkConf.getInt(amMemoryOverheadConf,
    math.max((MEMORY_OVERHEAD_FACTOR * amMemory).toInt, MEMORY_OVERHEAD_MIN))

  /**
   * 为Executor分配的最小MEMORY_OVERHEAD_MIN也是384M
   */
  val executorMemoryOverhead = sparkConf.getInt("spark.yarn.executor.memoryOverhead",
    math.max((MEMORY_OVERHEAD_FACTOR * executorMemory).toInt, MEMORY_OVERHEAD_MIN))

  /** Load any default arguments provided through environment variables and Spark properties. */
  private def loadEnvironmentArgs(): Unit = {
    // For backward compatibility, SPARK_YARN_DIST_{ARCHIVES/FILES} should be resolved to hdfs://,
    // while spark.yarn.dist.{archives/files} should be resolved to file:// (SPARK-2051).
    files = Option(files)
      .orElse(sparkConf.getOption("spark.yarn.dist.files").map(p => Utils.resolveURIs(p)))
      .orElse(sys.env.get("SPARK_YARN_DIST_FILES"))
      .orNull
    archives = Option(archives)
      .orElse(sparkConf.getOption("spark.yarn.dist.archives").map(p => Utils.resolveURIs(p)))
      .orElse(sys.env.get("SPARK_YARN_DIST_ARCHIVES"))
      .orNull
    // If dynamic allocation is enabled, start at the configured initial number of executors.
    // Default to minExecutors if no initialExecutors is set.
    numExecutors = YarnSparkHadoopUtil.getInitialTargetExecutorNumber(sparkConf, numExecutors)
    principal = Option(principal)
      .orElse(sparkConf.getOption("spark.yarn.principal"))
      .orNull
    keytab = Option(keytab)
      .orElse(sparkConf.getOption("spark.yarn.keytab"))
      .orNull
  }

  /**
   * Fail fast if any arguments provided are invalid.
   * This is intended to be called only after the provided arguments have been parsed.
   */
  private def validateArgs(): Unit = {
    if (numExecutors < 0 || (!isDynamicAllocationEnabled && numExecutors == 0)) {
      throw new IllegalArgumentException(
        s"""
           |Number of executors was $numExecutors, but must be at least 1
           |(or 0 if dynamic executor allocation is enabled).
           |${getUsageMessage()}
         """.stripMargin)
    }
    if (executorCores < sparkConf.getInt("spark.task.cpus", 1)) {
      throw new SparkException("Executor cores must not be less than " +
        "spark.task.cpus.")
    }
    // scalastyle:off println
    if (isClusterMode) {
      for (key <- Seq(amMemKey, amMemOverheadKey, amCoresKey)) {
        if (sparkConf.contains(key)) {
          println(s"$key is set but does not apply in cluster mode.")
        }
      }

      /** *
        * 在集群模式下，amMemory和driverMemory是一样的
        * 在集群模式下，amCores和driverCores是一样的，都来自于--driver-cores或者spark.driver.cores配置项
        */
      amMemory = driverMemory
      amCores = driverCores
    } else {
      for (key <- Seq(driverMemOverheadKey, driverCoresKey)) {
        if (sparkConf.contains(key)) {
          println(s"$key is set but does not apply in client mode.")
        }
      }
      sparkConf.getOption(amMemKey)
        .map(Utils.memoryStringToMb)
        .foreach { mem => amMemory = mem }
      sparkConf.getOption(amCoresKey)
        .map(_.toInt)
        .foreach { cores => amCores = cores }
    }
    // scalastyle:on println
  }

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    // scalastyle:off println
    while (!args.isEmpty) {
      args match {
        /***
          * 包含用户代码的jar包
          */
        case ("--jar") :: value :: tail =>
          userJar = value
          args = tail

        case ("--class") :: value :: tail =>
          userClass = value
          args = tail

        case ("--primary-py-file") :: value :: tail =>
          primaryPyFile = value
          args = tail

        case ("--primary-r-file") :: value :: tail =>
          primaryRFile = value
          args = tail

        /**
          *
          */
        case ("--args" | "--arg") :: value :: tail =>
          if (args(0) == "--args") {
            println("--args is deprecated. Use --arg instead.")
          }
          userArgs += value
          args = tail

        case ("--master-class" | "--am-class") :: value :: tail =>
          println(s"${args(0)} is deprecated and is not used anymore.")
          args = tail

        case ("--master-memory" | "--driver-memory") :: MemoryParam(value) :: tail =>
          if (args(0) == "--master-memory") {
            println("--master-memory is deprecated. Use --driver-memory instead.")
          }
          driverMemory = value
          args = tail

        case ("--driver-cores") :: IntParam(value) :: tail =>
          driverCores = value
          args = tail

        case ("--num-workers" | "--num-executors") :: IntParam(value) :: tail =>
          if (args(0) == "--num-workers") {
            println("--num-workers is deprecated. Use --num-executors instead.")
          }
          numExecutors = value
          args = tail

        case ("--worker-memory" | "--executor-memory") :: MemoryParam(value) :: tail =>
          if (args(0) == "--worker-memory") {
            println("--worker-memory is deprecated. Use --executor-memory instead.")
          }
          executorMemory = value
          args = tail

        case ("--worker-cores" | "--executor-cores") :: IntParam(value) :: tail =>
          if (args(0) == "--worker-cores") {
            println("--worker-cores is deprecated. Use --executor-cores instead.")
          }
          executorCores = value
          args = tail

        case ("--queue") :: value :: tail =>
          amQueue = value
          args = tail

        case ("--name") :: value :: tail =>
          appName = value
          args = tail

        case ("--addJars") :: value :: tail =>
          addJars = value
          args = tail

        case ("--py-files") :: value :: tail =>
          pyFiles = value.split(",")
          args = tail

        case ("--files") :: value :: tail =>
          files = value
          args = tail

        case ("--archives") :: value :: tail =>
          archives = value
          args = tail

        case ("--principal") :: value :: tail =>
          principal = value
          args = tail

        case ("--keytab") :: value :: tail =>
          keytab = value
          args = tail

        case Nil =>

        case _ =>
          throw new IllegalArgumentException(getUsageMessage(args))
      }
    }
    // scalastyle:on println

    if (primaryPyFile != null && primaryRFile != null) {
      throw new IllegalArgumentException("Cannot have primary-py-file and primary-r-file" +
        " at the same time")
    }
  }

  private def getUsageMessage(unknownParam: List[String] = null): String = {
    val message = if (unknownParam != null) s"Unknown/unsupported param $unknownParam\n" else ""
    val mem_mb = Utils.DEFAULT_DRIVER_MEM_MB
    message +
      s"""
      |Usage: org.apache.spark.deploy.yarn.Client [options]
      |Options:
      |  --jar JAR_PATH           Path to your application's JAR file (required in yarn-cluster
      |                           mode)
      |  --class CLASS_NAME       Name of your application's main class (required)
      |  --primary-py-file        A main Python file
      |  --primary-r-file         A main R file
      |  --arg ARG                Argument to be passed to your application's main class.
      |                           Multiple invocations are possible, each will be passed in order.
      |  --num-executors NUM      Number of executors to start (Default: 2)
      |  --executor-cores NUM     Number of cores per executor (Default: 1).
      |  --driver-memory MEM      Memory for driver (e.g. 1000M, 2G) (Default: $mem_mb Mb)
      |  --driver-cores NUM       Number of cores used by the driver (Default: 1).
      |  --executor-memory MEM    Memory per executor (e.g. 1000M, 2G) (Default: 1G)
      |  --name NAME              The name of your application (Default: Spark)
      |  --queue QUEUE            The hadoop queue to use for allocation requests (Default:
      |                           'default')
      |  --addJars jars           Comma separated list of local jars that want SparkContext.addJar
      |                           to work with.
      |  --py-files PY_FILES      Comma-separated list of .zip, .egg, or .py files to
      |                           place on the PYTHONPATH for Python apps.
      |  --files files            Comma separated list of files to be distributed with the job.
      |  --archives archives      Comma separated list of archives to be distributed with the job.
      """.stripMargin
  }
}
