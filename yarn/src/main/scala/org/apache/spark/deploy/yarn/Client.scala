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

import java.io.{ByteArrayInputStream, DataInputStream, File, FileOutputStream, IOException,
  OutputStreamWriter}
import java.net.{InetAddress, UnknownHostException, URI, URISyntaxException}
import java.nio.ByteBuffer
import java.security.PrivilegedExceptionAction
import java.util.{Properties, UUID}
import java.util.zip.{ZipEntry, ZipOutputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, ListBuffer, Map}
import scala.reflect.runtime.universe
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import com.google.common.base.Charsets.UTF_8
import com.google.common.base.Objects
import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.io.{DataOutputBuffer, Text}
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hadoop.util.StringUtils
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{YarnClient, YarnClientApplication}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException
import org.apache.hadoop.yarn.util.Records

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkContext, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.launcher.{LauncherBackend, SparkAppHandle, YarnCommandBuilderUtils}
import org.apache.spark.util.Utils

/**
 *  SPARK YARN CLUSTER模式下运行在提交作业进程(SparkSubmit)中用于向YARN集群提交作业，
 *  object Client的main函数执行提交作业的逻辑，这个代码逻辑在SparkSubmit中
 *
 *  问题：
 * @param args ClientArgument
 * @param hadoopConf Hadoop Configuration，
 * @param sparkConf Spark Configuration
 */
private[spark] class Client(
    val args: ClientArguments,
    val hadoopConf: Configuration, /**Hadoop Configuration是从配置文件中读取的？*/
    val sparkConf: SparkConf)
  extends Logging {

  import Client._

  /**
   * 通过SparkHadoopUtil.get.newConfiguration(spConf)获取Hadoop Configuration
   * @param clientArgs
   * @param spConf
   * @return
   */
  def this(clientArgs: ClientArguments, spConf: SparkConf) =
    this(clientArgs, SparkHadoopUtil.get.newConfiguration(spConf), spConf)

  /**
   * 访问RM的Client, 这是YARN API，通过Hadoop YARN API YarnClient创建。
   * YARNClient，顾名思义，是访问YARN RPC Server的YARN RPC Client
   */
  private val yarnClient = YarnClient.createYarnClient

  /**
   * YarnConfiguration
   */
  private val yarnConf = new YarnConfiguration(hadoopConf)
  private var credentials: Credentials = null

  /** *
    * amMemoryOverhead是一个val，也就是说它的值是不会发生变化的，只有这一个赋值的地方
    */
  private val amMemoryOverhead = args.amMemoryOverhead // MB
  private val executorMemoryOverhead = args.executorMemoryOverhead // MB
  private val distCacheMgr = new ClientDistributedCacheManager()
  private val isClusterMode = args.isClusterMode

  private var loginFromKeytab = false
  private var principal: String = null
  private var keytab: String = null

  /** *
    * 创建LauncherBackend对象
    */
  private val launcherBackend = new LauncherBackend() {
    override def onStopRequest(): Unit = {
      if (isClusterMode && appId != null) {
        yarnClient.killApplication(appId)
      } else {
        setState(SparkAppHandle.State.KILLED)
        stop()
      }
    }
  }

  /** *
    * Client进程是否在Application启动后立即退出，判定条件是
    * 1. 在Cluster模式下，
    * 2. 需要将配置项spark.yarn.submit.waitAppCompletion配置为false(表示不等待Application运行结束)，
    * spark.yarn.submit.waitAppCompletion默认配置为true，
    * 所以，fireAndForget返回false，表示Client进程等待Application进程运行结束才推出
    */
  private val fireAndForget = isClusterMode && !sparkConf.getBoolean("spark.yarn.submit.waitAppCompletion", true)

  /** *
    * 该Application的ID，类型为ApplicationId
    *
    */
  private var appId: ApplicationId = null

  def reportLauncherState(state: SparkAppHandle.State): Unit = {
    launcherBackend.setState(state)
  }

  def stop(): Unit = {
    launcherBackend.close()
    yarnClient.stop()
    // Unset YARN mode system env variable, to allow switching between cluster types.
    System.clearProperty("SPARK_YARN_MODE")
  }

  /**
   * Submit an application running our ApplicationMaster to the ResourceManager.
   *
   * The stable Yarn API provides a convenience method (YarnClient#createApplication) for
   * creating applications and setting up the application submission context. This was not
   * available in the alpha API.
   */
  def submitApplication(): ApplicationId = {
    var appId: ApplicationId = null
    try {
      launcherBackend.connect()

      /** *
        *  Setup the credentials before doing anything else,so we have don't have issues at any point.
        *  Kerberos认证相关的操作
        */
      setupCredentials()

      /**
       * 调用init和start方法初始化YarnClient，使YarnClient可用, Yarn中定义的很多Client API都是这种思路，首先
       * 使用某个配置初始化，然后调用start方法使该Client可用
       */
      yarnClient.init(yarnConf)
      yarnClient.start()

      /** *
        * 打印当前YARN集群中有多少个NodeManager，YARN是ResourceManager + 各个NodeManager的主从架构的分布式系统
        */
      logInfo("Requesting a new application from cluster with %d NodeManagers"
        .format(yarnClient.getYarnClusterMetrics.getNumNodeManagers))

      //
      /**
       * Get a new application from our RM
       * 调用YarnClient的createApplication方法创建一个新Application实例，
       *
       * 1. YarnClient.createApplication获取一个YarnClientApplication实例，
       * 2. 该YarnClientApplication示例封装了两方面的信息GetNewApplicationResponse和ApplicationSubmissionContext
       *
       * 问题：调用完YarnClient.createApplication方法后，对Yarn的影响是什么？此时此刻，Application的状态是什么？应该是NEW(参见YarnApplicationStage)
       */
      val newApp = yarnClient.createApplication()

      /**
       * 获得GetNewApplicationResponse,从GetNewApplicationResponse中可以获取ResourceManager分配给该应用的ApplicationID
       *
       */
      val newAppResponse = newApp.getNewApplicationResponse()
      appId = newAppResponse.getApplicationId()

      /** *
        * 从GetNewApplicationResponse中也可以获取出ResourceManager可供使用的资源的最大容量(包括内存数和内核数)
        */
      val resource = newAppResponse.getMaximumResourceCapability;
      val memory : Int  = resource.getMemory
      val vcores: Int = resource.getVirtualCores

      reportLauncherState(SparkAppHandle.State.SUBMITTED)
      launcherBackend.setAppId(appId.toString())


      //
      /**
       * Verify whether the cluster has enough resources for our AM
       *
       * 检查Yarn集群的***Container***是否有充足的内存分配给AM使用(在YARN-Cluster模式下，分配给AM使用的内存是通过driver-memory指定的
       *
       * 如果集群资源足够如何，如果集群资源不够又当如何？ 内存不足则抛出异常结束Application
       *
       */
      verifyClusterResources(newAppResponse)

      /** *
        * Set up the appropriate contexts to launch our AM
        *  创建启动AM的Container Launch Context, Container LaunchContext应该记录了启动Application Master的脚本
        *  问题：这个脚本怎么分发出去
        */
      val amContainerLaunchContext = createContainerLaunchContext(newAppResponse)

      /**创建提交AM的Context，提交AM需要使用Container Launch Context，因此这里将containerContext传递给ApplicationSubmissionContext*/
      val appContext = createApplicationSubmissionContext(newApp, amContainerLaunchContext)

      // Finally, submit and monitor the application
      logInfo(s"Submitting application ${appId.getId} to ResourceManager")

      /**
       * YarnClient的submitApplication是一个阻塞等待的操作，等待ResourceManager accept该application
       * 代码运行到此处是否意味着ApplicationMaster可以启动了？
       */
      yarnClient.submitApplication(appContext)
      appId
    } catch {
      case e: Throwable =>
        if (appId != null) {
          cleanupStagingDir(appId)
        }
        throw e
    }
  }

  /**
   * Cleanup application staging directory.
   *
   * 清除应用程序的工作目录？
    *
    * @param appId
    */
  private def cleanupStagingDir(appId: ApplicationId): Unit = {
    /** *
      * 根据appId获取application的staging directory， appStagingDir是.sparkStaging + "/" + appId，
      * 也就是说这是一个相对路径
      */
    val appStagingDir = getAppStagingDir(appId)
    try {

      /** *
        * 是否要保留staging files
        */
      val preserveFiles = sparkConf.getBoolean("spark.yarn.preserve.staging.files", false)

      /** *
        * 构造HDFS的Path对象
        */
      val stagingDirPath = new Path(appStagingDir)

      /** *
        * 获取HDFS的FileSystem
        * val fs = stagingDirPath.getFileSystem(hadoopConf)
        */
      val fs = FileSystem.get(hadoopConf)

      /** *
        * 如果不保留并且stagingDirPath存在，那么将stagingDirPath递归删除
        */
      if (!preserveFiles && fs.exists(stagingDirPath)) {
        logInfo("Deleting staging directory " + stagingDirPath)
        //stagingDir本身不应该被删除吧，但是此处貌似被删了
        fs.delete(stagingDirPath, true)
      }
    } catch {
      case ioe: IOException =>
        logWarning("Failed to cleanup staging dir " + appStagingDir, ioe)
    }
  }

  /**
   * Set up the context for submitting our ApplicationMaster.
   * This uses the YarnClientApplication not available in the Yarn alpha API.
    *
    * @param newApp
    * @param amContainerLaunchContext
    * @return
    */
  def createApplicationSubmissionContext(
      newApp: YarnClientApplication,
      amContainerLaunchContext: ContainerLaunchContext): ApplicationSubmissionContext = {
    val appContext = newApp.getApplicationSubmissionContext

    /** *
      * 应用的名称，是用户在SparkSubmit时通过--name提供的应用程序的名字
      */
    appContext.setApplicationName(args.appName)

    /** *
      * 应用运行在YARN调度器中的队列的名字
      */
    appContext.setQueue(args.amQueue)

    /** *
      * ApplicationSubmissionContext需要关联一个启动ApplicationMaster的Container的ContainerLaunchContext
      */
    appContext.setAMContainerSpec(amContainerLaunchContext)

    /** *
      * Application Type，对于Spark应用程序来说，就是SPARK
      */
    appContext.setApplicationType("SPARK")

    /** *
      * spark.yarn.tags
      *Comma-separated list of strings to pass through as YARN application tags appearing  in YARN ApplicationReports,
      * which can be used for filtering when querying YARN.
      */
    sparkConf.getOption(CONF_SPARK_YARN_APPLICATION_TAGS)
      .map(StringUtils.getTrimmedStringCollection(_)) /**_是将方法转换为函数的方式,此处是将逗号分隔的字符串按照逗号切割为数组，数目每个元素做trim*/
      .filter(!_.isEmpty()) /**此处是过滤掉为空的元素*/
      .foreach { tagCollection => /**tagCollection为什么是个Collection[String],因为foreach是Option[Array[String]]的方法而不是Array[String]的方法*/
        try {
          // The setApplicationTags method was only introduced in Hadoop 2.4+, so we need to use
          // reflection to set it, printing a warning if a tag was specified but the YARN version
          // doesn't support it.
          val method = appContext.getClass().getMethod(
            "setApplicationTags", classOf[java.util.Set[String]])
          method.invoke(appContext, new java.util.HashSet[String](tagCollection))
        } catch {
          case e: NoSuchMethodException =>
            logWarning(s"Ignoring $CONF_SPARK_YARN_APPLICATION_TAGS because this version of " +
              "YARN does not support it")
        }
      }

    /** *
      * 设置spark application的尝试次数
      * 问题：默认是多少？
      */
    sparkConf.getOption("spark.yarn.maxAppAttempts").map(_.toInt) match {
      case Some(v) => appContext.setMaxAppAttempts(v)
      case None => logDebug("spark.yarn.maxAppAttempts is not set. " +
          "Cluster's default value will be used.")
    }

    if (sparkConf.contains("spark.yarn.am.attemptFailuresValidityInterval")) {
      try {
        val interval = sparkConf.getTimeAsMs("spark.yarn.am.attemptFailuresValidityInterval")
        val method = appContext.getClass().getMethod(
          "setAttemptFailuresValidityInterval", classOf[Long])
        method.invoke(appContext, interval: java.lang.Long)
      } catch {
        case e: NoSuchMethodException =>
          logWarning("Ignoring spark.yarn.am.attemptFailuresValidityInterval because the version " +
            "of YARN does not support it")
      }
    }

    /** *
      * 通过appContext.setResource()设置capability，包括内存量以及内核数
      * appContext.getResource()可以获取Application Master所需要的内存和内核
      */
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(args.amMemory + amMemoryOverhead)

    /** *
      * 这里设置的VirtualCores这里就是--driver-cores内核数
      */
    capability.setVirtualCores(args.amCores)

    /** *
      * 如果SparkConf中配置了spark.yarn.am.nodeLabelExpression，那么需要对am nodeLabelExpression进行处理
      */
    if (sparkConf.contains("spark.yarn.am.nodeLabelExpression")) {
      try {
        //创建ResourceRequest Record
        val amRequest = Records.newRecord(classOf[ResourceRequest])
        amRequest.setResourceName(ResourceRequest.ANY)
        amRequest.setPriority(Priority.newInstance(0))
        amRequest.setCapability(capability)
        //设置Container的数目
        amRequest.setNumContainers(1)
        val amLabelExpression = sparkConf.get("spark.yarn.am.nodeLabelExpression")

        //通过反射的方式调用amRequest的setNodeLabelExpression以及appContext的setAMContainerResourceRequest方法，目的是
        //为了保持版本兼容性，不会出现编译错误
        val setNodeLabelExpressionMethod = amRequest.getClass.getMethod("setNodeLabelExpression", classOf[String])
        setNodeLabelExpressionMethod.invoke(amRequest, amLabelExpression)

        val setAMContainerResourceRequestMethod =
          appContext.getClass.getMethod("setAMContainerResourceRequest", classOf[ResourceRequest])
        setAMContainerResourceRequestMethod.invoke(appContext, amRequest)
      } catch {
        case e: NoSuchMethodException =>
          logWarning("Ignoring spark.yarn.am.nodeLabelExpression because the version " +
            "of YARN does not support it")

          /** *
            * 通过try/catch试错机制进行版本兼容
            */
          appContext.setResource(capability)
      }
    } else {
      appContext.setResource(capability)
    }

    appContext
  }

  /** Set up security tokens for launching our ApplicationMaster container. */
  private def setupSecurityToken(amContainer: ContainerLaunchContext): Unit = {
    val dob = new DataOutputBuffer
    credentials.writeTokenStorageToStream(dob)
    amContainer.setTokens(ByteBuffer.wrap(dob.getData))
  }

  /** *
    * Get the application report from the ResourceManager for an application we have submitted.
    * YarnClient是Spark提供的API，根据appId获取ApplicationReport
    * @param appId
    * @return
    */
  def getApplicationReport(appId: ApplicationId): ApplicationReport =
    yarnClient.getApplicationReport(appId)

  /**
   * Return the security token used by this client to communicate with the ApplicationMaster.
   * If no security is enabled, the token returned by the report is null.
   */
  private def getClientToken(report: ApplicationReport): String =
    Option(report.getClientToAMToken).map(_.toString).getOrElse("")

  /**
   * Fail fast if we have requested more resources per container than is available in the cluster.
   *
   * 快速验证Yarn集群中的单个Container的内存容量是否能够满足ApplicationMaster和Executor申请的内存量+相应的overhead
   *
   * 注意：快速验证时只关注了内存是否满足，而没有关注内核是否满足
    *
    * @param newAppResponse
    */
  private def verifyClusterResources(newAppResponse: GetNewApplicationResponse): Unit = {

    /**
     * maxMem指的是YARN Container的内存最大值，yarn.scheduler.maximum-allocation-mb的默认值是8G
     */
    val maxMemPerContainer :Int = newAppResponse.getMaximumResourceCapability().getMemory()

    /** *
      * 打印日志，提示当前正在检查应用申请的内存量不超过单个Container的最大内存量
      */
    logInfo("Verifying our application has not requested more than the maximum " +
      s"memory capability of the cluster ($maxMemPerContainer MB per container)")

    /** *
      *  Executor实际使用的内存=指定的Executor内存+Executor内存Overhead
      */
    val executorMem = args.executorMemory + executorMemoryOverhead

    /** *
      * 如果申请的Executor内存超过单个Container的内存最大值，那么抛出异常
      */
    if (executorMem > maxMemPerContainer) {
      throw new IllegalArgumentException(s"Required executor memory (${args.executorMemory}" +
        s"+$executorMemoryOverhead MB) is above the max threshold ($maxMemPerContainer MB) of this cluster! " +
        "Please check the values of 'yarn.scheduler.maximum-allocation-mb' and/or " +
        "'yarn.nodemanager.resource.memory-mb'.")
    }

    /** *
      * 检查ApplicationMaster的内存使用情况
      *
      */
    val amMem = args.amMemory + amMemoryOverhead
    if (amMem > maxMemPerContainer) {
      throw new IllegalArgumentException(s"Required AM memory (${args.amMemory}" +
        s"+$amMemoryOverhead MB) is above the max threshold ($maxMemPerContainer MB) of this cluster! " +
        "Please increase the value of 'yarn.scheduler.maximum-allocation-mb'.")
    }

    /** *
      *  打印日志记录Yarn集群的Container内存容量足以运行ApplicationMaster
      */
    logInfo("Will allocate AM container, with %d MB memory including %d MB overhead".format(
      amMem,
      amMemoryOverhead))

    // We could add checks to make sure the entire cluster has enough resources but that involves
    // getting all the node reports and computing ourselves.
  }

  /**
   * Copy the given file to a remote file system (e.g. HDFS) if needed.
   * The file is only copied if the source and destination file systems are different. This is used
   * for preparing resources for launching the ApplicationMaster container. Exposed for testing.
   */
  private[yarn] def copyFileToRemote(
      destDir: Path,
      srcPath: Path,
      replication: Short): Path = {
    val destFs = destDir.getFileSystem(hadoopConf)
    val srcFs = srcPath.getFileSystem(hadoopConf)
    var destPath = srcPath
    if (!compareFs(srcFs, destFs)) {
      destPath = new Path(destDir, srcPath.getName())
      logInfo(s"Uploading resource $srcPath -> $destPath")
      FileUtil.copy(srcFs, srcPath, destFs, destPath, false, hadoopConf)
      destFs.setReplication(destPath, replication)
      destFs.setPermission(destPath, new FsPermission(APP_FILE_PERMISSION))
    } else {
      logInfo(s"Source and destination file systems are the same. Not copying $srcPath")
    }
    // Resolve any symlinks in the URI path so using a "current" symlink to point to a specific
    // version shows the specific version in the distributed cache configuration
    val qualifiedDestPath = destFs.makeQualified(destPath)
    val fc = FileContext.getFileContext(qualifiedDestPath.toUri(), hadoopConf)
    fc.resolvePath(qualifiedDestPath)
  }

  /**
   * Upload any resources to the distributed cache if needed. If a resource is intended to be
   * consumed locally, set up the appropriate config for downstream code to handle it properly.
   * This is used for setting up a container launch context for our ApplicationMaster.
   * Exposed for testing.
   */
  def prepareLocalResources(
      appStagingDir: String,
      pySparkArchives: Seq[String]): HashMap[String, LocalResource] = {
    logInfo("Preparing resources for our AM container")
    // Upload Spark and the application JAR to the remote file system if necessary,
    // and add them as local resources to the application master.
    val fs = FileSystem.get(hadoopConf)

    /**
      * /user/{user.name}/{appStagingDir}
      */
    val dst = new Path(fs.getHomeDirectory(), appStagingDir)
    val nns = YarnSparkHadoopUtil.get.getNameNodesToAccess(sparkConf) + dst
    YarnSparkHadoopUtil.get.obtainTokensForNamenodes(nns, hadoopConf, credentials)
    // Used to keep track of URIs added to the distributed cache. If the same URI is added
    // multiple times, YARN will fail to launch containers for the app with an internal
    // error.
    val distributedUris = new HashSet[String]
    obtainTokenForHiveMetastore(sparkConf, hadoopConf, credentials)
    obtainTokenForHBase(sparkConf, hadoopConf, credentials)

    val replication = sparkConf.getInt("spark.yarn.submit.file.replication",
      fs.getDefaultReplication(dst)).toShort
    val localResources = HashMap[String, LocalResource]()
    FileSystem.mkdirs(fs, dst, new FsPermission(STAGING_DIR_PERMISSION))

    val statCache: Map[URI, FileStatus] = HashMap[URI, FileStatus]()

    val oldLog4jConf = Option(System.getenv("SPARK_LOG4J_CONF"))
    if (oldLog4jConf.isDefined) {
      logWarning(
        "SPARK_LOG4J_CONF detected in the system environment. This variable has been " +
          "deprecated. Please refer to the \"Launching Spark on YARN\" documentation " +
          "for alternatives.")
    }

    def addDistributedUri(uri: URI): Boolean = {
      val uriStr = uri.toString()
      if (distributedUris.contains(uriStr)) {
        logWarning(s"Resource $uri added multiple times to distributed cache.")
        false
      } else {
        distributedUris += uriStr
        true
      }
    }

    /**
     * Distribute a file to the cluster.
     *
     * If the file's path is a "local:" URI, it's actually not distributed. Other files are copied
     * to HDFS (if not already there) and added to the application's distributed cache.
     *
     * @param path URI of the file to distribute.
     * @param resType Type of resource being distributed.
     * @param destName Name of the file in the distributed cache.
     * @param targetDir Subdirectory where to place the file.
     * @param appMasterOnly Whether to distribute only to the AM.
     * @return A 2-tuple. First item is whether the file is a "local:" URI. Second item is the
     *         localized path for non-local paths, or the input `path` for local paths.
     *         The localized path will be null if the URI has already been added to the cache.
     *
     *         将文件广播到HADOOP集群
      *
      * 将资源通过DistributedCache进行分发
     */
    def distribute(
        path: String,
        resType: LocalResourceType = LocalResourceType.FILE,
        destName: Option[String] = None,
        targetDir: Option[String] = None,
        appMasterOnly: Boolean = false): (Boolean, String) = {
      val trimmedPath = path.trim()
      val localURI = Utils.resolveURI(trimmedPath)

      //对于file://abc.jar，它的scheme是file
      if (localURI.getScheme != LOCAL_SCHEME) {
        if (addDistributedUri(localURI)) {
          val localPath = getQualifiedLocalPath(localURI, hadoopConf)
          val linkname = targetDir.map(_ + "/").getOrElse("") +
            destName.orElse(Option(localURI.getFragment())).getOrElse(localPath.getName())
          val destPath = copyFileToRemote(dst, localPath, replication)
          val destFs = FileSystem.get(destPath.toUri(), hadoopConf)
          distCacheMgr.addResource(
            destFs, hadoopConf, destPath, localResources, resType, linkname, statCache,
            appMasterOnly = appMasterOnly)
          (false, linkname)
        } else {
          (false, null)
        }
      } else {
        (true, trimmedPath)
      }
    }

    // If we passed in a keytab, make sure we copy the keytab to the staging directory on
    // HDFS, and setup the relevant environment vars, so the AM can login again.
    if (loginFromKeytab) {
      logInfo("To enable the AM to login from keytab, credentials are being copied over to the AM" +
        " via the YARN Secure Distributed Cache.")

      /**
        *
        * 广播keytab文件
        */

      val (_, localizedPath) = distribute(keytab,
        destName = Some(sparkConf.get("spark.yarn.keytab")),
        appMasterOnly = true)
      require(localizedPath != null, "Keytab file already distributed.")
    }

    /**
     * Copy the given main resource to the distributed cache if the scheme is not "local".
     * Otherwise, set the corresponding key in our SparkConf to handle it downstream.
     * Each resource is represented by a 3-tuple of:
     *   (1) destination resource name,
     *   (2) local path to the resource,
     *   (3) Spark property key to set if the scheme is not local
     */
    List(
      (SPARK_JAR, sparkJar(sparkConf), CONF_SPARK_JAR),  /**spark assemly jar,spark本身的jar包*/
      (APP_JAR, args.userJar, CONF_SPARK_USER_JAR), /**spark user jar, 包含用户代码的jar包， args.userJar是以file:///开头的jar文件*/
      ("log4j.properties", oldLog4jConf.orNull, null)
    ).foreach { case (destName, path, confKey) =>
      if (path != null && !path.trim().isEmpty()) {

        /**
          * isLocal表示什么含义？
          * localizedPath表示HDFS上缓存的路径？
          */
        val (isLocal, localizedPath) = distribute(path, destName = Some(destName))


        if (isLocal && confKey != null) {
          require(localizedPath != null, s"Path $path already distributed.")
          // If the resource is intended for local use only, handle this downstream
          // by setting the appropriate property

          /***
            * 将"spark.yarn.user.jar写到SparkConfiguration中
            */
          sparkConf.set(confKey, localizedPath)
        }
      }
    }

    /**
     * Do the same for any additional resources passed in through ClientArguments.
     * Each resource category is represented by a 3-tuple of:
     *   (1) comma separated list of resources in this category,
     *   (2) resource type, and
     *   (3) whether to add these resources to the classpath
     */
    val cachedSecondaryJarLinks = ListBuffer.empty[String]
    List(
      (args.addJars, LocalResourceType.FILE, true),
      (args.files, LocalResourceType.FILE, false),
      (args.archives, LocalResourceType.ARCHIVE, false)
    ).foreach { case (flist, resType, addToClasspath) =>
      if (flist != null && !flist.isEmpty()) {
        flist.split(',').foreach { file =>
          val (_, localizedPath) = distribute(file, resType = resType)
          require(localizedPath != null)
          if (addToClasspath) {
            cachedSecondaryJarLinks += localizedPath
          }
        }
      }
    }
    if (cachedSecondaryJarLinks.nonEmpty) {
      sparkConf.set(CONF_SPARK_YARN_SECONDARY_JARS, cachedSecondaryJarLinks.mkString(","))
    }

    if (isClusterMode && args.primaryPyFile != null) {
      distribute(args.primaryPyFile, appMasterOnly = true)
    }

    pySparkArchives.foreach { f => distribute(f) }

    // The python files list needs to be treated especially. All files that are not an
    // archive need to be placed in a subdirectory that will be added to PYTHONPATH.
    args.pyFiles.foreach { f =>
      val targetDir = if (f.endsWith(".py")) Some(LOCALIZED_PYTHON_DIR) else None
      distribute(f, targetDir = targetDir)
    }

    // Distribute an archive with Hadoop and Spark configuration for the AM.
    val (_, confLocalizedPath) = distribute(createConfArchive().toURI().getPath(),
      resType = LocalResourceType.ARCHIVE,
      destName = Some(LOCALIZED_CONF_DIR),
      appMasterOnly = true)
    require(confLocalizedPath != null)

    localResources
  }

  /**
   * Create an archive with the config files for distribution.
   *
   * These are only used by the AM, since executors will use the configuration object broadcast by
   * the driver. The files are zipped and added to the job as an archive, so that YARN will explode
   * it when distributing to the AM. This directory is then added to the classpath of the AM
   * process, just to make sure that everybody is using the same default config.
   *
   * This follows the order of precedence set by the startup scripts, in which HADOOP_CONF_DIR
   * shows up in the classpath before YARN_CONF_DIR.
   *
   * Currently this makes a shallow copy of the conf directory. If there are cases where a
   * Hadoop config directory contains subdirectories, this code will have to be fixed.
   *
   * The archive also contains some Spark configuration. Namely, it saves the contents of
   * SparkConf in a file to be loaded by the AM process.
   *
   * AM进程要使用的SparkConfiguration也放在这个压缩文件
   */
  private def createConfArchive(): File = {
    val hadoopConfFiles = new HashMap[String, File]()

    // Uploading $SPARK_CONF_DIR/log4j.properties file to the distributed cache to make sure that
    // the executors will use the latest configurations instead of the default values. This is
    // required when user changes log4j.properties directly to set the log configurations. If
    // configuration file is provided through --files then executors will be taking configurations
    // from --files instead of $SPARK_CONF_DIR/log4j.properties.
    val log4jFileName = "log4j.properties"
    Option(Utils.getContextOrSparkClassLoader.getResource(log4jFileName)).foreach { url =>
      if (url.getProtocol == "file") {
        hadoopConfFiles(log4jFileName) = new File(url.getPath)
      }
    }

    /**
     * 先提取HADOOP_CONF_DIR和YARN_CONF_DIR环境变量下的配置文件放到hadoopConfFiles中
     */
    Seq("HADOOP_CONF_DIR", "YARN_CONF_DIR").foreach { envKey =>
      sys.env.get(envKey).foreach { path =>
        val dir = new File(path)
        if (dir.isDirectory()) {
          dir.listFiles().foreach { file =>
            if (file.isFile && !hadoopConfFiles.contains(file.getName())) {
              hadoopConfFiles(file.getName()) = file
            }
          }
        }
      }
    }

    /**
     * 创建__spark_conf__.zip文件，写入Hadoop的配置文件以及Spark的配置文件
     */
    val confArchive = File.createTempFile(LOCALIZED_CONF_DIR, ".zip",
      new File(Utils.getLocalDir(sparkConf)))
    val confStream = new ZipOutputStream(new FileOutputStream(confArchive))

    /**
     * 1. 写入Hadoop配置文件
     */
    try {
      confStream.setLevel(0)
      hadoopConfFiles.foreach { case (name, file) =>
        if (file.canRead()) {
          confStream.putNextEntry(new ZipEntry(name))
          Files.copy(file, confStream)
          confStream.closeEntry()
        }
      }

      // Save Spark configuration to a file in the archive.
      /**
       * 2. 将Spark Configuration配置添加到压缩文件中
       *
       */
      val props = new Properties()
      sparkConf.getAll.foreach { case (k, v) => props.setProperty(k, v) }
      confStream.putNextEntry(new ZipEntry(SPARK_CONF_FILE))
      val writer = new OutputStreamWriter(confStream, UTF_8)
      props.store(writer, "Spark configuration.")
      writer.flush()
      confStream.closeEntry()
    } finally {
      confStream.close()
    }
    confArchive
  }

  /**
   * Get the renewal interval for tokens.
   */
  private def getTokenRenewalInterval(stagingDirPath: Path): Long = {
    // We cannot use the tokens generated above since those have renewer yarn. Trying to renew
    // those will fail with an access control issue. So create new tokens with the logged in
    // user as renewer.
    val creds = new Credentials()
    val nns = YarnSparkHadoopUtil.get.getNameNodesToAccess(sparkConf) + stagingDirPath
    YarnSparkHadoopUtil.get.obtainTokensForNamenodes(
      nns, hadoopConf, creds, Some(sparkConf.get("spark.yarn.principal")))
    val t = creds.getAllTokens.asScala
      .filter(_.getKind == DelegationTokenIdentifier.HDFS_DELEGATION_KIND)
      .head
    val newExpiration = t.renew(hadoopConf)
    val identifier = new DelegationTokenIdentifier()
    identifier.readFields(new DataInputStream(new ByteArrayInputStream(t.getIdentifier)))
    val interval = newExpiration - identifier.getIssueDate
    logInfo(s"Renewal Interval set to $interval")
    interval
  }

  /**
   * Set up the environment for launching our ApplicationMaster container.
   *
   * 为启动ApplicationMaster Container，配置系统环境(System.env)
    *
    * LaunchEnv是一个HashMap，那么
    * 1. 这个LaunchEnv Map里都包含哪些东西？
    * 2. 这个Map中定义的K和V如何使用？
    *
   */
  private def setupLaunchEnv(
      stagingDir: String,
      pySparkArchives: Seq[String]): HashMap[String, String] = {
    logInfo("Setting up the launch environment for our AM container")

    /** *
      * env记录了Container Launch Env的Key和Value
      */
    val env = new HashMap[String, String]()

    /**
     * spark.driver.extraClassPath
     */
    val driverExtraClassPath = sparkConf.getOption("spark.driver.extraClassPath")


    /**
      * 为LaunchEnv设置Classpath？
      */
    populateClasspath(args, yarnConf, sparkConf, env, true, driverExtraClassPath)

    /**
      * 设置SPARK_YARN_MODE、SPARK_USER和SPARK_YARN_STAGING_DIR
      */
    env("SPARK_YARN_MODE") = "true"
    env("SPARK_YARN_STAGING_DIR") = stagingDir
    env("SPARK_USER") = UserGroupInformation.getCurrentUser().getShortUserName()
    if (loginFromKeytab) {
      val remoteFs = FileSystem.get(hadoopConf)
      val stagingDirPath = new Path(remoteFs.getHomeDirectory, stagingDir)
      val credentialsFile = "credentials-" + UUID.randomUUID().toString
      sparkConf.set(
        "spark.yarn.credentials.file", new Path(stagingDirPath, credentialsFile).toString)
      logInfo(s"Credentials file set to: $credentialsFile")
      val renewalInterval = getTokenRenewalInterval(stagingDirPath)
      sparkConf.set("spark.yarn.token.renewal.interval", renewalInterval.toString)
    }

    // Pick up any environment variables for the AM provided through spark.yarn.appMasterEnv.*
    val amEnvPrefix = "spark.yarn.appMasterEnv."
    sparkConf.getAll
      .filter { case (k, v) => k.startsWith(amEnvPrefix) }
      .map { case (k, v) => (k.substring(amEnvPrefix.length), v) }
      .foreach { case (k, v) => YarnSparkHadoopUtil.addPathToEnvironment(env, k, v) }

    // Keep this for backwards compatibility but users should move to the config
    sys.env.get("SPARK_YARN_USER_ENV").foreach { userEnvs =>
    // Allow users to specify some environment variables.
      YarnSparkHadoopUtil.setEnvFromInputString(env, userEnvs)
      // Pass SPARK_YARN_USER_ENV itself to the AM so it can use it to set up executor environments.
      env("SPARK_YARN_USER_ENV") = userEnvs
    }

    // If pyFiles contains any .py files, we need to add LOCALIZED_PYTHON_DIR to the PYTHONPATH
    // of the container processes too. Add all non-.py files directly to PYTHONPATH.
    //
    // NOTE: the code currently does not handle .py files defined with a "local:" scheme.
    val pythonPath = new ListBuffer[String]()
    val (pyFiles, pyArchives) = args.pyFiles.partition(_.endsWith(".py"))
    if (pyFiles.nonEmpty) {
      pythonPath += buildPath(YarnSparkHadoopUtil.expandEnvironment(Environment.PWD),
        LOCALIZED_PYTHON_DIR)
    }
    (pySparkArchives ++ pyArchives).foreach { path =>
      val uri = Utils.resolveURI(path)
      if (uri.getScheme != LOCAL_SCHEME) {
        pythonPath += buildPath(YarnSparkHadoopUtil.expandEnvironment(Environment.PWD),
          new Path(uri).getName())
      } else {
        pythonPath += uri.getPath()
      }
    }

    // Finally, update the Spark config to propagate PYTHONPATH to the AM and executors.
    if (pythonPath.nonEmpty) {
      val pythonPathStr = (sys.env.get("PYTHONPATH") ++ pythonPath)
        .mkString(YarnSparkHadoopUtil.getClassPathSeparator)
      env("PYTHONPATH") = pythonPathStr
      sparkConf.setExecutorEnv("PYTHONPATH", pythonPathStr)
    }

    // In cluster mode, if the deprecated SPARK_JAVA_OPTS is set, we need to propagate it to
    // executors. But we can't just set spark.executor.extraJavaOptions, because the driver's
    // SparkContext will not let that set spark* system properties, which is expected behavior for
    // Yarn clients. So propagate it through the environment.
    //
    // Note that to warn the user about the deprecation in cluster mode, some code from
    // SparkConf#validateSettings() is duplicated here (to avoid triggering the condition
    // described above).
    if (isClusterMode) {
      sys.env.get("SPARK_JAVA_OPTS").foreach { value =>
        val warning =
          s"""
            |SPARK_JAVA_OPTS was detected (set to '$value').
            |This is deprecated in Spark 1.0+.
            |
            |Please instead use:
            | - ./spark-submit with conf/spark-defaults.conf to set defaults for an application
            | - ./spark-submit with --driver-java-options to set -X options for a driver
            | - spark.executor.extraJavaOptions to set -X options for executors
          """.stripMargin
        logWarning(warning)
        for (proc <- Seq("driver", "executor")) {
          val key = s"spark.$proc.extraJavaOptions"
          if (sparkConf.contains(key)) {
            throw new SparkException(s"Found both $key and SPARK_JAVA_OPTS. Use only the former.")
          }
        }
        env("SPARK_JAVA_OPTS") = value
      }
    }

    sys.env.get(ENV_DIST_CLASSPATH).foreach { dcp =>
      env(ENV_DIST_CLASSPATH) = dcp
    }

    env
  }

  /**
   * Set up a ContainerLaunchContext to launch our ApplicationMaster container.
   * This sets up the launch environment, java options, and the command for launching the AM.
   *
   * 在ContainerLaunchContext包含了一个NodeManager启动一个Container所需要的所有信息
   * NodeManager如何启动Container？ApplicationMaster在Container中运行，何解？
   * @see
   */
  private def createContainerLaunchContext(newAppResponse: GetNewApplicationResponse)
    : ContainerLaunchContext = {

    /**
     * ApplicationMaster进程运行于一个YARN Container中，此处setup AM Container
     */
    logInfo("Setting up container launch context for our AM")
    val appId = newAppResponse.getApplicationId

    /**
     * appStagingDir是一个相对目录，获取staging directory
     */
    val appStagingDir = getAppStagingDir(appId)

    /** *
      * python application，那么需要配置spark.yarn.isPython为true
      */
    val pySparkArchives =
      if (sparkConf.getBoolean("spark.yarn.isPython", false)) {
        findPySparkArchives()
      } else {
        Nil
      }

    /**
     * launchEnv是一个HashMap，setupLaunchEnv接受两个参数：appStagingDir, pySparkArchives
     * 问题：这两个参数干啥的？
     */
    val launchEnv = setupLaunchEnv(appStagingDir, pySparkArchives)

    /**
      * 上传本地文件到HADOOP HDFS，比如HADOOP的配置文件以及Spark的配置信息
      */
    val localResources = prepareLocalResources(appStagingDir, pySparkArchives)

    // Set the environment variables to be passed on to the executors.
    distCacheMgr.setDistFilesEnv(launchEnv)
    distCacheMgr.setDistArchivesEnv(launchEnv)

    /**
      * 创建ContainerLaunchContext,它是本方法返回的对象
      */
    val amContainerLaunchContext = Records.newRecord(classOf[ContainerLaunchContext])

    /**
      * 设置ContainerLaunchContext的LocalResources和Environment
      */
    amContainerLaunchContext.setLocalResources(localResources.asJava)
    amContainerLaunchContext.setEnvironment(launchEnv.asJava)

    val javaOpts = ListBuffer[String]()

    // Set the environment variable through a command prefix
    // to append to the existing value of the variable
    var prefixEnv: Option[String] = None

    // Add Xmx for AM memory
    javaOpts += "-Xmx" + args.amMemory + "m"

    val tmpDir = new Path(
      YarnSparkHadoopUtil.expandEnvironment(Environment.PWD),
      YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR
    )
    javaOpts += "-Djava.io.tmpdir=" + tmpDir

    // TODO: Remove once cpuset version is pushed out.
    // The context is, default gc for server class machines ends up using all cores to do gc -
    // hence if there are multiple containers in same node, Spark GC affects all other containers'
    // performance (which can be that of other Spark containers)
    // Instead of using this, rely on cpusets by YARN to enforce "proper" Spark behavior in
    // multi-tenant environments. Not sure how default Java GC behaves if it is limited to subset
    // of cores on a node.
    val useConcurrentAndIncrementalGC = launchEnv.get("SPARK_USE_CONC_INCR_GC").exists(_.toBoolean)
    if (useConcurrentAndIncrementalGC) {
      // In our expts, using (default) throughput collector has severe perf ramifications in
      // multi-tenant machines
      javaOpts += "-XX:+UseConcMarkSweepGC"
      javaOpts += "-XX:MaxTenuringThreshold=31"
      javaOpts += "-XX:SurvivorRatio=8"
      javaOpts += "-XX:+CMSIncrementalMode"
      javaOpts += "-XX:+CMSIncrementalPacing"
      javaOpts += "-XX:CMSIncrementalDutyCycleMin=0"
      javaOpts += "-XX:CMSIncrementalDutyCycle=10"
    }

    // Include driver-specific java options if we are launching a driver
    if (isClusterMode) {
      val driverOpts = sparkConf.getOption("spark.driver.extraJavaOptions")
        .orElse(sys.env.get("SPARK_JAVA_OPTS"))
      driverOpts.foreach { opts =>
        javaOpts ++= Utils.splitCommandString(opts).map(YarnSparkHadoopUtil.escapeForShell)
      }
      val libraryPaths = Seq(sys.props.get("spark.driver.extraLibraryPath"),
        sys.props.get("spark.driver.libraryPath")).flatten
      if (libraryPaths.nonEmpty) {
        prefixEnv = Some(getClusterPath(sparkConf, Utils.libraryPathEnvPrefix(libraryPaths)))
      }
      if (sparkConf.getOption("spark.yarn.am.extraJavaOptions").isDefined) {
        logWarning("spark.yarn.am.extraJavaOptions will not take effect in cluster mode")
      }
    } else {
      // Validate and include yarn am specific java options in yarn-client mode.
      val amOptsKey = "spark.yarn.am.extraJavaOptions"
      val amOpts = sparkConf.getOption(amOptsKey)
      amOpts.foreach { opts =>
        if (opts.contains("-Dspark")) {
          val msg = s"$amOptsKey is not allowed to set Spark options (was '$opts'). "
          throw new SparkException(msg)
        }
        if (opts.contains("-Xmx") || opts.contains("-Xms")) {
          val msg = s"$amOptsKey is not allowed to alter memory settings (was '$opts')."
          throw new SparkException(msg)
        }
        javaOpts ++= Utils.splitCommandString(opts).map(YarnSparkHadoopUtil.escapeForShell)
      }

      sparkConf.getOption("spark.yarn.am.extraLibraryPath").foreach { paths =>
        prefixEnv = Some(getClusterPath(sparkConf, Utils.libraryPathEnvPrefix(Seq(paths))))
      }
    }

    // For log4j configuration to reference
    javaOpts += ("-Dspark.yarn.app.container.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR)
    YarnCommandBuilderUtils.addPermGenSizeOpt(javaOpts)

    /**
     * 如果是Cluster模式，那么会添加--class参数，如果是Client模式，则不添加
     * ApplicationMaster进程有--class参数，ExecutorLauncher进程没有--class参数
     */
    val userClass =
      if (isClusterMode) {
        Seq("--class", YarnSparkHadoopUtil.escapeForShell(args.userClass))
      } else {
        Nil
      }
    val userJar =
      if (args.userJar != null) {
        Seq("--jar", args.userJar)
      } else {
        Nil
      }
    val primaryPyFile =
      if (isClusterMode && args.primaryPyFile != null) {
        Seq("--primary-py-file", new Path(args.primaryPyFile).getName())
      } else {
        Nil
      }
    val primaryRFile =
      if (args.primaryRFile != null) {
        Seq("--primary-r-file", args.primaryRFile)
      } else {
        Nil
      }

    /**
     * 如果是Cluster模式，那么启动ApplicationMaster进程；如果是Client模式，那么启动ExecutorLauncher进程
     */
    val amClass =
      if (isClusterMode) {
        Utils.classForName("org.apache.spark.deploy.yarn.ApplicationMaster").getName
      } else {
        Utils.classForName("org.apache.spark.deploy.yarn.ExecutorLauncher").getName
      }
    if (args.primaryRFile != null && args.primaryRFile.endsWith(".R")) {
      args.userArgs = ArrayBuffer(args.primaryRFile) ++ args.userArgs
    }
    val userArgs = args.userArgs.flatMap { arg =>
      Seq("--arg", YarnSparkHadoopUtil.escapeForShell(arg))
    }

    /**
     * 在此处指定--properties-file参数，buildPath的入参是$PWD，那么$PWD的路径指向哪里？
     */
    val amArgs =
      Seq(amClass) ++ userClass ++ userJar ++ primaryPyFile ++ primaryRFile ++
        userArgs ++ Seq(
          "--executor-memory", args.executorMemory.toString + "m",
          "--executor-cores", args.executorCores.toString,
          "--properties-file", buildPath(YarnSparkHadoopUtil.expandEnvironment(Environment.PWD), /**ExecutorLauncher的--properties-file参数，为啥是当前目录*/
            LOCALIZED_CONF_DIR, SPARK_CONF_FILE))

    // Command for the ApplicationMaster
    val commands = prefixEnv ++ Seq(
        YarnSparkHadoopUtil.expandEnvironment(Environment.JAVA_HOME) + "/bin/java", "-server"
      ) ++
      javaOpts ++ amArgs ++
      Seq(
        "1>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
        "2>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")

    // TODO: it would be nicer to just make sure there are no null commands here
    val printableCommands = commands.map(s => if (s == null) "null" else s).toList
    amContainerLaunchContext.setCommands(printableCommands.asJava)

    logDebug("===============================================================================")
    logDebug("YARN AM launch context:")
    logDebug(s"    user class: ${Option(args.userClass).getOrElse("N/A")}")
    logDebug("    env:")
    launchEnv.foreach { case (k, v) => logDebug(s"        $k -> $v") }
    logDebug("    resources:")
    localResources.foreach { case (k, v) => logDebug(s"        $k -> $v")}
    logDebug("    command:")
    logDebug(s"        ${printableCommands.mkString(" ")}")
    logDebug("===============================================================================")

    // send the acl settings into YARN to control who has access via YARN interfaces
    val securityManager = new SecurityManager(sparkConf)
    amContainerLaunchContext.setApplicationACLs(
      YarnSparkHadoopUtil.getApplicationAclsForYarn(securityManager).asJava)
    setupSecurityToken(amContainerLaunchContext)
    UserGroupInformation.getCurrentUser().addCredentials(credentials)

    amContainerLaunchContext
  }

  /** *
    * Kerbero认证
    */
  def setupCredentials(): Unit = {
    /** *
      * 首先判断用户提交的作业是否需要进行Kerberos认证，从两方面进行判断:
      *
      * 1. SparkSumbit是否带有--principal参数，如果有则进行Kerberos认证
      * 2. Spark应用中是否配置了spark.yarn.principal选项，如果配置了那么也需要进行Kerberos认证
      */
    loginFromKeytab = args.principal != null || sparkConf.contains("spark.yarn.principal")

    /** *
      * 如果需要Kerberos认证，那么需要解析出principal和keytab
      */
    if (loginFromKeytab) {
      principal =
        if (args.principal != null) args.principal else sparkConf.get("spark.yarn.principal")
      keytab = {
        if (args.keytab != null) {
          args.keytab
        } else {
          sparkConf.getOption("spark.yarn.keytab").orNull
        }
      }

      /** *
        * 代码运行到这里意味着principal不为空，因此这里需要检查keytab也不会空
        * 打印日志：使用principal和keytab登录Kerberos
        */
      require(keytab != null, "Keytab must be specified when principal is specified.")
      logInfo("Attempting to login to the Kerberos" +
        s" using principal: $principal and keytab: $keytab")

      /** *
        * keytab是一个文件，
        */
      val f = new File(keytab)


      /** *
        *   Generate a file name that can be used for the keytab file, that does not conflict with any user file.
        *   问题：
        *   用户可能已经配置了spark.yarn.keytab，并且前面也读取了该配置，为什么这里要重写呢？
        */
      val keytabFileName = f.getName + "-" + UUID.randomUUID().toString
      sparkConf.set("spark.yarn.keytab", keytabFileName)
      sparkConf.set("spark.yarn.principal", principal)
    }
    val userGroupInformation = UserGroupInformation.getCurrentUser
    credentials = userGroupInformation .getCredentials
  }

  /**
   * Report the state of an application until it has exited, either successfully or
   * due to some failure, then return a pair of the yarn application state (FINISHED, FAILED,
   * KILLED, or RUNNING) and the final application state (UNDEFINED, SUCCEEDED, FAILED,
   * or KILLED).
   *
   * @param appId ID of the application to monitor.
   * @param returnOnRunning Whether to also return the application state when it is RUNNING.
   * @param logApplicationReport Whether to log details of the application report every iteration.
   * @return A pair of the yarn application state and the final application state
   *         返回值类型为(YarnApplicationState， FinalApplicationStatus)
   */
  def monitorApplication(
      appId: ApplicationId,
      returnOnRunning: Boolean = false,
      logApplicationReport: Boolean = true): (YarnApplicationState, FinalApplicationStatus) = {
    /** *
      * 每个一秒钟获取一次report信息
      */
    val interval = sparkConf.getLong("spark.yarn.report.interval", 1000)
    var lastState: YarnApplicationState = null
    while (true) {
      /** *
        * 休息一秒钟，等待获取最新状态，记录到lastState中
        */
      Thread.sleep(interval)

      /** *
        * 根据applicationId获取ApplicationReport
        */
      val report: ApplicationReport =
        try {
          getApplicationReport(appId)
        } catch {
          case e: ApplicationNotFoundException =>
            logError(s"Application $appId not found.")
            return (YarnApplicationState.KILLED, FinalApplicationStatus.KILLED)
          case NonFatal(e) =>
            logError(s"Failed to contact YARN for application $appId.", e)
            return (YarnApplicationState.FAILED, FinalApplicationStatus.FAILED)
        }

      /** *
        * stage记录的是本轮Application Report返回的状态信息
        */
      val state = report.getYarnApplicationState

      /** *
        * 如果要将应用程序的汇报信息写日志，那么需要执行写日志操作
        */
      if (logApplicationReport) {

        /** *
          * Application汇报状态
          */
        logInfo(s"Application report for $appId (state: $state)")

        // If DEBUG is enabled, log report details every iteration
        // Otherwise, log them every time the application changes state
        if (log.isDebugEnabled) {
          logDebug(formatReportDetails(report))
        } else if (lastState != state) {
          /** *
            * lastStage和stage分别记录的是什么状态？
            * lastStage上一轮Application Report记录的状态，stage记录的是本轮Application Report记录的状态
            */
          logInfo(formatReportDetails(report))
        }
      }

      if (lastState != state) {
        state match {
          case YarnApplicationState.RUNNING =>
            reportLauncherState(SparkAppHandle.State.RUNNING)
          case YarnApplicationState.FINISHED =>
            reportLauncherState(SparkAppHandle.State.FINISHED)
          case YarnApplicationState.FAILED =>
            reportLauncherState(SparkAppHandle.State.FAILED)
          case YarnApplicationState.KILLED =>
            reportLauncherState(SparkAppHandle.State.KILLED)
          case _ =>
        }
      }

      /** *
        * 应用运行完成，那么首先清楚StagingDir，然后再返回state和final application status
        */
      if (state == YarnApplicationState.FINISHED ||
        state == YarnApplicationState.FAILED ||
        state == YarnApplicationState.KILLED) {
        cleanupStagingDir(appId)
        return (state, report.getFinalApplicationStatus)
      }

      /** *
        * 当YarnApplicationState进行运行态，并且returnOnRunning为true(即只要等待应用运行起来，而不是等到应用运行结束就返回)
        */
      if (returnOnRunning && state == YarnApplicationState.RUNNING) {

        /** *
          * report对象包含了FinalApplicationStatus，应用程序的运行结果：成功、失败以及Killed(如果应用程序还没执行完，那么状态UNDEFINED)
          */
        return (state, report.getFinalApplicationStatus)
      }

      lastState = state
    }

    // Never reached, but keeps compiler happy
    throw new SparkException("While loop is depleted! This should never happen...")
  }

  /** *
    * 将ApplicationReport格式化为字符串
    * @param report
    * @return
    */
  private def formatReportDetails(report: ApplicationReport): String = {

    /** *
      * details记录的是类型为(String,String)的集合，每个二元组的第一个元素是名称，第二个元素是值
      */
    val details = Seq[(String, String)](
      ("client token", getClientToken(report)),
      ("diagnostics", report.getDiagnostics),
      ("ApplicationMaster host", report.getHost),
      ("ApplicationMaster RPC port", report.getRpcPort.toString),
      ("queue", report.getQueue),
      ("start time", report.getStartTime.toString),
      ("final status", report.getFinalApplicationStatus.toString),
      ("tracking URL", report.getTrackingUrl),
      ("user", report.getUser) /**获取提交应用的用户名*/
    )

    /** *
      *  Use more loggable format if value is null or empty
      *  将v包装成Option(v),如果有值那么返回v，如果没有值则返回N/A
      *
      *  问题：如果v是空字符串，那么结果是什么？nonEmpty是作用于字符串上，也就是这里的逻辑是
      *  首先对v进行filter操作，如果v是空字符串或者null，那么返回None，都会返回N/A
      *  所以，
      */
    details.map { case (k, v) =>
      val newValue = Option(v).filter(_.nonEmpty).getOrElse("N/A")
      s"\n\t $k: $newValue"
    }.mkString("")
  }

  /**
   * Submit an application to the ResourceManager.
   * If set spark.yarn.submit.waitAppCompletion to true, it will stay alive
   * reporting the application's status until the application has exited for any reason.
   * Otherwise, the client process will exit after submission.
   * If the application finishes with a failed, killed, or undefined status,
   * throw an appropriate SparkException.
   */
  def run(): Unit = {

    /**
     * submitApplication，在submitApplication方法中做了如下事情：
     * Client的submitApplication方法与submitApplication与YARN提供的submitApplication API的含义不同，YARN提供的submitApplication的含义是向RM提交一个Application并返回一个APPID，
     * 而client基于这个APPID，用户还需要继续使用YARN API提交AM
     */
    this.appId = submitApplication()

    /**
     * Client是否立即退出,默认配置下fireAndForget为false，因此Client进程不会立即退出
     * 如果 launcherBackend未连接，并且提交任务后立即退出，那么将汇报任务的状态
      */
    if (!launcherBackend.isConnected() && fireAndForget) {
      val report = getApplicationReport(appId)
      val state = report.getYarnApplicationState
      logInfo(s"Application report for $appId (state: $state)")
      logInfo(formatReportDetails(report))
      if (state == YarnApplicationState.FAILED || state == YarnApplicationState.KILLED) {
        throw new SparkException(s"Application $appId finished with status: $state")
      }
    } else {
      /**
       * monitorApplication将循环等待应用程序结束，如果ApplicationState为FAILED或者KILLED或者UNDEFINED，则抛出SparkException
       * monitorApplication是一个同步过程？何时返回？
       * 因为monitorApplication方法的returnOnRunning使用了默认值false，表示monitorApplication方法在应用程序进入RUNNINING状态时不会返回，
       * 只有在应用程序结束时，才返回。如果returnOnRunning表示Client在应用程序进入运行状态就会返回
       * 因此该方法返回时，Application进入了Running状态
       */
      val (yarnApplicationState, finalApplicationStatus) = monitorApplication(appId)

      /** *
        * Yarn Application的state或者status为FAILED，那么抛出异常
        */
      if (yarnApplicationState == YarnApplicationState.FAILED ||
        finalApplicationStatus == FinalApplicationStatus.FAILED) {
        throw new SparkException(s"Application $appId finished with failed status")
      }

      /** *
        * Yarn Application的state或者status为KILLED，那么抛出异常
        */
      if (yarnApplicationState == YarnApplicationState.KILLED ||
        finalApplicationStatus == FinalApplicationStatus.KILLED) {
        throw new SparkException(s"Application $appId is killed")
      }

      /** *
        * application没有运行完则抛出异常，意味着代码运行此处应该运行完成
        */
      if (finalApplicationStatus == FinalApplicationStatus.UNDEFINED) {
        throw new SparkException(s"The final status of application $appId is undefined")
      }
    }
  }

  private def findPySparkArchives(): Seq[String] = {
    sys.env.get("PYSPARK_ARCHIVES_PATH")
      .map(_.split(",").toSeq)
      .getOrElse {
        val pyLibPath = Seq(sys.env("SPARK_HOME"), "python", "lib").mkString(File.separator)
        val pyArchivesFile = new File(pyLibPath, "pyspark.zip")
        require(pyArchivesFile.exists(),
          "pyspark.zip not found; cannot run pyspark application in YARN mode.")
        val py4jFile = new File(pyLibPath, "py4j-0.9.1-src.zip")
        require(py4jFile.exists(),
          "py4j-0.9.1-src.zip not found; cannot run pyspark application in YARN mode.")
        Seq(pyArchivesFile.getAbsolutePath(), py4jFile.getAbsolutePath())
      }
  }

}

/**
 * YARN CLUSTER模式下，SparkSubmit运行的main方法，注意，该main方法是在SparkSubmit进程中通过反射的方式启动的，
 * 也就是说，运行Client的代码与运行SparkSubmit的代码运行在同一个JVM中，因此它们共享system properties
 *
 */
object Client extends Logging {

  /** *
    *
    * @param argStrings argStrings是构造Client对象的第一个参数ClientArguments类的构造参数
    */
  def main(argStrings: Array[String]) {
    /**
     * SparkSubmit中专门为YARN_CLUSTER模式设置了这个系统变量
     */
    if (!sys.props.contains("SPARK_SUBMIT")) {
      logWarning("WARNING: This client is deprecated and will be removed in a " +
        "future version of Spark. Use ./bin/spark-submit with \"--master yarn\"")
    }

    // Set an env variable indicating we are running in YARN mode.
    // Note that any env variable with the SPARK_ prefix gets propagated to all (remote) processes
    /**
     * 以SPARK开头的环境变量会分发到各个节点进程(Executor,具体点说是YARN Container？)
     */
    System.setProperty("SPARK_YARN_MODE", "true")
    val sparkConf = new SparkConf

    /**
     * 在yarn-cluster模式下，SparkSubmit通过反射的方式调用Client的main方法将命令行参数传递给Client，Client解析为ClientArguments对象
     */
    val args = new ClientArguments(argStrings, sparkConf)
    // to maintain backwards-compatibility
    if (!Utils.isDynamicAllocationEnabled(sparkConf)) {
      /** *
        * 如果不是动态分配，那么为spark.executor.instances配置选项
        */
      sparkConf.setIfMissing("spark.executor.instances", args.numExecutors.toString)
    }

    /**
     * 创建Client实例并调用run方法, 这是客户端进程唯一做的事情
     */
    new Client(args, sparkConf).run()
  }

  // Alias for the Spark assembly jar and the user jar
  val SPARK_JAR: String = "__spark__.jar"
  val APP_JAR: String = "__app__.jar"

  // URI scheme that identifies local resources
  val LOCAL_SCHEME = "local"

  /** *
    * Staging directory for any temporary jars or files
    * application的临时工作目录
    */
  val SPARK_STAGING: String = ".sparkStaging"

  // Location of any user-defined Spark jars
  val CONF_SPARK_JAR = "spark.yarn.jar"
  val ENV_SPARK_JAR = "SPARK_JAR"

  // Internal config to propagate the location of the user's jar to the driver/executors
  val CONF_SPARK_USER_JAR = "spark.yarn.user.jar"

  // Internal config to propagate the locations of any extra jars to add to the classpath
  // of the executors
  val CONF_SPARK_YARN_SECONDARY_JARS = "spark.yarn.secondary.jars"

  // Comma-separated list of strings to pass through as YARN application tags appearing
  // in YARN ApplicationReports, which can be used for filtering when querying YARN.
  val CONF_SPARK_YARN_APPLICATION_TAGS = "spark.yarn.tags"

  // Staging directory is private! -> rwx--------
  val STAGING_DIR_PERMISSION: FsPermission =
    FsPermission.createImmutable(Integer.parseInt("700", 8).toShort)

  // App files are world-wide readable and owner writable -> rw-r--r--
  val APP_FILE_PERMISSION: FsPermission =
    FsPermission.createImmutable(Integer.parseInt("644", 8).toShort)

  // Distribution-defined classpath to add to processes
  val ENV_DIST_CLASSPATH = "SPARK_DIST_CLASSPATH"

  // Subdirectory where the user's Spark and Hadoop config files will be placed.
  val LOCALIZED_CONF_DIR = "__spark_conf__"

    /** *
      *  Name of the file in the conf archive containing Spark configuration.
      *  Spark配置信息组成的配置文件
       */
  val SPARK_CONF_FILE = "__spark_conf__.properties"

  // Subdirectory where the user's python files (not archives) will be placed.
  val LOCALIZED_PYTHON_DIR = "__pyfiles__"

  /**
   * Find the user-defined Spark jar if configured, or return the jar containing this
   * class if not.
   *
   * This method first looks in the SparkConf object for the CONF_SPARK_JAR key, and in the
   * user environment if that is not found (for backwards compatibility).
    *
    *
    * sparkJar是获得spark assembly jar的位置，如果没有配置，那么会查找Client类所在的jar(就是spark assembly jar)
    *
    *
   */
  private def sparkJar(conf: SparkConf): String = {
    if (conf.contains(CONF_SPARK_JAR)) {
      conf.get(CONF_SPARK_JAR)
    } else if (System.getenv(ENV_SPARK_JAR) != null) {
      logWarning(
        s"$ENV_SPARK_JAR detected in the system environment. This variable has been deprecated " +
          s"in favor of the $CONF_SPARK_JAR configuration variable.")
      System.getenv(ENV_SPARK_JAR)
    } else {
      SparkContext.jarOfClass(this.getClass).getOrElse(throw new SparkException("Could not "
        + "find jar containing Spark classes. The jar can be defined using the "
        + "spark.yarn.jar configuration option. If testing Spark, either set that option or "
        + "make sure SPARK_PREPEND_CLASSES is not set."))
    }
  }

  /**
   * Return the path to the given application's staging directory.
   *
   * getAppStagingDir的值是.sparkStaging/{appId},这是个相对路径，它的路径前缀是哪个？
    *
    * @param appId
    * @return
    */
  private def getAppStagingDir(appId: ApplicationId): String = {
    buildPath(SPARK_STAGING, appId.toString())
  }

  /**
   * Populate the classpath entry in the given environment map with any application
   * classpath specified through the Hadoop and Yarn configurations.
   */
  private[yarn] def populateHadoopClasspath(conf: Configuration, env: HashMap[String, String])
    : Unit = {
    val classPathElementsToAdd = getYarnAppClasspath(conf) ++ getMRAppClasspath(conf)
    for (c <- classPathElementsToAdd.flatten) {
      YarnSparkHadoopUtil.addPathToEnvironment(env, Environment.CLASSPATH.name, c.trim)
    }
  }

  private def getYarnAppClasspath(conf: Configuration): Option[Seq[String]] =
    Option(conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH)) match {
      case Some(s) => Some(s.toSeq)
      case None => getDefaultYarnApplicationClasspath
    }

  private def getMRAppClasspath(conf: Configuration): Option[Seq[String]] =
    Option(conf.getStrings("mapreduce.application.classpath")) match {
      case Some(s) => Some(s.toSeq)
      case None => getDefaultMRApplicationClasspath
    }

  private[yarn] def getDefaultYarnApplicationClasspath: Option[Seq[String]] = {
    val triedDefault = Try[Seq[String]] {
      val field = classOf[YarnConfiguration].getField("DEFAULT_YARN_APPLICATION_CLASSPATH")
      val value = field.get(null).asInstanceOf[Array[String]]
      value.toSeq
    } recoverWith {
      case e: NoSuchFieldException => Success(Seq.empty[String])
    }

    triedDefault match {
      case f: Failure[_] =>
        logError("Unable to obtain the default YARN Application classpath.", f.exception)
      case s: Success[Seq[String]] =>
        logDebug(s"Using the default YARN application classpath: ${s.get.mkString(",")}")
    }

    triedDefault.toOption
  }

  private[yarn] def getDefaultMRApplicationClasspath: Option[Seq[String]] = {
    val triedDefault = Try[Seq[String]] {
      val field = classOf[MRJobConfig].getField("DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH")
      StringUtils.getStrings(field.get(null).asInstanceOf[String]).toSeq
    } recoverWith {
      case e: NoSuchFieldException => Success(Seq.empty[String])
    }

    triedDefault match {
      case f: Failure[_] =>
        logError("Unable to obtain the default MR Application classpath.", f.exception)
      case s: Success[Seq[String]] =>
        logDebug(s"Using the default MR application classpath: ${s.get.mkString(",")}")
    }

    triedDefault.toOption
  }

  /**
   * Populate the classpath entry in the given environment map.
   *
   * User jars are generally not added to the JVM's system classpath; those are handled by the AM
   * and executor backend. When the deprecated `spark.yarn.user.classpath.first` is used, user jars
   * are included in the system classpath, though. The extra class path and other uploaded files are
   * always made available through the system class path.
   *
   * @param args Client arguments (when starting the AM) or null (when starting executors).
   * @param conf
   * @param sparkConf
   * @param env
   * @param isAM
   * @param extraClassPath 对于Driver而言就是spark.driver.extraClassPath，对于Executor而言就是spark.executor.extraClassPath
   */
  private[yarn] def populateClasspath(
      args: ClientArguments,
      conf: Configuration,
      sparkConf: SparkConf,
      env: HashMap[String, String],
      isAM: Boolean,
      extraClassPath: Option[String] = None): Unit = {

    /** *
      * extraClassPath是一个Option，它有一个foreach方法
      *
      */
    extraClassPath.foreach { classpath =>

      /**
        * clusterPath指的是什么？ 可以不关心细节，认为是classpath值即可
        */
      val clusterPath = getClusterPath(sparkConf, classpath)

      /**
        * clusterPath可能是一个以逗号分割的路径，所以addClasspathEntry应该有按照逗号进行分割的逻辑
        */
      addClasspathEntry(clusterPath, env)
    }



    addClasspathEntry(YarnSparkHadoopUtil.expandEnvironment(Environment.PWD), env)

    /**
      * 如果是AM，则将__spark_conf__加入到Classpath
      */
    if (isAM) {
      addClasspathEntry(
        YarnSparkHadoopUtil.expandEnvironment(Environment.PWD) + Path.SEPARATOR +
          LOCALIZED_CONF_DIR, env)
    }

    /**
      * 如果user jar优先
      *
      */
    if (sparkConf.getBoolean("spark.yarn.user.classpath.first", false)) {
      // in order to properly add the app jar when user classpath is first
      // we have to do the mainJar separate in order to send the right thing
      // into addFileToClasspath
      val mainJar =
        if (args != null) {
          getMainJarUri(Option(args.userJar))
        } else {
          getMainJarUri(sparkConf.getOption(CONF_SPARK_USER_JAR))
        }
      mainJar.foreach(addFileToClasspath(sparkConf, conf, _, APP_JAR, env))

      val secondaryJars =
        if (args != null) {
          getSecondaryJarUris(Option(args.addJars))
        } else {
          getSecondaryJarUris(sparkConf.getOption(CONF_SPARK_YARN_SECONDARY_JARS))
        }
      secondaryJars.foreach { x =>
        addFileToClasspath(sparkConf, conf, x, null, env)
      }
    }

    /**
      * 将Spark的assembly jar加到classpath上
      */
    addFileToClasspath(sparkConf, conf, new URI(sparkJar(sparkConf)), SPARK_JAR, env)


    /**
      * 将YARN和Hadoop
      */
    populateHadoopClasspath(conf, env)

    sys.env.get(ENV_DIST_CLASSPATH).foreach { cp =>
      addClasspathEntry(getClusterPath(sparkConf, cp), env)
    }
  }

  /**
   * Returns a list of URIs representing the user classpath.
    *
    * 用户jar放在spark.yarn.user.jar中
   *
   * @param conf Spark configuration.
   */
  def getUserClasspath(conf: SparkConf): Array[URI] = {
    val mainUri = getMainJarUri(conf.getOption(CONF_SPARK_USER_JAR))
    val secondaryUris = getSecondaryJarUris(conf.getOption(CONF_SPARK_YARN_SECONDARY_JARS))
    (mainUri ++ secondaryUris).toArray
  }

  private def getMainJarUri(mainJar: Option[String]): Option[URI] = {
    mainJar.flatMap { path =>
      val uri = Utils.resolveURI(path)
      if (uri.getScheme == LOCAL_SCHEME) Some(uri) else None
    }.orElse(Some(new URI(APP_JAR)))
  }

  private def getSecondaryJarUris(secondaryJars: Option[String]): Seq[URI] = {
    secondaryJars.map(_.split(",")).toSeq.flatten.map(new URI(_))
  }

  /**
   * Adds the given path to the classpath, handling "local:" URIs correctly.
   *
   * If an alternate name for the file is given, and it's not a "local:" file, the alternate
   * name will be added to the classpath (relative to the job's work directory).
   *
   * If not a "local:" file and no alternate name, the linkName will be added to the classpath.
   *
   * @param conf        Spark configuration.
   * @param hadoopConf  Hadoop configuration.
   * @param uri         URI to add to classpath (optional).
   * @param fileName    Alternate name for the file (optional).
   * @param env         Map holding the environment variables.
   */
  private def addFileToClasspath(
      conf: SparkConf,
      hadoopConf: Configuration,
      uri: URI,
      fileName: String,
      env: HashMap[String, String]): Unit = {
    if (uri != null && uri.getScheme == LOCAL_SCHEME) {
      addClasspathEntry(getClusterPath(conf, uri.getPath), env)
    } else if (fileName != null) {
      addClasspathEntry(buildPath(
        YarnSparkHadoopUtil.expandEnvironment(Environment.PWD), fileName), env)
    } else if (uri != null) {
      val localPath = getQualifiedLocalPath(uri, hadoopConf)
      val linkName = Option(uri.getFragment()).getOrElse(localPath.getName())
      addClasspathEntry(buildPath(
        YarnSparkHadoopUtil.expandEnvironment(Environment.PWD), linkName), env)
    }
  }

  /**
   * Add the given path to the classpath entry of the given environment map.
   * If the classpath is already set, this appends the new path to the existing classpath.
   *
   * 将path添加到CLASSPATH环境变量中
   */
  private def addClasspathEntry(path: String, env: HashMap[String, String]): Unit =
    {
      val key = Environment.CLASSPATH.name

      /**
        * 将路径加到Environment中，key是CLASSPATH
        */
      YarnSparkHadoopUtil.addPathToEnvironment(env, key, path)
    }

  /**
   * Returns the path to be sent to the NM for a path that is valid on the gateway.
   *
   * This method uses two configuration values:
   *
   *  - spark.yarn.config.gatewayPath: a string that identifies a portion of the input path that may
   *    only be valid in the gateway node.
   *  - spark.yarn.config.replacementPath: a string with which to replace the gateway path. This may
   *    contain, for example, env variable references, which will be expanded by the NMs when
   *    starting containers.
   *
   * If either config is not available, the input path is returned.
   */
  def getClusterPath(conf: SparkConf, path: String): String = {

    /**
      * 把path中的localPath替换为clusterPath
      */
    val localPath = conf.get("spark.yarn.config.gatewayPath", null)
    val clusterPath = conf.get("spark.yarn.config.replacementPath", null)
    if (localPath != null && clusterPath != null) {
      path.replace(localPath, clusterPath)
    } else {
      path
    }
  }

  /**
   * Obtains token for the Hive metastore and adds them to the credentials.
   */
  private def obtainTokenForHiveMetastore(
      sparkConf: SparkConf,
      conf: Configuration,
      credentials: Credentials) {
    if (shouldGetTokens(sparkConf, "hive") && UserGroupInformation.isSecurityEnabled) {
      YarnSparkHadoopUtil.get.obtainTokenForHiveMetastore(conf).foreach {
        credentials.addToken(new Text("hive.server2.delegation.token"), _)
      }
    }
  }

  /**
   * Obtain a security token for HBase.
   */
  def obtainTokenForHBase(
      sparkConf: SparkConf,
      conf: Configuration,
      credentials: Credentials): Unit = {
    if (shouldGetTokens(sparkConf, "hbase") && UserGroupInformation.isSecurityEnabled) {
      YarnSparkHadoopUtil.get.obtainTokenForHBase(conf).foreach { token =>
        credentials.addToken(token.getService, token)
        logInfo("Added HBase security token to credentials.")
      }
    }
  }

  /**
   * Return whether the two file systems are the same.
   */
  private def compareFs(srcFs: FileSystem, destFs: FileSystem): Boolean = {
    val srcUri = srcFs.getUri()
    val dstUri = destFs.getUri()
    if (srcUri.getScheme() == null || srcUri.getScheme() != dstUri.getScheme()) {
      return false
    }

    var srcHost = srcUri.getHost()
    var dstHost = dstUri.getHost()

    // In HA or when using viewfs, the host part of the URI may not actually be a host, but the
    // name of the HDFS namespace. Those names won't resolve, so avoid even trying if they
    // match.
    if (srcHost != null && dstHost != null && srcHost != dstHost) {
      try {
        srcHost = InetAddress.getByName(srcHost).getCanonicalHostName()
        dstHost = InetAddress.getByName(dstHost).getCanonicalHostName()
      } catch {
        case e: UnknownHostException =>
          return false
      }
    }

    Objects.equal(srcHost, dstHost) && srcUri.getPort() == dstUri.getPort()
  }

  /**
   * Given a local URI, resolve it and return a qualified local path that corresponds to the URI.
   * This is used for preparing local resources to be included in the container launch context.
   */
  private def getQualifiedLocalPath(localURI: URI, hadoopConf: Configuration): Path = {
    val qualifiedURI =
      if (localURI.getScheme == null) {
        // If not specified, assume this is in the local filesystem to keep the behavior
        // consistent with that of Hadoop
        new URI(FileSystem.getLocal(hadoopConf).makeQualified(new Path(localURI)).toString)
      } else {
        localURI
      }
    new Path(qualifiedURI)
  }

  /**
   * Whether to consider jars provided by the user to have precedence over the Spark jars when
   * loading user classes.
   */
  def isUserClassPathFirst(conf: SparkConf, isDriver: Boolean): Boolean = {
    if (isDriver) {
      conf.getBoolean("spark.driver.userClassPathFirst", false)
    } else {
      conf.getBoolean("spark.executor.userClassPathFirst", false)
    }
  }

  /**
   * Joins all the path components using Path.SEPARATOR.
   *
    * @param components 将components这个字符串数组的元素通过"/”连接起来
    * @return
    */
  def buildPath(components: String*): String = {
    components.mkString(Path.SEPARATOR)
  }

  /**
   * Return whether delegation tokens should be retrieved for the given service when security is
   * enabled. By default, tokens are retrieved, but that behavior can be changed by setting
   * a service-specific configuration.
   */
  def shouldGetTokens(conf: SparkConf, service: String): Boolean = {
    conf.getBoolean(s"spark.yarn.security.tokens.${service}.enabled", true)
  }

}
