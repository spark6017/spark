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

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.util.regex.Matcher
import java.util.regex.Pattern

import scala.collection.mutable.HashMap
import scala.reflect.runtime._
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{JobConf, Master}
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.{Token, TokenIdentifier}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records.{ApplicationAccessType, ContainerId, Priority}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.launcher.YarnCommandBuilderUtils
import org.apache.spark.util.Utils

/**
 * Contains util methods to interact with Hadoop from spark.
 */
class YarnSparkHadoopUtil extends SparkHadoopUtil {

  private var tokenRenewer: Option[ExecutorDelegationTokenUpdater] = None

  override def transferCredentials(source: UserGroupInformation, dest: UserGroupInformation) {
    dest.addCredentials(source.getCredentials())
  }

  // Note that all params which start with SPARK are propagated all the way through, so if in yarn
  // mode, this MUST be set to true.
  override def isYarnMode(): Boolean = { true }

  // Return an appropriate (subclass) of Configuration. Creating a config initializes some Hadoop
  // subsystems. Always create a new config, dont reuse yarnConf.
  override def newConfiguration(conf: SparkConf): Configuration =
    new YarnConfiguration(super.newConfiguration(conf))

  // Add any user credentials to the job conf which are necessary for running on a secure Hadoop
  // cluster
  override def addCredentials(conf: JobConf) {
    val jobCreds = conf.getCredentials()
    jobCreds.mergeAll(UserGroupInformation.getCurrentUser().getCredentials())
  }

  override def getCurrentUserCredentials(): Credentials = {
    UserGroupInformation.getCurrentUser().getCredentials()
  }

  override def addCurrentUserCredentials(creds: Credentials) {
    UserGroupInformation.getCurrentUser().addCredentials(creds)
  }

  override def addSecretKeyToUserCredentials(key: String, secret: String) {
    val creds = new Credentials()
    creds.addSecretKey(new Text(key), secret.getBytes(UTF_8))
    addCurrentUserCredentials(creds)
  }

  override def getSecretKeyFromUserCredentials(key: String): Array[Byte] = {
    val credentials = getCurrentUserCredentials()
    if (credentials != null) credentials.getSecretKey(new Text(key)) else null
  }

  /**
   * Get the list of namenodes the user may access.
    *
    * 获取Hadoop HDFS的namenode信息
    *
    * @param sparkConf
    * @return
    */
  def getNameNodesToAccess(sparkConf: SparkConf): Set[Path] = {
    sparkConf.get("spark.yarn.access.namenodes", "")
      .split(",")
      .map(_.trim())
      .filter(!_.isEmpty)
      .map(new Path(_))
      .toSet
  }

  def getTokenRenewer(conf: Configuration): String = {
    val delegTokenRenewer = Master.getMasterPrincipal(conf)
    logDebug("delegation token renewer is: " + delegTokenRenewer)
    if (delegTokenRenewer == null || delegTokenRenewer.length() == 0) {
      val errorMessage = "Can't get Master Kerberos principal for use as renewer"
      logError(errorMessage)
      throw new SparkException(errorMessage)
    }
    delegTokenRenewer
  }

  /**
   * Obtains tokens for the namenodes passed in and adds them to the credentials.
   */
  def obtainTokensForNamenodes(
    paths: Set[Path],
    conf: Configuration,
    creds: Credentials,
    renewer: Option[String] = None
  ): Unit = {
    if (UserGroupInformation.isSecurityEnabled()) {
      val delegTokenRenewer = renewer.getOrElse(getTokenRenewer(conf))
      paths.foreach { dst =>
        val dstFs = dst.getFileSystem(conf)
        logInfo("getting token for namenode: " + dst)
        dstFs.addDelegationTokens(delegTokenRenewer, creds)
      }
    }
  }

  private[spark] override def startExecutorDelegationTokenRenewer(sparkConf: SparkConf): Unit = {
    tokenRenewer = Some(new ExecutorDelegationTokenUpdater(sparkConf, conf))
    tokenRenewer.get.updateCredentialsIfRequired()
  }

  private[spark] override def stopExecutorDelegationTokenRenewer(): Unit = {
    tokenRenewer.foreach(_.stop())
  }

  /** *
    * 获取ContainerId
    * @return
    */
  private[spark] def getContainerId: ContainerId = {
    /** *
      * 获取包含containerId字符串的环境变量CONTAINER_ID
      */
    val containerIdString = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name())

    /** *
      * 将containerId字符串转换成ContainerId对象
      */
    ConverterUtils.toContainerId(containerIdString)
  }

  /**
   * Obtains token for the Hive metastore, using the current user as the principal.
   * Some exceptions are caught and downgraded to a log message.
   * @param conf hadoop configuration; the Hive configuration will be based on this
   * @return a token, or `None` if there's no need for a token (no metastore URI or principal
   *         in the config), or if a binding exception was caught and downgraded.
   */
  def obtainTokenForHiveMetastore(conf: Configuration): Option[Token[DelegationTokenIdentifier]] = {
    try {
      obtainTokenForHiveMetastoreInner(conf, UserGroupInformation.getCurrentUser().getUserName)
    } catch {
      case e: ClassNotFoundException =>
        logInfo(s"Hive class not found $e")
        logDebug("Hive class not found", e)
        None
    }
  }

  /**
   * Inner routine to obtains token for the Hive metastore; exceptions are raised on any problem.
   * @param conf hadoop configuration; the Hive configuration will be based on this.
   * @param username the username of the principal requesting the delegating token.
   * @return a delegation token
   */
  private[yarn] def obtainTokenForHiveMetastoreInner(conf: Configuration,
      username: String): Option[Token[DelegationTokenIdentifier]] = {
    val mirror = universe.runtimeMirror(Utils.getContextOrSparkClassLoader)

    // the hive configuration class is a subclass of Hadoop Configuration, so can be cast down
    // to a Configuration and used without reflection
    val hiveConfClass = mirror.classLoader.loadClass("org.apache.hadoop.hive.conf.HiveConf")
    // using the (Configuration, Class) constructor allows the current configuratin to be included
    // in the hive config.
    val ctor = hiveConfClass.getDeclaredConstructor(classOf[Configuration],
      classOf[Object].getClass)
    val hiveConf = ctor.newInstance(conf, hiveConfClass).asInstanceOf[Configuration]
    val metastoreUri = hiveConf.getTrimmed("hive.metastore.uris", "")

    // Check for local metastore
    if (metastoreUri.nonEmpty) {
      require(username.nonEmpty, "Username undefined")
      val principalKey = "hive.metastore.kerberos.principal"
      val principal = hiveConf.getTrimmed(principalKey, "")
      require(principal.nonEmpty, "Hive principal $principalKey undefined")
      logDebug(s"Getting Hive delegation token for $username against $principal at $metastoreUri")
      val hiveClass = mirror.classLoader.loadClass("org.apache.hadoop.hive.ql.metadata.Hive")
      val closeCurrent = hiveClass.getMethod("closeCurrent")
      try {
        // get all the instance methods before invoking any
        val getDelegationToken = hiveClass.getMethod("getDelegationToken",
          classOf[String], classOf[String])
        val getHive = hiveClass.getMethod("get", hiveConfClass)

        // invoke
        val hive = getHive.invoke(null, hiveConf)
        val tokenStr = getDelegationToken.invoke(hive, username, principal).asInstanceOf[String]
        val hive2Token = new Token[DelegationTokenIdentifier]()
        hive2Token.decodeFromUrlString(tokenStr)
        Some(hive2Token)
      } finally {
        Utils.tryLogNonFatalError {
          closeCurrent.invoke(null)
        }
      }
    } else {
      logDebug("HiveMetaStore configured in localmode")
      None
    }
  }

  /**
   * Obtain a security token for HBase.
   *
   * Requirements
   *
   * 1. `"hbase.security.authentication" == "kerberos"`
   * 2. The HBase classes `HBaseConfiguration` and `TokenUtil` could be loaded
   * and invoked.
   *
   * @param conf Hadoop configuration; an HBase configuration is created
   *             from this.
   * @return a token if the requirements were met, `None` if not.
   */
  def obtainTokenForHBase(conf: Configuration): Option[Token[TokenIdentifier]] = {
    try {
      obtainTokenForHBaseInner(conf)
    } catch {
      case e: ClassNotFoundException =>
        logInfo(s"HBase class not found $e")
        logDebug("HBase class not found", e)
        None
    }
  }

  /**
   * Obtain a security token for HBase if `"hbase.security.authentication" == "kerberos"`
   *
   * @param conf Hadoop configuration; an HBase configuration is created
   *             from this.
   * @return a token if one was needed
   */
  def obtainTokenForHBaseInner(conf: Configuration): Option[Token[TokenIdentifier]] = {
    val mirror = universe.runtimeMirror(getClass.getClassLoader)
    val confCreate = mirror.classLoader.
      loadClass("org.apache.hadoop.hbase.HBaseConfiguration").
      getMethod("create", classOf[Configuration])
    val obtainToken = mirror.classLoader.
      loadClass("org.apache.hadoop.hbase.security.token.TokenUtil").
      getMethod("obtainToken", classOf[Configuration])
    val hbaseConf = confCreate.invoke(null, conf).asInstanceOf[Configuration]
    if ("kerberos" == hbaseConf.get("hbase.security.authentication")) {
      logDebug("Attempting to fetch HBase security token.")
      Some(obtainToken.invoke(null, hbaseConf).asInstanceOf[Token[TokenIdentifier]])
    } else {
      None
    }
  }

}

object YarnSparkHadoopUtil {
  // Additional memory overhead
  // 10% was arrived at experimentally. In the interest of minimizing memory waste while covering
  // the common cases. Memory overhead tends to grow with container size.

  /** *
    * Memory Overhead的内存量占要使用的内存量的比例，比如--executor-memory是8G，那么Overhead Memory = 8G * 0.1 = 1024*0.8
    */
  val MEMORY_OVERHEAD_FACTOR = 0.10

  /**
   * 运行在Yarn Container上的进程，额外的内存空间(最小384M)
   */
  val MEMORY_OVERHEAD_MIN = 384

  /** *
    * Container分配到任务Host上
    */
  val ANY_HOST = "*"

  val DEFAULT_NUMBER_EXECUTORS = 2

  /** **
    * All RM requests are issued with same priority : we do not (yet) have any distinction between request types (like map/reduce in hadoop for example)
    *
    * 向RM申请资源的优先级，对于Spark来说，所有的资源的优先级都是一样的，给1
    */
  val RM_REQUEST_PRIORITY = Priority.newInstance(1)

  /** *
    * 获取YarnSparkHadoopUtil对象
    * @return
    */
  def get: YarnSparkHadoopUtil = {

    /** *
      * 首先检查system properties和system env是否定义了SPARK_YARN_MODE=true\false属性
      * 如果没有定义表示Application不是运行在YARN mode上，报错
      * 如果Application运行在YARN mode上，那么获取YarnSparkHadoopUtil实例
      */
    val yarnMode = java.lang.Boolean.valueOf(
      System.getProperty("SPARK_YARN_MODE", System.getenv("SPARK_YARN_MODE")))
    if (!yarnMode) {
      throw new SparkException("YarnSparkHadoopUtil is not available in non-YARN mode!")
    }
    SparkHadoopUtil.get.asInstanceOf[YarnSparkHadoopUtil]
  }
  /**
   * Add a path variable to the given environment map.
   * If the map already contains this key, append the value to the existing value instead.
    *
    * 将key/value加入到env这个HashMap中
   */
  def addPathToEnvironment(env: HashMap[String, String], key: String, value: String): Unit = {

    /**
      * 如果env中已经包含了key这个entry，那么调用getClassPathSeparator获得分隔符拼上value
      * 如果env中没有key这个entry，那么直接加入到env这个HashMap中
      */
    val newValue = if (env.contains(key)) { env(key) + getClassPathSeparator  + value } else value
    env.put(key, newValue)
  }

  /**
   * Set zero or more environment variables specified by the given input string.
   * The input string is expected to take the form "KEY1=VAL1,KEY2=VAL2,KEY3=VAL3".
   */
  def setEnvFromInputString(env: HashMap[String, String], inputString: String): Unit = {
    if (inputString != null && inputString.length() > 0) {
      val childEnvs = inputString.split(",")
      val p = Pattern.compile(environmentVariableRegex)
      for (cEnv <- childEnvs) {
        val parts = cEnv.split("=") // split on '='
        val m = p.matcher(parts(1))
        val sb = new StringBuffer
        while (m.find()) {
          val variable = m.group(1)
          var replace = ""
          if (env.get(variable) != None) {
            replace = env.get(variable).get
          } else {
            // if this key is not configured for the child .. get it from the env
            replace = System.getenv(variable)
            if (replace == null) {
            // the env key is note present anywhere .. simply set it
              replace = ""
            }
          }
          m.appendReplacement(sb, Matcher.quoteReplacement(replace))
        }
        m.appendTail(sb)
        // This treats the environment variable as path variable delimited by `File.pathSeparator`
        // This is kept for backward compatibility and consistency with Hadoop's behavior
        addPathToEnvironment(env, parts(0), sb.toString)
      }
    }
  }

  private val environmentVariableRegex: String = {
    if (Utils.isWindows) {
      "%([A-Za-z_][A-Za-z0-9_]*?)%"
    } else {
      "\\$([A-Za-z_][A-Za-z0-9_]*)"
    }
  }

  /**
   * The handler if an OOM Exception is thrown by the JVM must be configured on Windows
   * differently: the 'taskkill' command should be used, whereas Unix-based systems use 'kill'.
   *
   * As the JVM interprets both %p and %%p as the same, we can use either of them. However,
   * some tests on Windows computers suggest, that the JVM only accepts '%%p'.
   *
   * Furthermore, the behavior of the character '%' on the Windows command line differs from
   * the behavior of '%' in a .cmd file: it gets interpreted as an incomplete environment
   * variable. Windows .cmd files escape a '%' by '%%'. Thus, the correct way of writing
   * '%%p' in an escaped way is '%%%%p'.
   *
   * @return The correct OOM Error handler JVM option, platform dependent.
   */
  def getOutOfMemoryErrorArgument: String = {
    if (Utils.isWindows) {
      escapeForShell("-XX:OnOutOfMemoryError=taskkill /F /PID %%%%p")
    } else {
      "-XX:OnOutOfMemoryError='kill %p'"
    }
  }

  /**
   * Escapes a string for inclusion in a command line executed by Yarn. Yarn executes commands
   * using either
   *
   * (Unix-based) `bash -c "command arg1 arg2"` and that means plain quoting doesn't really work.
   * The argument is enclosed in single quotes and some key characters are escaped.
   *
   * (Windows-based) part of a .cmd file in which case windows escaping for each argument must be
   * applied. Windows is quite lenient, however it is usually Java that causes trouble, needing to
   * distinguish between arguments starting with '-' and class names. If arguments are surrounded
   * by ' java takes the following string as is, hence an argument is mistakenly taken as a class
   * name which happens to start with a '-'. The way to avoid this, is to surround nothing with
   * a ', but instead with a ".
   *
   * @param arg A single argument.
   * @return Argument quoted for execution via Yarn's generated shell script.
   */
  def escapeForShell(arg: String): String = {
    if (arg != null) {
      if (Utils.isWindows) {
        YarnCommandBuilderUtils.quoteForBatchScript(arg)
      } else {
        val escaped = new StringBuilder("'")
        for (i <- 0 to arg.length() - 1) {
          arg.charAt(i) match {
            case '$' => escaped.append("\\$")
            case '"' => escaped.append("\\\"")
            case '\'' => escaped.append("'\\''")
            case c => escaped.append(c)
          }
        }
        escaped.append("'").toString()
      }
    } else {
      arg
    }
  }

  def getApplicationAclsForYarn(securityMgr: SecurityManager)
      : Map[ApplicationAccessType, String] = {
    Map[ApplicationAccessType, String] (
      ApplicationAccessType.VIEW_APP -> securityMgr.getViewAcls,
      ApplicationAccessType.MODIFY_APP -> securityMgr.getModifyAcls
    )
  }

  /**
   * Expand environment variable using Yarn API.
   * If environment.$$() is implemented, return the result of it.
   * Otherwise, return the result of environment.$()
   * Note: $$() is added in Hadoop 2.4.
   */
  private lazy val expandMethod =
    Try(classOf[Environment].getMethod("$$"))
      .getOrElse(classOf[Environment].getMethod("$"))

  /**
   *  expandMethod是Environment类的$或者$$方法，
   *  传入Environment对象，执行expandMethod方法
   * @param environment
   * @return
   */
  def expandEnvironment(environment: Environment): String =
    expandMethod.invoke(environment).asInstanceOf[String]

  /**
   * Get class path separator using Yarn API.
   * If ApplicationConstants.CLASS_PATH_SEPARATOR is implemented, return it.
   * Otherwise, return File.pathSeparator
   * Note: CLASS_PATH_SEPARATOR is added in Hadoop 2.4.
   */
  private lazy val classPathSeparatorField =
    Try(classOf[ApplicationConstants].getField("CLASS_PATH_SEPARATOR"))
      .getOrElse(classOf[File].getField("pathSeparator"))

  def getClassPathSeparator(): String = {
    classPathSeparatorField.get(null).asInstanceOf[String]
  }

  /**
   * Getting the initial target number of executors depends on whether dynamic allocation is
   * enabled.
   * If not using dynamic allocation it gets the number of executors requested by the user.
    *
    * 获取要分配的Executor数目的初始值：
   * 1. 对于静态分配模式，初始值就是用户指定的executor个数
   * 2. 对于动态分配模式，初始值就是用户通过spark.dynamicAllocation.initialExecutors参数指定的个数
   * 动态申请和释放过程中，持有的最大值由spark.dynamicAllocation.maxExecutors控制，持有的最小值由spark.dynamicAllocation.maxExecutors控制
   *
   *
   * @param conf
   * @param numExecutors 默认值2
   * @return
   */
  def getInitialTargetExecutorNumber(
      conf: SparkConf,
      numExecutors: Int = DEFAULT_NUMBER_EXECUTORS): Int = {

    /** *
      * 如果Application是使用Executor动态分配方式
      */
    if (Utils.isDynamicAllocationEnabled(conf)) {
      /**
       * 首先获得最小分配数目，默认值是0
       */
      val minNumExecutors = conf.getInt("spark.dynamicAllocation.minExecutors", 0)

      /** *
        * 其次获取初始的分配数目，默认值是最小分配数目
        */
      val initialNumExecutors =
        conf.getInt("spark.dynamicAllocation.initialExecutors", minNumExecutors)

      /** *
        * 其后获取最大的分配数目，默认值是Integer的最大值
        */
      val maxNumExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors", Int.MaxValue)

      /** *
        * 初始值要大于等于最小值，并且最大值要大于等于初始值
        * 最大值>=初始值>=最小值
        */
      require(initialNumExecutors >= minNumExecutors && initialNumExecutors <= maxNumExecutors,
        s"initial executor number $initialNumExecutors must between min executor number" +
          s"$minNumExecutors and max executor number $maxNumExecutors")

      initialNumExecutors
    }

    /** *
      * 如果Application是使用Executor静态分配方式，那么如果用户指定了申请的个数，则返回用户指定的个数；否则，返回默认的2
      */
    else {
      /** *
        * 第一步：首先从SPARK_EXECUTOR_INSTANCES环境变量中获取，如果获取不到，则取默认值2
        * 第二步：再从spark.executor.instances配置项中获取，如果获取不到那么取第一步获取的executor的数目
        */
      val targetNumExecutors =
        sys.env.get("SPARK_EXECUTOR_INSTANCES").map(_.toInt).getOrElse(numExecutors)
      // System property can override environment variable.
      conf.getInt("spark.executor.instances", targetNumExecutors)
    }
  }
}

