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

import java.util.{List => JList}

import scala.collection.{Map, Set}
import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.webapp.util.WebAppUtils

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.SplitInfo
import org.apache.spark.util.Utils

/**
 * Handles registering and unregistering the application with the YARN ResourceManager.
 */
private[spark] class YarnRMClient(args: ApplicationMasterArguments) extends Logging {

  /** *
    * 定义AMRMClient，AMRMClient是一个泛型类型，此处指定的泛型类是ContainerRequest
    * 问题：一个ContainerRequest包含哪些信息？包含资源量、资源位置信息
    */
  private var am2rmClient: AMRMClient[ContainerRequest] = _

  /** *
    * 问题：这个uiHistoryAddress，是Spark History Address的地址？
    */
  private var uiHistoryAddress: String = _
  private var registered: Boolean = false

  /**
   * Registers the application master with the RM.
   * 将ApplicationMaster注册到ResourceManager，这是ApplicationMaster进程自己的事情，它发生在ApplicationMaster进程已经启动，然后
   * ApplicationMaster进程主动将它自身注册给ResourceManager
   *
   * @param conf The Yarn configuration.
   * @param sparkConf The Spark configuration.
   * @param uiAddress Address of the SparkUI.
   * @param uiHistoryAddress Address of the application on the Spark History Server.
    * @param driverUrl
    * @param driverRef Driver Endpoint Ref
    * @param securityMgr
    * @return
    */
  def register(
      driverUrl: String,
      driverRef: RpcEndpointRef,
      conf: YarnConfiguration,
      sparkConf: SparkConf,
      applicationTrackingUrl: String,
      uiHistoryAddress: String,
      securityMgr: SecurityManager
    ): YarnAllocator = {

    /**
      * 创建、初始化、启动ApplicationMaster向ResourceManager发送RPC请求的Client
      */
    am2rmClient = AMRMClient.createAMRMClient()
    am2rmClient.init(conf)
    am2rmClient.start()
    this.uiHistoryAddress = uiHistoryAddress

    logInfo("Registering the ApplicationMaster")

    /**
      * AM向RM发送Application Master 注册请求，这是在Application Master Container将Application Master进程启动起来后，
     * Application Master做的事情
      */
    synchronized {
      am2rmClient.registerApplicationMaster(Utils.localHostName(), 0, applicationTrackingUrl)
      registered = true
    }

    /**
      * 初始化一个YarnAllocator对象,args是一个关键对象,它是ApplicationMasterArguments类型的的对象
      * ApplicationMasterArguments包含了一个--properties-file参数，这个参数指定了要申请多少个executor
      */
    new YarnAllocator(driverUrl, driverRef, conf, sparkConf, am2rmClient, getAttemptId(), args,
      securityMgr)
  }

  /**
   * Unregister the AM. Guaranteed to only be called once.
   *
   * 注销Application Master
   *
   * @param status The final status of the AM.
   * @param diagnostics Diagnostics message to include in the final status.
   */
  def unregister(status: FinalApplicationStatus, diagnostics: String = ""): Unit = synchronized {
    if (registered) {
      am2rmClient.unregisterApplicationMaster(status, diagnostics, uiHistoryAddress)
    }
  }

  /** *
    * Returns the attempt ID.
    * YarnRMClient定义的获取ApplicationAttemptId对象的方法
    * @return
    */
  def getAttemptId(): ApplicationAttemptId = {

    /** *
      * 1. 首先获取YarnSparkHadoopUtil实例
      * 2. 调用YarnSparkHadoopUtil的getContainerId获得ContainerId对象
      * 3. 调用ContainerId的getApplicationAttemptId方法
      *
      * 问题：这个跟ContainerId什么关系？为什么要通过这个类型的对象去获取ApplicationAttemptId
      */
    YarnSparkHadoopUtil.get.getContainerId.getApplicationAttemptId()
  }

  /** Returns the configuration for the AmIpFilter to add to the Spark UI. */
  def getAmIpFilterParams(conf: YarnConfiguration, proxyBase: String): Map[String, String] = {
    // Figure out which scheme Yarn is using. Note the method seems to have been added after 2.2,
    // so not all stable releases have it.
    val prefix = Try(classOf[WebAppUtils].getMethod("getHttpSchemePrefix", classOf[Configuration])
      .invoke(null, conf).asInstanceOf[String]).getOrElse("http://")

    // If running a new enough Yarn, use the HA-aware API for retrieving the RM addresses.
    try {
      val method = classOf[WebAppUtils].getMethod("getProxyHostsAndPortsForAmFilter",
        classOf[Configuration])
      val proxies = method.invoke(null, conf).asInstanceOf[JList[String]]
      val hosts = proxies.asScala.map { proxy => proxy.split(":")(0) }
      val uriBases = proxies.asScala.map { proxy => prefix + proxy + proxyBase }
      Map("PROXY_HOSTS" -> hosts.mkString(","), "PROXY_URI_BASES" -> uriBases.mkString(","))
    } catch {
      case e: NoSuchMethodException =>
        val proxy = WebAppUtils.getProxyHostAndPort(conf)
        val parts = proxy.split(":")
        val uriBase = prefix + proxy + proxyBase
        Map("PROXY_HOST" -> parts(0), "PROXY_URI_BASE" -> uriBase)
    }
  }

  /** Returns the maximum number of attempts to register the AM. */
  def getMaxRegAttempts(sparkConf: SparkConf, yarnConf: YarnConfiguration): Int = {
    val sparkMaxAttempts = sparkConf.getOption("spark.yarn.maxAppAttempts").map(_.toInt)
    val yarnMaxAttempts = yarnConf.getInt(
      YarnConfiguration.RM_AM_MAX_ATTEMPTS, YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS)
    val retval: Int = sparkMaxAttempts match {
      case Some(x) => if (x <= yarnMaxAttempts) x else yarnMaxAttempts
      case None => yarnMaxAttempts
    }

    retval
  }

}
