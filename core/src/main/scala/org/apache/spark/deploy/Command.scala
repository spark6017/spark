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

package org.apache.spark.deploy

import scala.collection.Map

/***
  * 启动Java进程相关的配置项
  * 问题： 怎么把Command的这些属性用起来
  * @param mainClass 主类
  * @param arguments 提供给主类使用的参数列表
  * @param environment 环境变量信息 每个Java进程都有自己的System Properties
  * @param classPathEntries Java进程依赖的classpath
  * @param libraryPathEntries Java进程依赖的库文件
  * @param javaOpts JVM配置项，比如设置内存-Xms -Xmx等
  */
private[spark] case class Command(
    mainClass: String,
    arguments: Seq[String],
    environment: Map[String, String],
    classPathEntries: Seq[String],
    libraryPathEntries: Seq[String],
    javaOpts: Seq[String]) {
}
