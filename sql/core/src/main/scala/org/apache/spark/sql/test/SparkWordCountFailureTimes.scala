// scalastyle:off println


package org.apache.spark.sql.test

import java.io.FileNotFoundException

import org.apache.spark._

/**
  * 第一次由于文件不存在而失败，失败后立即重建，第二次重试时会成功
  */
object SparkWordCountFailureTimes {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkWordCount").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(1))
    val curTime = System.currentTimeMillis()
    rdd.map(x => {
      val file = new java.io.File("/home/yuzt/123.txt." + curTime)
      if (!file.exists()) {
        file.createNewFile();
        throw new FileNotFoundException("File doesn't exist for the first time");
      }
      x
    }).foreach(x => println("==========================> " + x))

    scala.io.StdIn.readLine()
    sc.stop()
  }
}

// scalastyle:off println
