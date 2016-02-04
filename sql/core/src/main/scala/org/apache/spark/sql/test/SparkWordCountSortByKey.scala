package org.apache.spark.sql.test

import org.apache.spark._

object SparkWordCountSortByKey {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkWordCount").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val file = if (OS.linux) {
      "file:////home/yuzt/development/openprojects/spark-2.0.0-snapshot/core/src/main/scala/org/apache/spark/SparkContext.scala"
    } else {
      "file:///D:/opensourceprojects/spark20160202/core/src/main/scala/org/apache/spark/SparkContext.scala"
    }

    val rdd = sc.textFile(file, 5)
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _, 3)
      .map(_.swap)
      .sortByKey(numPartitions = 3)
    rdd.saveAsTextFile("file:///home/yuzt/" + System.currentTimeMillis())
    scala.io.StdIn.readLine()
    sc.stop()
  }
}
