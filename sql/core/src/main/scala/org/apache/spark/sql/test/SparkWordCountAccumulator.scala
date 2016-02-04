package org.apache.spark.sql.test

import org.apache.spark._

object SparkWordCountAccumulator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkWordCount").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val file = if (OS.linux) {
      "file:////home/yuzt/development/openprojects/spark-2.0.0-snapshot/core/src/main/scala/org/apache/spark/SparkContext.scala"
    } else {
      "file:///D:/opensourceprojects/spark20160202/core/src/main/scala/org/apache/spark/SparkContext.scala"
    }
    val counter = sc.accumulator(0)
    sc.textFile(file, 5)
      .flatMap(_.split(" "))
      .map(word => {
        counter += 1
        (word, 1)
      })
      .reduceByKey(_ + _, 3)
      .foreach(println)
    println("==============>" + counter.value)
    scala.io.StdIn.readLine()
    sc.stop()
  }
}
