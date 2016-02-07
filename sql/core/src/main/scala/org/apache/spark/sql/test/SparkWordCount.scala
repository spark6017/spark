package org.apache.spark.sql.test
import org.apache.spark._

object SparkWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkWordCount").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val file = if (OS.linux) {
      "file:////home/yuzt/development/openprojects/spark-2.0.0-snapshot/core/src/main/scala/org/apache/spark/SparkContext.scala"
    } else {
      "file:///c:/abcdx1.txt"
    }

    val rdd = sc.textFile(file, 5)
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _, 3)
    rdd.foreach(println)
    scala.io.StdIn.readLine()
    sc.stop()
  }
}
