package org.apache.spark.sql.test
import org.apache.spark._

object SparkWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkWordCount").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("file:///D:/opensourceprojects/spark20160202/core/src/main/scala/org/apache/spark/SparkContext.scala", 5)
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _, 3)
    rdd.foreach(println)
    scala.io.StdIn.readLine()
    sc.stop()
  }
}
