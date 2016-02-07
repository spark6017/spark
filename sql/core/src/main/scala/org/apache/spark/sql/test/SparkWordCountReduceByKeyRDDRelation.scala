package org.apache.spark.sql.test

import org.apache.spark._

object SparkWordCountReduceByKeyRDDRelation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkWordCount").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val file = if (OS.linux) {
      "file:////home/yuzt/development/openprojects/spark-2.0.0-snapshot/core/src/main/scala/org/apache/spark/SparkContext.scala"
    } else {
      "file:///D:/opensourceprojects/spark20160202/core/src/main/scala/org/apache/spark/SparkContext.scala"
    }


    /**
     * HadoopRDD=>MapPartitionsRDD=>MapPartitionsRDD=>MapPartitionsRDD=>ShuffledRDD
     */
    val rdd = sc.textFile(file, 5)
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _, 3)

    println(rdd.toDebugString)

    rdd.count

    scala.io.StdIn.readLine()
    sc.stop()
  }
}
