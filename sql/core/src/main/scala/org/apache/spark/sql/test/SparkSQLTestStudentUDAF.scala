package org.apache.spark.sql.test

import java.io.File

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object SparkSQLTestStudentUDAF {
  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("SparkSQLTestStudent").setMaster("local");
//    val sc = new SparkContext(conf);
//    val ssc = new SQLContext(sc);

    val m = Map(1->2,2->3)
    val b = m(1)
    println(b.getClass.getName)
    println(b)

  }
}
