package org.apache.spark.sql.test

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQLTestCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQLTestCount").setMaster("local");
    val sc = new SparkContext(conf);
    val ssc = new SQLContext(sc);
    val path = "D:/opensourceprojects/spark20160202/examples/src/main/resources/users.parquet"
    ssc.read.parquet(path).registerTempTable("TBL_USER");
    val df = ssc.sql("select count(favorite_color)  from TBL_USER");
    println(df.printSchema())
    df.show(20)
    sc.stop
  }


}