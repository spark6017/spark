package org.apache.spark.sql.test

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}


object SparkSQLTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkSQLTestCount").setMaster("local");
    val sc = new SparkContext(conf);
    val ssc = new SQLContext(sc);
    val path = "D:/opensourceprojects/spark20160202/examples/src/main/resources/users.parquet"
    ssc.read.parquet(path).registerTempTable("TBL_USER");
    val df = ssc.sql("select count(name) from TBL_USER where name > 'abc'");
    println(df.queryExecution)
    df.show(20)
    readLine()
    sc.stop
  }
}
