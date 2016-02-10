package org.apache.spark.sql.test.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite


class SparkSQLTest extends FunSuite {
  test("Test Count  (Count.Scala)") {
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local");
    val sc = new SparkContext(conf);
    val ssc = new SQLContext(sc);
    val path = "D:/opensourceprojects/spark20160202/examples/src/main/resources/users.parquet"
    ssc.read.parquet(path).registerTempTable("TBL_USER");
    val df = ssc.sql("select count(name) from TBL_USER");
    println(df.queryExecution)
    df.show(20)
    sc.stop
  }

}
