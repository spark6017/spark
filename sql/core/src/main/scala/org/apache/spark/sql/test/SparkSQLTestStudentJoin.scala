package org.apache.spark.sql.test

import java.io.File

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object SparkSQLTestStudentJoin {
  def addAdditionalConfigutions(conf : SparkConf, key: String, value : String): Unit = {
    conf.set(key,value)
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkSQLTestStudent").setMaster("local")
//    addAdditionalConfigutions(conf, "spark.sql.TungstenAggregate.testFallbackStartsAt", "5")
    val sc = new SparkContext(conf);
    val ssc = new SQLContext(sc);

    val path =
      if (!OS.linux) {
        "D:\\opensourceprojects\\spark20160202\\sql\\core\\src\\main\\scala\\org\\apache\\spark\\sql\\test\\students.txt"
      } else {
        val projectHome = new File("").getAbsolutePath
        projectHome + "/sql/core/src/main/scala/org/apache/spark/sql/test/students.txt";
      }
    import ssc.implicits._

    val df = sc.textFile(path).map(x => x.split(" ")).map(x => Student(x(0), x(1), x(2).toInt, x(3))).toDF()
    df.registerTempTable("TBL_STUDENT")

    val df2 = ssc.sql("select count(a.name) from TBL_STUDENT a join TBL_STUDENT b on a.classId = b.classId");
//    println(df2.rdd.toDebugString)
//    println(df2.rdd.partitions.length)
    println(df2.queryExecution)
    df2.show(20)
    readLine()
    sc.stop
  }
}
