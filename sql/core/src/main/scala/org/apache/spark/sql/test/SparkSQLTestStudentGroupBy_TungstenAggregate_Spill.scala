package org.apache.spark.sql.test

import java.io.File

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object SparkSQLTestStudentGroupBy_TungstenAggregate_Spill {
  def addAdditionalConfigutions(conf : SparkConf, key: String, value : String): Unit = {
    conf.set(key,value)
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkSQLTestStudent").setMaster("local")

    //force to spill
    addAdditionalConfigutions(conf, "spark.sql.TungstenAggregate.testFallbackStartsAt", "2")
    val sc = new SparkContext(conf);
    val ssc = new SQLContext(sc);

    val path =
      if (!OS.linux) {
        "D:\\opensourceprojects\\spark20160202\\sql\\core\\src\\main\\scala\\org\\apache\\spark\\sql\\test\\students_tungstenaggregate_spill.txt"
      } else {
        val projectHome = new File("").getAbsolutePath
        projectHome + "/sql/core/src/main/scala/org/apache/spark/sql/test/students_tungstenaggregate_spill.txt";
      }
    import ssc.implicits._

    val df = sc.textFile(path).map(x => x.split(" ")).map(x => Student(x(0), x(1), x(2).toInt, x(3))).toDF()
    df.registerTempTable("TBL_STUDENT")

    val df2 = ssc.sql("select count(name) from TBL_STUDENT group by classId");

    println(df2.queryExecution)
    df2.show(20)
    readLine()
    sc.stop
  }
}
