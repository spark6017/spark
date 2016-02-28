package org.apache.spark.sql.test

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object SparkSQLTestStudent {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkSQLTestStudent").setMaster("local");
    val sc = new SparkContext(conf);
    val ssc = new SQLContext(sc);

    val path =
      if (!OS.linux) {
        "D:/opensourceprojects/spark20160202/examples/src/main/resources/users.parquet"
      } else {
        "/home/yuzt/development/openprojects/spark-2.0.0-snapshot/sql/core/src/main/scala/org/apache/spark/sql/test/students.txt"
      }
    import ssc.implicits._

    val df = sc.textFile(path).map(x => x.split(" ")).map(x => Student(x(0), x(1), x(2).toInt, x(3))).toDF()
    df.registerTempTable("TBL_STUDENT")

//    val df2 = ssc.sql("select classId, count(id) from TBL_STUDENT group by classId");
//val df2 = ssc.sql("select classId, avg(age) from TBL_STUDENT group by classId");
//val df2 = ssc.sql("select classId, avg(age) as avg from TBL_STUDENT group by classId order by avg");

    val df2 = ssc.sql("select classId,name,age from TBL_STUDENT");
    println(df2.queryExecution)
    df2.show(20)
    readLine()
    sc.stop
  }
}
