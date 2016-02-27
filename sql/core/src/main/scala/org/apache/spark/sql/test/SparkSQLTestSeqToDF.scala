package org.apache.spark.sql.test

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.{SparkConf, SparkContext}

class MutableA(var age: Int) {
  def set(age: Int) = {
    this.age = age;
    this.age
  }
}

object SparkSQLTestSeqToDF {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkSQLTestCount").setMaster("local");
    val sc = new SparkContext(conf);
    val ssc = new SQLContext(sc);
    //    import ssc.implicits.intRddToDataFrameHolder
    //    val df = sc.parallelize(Seq(1,2,3,4,5,6,7,8,9,10,1,2,3,4,5,6,7,8,9,10)).toDF
    //    df.show()

    sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).mapPartitions({

      iter =>
        val a = new MutableA(0)
        iter.map(x => a.set(x + 100))
    }).foreach(println)

    sc.stop
  }
}
