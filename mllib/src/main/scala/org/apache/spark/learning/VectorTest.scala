package org.apache.spark.learning

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

object VectorTest {
  def main(args: Array[String]): Unit = {
    val libsvmFile = "file:///D:/opensourceprojects/spark20160202/data/mllib/sample_libsvm_data.txt"
    val conf = new SparkConf().setMaster("local[2]").setAppName("VectorTest")
    val sc = new SparkContext(conf);
    val rdd: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, libsvmFile)

    //All the feature size is the same
    rdd.foreach(x => println(x.features.size))

  }
}
