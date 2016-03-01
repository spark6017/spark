package org.apache.spark.sql.test

/**
  * Created by yuzt on 16-3-1.
  */
object SeqTest {
  def main(args : Array[String]): Unit = {
    val s = Seq(0,1,2)
    println(s(1).getClass.getSimpleName);
  }
}
