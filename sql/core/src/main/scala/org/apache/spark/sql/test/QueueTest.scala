package org.apache.spark.sql.test

import scala.collection.immutable.Queue

object QueueTest {
  def main(args: Array[String]): Unit = {
    val a = Queue(1,2,3);
    println(a.front)
    println(a.tail)
  }
}
