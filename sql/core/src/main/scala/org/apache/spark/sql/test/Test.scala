package org.apache.spark.sql.test

object Test {
  def main(args: Array[String]): Unit = {
//    val a = Map((1, 2) -> 12, (3, 4) -> 34)
//    println(a(1, 2))
//
//
//    val b = Map(1->"1", 2->"2")
//    println(b.get(1).map(_.toInt))
//    println(b.get(3).map(_.toInt))
//
//
//    val c = List(3,5,1,2)
//    println(c.sortBy(x=>x).reverse)
//    println(c.sorted.reverse)
    println(Runtime.getRuntime.maxMemory()/ (1024*1024))
  }
}
