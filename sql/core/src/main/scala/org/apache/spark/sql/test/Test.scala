package org.apache.spark.sql.test

object Test {
  def main(args: Array[String]): Unit = {
    val a = Map((1, 2) -> 12, (3, 4) -> 34)
    println(a(1, 2))
  }
}
