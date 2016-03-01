package org.apache.spark.sql.test

case class ABC(val name: String, val age: Int)
object HelloWorld {
  def main(args: Array[String]) {
    val x = ABC("abc", 10)
    println(x.isInstanceOf[Product])

    println(x.isInstanceOf[Tuple2[String,Int]])


    println((1,2,3).isInstanceOf[Product])
    println(("1",2,3).isInstanceOf[Product])
    println((1,2,3).isInstanceOf[Tuple3[Int,Int,Int]])
    println(("1",2,3).isInstanceOf[Tuple3[String, Int, Int]])
  }
}
