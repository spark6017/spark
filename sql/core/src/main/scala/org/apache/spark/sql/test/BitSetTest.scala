package org.apache.spark.sql.test

object BitSetTest {
  def main(args: Array[String]): Unit = {
    println("1>>6=" + (1 >> 6))
    println("2>>6=" + (2 >> 6))
    println("1&(0x3f)=" + (1&(0x3f))) //3f是64-1
    println("2&(0x3f)=" + (2&(0x3f)))
    println("64&(0x3f)=" + (64&(0x3f)))
  }
}
