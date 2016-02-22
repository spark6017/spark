package org.apache.spark.sql.test

import java.net.URI

/**
  * Created by yuzt on 16-2-22.
  */
object URISchemeTest {
  def main(args: Array[String]) {
    val a = new URI("file://abc.jar")
    println(a.getScheme)

    val b = new URI("local://abc.jar")
    println(b.getScheme)

  }

}
