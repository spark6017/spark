package org.apache.spark.sql.test

import java.io.{FileReader, BufferedReader}

/**
 */
object Hello {
  def main(args: Array[String]) {
    val file = new java.io.File("D:\\1\\jcollector\\jcollector\\conf\\ClientConfig.properties")
    val br = new BufferedReader(new FileReader(file));
    val p = new java.util.Properties();
    p.load(br)
    br.close()
    val keys = p.propertyNames()
    while (keys.hasMoreElements) {
      val key = keys.nextElement();
      println(key + "=" + p.get(key))
    }

  }
}
