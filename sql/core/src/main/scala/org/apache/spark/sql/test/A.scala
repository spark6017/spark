package org.apache.spark.sql.test

/**
 * Created by yuzhitao on 2016/2/20.
 */
object A {

  def getClassloader = {
    println("classname is " + getClass.getName)
    getClass.getClassLoader
  }
}
