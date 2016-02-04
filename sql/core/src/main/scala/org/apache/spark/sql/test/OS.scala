package org.apache.spark.sql.test


object OS {
  def linux: Boolean = {
    val os = System.getProperty("os.name")
    os.toLowerCase.contains("linux")
  }
}
