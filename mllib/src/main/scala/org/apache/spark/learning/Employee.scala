package org.apache.spark.learning

import org.apache.spark.{SparkContext, SparkConf}

case class Employee(name: String, salary: Double)



object RDDTest {
  def main(args: Array[String]): Unit = {
    val e1 = Employee("abc", 10.1)
    val e2 = Employee("def", 1000.1)
    val e3 = Employee("msn", 100.1)
    val e4 = Employee("xyz", 500.1)
    val e5 = Employee("uvw", 300.1)

    implicit  val employeeOrdering = new  Ordering[Employee] {
      override def compare(x: Employee, y: Employee): Int = if (x.salary > y.salary) 1 else -1
    }
    val ee = Seq(e1, e2, e3, e4, e5)
    val conf = new SparkConf().setMaster("local").setAppName("RDDTest");
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(ee);
    val top = rdd.top(3)

    top.foreach(x=>println(x.name + "," + x.salary))

  }
}
