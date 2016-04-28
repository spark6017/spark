package org.apache.spark.learning

import org.apache.spark.{SparkContext, SparkConf}

case class Employee2(name: String, salary: Double) extends Ordering[Employee] {
  override def compare(x: Employee, y: Employee): Int = if (x.salary > y.salary) 1 else -1
}


object RDDTest2 {
  def main(args: Array[String]): Unit = {
    val e1 = Employee2("abc", 10.1)
    val e2 = Employee2("def", 1000.1)
    val e3 = Employee2("msn", 100.1)
    val e4 = Employee2("xyz", 500.1)
    val e5 = Employee2("uvw", 300.1)

        implicit  val employeeOrdering = new  Ordering[Employee2] {
          override def compare(x: Employee2, y: Employee2): Int = if (x.salary > y.salary) 1 else -1
        }
    val ee = Seq(e1, e2, e3, e4, e5)
    val conf = new SparkConf().setMaster("local").setAppName("RDDTest");
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(ee);
    val top = rdd.top(4)

    top.foreach(x => println(x.name + "----" + x.salary))

  }
}
