package org.apache.spark.learning.MachineLearningWithSpark.ch04recommend

import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object MovieLenTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("MovieLenTest");
    val sc = new SparkContext(conf)
    val udata = sc.textFile("file:///D:/documents/Spark/SparkEbooks/ml-100k/ml-100k/u.data")

    udata.cache()

    //u.data文件中的第一行数据(类型为't'分割的字符串)，196	242	3	881250949
    val first = udata.first()
    println("first user data: " + first)

    //剔除时间戳字段，同时将userId,productId以及rating转换为Rating对象
    val ratings = udata.map(_.split("\t")).map { case Array(userId, movieId, rating, _) => Rating(userId.toInt, movieId.toInt, rating.toDouble) }
    println("first rating data: " + ratings.first())

    //基于训练数据集创建模型(或者称为训练模型),返回的是一个MatrixFactorizationModel
    val model: MatrixFactorizationModel = ALS.train(ratings, 50, 10)

    //user-factor矩阵，每一行是一个二元元组，元组的第一个元素是UserID，元组的第二个元素是latent feature
    val userMetrix: RDD[(Int, Array[Double])] = model.userFeatures
    val productMetrix: RDD[(Int, Array[Double])] = model.productFeatures

    val userCount = userMetrix.count()
    val productCount = productMetrix.count();

    //用户数和电影数，分别跟u.user和u.item文件的行数一致
    println(s"Count of userMetrix is $userCount , Count of productMetrix is $productCount")
    //first user id is 1
    println(s"The first userId  is ${userMetrix.first()._1}, the latent feature ${userMetrix.first()._2.length }, they are \n ${userMetrix.first()._2.mkString("\n")}")

    //使用模型进行预测
    val score = model.predict(789,123)
    println(s"score is : $score")

    //仅仅列出Rating集合，即只有user id, product id, score
    val products = model.recommendProducts(789, 10)
    products.foreach(println)


    //将推荐的电影名等信息列出来
    val itemData = sc.textFile("file:///D:/documents/Spark/SparkEbooks/ml-100k/ml-100k/u.item")
    val movieId2TitleMap = itemData.map(_.split("\\|")).map(x=>x.take(2)).map(x => (x(0).toInt, x(1))).collectAsMap()
    println("The title of 123th movie is " +movieId2TitleMap(123))

    val ratingsBy789 = ratings.keyBy(_.user).lookup(789)
    ratingsBy789.foreach(x => println(x.user + ", " + x.product + "," + movieId2TitleMap(x.product) + ", " + x.rating))

    readLine()
  }
}
