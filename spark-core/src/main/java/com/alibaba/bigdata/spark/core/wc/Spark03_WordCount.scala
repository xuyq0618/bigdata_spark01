package com.alibaba.bigdata.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}


object Spark03_WordCount {
  def main(args: Array[String]): Unit = {

    //1、建立和spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)


//    2、执行业务操作
    println("执行业务操作。。。")

    val lines = sc.textFile("datas/*")


    val words = lines.flatMap(_.split(" "))

    val wordToOne = words.map(word => (word, 1))


    val wordToCount = wordToOne.reduceByKey(_ + _)// spark特有


    val array = wordToCount.collect()
    array.foreach(println)


//    3、关闭连接
    sc.stop()

  }

}
