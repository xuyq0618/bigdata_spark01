package com.alibaba.bigdata.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}


object Spark02_WordCount {
  def main(args: Array[String]): Unit = {

    //1、建立和spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)


//    2、执行业务操作
    println("执行业务操作。。。")

    val lines = sc.textFile("datas/*")


    val words = lines.flatMap(_.split(" "))

    val wordToOne = words.map(word => (word, 1))


    val wordGroup = wordToOne.groupBy(t => t._1)


    val wordToCount = wordGroup.map {
      case (word, list) => {
       list.reduce((t1,t2)=>{(t1._1,t1._2+t2._2)})
      }
    }


    val array = wordToCount.collect()
    array.foreach(println)


//    3、关闭连接
    sc.stop()

  }

}
