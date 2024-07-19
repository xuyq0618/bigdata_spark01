package com.alibaba.bigdata.spark.core.persist

import org.apache.spark.{SparkConf, SparkContext}


object Spark02_persist_checkpoint {
  def main(args: Array[String]): Unit = {

    //1、建立和spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    sc.setCheckpointDir("cp")

//    2、执行业务操作
    println("执行业务操作。。。")

    val lines = sc.textFile("datas/1.txt")


    val words = lines.flatMap(_.split(" "))

    val wordToOne = words.map(word => {
      println("@@@@@@@@@@@@@@@@@@@")
      (word, 1)
    })

    //需要指定路径，任务结束后不会删除。persist任务结束会删除的
    wordToOne.checkpoint()

    val wordToCount = wordToOne.reduceByKey(_ + _)// spark特有
    wordToCount.collect().foreach(println)

    println("=================================================")

    val wordToCoun1 = wordToOne.groupByKey() // spark特有
    wordToCoun1.collect().foreach(println)


//    3、关闭连接
    sc.stop()

  }

}
