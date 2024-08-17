package com.alibaba.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))





    println("===============================")
    //TODO 逻辑处理
    //获取端口数据
    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))

    val wordToOne = words.map((_, 1))

    val wordToCount = wordToOne.reduceByKey(_ + _)

    wordToCount.print()


//
//    //TODO 关闭环境
//    ssc.stop()

    //1、启动采集器
    ssc.start()
    //2、等待采集器的关闭
    ssc.awaitTermination()

  }

}
