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



    //TODO 关闭环境
    ssc.stop()
  }

}
