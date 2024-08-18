package com.alibaba.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming08_State_Window {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(6))

    //窗口的范围应该是采集周期的整数倍
    //窗口默认滑动步长默认是一个采集周期，但是又可能会出现重复计算，所以可以改变滑动的步长
    val lines = ssc.socketTextStream("localhost", 9999)
    val wordToOne = lines.map((_, 1))
    val windowDS = wordToOne.window(Seconds(6),Seconds(6))
    val wordToCount = windowDS.reduceByKey(_ + _)
    wordToCount.print()






    ssc.start()
    ssc.awaitTermination()


  }
}
