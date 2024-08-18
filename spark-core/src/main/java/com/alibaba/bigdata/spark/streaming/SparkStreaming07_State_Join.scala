package com.alibaba.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming07_State_Join {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(10))


    val data9999 = ssc.socketTextStream("localhost", 9999)
    val data8888 = ssc.socketTextStream("localhost", 8888)

    val map9999 = data9999.map((_, 9))
    val map8888 = data8888.map((_, 8))

    //所谓的DStream的Join操作，就是两个RDD的join
    val joinDS = map9999.join(map8888)
    joinDS.print()






    ssc.start()
    ssc.awaitTermination()


  }
}
