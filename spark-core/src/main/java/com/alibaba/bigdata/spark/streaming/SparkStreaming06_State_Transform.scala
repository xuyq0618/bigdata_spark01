package com.alibaba.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Transform {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines = ssc.socketTextStream("localhost", 9999)
//    val newDS =  lines.transform(rdd => rdd)
//    val newDS =  lines.transform(rdd => rdd.map(str=>str))

    //transform方法可以将底层RDD获取到后进行操作
    //应用场景：1、DSstream功能不完善 2、需要代码周期性执行
    //Code:Driver端
    val newDS =  lines.transform(
      rdd => {
        //Code:Driver端（周期性执行）
        rdd.map(
          str=> {
            //Code:Executor端
            str
          }
        )
      }
)

//    val newDS1 = lines.map(str => str)
    //Code:Driver端
    val newDS1 = lines.map(
      //Code:Executor端
      data => data
    )

    ssc.start()
    ssc.awaitTermination()


  }
}
