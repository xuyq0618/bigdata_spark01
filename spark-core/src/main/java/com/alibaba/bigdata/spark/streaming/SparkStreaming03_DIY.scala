package com.alibaba.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.Random

object SparkStreaming03_DIY {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val message = ssc.receiverStream(new MyReceiver)
    message.print()


    ssc.start()
    ssc.awaitTermination()

  }

  /**
   * 自定义数据采集器
   */
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
    private var flg = true
    override def onStart(): Unit = {
      new Thread(
        new Runnable {
          override def run(): Unit = {
            while(flg) {
              val message = "采集的数据为："+ new Random().nextInt(10).toString
              store(message)
              Thread.sleep(500)
            }
          }
        }
      ).start()
    }

    override def onStop(): Unit = {
      flg = false
    }
  }
}
