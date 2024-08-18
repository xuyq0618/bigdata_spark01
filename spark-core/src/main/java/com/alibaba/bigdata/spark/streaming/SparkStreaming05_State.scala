package com.alibaba.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object SparkStreaming05_State {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp1")

    //无状态数据操作，只对当前采集周期内的数据进行处理
    //在某些场合下，需要保留数据统计结果（状态）,实现数据的汇总
    val datas = ssc.socketTextStream("localhost", 9999)
    val wordToOne= datas.map((_, 1))
//    val wordToCount = wordToOne.reduceByKey(_ + _)

    //updateStateByKey：根据key对数据的状态进行更新
    val state = wordToOne.updateStateByKey(
        (seq: Seq[Int], buff: Option[Int]) => {
          val newCount = buff.getOrElse(0) + seq.sum
          Option(newCount)
        }
      )
    state.print()


    ssc.start()
    ssc.awaitTermination()


  }
}
