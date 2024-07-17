package com.alibaba.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //从内存中创建
    val seq = Seq(1,2,3,4)

    val rdd = sc.makeRDD(seq,2) //2个分区，不填默认主机核数
    rdd.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }

}
