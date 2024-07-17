package com.alibaba.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 创建RDD
    //从文件中创建
//    val rdd = sc.textFile("datas/*") //以行为单位
    val rdd = sc.wholeTextFiles("datas/*") //识别数据来自于哪个文件
    rdd.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }

}
