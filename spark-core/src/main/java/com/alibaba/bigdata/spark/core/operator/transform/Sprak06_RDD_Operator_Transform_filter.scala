package com.alibaba.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Sprak06_RDD_Operator_Transform_filter {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List("hello","spark","hello","scala" ),2)


    val  fltRDD = rdd.filter(_.charAt(0)!="h")

    fltRDD.collect().foreach(
      println
    )

//    (h, CompactBuffer(hello, hello))
//    (s, CompactBuffer(spark, scala))


    //TODO 关闭环境
    sc.stop()

  }

}
