package com.alibaba.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Sprak05_RDD_Operator_Transform_groupby {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List("hello","spark","hello","scala" ),2)

    //分区和分组没有必然联系
    val grpRDD = rdd.groupBy(_.charAt(0))

    grpRDD.collect().foreach(
      println
    )

//    (h, CompactBuffer(hello, hello))
//    (s, CompactBuffer(spark, scala))


    //TODO 关闭环境
    sc.stop()

  }

}
