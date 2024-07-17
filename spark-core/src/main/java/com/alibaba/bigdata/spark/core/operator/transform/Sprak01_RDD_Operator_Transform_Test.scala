package com.alibaba.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Sprak01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //算子-map

    val rdd = sc.textFile("datas/apache.log")
    val mapRDD = rdd.map(
      line => {
        val data = line.split(" ")
        data(6)
      }
    )
    mapRDD.collect().foreach(println)



    //TODO 关闭环境
    sc.stop()

  }

}
