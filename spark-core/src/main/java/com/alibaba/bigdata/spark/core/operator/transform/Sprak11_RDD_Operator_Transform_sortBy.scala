package com.alibaba.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Sprak11_RDD_Operator_Transform_sortBy {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(2,1,3,4),2)

//分区内有序（根据指定规则）
    val value = rdd.sortBy(num => num) //

    value.foreach(println)



    //TODO 关闭环境
    sc.stop()

  }

}
