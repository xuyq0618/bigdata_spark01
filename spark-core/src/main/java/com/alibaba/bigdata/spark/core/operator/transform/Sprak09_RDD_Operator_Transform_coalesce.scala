package com.alibaba.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Sprak09_RDD_Operator_Transform_coalesce {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(1,2,3,4),4)

//缩减分区，默认不会将分区数据打乱重新组合，可能会出现数据倾斜
    //第二个参数是允许shuffle
    val value = rdd.coalesce(2,true)

    value.saveAsTextFile("output1")



    //TODO 关闭环境
    sc.stop()

  }

}
