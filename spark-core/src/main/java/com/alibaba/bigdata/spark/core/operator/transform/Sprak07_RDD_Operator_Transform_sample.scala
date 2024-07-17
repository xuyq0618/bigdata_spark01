package com.alibaba.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Sprak07_RDD_Operator_Transform_sample {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10 ),2)


    println(rdd.sample(false,0.4).collect().mkString(","))



    //TODO 关闭环境
    sc.stop()

  }

}
