package com.alibaba.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Sprak03_RDD_Operator_Transform_flatMap {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List("HELLO SCALA, HELLO SPARK"),2)

    val mpi = rdd.flatMap(_.split(" "))

    mpi.collect().foreach(println)



    //TODO 关闭环境
    sc.stop()

  }

}
