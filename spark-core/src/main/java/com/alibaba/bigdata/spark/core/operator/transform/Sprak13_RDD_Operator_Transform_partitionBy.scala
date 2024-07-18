package com.alibaba.bigdata.spark.core.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Sprak13_RDD_Operator_Transform_partitionBy {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List((1,2),(3,4)),2)

  //根据一定规则，重分区
    val value = rdd.partitionBy(new HashPartitioner(2)) //

    value.foreach(println)



    //TODO 关闭环境
    sc.stop()

  }

}
