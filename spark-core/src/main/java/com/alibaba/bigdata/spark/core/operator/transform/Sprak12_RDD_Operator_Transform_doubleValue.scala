package com.alibaba.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Sprak12_RDD_Operator_Transform_doubleValue {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-双value类型
    val rdd1 = sc.makeRDD(List(1,2,3,4))
    val rdd2 = sc.makeRDD(List(3,4,5,6))


    //交集
    rdd1.intersection(rdd2)

    //并集
    rdd1.union(rdd2)

    //差集
    rdd1.subtract(rdd2)

    //拉链
    rdd1.zip(rdd2)




    //TODO 关闭环境
    sc.stop()

  }

}
