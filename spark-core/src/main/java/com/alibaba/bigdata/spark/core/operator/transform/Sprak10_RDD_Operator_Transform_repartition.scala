package com.alibaba.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Sprak10_RDD_Operator_Transform_repartition {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(1,2,3,4),2)

//扩大分区,底层就是调用coa并且是true
    val value = rdd.repartition(4)//

    value.saveAsTextFile("output1")



    //TODO 关闭环境
    sc.stop()

  }

}
