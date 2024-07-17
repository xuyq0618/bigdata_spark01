package com.alibaba.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Sprak08_RDD_Operator_Transform_distinct {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(1,1,2,3,4,5,2,3,4,5,6,7,8,9,10 ),2)


    val value = rdd.distinct()

    value.collect().foreach(println)



    //TODO 关闭环境
    sc.stop()

  }

}
