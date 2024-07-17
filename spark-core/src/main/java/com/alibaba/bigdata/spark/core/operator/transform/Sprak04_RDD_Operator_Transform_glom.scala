package com.alibaba.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Sprak04_RDD_Operator_Transform_glom {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(1,2,3,4 ),2)

    //将同一个分区的数据直接转换为相同类型的内存是数组进行处理，分区不变
    val glm = rdd.glom()


    glm.collect().foreach(
      data=>println(data.mkString(","))
    )



    //TODO 关闭环境
    sc.stop()

  }

}
