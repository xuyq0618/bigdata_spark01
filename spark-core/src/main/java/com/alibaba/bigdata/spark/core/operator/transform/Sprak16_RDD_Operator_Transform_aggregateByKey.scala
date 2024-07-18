package com.alibaba.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Sprak16_RDD_Operator_Transform_aggregateByKey {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(("a",1),("a",4),("a",3),("a",2)),2)

    val value = rdd.aggregateByKey(0)(
      (x,y) => math.max(x,y), // 分区内取最大值
      (x,y) => x + y          //分区间求和
    )

    value.collect().foreach(println)



    //TODO 关闭环境
    sc.stop()

  }

}
