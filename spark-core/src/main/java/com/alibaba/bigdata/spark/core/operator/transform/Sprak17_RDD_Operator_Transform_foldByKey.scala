package com.alibaba.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Sprak17_RDD_Operator_Transform_foldByKey {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(("a",1),("a",4),("a",3),("a",2)),2)

    val value = rdd.aggregateByKey(0)(
      (x,y) => math.max(x,y), // 分区内取最大值
      (x,y) => x + y          //分区间求和
    )

    //if分区间和分区内规则相同 ==》foldByKey
    value.collect().foreach(println)

    //combineByKey 对aggregateByKey相同key的第一个数据进行了一定的处理



    //TODO 关闭环境
    sc.stop()

  }

}
