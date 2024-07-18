package com.alibaba.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Sprak18_RDD_Operator_Transform_join {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd1 = sc.makeRDD(List(("a",1),("b",3),("c",2)))
    val rdd2 = sc.makeRDD(List(("b",1),("a",4),("c",3),("d",1)))

    val value = rdd1.join(rdd2)
    val value2 = rdd1.leftOuterJoin(rdd2)
    val value3 = rdd1.rightOuterJoin(rdd2)

    //cogroup：connect+group
    val value4 = rdd1.cogroup(rdd2)


    //if分区间和分区内规则相同 ==》foldByKey
//    println("=============")
//    value.collect().foreach(println)
//    println("=============")
//    value2.collect().foreach(println)
//    println("=============")
//    value3.collect().foreach(println)
      value4.collect().foreach(println)

//    (a, (1, 4))
//    (b, (3, 1))
//    (c, (2, 3))

    //TODO 关闭环境
    sc.stop()

  }

}
