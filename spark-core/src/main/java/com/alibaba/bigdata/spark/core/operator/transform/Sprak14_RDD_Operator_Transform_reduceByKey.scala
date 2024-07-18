package com.alibaba.bigdata.spark.core.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Sprak14_RDD_Operator_Transform_reduceByKey {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(("a",1),("a",4),("a",3),("b",2)))

  //reduceByKey:相同的 KEY的数据进行value的聚合
    val value = rdd.reduceByKey(_+_)

    value.collect().foreach(println)



    //TODO 关闭环境
    sc.stop()

  }

}
