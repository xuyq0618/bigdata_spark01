package com.alibaba.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Sprak15_RDD_Operator_Transform_groupByKey {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(("a",1),("a",4),("a",3),("b",2)))

    //groupByKey:将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
    //元组中的第一个元素就是key
    //元组中的第二个元素就是相同key的value的集合
    val value = rdd.groupByKey()

    //和groupby区别：
    // 1、groupby不确定根据什么分组，需要指定。groupByKey确定就是根据key分组
//    2、返回值不同：groupby没有将key单独拎出来




    value.collect().foreach(println)
//    (a,CompactBuffer(1, 4, 3))
//    (b,CompactBuffer(2))


    //TODO 关闭环境
    sc.stop()

  }

}
