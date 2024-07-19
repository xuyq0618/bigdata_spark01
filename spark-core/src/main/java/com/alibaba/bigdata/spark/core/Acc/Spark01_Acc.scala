package com.alibaba.bigdata.spark.core.Acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {
  def main(args: Array[String]): Unit = {
    //1、建立和spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //2、累加器
    val rdd = sc.makeRDD(List(1,2,3,4))
    //获取系统累加器
    val sumAcc = sc.longAccumulator("sum")

    rdd.foreach(
      num => {
        sumAcc.add(num)
      }
    )

    //获取累加器的值
    println(sumAcc.value)

    //    3、关闭连接
    sc.stop()
  }

}
