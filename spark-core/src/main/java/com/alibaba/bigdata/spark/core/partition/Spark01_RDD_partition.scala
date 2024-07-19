package com.alibaba.bigdata.spark.core.partition

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Spark01_RDD_partition {
  def main(args: Array[String]): Unit = {
    //1、建立和spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //2、执行业务操作
    val rdd = sc.makeRDD(List(
      ("NBA", "XXXXXXXXXXXXXXX"),
      ("CBA", "XXXXXXXXXXXXXXX"),
      ("WNBA", "XXXXXXXXXXXXXXX"),
      ("NBA", "XXXXXXXXXXXXXXX")
    ))


    val parRDD = rdd.partitionBy(new MyPartitioner) //分区规则

    parRDD.saveAsTextFile("output1")

    //    3、关闭连接
    sc.stop()
  }

  /**
   * 自定义分区器
   * 1、继承Partitioner
   * 2、重写方法
   */
  class MyPartitioner extends Partitioner {
    //分区数量
    override def numPartitions: Int = 3

    //根据数据的key值返回数据的分区索引（从0开始）
    override def getPartition(key: Any): Int = {
          key match {
            case "NBA" => 0
            case "CBA" => 1
            case _ => 2
          }

    }
  }

}
