package com.alibaba.bigdata.spark.core.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Sprak01_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd1 = sc.makeRDD(List(1,2,3,4))
    val rdd2 = sc.makeRDD(List(("a",1),("b",2),("c",3),("d",4)))

    //reduce 两两相加
//    val value = rdd1.reduce(_ + _)
//    println(value)

    //collect :以分区顺序采集到driver端
//    rdd1.collect() //行动算子（能够触发作业执行的方法）

    //count:数据源中数据的个数
//    val l = rdd1.count()
//    println(l)

    //fisrt:数据源中第一个
//    val l = rdd1.first()
//    println(l)

    //take 获取n个数据
//        val l = rdd1.take(3)
//        println(l.mkString(","))

    //takeorderd: 获取n个数据
//    val l = rdd1.takeOrdered(3)
//    println(l.mkString(","))

    //aggregate(直接触发作业执行)（初始值参与分区间和分区内计算），这是与aggregateByKey的区别
//    val result = rdd1.aggregate(0)(_ + _,_ + _)
//    println(result)

    //fold(直接触发作业执行)（初始值参与分区间和分区内计算），这是与aggregateByKey的区别
//    val result = rdd1.fold(0)( _ + _)
//    println(result)

    //countByKey:统计key出现的次数
//    val intToLong = rdd2.countByKey()
//    println(intToLong)

    //countByValue:统计value出现的次数
//    val intToLong = rdd1.countByValue()
//    println(intToLong)


    //save
//    rdd1.saveAsTextFile("output1")
//    rdd1.saveAsObjectFile("output2")
//    rdd2.saveAsSequenceFile("output3")

//foreach
    rdd1.collect().foreach(println) //以分区顺序采集到driver端，再循环打印
    rdd1.foreach(println) //在Executor端打印，并行计算，打印先后顺序不一定

    //算子：Operator(操作)
//    RDD方法和scala集合对象的方法不一样
//    集合对象的方法都是在同一个节点的内存中完成的
//    RDD的方法可以将计算逻辑发送到Executor端（分布式节点）执行
//    为了区分不同的处理效果，所以将RDD的方法称之为算子
//    RDD方法外部操作都是在Driver端执行，RDD方法内部逻辑代码是在Executor端执行


    //TODO 关闭环境
    sc.stop()

  }

}
