package com.alibaba.bigdata.spark.core.serial

import org.apache.spark.{SparkConf, SparkContext}

object Sprak01_RDD_Serial {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.makeRDD(List(1,2,3,4))
//    val rdd = sc.makeRDD(List[Int]())
    val user = new User()

    //rdd算子中传递的函数是会包含闭包操作，那么就会进行检测功能(内部用到了外部的变量，改变了变量的生命中周期)
    //闭包检测
    rdd.foreach(num => {println("age="+(user.age + num))}) //算子内部在Executor端执行，但用到了外部的对象（Driver端）


    //TODO 关闭环境
    sc.stop()

  }
//  java.io.NotSerializableException: com.alibaba.bigdata.spark.core.serial.Sprak01_RDD_Serial$User
  class User extends Serializable  {
    var age : Int =30
  }

}
