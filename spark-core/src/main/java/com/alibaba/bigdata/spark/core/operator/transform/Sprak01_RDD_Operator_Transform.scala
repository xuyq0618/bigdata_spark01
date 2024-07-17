package com.alibaba.bigdata.spark.core.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Sprak01_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    //算子-map/mapPartition/mapPartitionsWithIndex
    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)

//    val mapRDD = rdd.map(_ * 2)
//    val mpp = rdd.mapPartitions(iter => iter.map(_ * 2)) //按分区处理。内存小的时候，容易造成内存溢出。因为处理完不会释放掉数据
    val mpi = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.iterator
        }
      }
    )

    mpi.collect().foreach(println)



    //TODO 关闭环境
    sc.stop()

  }

}
