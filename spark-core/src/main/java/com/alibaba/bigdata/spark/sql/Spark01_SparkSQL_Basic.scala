package com.alibaba.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    //TODO 创建sparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    //TODO 执行逻辑操作
    println("==================================执行业务逻辑=======================")
//
//    //DataFrame
//    val df = spark.read.json("datas/user.json")
//    df.show()
//
//    //df ==>sql
//    df.createTempView("user")
//    spark.sql("select * from user").show
//    //df ==>DSL
//    df.select("username","age").show
//    df.select($"age"+111).show()
//
//
//    //DataSet dataframe其实就是特定泛型的Dataset
//    val seq = Seq(1,2,3)
//    val ds = seq.toDS()
//    ds.show()


    //RDD <=> DataFrame
    val rdd = spark.sparkContext.makeRDD(List((1, "ZHANGSAN"), (2, "LISI"), (3, "WANGWU")))
    val df = rdd.toDF("id", "username")
    df.show()

    val rddFromDf = df.rdd
    rddFromDf.foreach(println)
//      [2, LISI]
//      [3, WANGWU]
//      [1, ZHANGSAN]

    //DataFrame <=> DataSet
    val ds = df.as[User]
    ds.show()
    df.toDF()

    //RDD <=> DataSet
    val ds1 = rdd.map{
      case (id, name) => {
        User(id, name)
      }
    }.toDS()
    ds1.show()
    val rddFromDs = ds1.rdd
    rddFromDs.foreach(println)



    //TODO 关闭环境
    spark.close()
  }
  case class User(id:Int,username:String)
}
