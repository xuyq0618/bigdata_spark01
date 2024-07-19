package com.alibaba.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark01_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    //TODO 创建sparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    //TODO 执行逻辑操作
    println("==================================执行业务逻辑=======================")

    val df = spark.read.json("datas/user.json")
    df.createTempView("user")

    spark.udf.register("prefixName","Name:"+_)
    spark.sql("select age,prefixName(username) from user" ).show()



    //TODO 关闭环境
    spark.close()
  }
  case class User(id:Int,username:String)
}
