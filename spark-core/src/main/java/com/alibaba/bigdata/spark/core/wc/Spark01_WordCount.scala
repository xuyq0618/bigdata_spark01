package com.alibaba.bigdata.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}


object Spark01_WordCount {
  def main(args: Array[String]): Unit = {

    //1、建立和spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

//    2、执行业务操作
    println("执行业务操作。。。")
//    1、读取文件，获取一行行数据
//    hello world
    val lines = sc.textFile("datas/*")

//    2、将一行数据进行拆分，形成一个个单词（分词）
//    hello,world,hello,world
    val words = lines.flatMap(_.split(" "))

//    3、将数据根据单词进行分组，便于统计
//    (hello,hello,hello),(world,world)
    val wordGroup = words.groupBy(word => word)
//    println(wordGroup)

//    4、分组后的数据进行转换
//    (hello,3),(world,2)
    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

//    5、将转换结果采集到控制台打印出来
    val array = wordToCount.collect()
    array.foreach(println)

//    3、关闭连接
    sc.stop()

  }

}
