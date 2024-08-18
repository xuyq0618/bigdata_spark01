//package com.alibaba.bigdata.spark.streaming
//
//import org.apache.spark.SparkConf
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.receiver.Receiver
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//import scala.util.Random
//
//object SparkStreaming04_Kafka {
//  def main(args: Array[String]): Unit = {
//    //1.创建 SparkConf
//    val sparkConf: SparkConf = new
//        SparkConf().setAppName("ReceiverWordCount").setMaster("local[*]")
//
//    //2.创建 StreamingContext
//    val ssc = new StreamingContext(sparkConf, Seconds(3))
//
//    //3.定义 Kafka 参数
//    val kafkaPara: Map[String, Object] = Map[String, Object](
//      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
//        "linux1:9092,linux2:9092,linux3:9092",
//      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
//      "key.deserializer" ->
//        "org.apache.kafka.common.serialization.StringDeserializer",
//      "value.deserializer" ->
//        "org.apache.kafka.common.serialization.StringDeserializer"
//    )
//
//    //4.读取 Kafka 数据创建 DStream
//    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
//      KafkaUtils.createDirectStream[String, String](ssc,
//        LocationStrategies.PreferConsistent,
//        ConsumerStrategies.Subscribe[String, String](Set("atguigu"), kafkaPara))
//
//    //5.将每条消息的 KV 取出
//    val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())
//
//    //6.计算 WordCount
//    valueDStream.flatMap(_.split(" "))
//      .map((_, 1))
//      .reduceByKey(_ + _)
//      .print()
//
//    //7.开启任务
//    ssc.start()
//    ssc.awaitTermination()
//
//  }
//}
