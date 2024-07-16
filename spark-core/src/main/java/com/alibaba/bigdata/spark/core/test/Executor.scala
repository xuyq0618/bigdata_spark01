package com.alibaba.bigdata.spark.core.test

import com.sun.corba.se.spi.ior.ObjectId
import org.sparkproject.jetty.util.thread.Scheduler.Task

import java.io.ObjectInputStream
import java.net.ServerSocket

object Executor {
  def main(args: Array[String]): Unit = {
    //启动服务器，接收数据
    val server = new ServerSocket(9999)
    println("付服务器启动，等待接收数据。。。。")

    //等待客户端连接
    val client = server.accept()
    val in = client.getInputStream
    val objIn = new ObjectInputStream(in)

    val task = objIn.readObject().asInstanceOf[Task]

    val ints = task.compute
    println("计算节点计算的结果为："+ints)

    objIn.close()
    client.close()
    server.close()


  }

}
