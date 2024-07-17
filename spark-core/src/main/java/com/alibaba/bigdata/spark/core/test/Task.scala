package com.alibaba.bigdata.spark.core.test

class Task extends Serializable {
  //数据
  val datas = List(1,2,3,4)

  //计算逻辑
  val logic:(Int)=>Int =  _ * 5

//  //计算
//  def compute = {
//    datas.map(logic)
//  }


}
