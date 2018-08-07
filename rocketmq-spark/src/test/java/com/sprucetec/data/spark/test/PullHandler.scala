package com.sprucetec.data.spark.test

import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.sprucetec.data.spark.streaming.MCQStreamingTpl
import org.apache.spark.rdd.RDD

class PullHandler extends MCQStreamingTpl {
  /**
    * 业务计算
    */
  override def compute(rdd: RDD[JSONObject], conf: JSONObject): Unit = {
    println("时间："+new Date()+"消费："+rdd.count()+"条数据")
  }
}
