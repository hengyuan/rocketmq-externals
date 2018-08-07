package com.sprucetec.data.spark.streaming

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.rocketmq.common.message.MessageExt
import org.apache.spark.rdd.RDD

abstract class MCQStreamingTpl extends BaseTpl[MessageExt,JSONObject]{
  messageQueue = new MCQOffsetManager()
  override def sysFilter(rdd: RDD[MessageExt], conf: JSONObject): RDD[JSONObject] = {
    val logs = rdd.filter(x=>{
      try {
        val line = x.getBody.map(_.toChar).mkString
        JSON.parseObject(line)
        true
      }catch {
        case ex:Exception=>false
      }
    }).map(x=>JSON.parseObject(x.getBody.map(_.toChar).mkString))
    logs
  }
}
