package com.sprucetec.data.spark.streaming

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.rocketmq.common.message.Message
import org.apache.spark.rdd.RDD


/**
  * 公司：美菜网
  * 部门：数据平台开发部
  * 作者：韩家富
  * 邮箱:hanjiafu@meicai.cn
  * 描述：
  */
abstract class MCQStreamingPushTpl extends BaseTpl[Message, JSONObject] {
  messageQueue = new MCQPushOffsetManager()

  override def sysFilter(rdd: RDD[Message], conf: JSONObject): RDD[JSONObject] = {
    rdd.filter(x => {
      try {
        val line = x.getBody.map(_.toChar).mkString
        JSON.parseObject(line)
        true
      } catch {
        case e: Exception => false
      }
    }).map(x => JSON.parseObject(x.getBody.map(_.toChar).mkString))
  }
}
