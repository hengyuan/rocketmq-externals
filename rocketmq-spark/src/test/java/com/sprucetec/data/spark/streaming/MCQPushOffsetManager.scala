package com.sprucetec.data.spark.streaming

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.alibaba.rocketmq.common.message.Message
import org.apache.rocketmq.spark.{RocketMQConfig, RocketMqUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.dstream.DStream
import sun.util.logging.resources.logging

/**
  * 公司：美菜网
  * 部门：数据平台开发部
  * 作者：韩家富
  * 邮箱:hanjiafu@meicai.cn
  * 描述：
  */
class MCQPushOffsetManager extends MessageQueueOffsetManager[Message]{

  override def getDStream(jssc: JavaStreamingContext,
                          conf: JSONObject): DStream[Message] = {
    val properties = new Properties()
    properties.setProperty(RocketMQConfig.NAME_SERVER_ADDR,
      conf.getString(Constants.BROKERS))
    properties.setProperty(RocketMQConfig.CONSUMER_GROUP,
      conf.getString(Constants.GROUP))
    properties.setProperty(RocketMQConfig.CONSUMER_TOPIC,
      conf.getString(Constants.TOPIC))
    val CONSUMER_OFFSET_RESET_TO = conf.getString(Constants.AUTOOFFSETRESET)
    CONSUMER_OFFSET_RESET_TO match {
      case "earliest" =>
        properties.setProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO,
          RocketMQConfig.CONSUMER_OFFSET_EARLIEST)
      case _ =>
        properties.setProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO,
          RocketMQConfig.CONSUMER_OFFSET_LATEST)
    }
    RocketMqUtils
      .createJavaMQPushStream(jssc, properties, StorageLevel.MEMORY_AND_DISK)
      .dstream
  }

  override def formatSmallestAndLargestOffset(rdd: RDD[Message],
                                              conf: JSONObject): JSONObject =
    conf

  override def persistsCheckpoint(dStream: DStream[Message],
                                  rdd: RDD[Message],
                                  conf: JSONObject): Unit = {}
}
