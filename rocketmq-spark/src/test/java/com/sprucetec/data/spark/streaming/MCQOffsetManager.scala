package com.sprucetec.data.spark.streaming

import java.{util => ju}

import com.alibaba.fastjson.JSONObject
import com.alibaba.rocketmq.common.message.MessageExt
import org.apache.rocketmq.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.dstream.DStream
import scala.collection.JavaConversions._

class MCQOffsetManager extends MessageQueueOffsetManager[MessageExt] with Logging {

  override def getDStream(jssc: JavaStreamingContext,
                          conf: JSONObject): DStream[MessageExt] = {
    val ssc = jssc.ssc
    val topic = conf.getString(Constants.TOPIC)
    val brokers = conf.getString(Constants.BROKERS)
    val group = conf.getString(Constants.GROUP)
    val autoOffsetReset = conf.getString(Constants.AUTOOFFSETRESET)
    val params = new ju.HashMap[String, String]()
    params.put(RocketMQConfig.NAME_SERVER_ADDR, brokers)
    params.put(RocketMQConfig.PULL_MAX_BATCH_SIZE,"32")
    params.put(RocketMQConfig.PULL_BATCH_INTERVAL,"1000")
    autoOffsetReset match {
      case "largest" =>
        RocketMqUtils.createMQPullStream(ssc,
          group,
          topic,
          ConsumerStrategy.lastest,
          false,
          false,
          false,
          params)
      case "smallest" =>
        RocketMqUtils.createMQPullStream(ssc,
          group,
          topic,
          ConsumerStrategy.earliest,
          false,
          false,
          false,
          params)
    }
  }

  override def formatSmallestAndLargestOffset(rdd: RDD[MessageExt],
                                              conf: JSONObject): JSONObject = {
    val startOffsets = new StringBuilder()
    val endOffsets = new StringBuilder()
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    offsetRanges.toMap.keys.toArray
      .sortBy(x => x.queueId())
      .foreach(x => {
        val offsetArr = offsetRanges.get(x)
        offsetArr.foreach(x => {
          val osr: OffsetRange = x
          val queueId = osr.queueId
          val fromOffset = osr.fromOffset
          val untilOffset = osr.untilOffset
          val broker = osr.brokerName
          startOffsets ++= queueId + ":" + broker + ":" + fromOffset
          endOffsets ++= queueId + ":" + broker + ":" + untilOffset
        })
        startOffsets ++= "|"
        endOffsets ++= "|"
      })
    log.info(
      "startOffsets:" + startOffsets.toString() + " endOffsets:" + endOffsets
        .toString())
    conf.put(Constants.STARTOFFSETS, startOffsets)
    conf.put(Constants.ENDOFFSETS, endOffsets)
    conf
  }

  override def persistsCheckpoint(dStream: DStream[MessageExt],
                                  rdd: RDD[MessageExt],
                                  conf: JSONObject): Unit = {
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    dStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
  }
}
