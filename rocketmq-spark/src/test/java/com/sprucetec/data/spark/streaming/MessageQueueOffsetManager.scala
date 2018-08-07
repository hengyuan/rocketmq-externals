package com.sprucetec.data.spark.streaming

import com.alibaba.fastjson.JSONObject
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.dstream.DStream


trait MessageQueueOffsetManager[T] {

  def getDStream(jssc: JavaStreamingContext, conf: JSONObject): DStream[T]

  def formatSmallestAndLargestOffset(rdd: RDD[T], conf: JSONObject): JSONObject

  def persistsCheckpoint(dStream: DStream[T], rdd: RDD[T], conf: JSONObject)
}
