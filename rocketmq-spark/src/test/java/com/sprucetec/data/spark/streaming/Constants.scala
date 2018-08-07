package com.sprucetec.data.spark.streaming

object Constants {
  val WINDOW = "window" //时间窗口
  val STARTOFFSETS = "startOffsets"
  //开始offset
  val ENDOFFSETS = "endOffsets"
  //结束offset
  val GROUP = "group"
  //kafka组
  val TOPIC = "topic"
  //kafka topic
  val ZKLIST = "zklist"
  //kafka zookeeper列表
  val BROKERS = "brokers" //kafka server地址
  val ISLOCAL = "islocal" //是否本地调试true:本地调试,false线上运行
  val REDISKEY = "rediskey"
  val TTL = "ttl"
  //过滤redis过期时间
  val LOG_TTL = "log_ttl"
  //exactlyonce中间表redis过期时间
  val SCHEMA = "schema"
  val AUTOOFFSETRESET = "autooffsetreset"
  val OFFSETDB = "offset_db_info"
  val SESSION_TIMEOUT = 10000
  val CONN_TIMEOUT = 10000
  val MAX_BYTES  = "100"
  val KAFKA_BROKERS_KEY = "metadata.broker.list"
  val KAFKA_GROUP_KEY = "group.id"
  val KAFKA_OFFSET_RESET_KEY = "auto.offset.reset"
  val STREAMING_STOP_GRACEFULLY_KEY = "spark.streaming.stopGracefullyOnShutdown"
  val SPARK_SERIALIZER_KEY = "spark.serializer"
  val SPARK_SERIALIZER = "org.apache.spark.serializer.KryoSerializer"
  val TRUE = "true"
  val FALSE = "false"

}
