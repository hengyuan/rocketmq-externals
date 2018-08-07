package com.sprucetec.data.spark.streaming

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.api.java.JavaStreamingContext

import scala.util.Try

trait BaseTpl[T, R] {
  var messageQueue: MessageQueueOffsetManager[T] = _
  def createContext(argsArr: Array[String]): JavaStreamingContext = {
    val args = argsCheck(argsArr)
    val userConfigure = userConf(args._1)
    var conf = mergeConf(userConfigure, args._2)
    val sparkConf = buildSparkConf(conf)
    val jssc = new JavaStreamingContext(sparkConf, Seconds(conf.getLong(Constants.WINDOW)))
    val messages = messageQueue.getDStream(jssc, conf)
    messages.foreachRDD((rdd, time) => {
      if (!rdd.isEmpty()) {
        conf = messageQueue.formatSmallestAndLargestOffset(rdd, conf)
        val filterRDD = sysFilter(rdd, conf)
        val userFilter = filter(filterRDD, conf)
        compute(userFilter, conf)
        messageQueue.persistsCheckpoint(messages, rdd, conf)
      }
    })
    jssc
  }

  /**
    * 参数校验
    */
  def argsCheck(argsArr: Array[String]): (JSONObject, JSONObject) = {
    if (argsArr.length != 2) {
      System.err.println(
        s"""Usage: args <userargs> <platformargs>
           |<userargs> //用户参数,参数类型为json
           |<platformargs>//系统参数,参数类型为json
        """.stripMargin)
      System.exit(1)
    } else if (Try(JSON.parseObject(argsArr(0))).isFailure) {
      System.err.println("用户参数请使用json数据格式")
      System.exit(1)
    } else if (Try(JSON.parseObject(argsArr(1))).isFailure) {
      System.err.println("系统参数请使用json数据格式")
      System.exit(1)
    }
    (JSON.parseObject(argsArr(0)), JSON.parseObject(argsArr(1)))
  }

  /**
    * 参数合并，用户参数会覆盖系统参数
    */
  def mergeConf(userConf: JSONObject, sysConf: JSONObject): JSONObject = {
    val conf = JSonUtil.merge(sysConf, userConf)
    conf
  }

  /**
    * 构建SparkConf，主要用于设置本地调试模式、线上运行模式
    */
  def buildSparkConf(conf: JSONObject): SparkConf = {
    val isLocal = conf.getBoolean(Constants.ISLOCAL)
    var sparkConf = new SparkConf()
    sparkConf.set(Constants.STREAMING_STOP_GRACEFULLY_KEY, Constants.TRUE)
    sparkConf.set(Constants.SPARK_SERIALIZER_KEY, Constants.SPARK_SERIALIZER)
    sparkConf.set("spark.streaming.backpressure.enabled","true")
    sparkConf.set("spark.streaming.backpressure.initialRate","1")
    sparkConf = setSparkConf(sparkConf, conf)
    if (isLocal) {
      sparkConf.setAppName("SparkStreamingLocal")
      sparkConf.setMaster("local[2]")
    }
    sparkConf
  }

  /**
    * 用户之定义参数
    */
  def userConf(userConf: JSONObject): JSONObject=userConf

  /**
    * 用户设置sparkconf
    */
  def setSparkConf(sparkConf: SparkConf, conf: JSONObject): SparkConf = sparkConf

  /**
    * 系统过滤
    */
  def sysFilter(rdd: RDD[T], conf: JSONObject): RDD[R]

  /**
    * 自定义过滤
    */
  def filter(rdd: RDD[R], conf: JSONObject): RDD[R] = rdd

  /**
    * 业务计算
    */
  def compute(rdd: RDD[R], conf: JSONObject)
}
