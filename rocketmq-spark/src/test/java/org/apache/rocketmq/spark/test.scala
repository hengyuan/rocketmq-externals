package org.apache.rocketmq.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
/**
  * Created with IntelliJ IDEA.
  * User: hanjiafu
  * Date: 18-8-8
  * Time: 上午7:33
  * Detail:
  */
object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("Structured")
      .getOrCreate()
    val df = spark
      .readStream
      .format("org.apache.spark.sql.rocketmq.RocketMQSourceProvider")
      .option("nameServer", "192.168.2.210:9876;192.168.2.211:9876")
      .option("topic", "testtopic")
      .option("group","streaming1")
      .load()
    df.printSchema()
    df.selectExpr("CAST(body AS STRING)").groupBy("body").count()
    val query = df.writeStream
      .outputMode("update")
      .format("console")
      .start()
    query.awaitTermination()
  }
}
