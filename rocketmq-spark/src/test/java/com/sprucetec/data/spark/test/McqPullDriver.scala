package com.sprucetec.data.spark.test
/*
* {\"islocal\":true} {\"topic\":\"testtopic\",\"group\":\"test5\",\"islocal\":false,\"window\":10,\"brokers\":\"192.168.2.210:9876;192.168.2.211:9876\",\"autooffsetreset\":\"largest\"}
* */

object McqPullDriver {
  def main(args: Array[String]): Unit = {
    val ssc = new PullHandler().createContext(args)
    ssc.start()
    ssc.awaitTermination()
  }
}
