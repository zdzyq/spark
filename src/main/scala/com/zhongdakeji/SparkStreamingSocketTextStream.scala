package com.zhongdakeji

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark com.personaltest
  *
  * Create by zyq on 2018/11/27 0027
  *
  */
object SparkStreamingSocketTextStream {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sparkstreamingtest").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))
    val line = ssc.socketTextStream("192.168.2.178",9999)
    val words  = line.flatMap(_.split("\\s+"))
    val word = words.map(x=>x)
    word.print()
    //    word.saveAsTextFiles("file:\\C:\\Users\\Administrator\\Desktop\\test\\")
    word.saveAsTextFiles("hdfs://bigdata01:8020/streamingtest/")
    ssc.start()
    ssc.awaitTermination()
  }

}
