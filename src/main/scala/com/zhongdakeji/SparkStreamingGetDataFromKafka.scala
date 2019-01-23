package com.zhongdakeji

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * spark com.zhongdakeji
  *
  * Create by zyq on 2018/11/27 0027
  *
  */
object SparkStreamingGetDataFromKafka {
  val conf = new SparkConf().setAppName("sparkStreamingGetDataFromKafka")
  val ssc = new StreamingContext(conf, Seconds(3))
  val kafkaParams = Map("bootstrap.servers" -> "bigdata01:9092,bigdata02:9092,bigdata03:9092",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "group.id" -> "sparkstreamingtest"
  )

  def sparkStreamingGetDataFromKafka() = {
    //需要两个参数：定位策略对象（LocationStrategy）和消费策略对象（ConsumerStrategy）
    val kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array("kafkatopic"), kafkaParams))
    //    kafkaStream.map(record=>(record.key(),record.value(),record.timestamp())).print()
    kafkaStream.map(x => x.value()).print()
    kafkaStream.foreachRDD(item => {
      item.saveAsTextFile("file:///C:\\Users\\Administrator\\Desktop\\streamingfile\\")//保存到本地
      item.saveAsTextFile("hdfs://bigdata01:8020/streamingtest/") //保存到hdfs
    })


    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    sparkStreamingGetDataFromKafka()
  }

}
