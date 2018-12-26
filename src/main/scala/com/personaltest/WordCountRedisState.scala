package com.personaltest

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * spark com.personaltest
  *
  * Create by zyq on 2018/12/20 0020
  *
  */
object WordCountRedisState {
  val conf = new SparkConf().setAppName("redis-integration").setMaster("local[*]")
  val ssc = new StreamingContext(conf, Seconds(5))

  def stateWordCount() = {
    val dstream = ssc.socketTextStream("bigdata01",8888)
    val result = dstream.flatMap(_.split("\\s+"))
      .map((_,1))
      .reduceByKey(_+_)

    result.foreachRDD(rdd=>{
      rdd.foreachPartition(items=>{
        val jedis = new Jedis("bigdata01", 6379)
        for(item<-items){
          jedis.hincrBy("wordcount",item._1,item._2.toLong)
        }
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    stateWordCount()
  }

}
