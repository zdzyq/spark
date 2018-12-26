package com.personaltest

import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * spark com.personaltest
  *
  * Create by zyq on 2018/12/19 0019
  *
  */
object SaveToRedisTest {
  val conf = new SparkConf().setMaster("local[*]").setAppName("savetoredis")
  val sc = SparkContext.getOrCreate(conf)

  def saveProductCategoryToRedis() = {
    val rdd = sc.textFile("file:///C:\\Users\\Administrator\\Desktop\\testdata\\ch04_data_products.txt")
//    val rdd = sc.textFile("/product/ch04_data_products.txt")

    rdd.map(x=>{
      val splits = x.split("\\|")
      (splits(0),splits(1))  //(productId,categoryId)
    }).foreachPartition(iterator=>{
      val jedis = new Jedis("bigdata01",6379)
      jedis.select(2)
      for(item<-iterator){
        jedis.hset("productCategory",item._1,item._2)
      }
    })
  }
  def main(args: Array[String]): Unit = {
    saveProductCategoryToRedis()
    sc.stop()
  }

}
