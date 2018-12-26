package com.personaltest

import redis.clients.jedis.Jedis

/**
  * spark com.personaltest
  *
  * Create by zyq on 2018/12/20 0020
  *
  */
object PublishTest {
  def main(args: Array[String]): Unit = {
    val jedis = new Jedis("bigdata01", 6379)
    (1 to 10).foreach(x=>{
      jedis.publish("topic1",s"msg$x")
    })
  }

}
