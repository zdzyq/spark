package com.personaltest

import redis.clients.jedis.{Jedis, JedisPubSub}

/**
  * spark com.personaltest
  *
  * Create by zyq on 2018/12/20 0020
  *
  */
object SubscibeTest {
  def main(args: Array[String]): Unit = {
    val jedis = new Jedis("bigdata01", 6379)
    jedis.subscribe(new JedisPubSub(){
      override def onMessage(channel: String, message: String): Unit = {
        println(s"从$channel 里面接收到消息：$message")
      }
    },"topic1")
  }

}
