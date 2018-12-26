package com.personaltest

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * spark com.personaltest
  *
  * Create by zyq on 2018/12/20 0020
  *
  */
object JedisPoolTest {
  val jedisPoolConfig = new JedisPoolConfig()
  jedisPoolConfig.setMaxTotal(10)    //最大的redis连接个数，最大的jedis实例个数
  jedisPoolConfig.setMaxIdle(5)    //最大空闲的redis个数
  jedisPoolConfig.setMaxWaitMillis(1000*100) //设置等待阻塞时间
  jedisPoolConfig.setTestOnReturn(true)  //在创建好redis实例后是否测试可用
  val pool = new JedisPool(jedisPoolConfig, "bigdata01", 6379)

  def getRedis() = pool.getResource

  def close(jedis:Jedis) = jedis.close()

  def main(args: Array[String]): Unit = {
    val jedis = getRedis()
    jedis.select(1)

    val keys = jedis.keys("*")

//    因为reducedList是java.util.HashMap, 没有foreach方法, 所以需要将其转换为Scala的集合类型,
//    因此需要在代码中加入如下内容(Scala支持与Java的隐式转换),
    import scala.collection.JavaConversions._
    for(k <- keys){
      println(s"key:$k")
    }
  }
}
