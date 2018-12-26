package com.learning

import java.nio.charset.Charset

import org.apache.flume.api.{RpcClient, RpcClientFactory}
import org.apache.flume.event.EventBuilder

/**
  * spark com.learning
  *
  * Create by zyq on 2018/12/17 0017
  *
  */
object FlumeClientUtils {
  val HOST_NAME = "bigdata01"
  val PORT = 8888

  def main(args: Array[String]): Unit = {
    val client = RpcClientFactory.getDefaultInstance(HOST_NAME,PORT)

    while (true){
      (1 to 100000).foreach(i=>{
        val event = EventBuilder.withBody(s"msg+$i",Charset.forName("UTF-8"))
        client.append(event)
      })
    }
    client.close()
  }
}
