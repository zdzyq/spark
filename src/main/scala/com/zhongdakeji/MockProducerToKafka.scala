package com.zhongdakeji

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * spark com.zhongdakeji
  *
  * Create by zyq on 2018/11/27 0027
  *
  */
object MockProducerToKafka {
  val properties = new Properties()
  properties.setProperty("bootstrap.servers","bigdata01:9092,bigdata02:9092,bigdata03:9092")
  properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
  properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String,String](properties)
  def sendMsg(msg:String)={
    val record = new ProducerRecord[String,String]("kafkatopic",msg)
    producer.send(record)
  }
  def main(args: Array[String]): Unit = {
    while (true){
      (1 to 10000).foreach(x=>{
        sendMsg(s"msg$x")
        Thread.sleep(500)
      })
    }
    producer.flush()
    producer.close()
  }

}
