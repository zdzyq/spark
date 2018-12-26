package com.zhongdakeji

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

/**
  * spark com.zhongdakeji
  *
  * Create by zyq on 2018/11/27 0027
  *
  */
object KafkaProducer {
  def main(args: Array[String]): Unit = {


    val properties = new Properties()
    val topic = "kafkatopic"
    properties.setProperty("bootstrap.servers", "bigdata01:9092,bigdata02:9092,bigdata03:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //0---producer不等待服务端反馈强制认为消息发送成功
    //1---服务端成功把消息写入到parition的主replication之后就认为消息成功发送，反馈给producer
    //-1(all)---服务端成功报消息写入到相应partition的每一个replication之后才会认为消息成功发送并反馈给producer
    properties.setProperty("acks", "-1")
    properties.setProperty("retries", "2")

    val producer = new KafkaProducer[String, String](properties)
    val content:Array[String]= new Array[String](5)
    content(0)="fl;ksdf;lk"
    content(1)="fl;ksdf;rtw lkgergerg"
    content(2)="fl;kgrtgsdf;lkregrgergg"
    content(3)="flgt;ksdf;lk g  reg reg r g"
    content(4)="fl;ksdf;lk gre eg"
    val msg = Random.nextString(content.size)
    val record = new ProducerRecord[String, String](topic, "key", msg)
    producer.send(record)
    record.headers()
    record.value()foreach(println)


    producer.flush()
    producer.close()

  }

}
