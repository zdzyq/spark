package com.zhongdakeji

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * spark com.personaltest
  *
  * Create by zyq on 2018/11/27 0027
  *
  */
//case class Nginx(timestamp:String,agent:String,bytes:String,referer:String,remote_addr:String,request:String,status:String,ups_resp_time:String,upstr_addr:String,upstr_host:String,x_forwarded:String)

object ReadJsonTest {
  val conf = new SparkConf().setAppName("read_json_test").setMaster("local[*]")
  val sc = SparkContext.getOrCreate(conf)
  def readJsonFiltTest()={
    val inputFile = "file:///C:\\Users\\Administrator\\Desktop\\a.json"
    val jsonStr = sc.textFile(inputFile)
    val result = jsonStr.map(s=>JSON.parseFull(s))
    result.foreach({
      r=>r match {
        case Some(map:Map[String,Any])=>println(map)
        case None=>println("数据解析失败")
        case other=>println("未知的数据结构"+other)
      }
    })
  }
  def main(args: Array[String]): Unit = {
    readJsonFiltTest()
  }


}
