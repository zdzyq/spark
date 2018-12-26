package com.zhongdakeji

import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark com.personaltest
  *
  * Create by zyq on 2018/11/27 0027
  *
  */
object CheckPointTest {

  val conf = new SparkConf().setAppName("checkpoint").setMaster("local[*]")
  val sc = SparkContext.getOrCreate(conf)

  def main(args: Array[String]): Unit = {
    val rdd = sc.parallelize(1 to 100)
    //    sc.setCheckpointDir("hdfs://bigdata01:8020/checkpoint")
    val checkPointDir = sc.setCheckpointDir("file:\\D:\\work\\spark\\src\\main\\checkpoint")
    rdd.checkpoint()
    rdd.foreach(println)



  }

}
