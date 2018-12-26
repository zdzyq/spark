package com.zhongdakeji

import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark com.zhongdakeji
  *
  * Create by zyq on 2018/12/24 0024
  *
  */
object PractiseTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)


    val kvRdd1 = sc.parallelize(List((1,"a"),(2,"b"),(3,"c"),(4,"d"),(5,"d"),(6,"e"),(3,"e")))
    kvRdd1.keys.foreach(println)
    kvRdd1.values.foreach(println)



  }

}
