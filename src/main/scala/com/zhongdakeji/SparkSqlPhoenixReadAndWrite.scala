package com.zhongdakeji

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * spark com.zhongdakeji
  *
  * Create by zyq on 2018/11/27 0027
  *
  */
object SparkSqlPhoenixReadAndWrite {
  //创建sparkSession
  val sparkSession = SparkSession.builder().master("local[*]").appName("sparksql_phoenix_test").getOrCreate()
  //  创建sc
  val sc = sparkSession.sparkContext
  //加载驱动
  Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
  //构建输入表的jdbc连接
  val url01 = "jdbc:phoenix:192.168.2.235:2181"
  val properties01 = new Properties()
  properties01.setProperty("user","root")
  properties01.setProperty("password","")
  //构建输出表的jdbc连接
  val url02 = "jdbc:phoenix:bigdata01:2181"
  val properties02 = new Properties()
  properties02.setProperty("user","root")
  properties02.setProperty("password","123456")

  val inputTable ="BIGDATADB.KPI"
  val outputTable = "kpitotal"
  //读phoenix表
  def readDataFromPhoenixTest()={
    //创建dataframe
    val df = sparkSession.read.jdbc(url01,inputTable,properties01)
    //打印表模式
    //    df.printSchema()
    //创建表名称
    df.createOrReplaceTempView("KPI")
    //    df.toDF("ID","REMOTE_ADDR","REMOTE_USER","TIME_LOCAL","REQUEST","STATUS","BODY_BYTES_SENT","HTTP_REFERER","HTTP_USER_AGENT","CLIENT_SYSTEM","CLIENT_BROWSER","UPSTREAM_RESPONSE_TIME","REQUEST_TIME","CREATETIME")
    val result = sparkSession.sql(
      s"""
         |select * from KPI limit 10
         |
         |
      """.stripMargin)
    result.show()

    //方法二：
    //    df.filter(df("TIME_LOCAL")>="19/Nov/2018:10:05:09 +0800"&&df("TIME_LOCAL")<"19/Nov/2018:10:21:34 +0800").show()
    sparkSession.close()
  }
  //写phoenix表
  def writeDataToPhoenixTest()={
    val df = sparkSession.read.jdbc(url01,inputTable,properties01)

    df.createOrReplaceTempView("aa")
    val result = sparkSession.sql(
      s"""
         |select ID,date_sub('2018-05-11',3) as dateT,STATUS,COUNT(REMOTE_ADDR) as PVCOUNT
         |from aa
         |group by
         |id,dateT,status
         |
      """.stripMargin)
    result.show()

    //   可以将sql查询的结果转换成rdd进行处理，后重新转换成df进行操作
    //    val resultRDD = result.rdd.map(item=>{
    //
    //      val id = item.get(0)
    //      val status = item.get(1)
    ////      val uuid = UUID.randomUUID()
    //
    //      (id.toString,status.toString,uuid.toString)
    //    })
    //    import sparkSession.implicits._
    //    val df11 = resultRDD.toDF("a","b","c")
    //    df11.show()
//    方法一：
    result.write
      .format("org.apache.phoenix.spark")
      .mode(SaveMode.Overwrite)
      .option("table", outputTable)
      .option("zkUrl","bigdata01:2181")
      .save()
//    方法二：（不成功）
//    result.write
//      .format("jdbc")
//      .mode(SaveMode.Append)
//      .jdbc(url02,outputTable,properties02)
//    方法三：(不成功)
//    result.write
//      .format("jdbc")
//      .mode(SaveMode.Overwrite)
//      .option("url","jdbc:phoenix:bigdata01:2181")
//      .option("dbtable",outputTable)
//      .option("user","root")
//      .option("password","123456")
//      .save()


    sparkSession.close()
  }
  def main(args: Array[String]): Unit = {
//    readDataFromPhoenixTest()
    writeDataToPhoenixTest()
  }
}
