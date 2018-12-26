package com.zhongdakeji

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * spark com.zhongdakeji
  *
  * Create by zyq on 2018/11/27 0027
  *
  */
object SparkSqlPvUvCount {
  val sparkSession = SparkSession.builder().appName("sparksql-pvuv-count").master("local[*]").getOrCreate()
  val sc = sparkSession.sparkContext

  //加载驱动
  Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
  //构建jdbc连接
  val url = "jdbc:phoenix:192.168.2.235:2181"
  val properties = new Properties()
  properties.setProperty("user","root")
  properties.setProperty("password","")
  val inputTable = ("BIGDATADB.KPI","BIGDATADB.IPVIEWTOTAL")
  val outputTable = "PVUV_COUNT"

  val date = new Date()
  val startTime = date.getTime
  val stopTime = date.getTime-600000
  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
  val formatStartTime = simpleDateFormat.format(startTime)
  val formatStopTime = simpleDateFormat.format(stopTime)

  def pvUvCount()={
    val df = sparkSession.read.jdbc(url,inputTable._1,properties)
    val df1 = sparkSession.read.jdbc(url,inputTable._2,properties)
    df.createOrReplaceTempView("kpi")
    df1.createOrReplaceTempView("ipviewtotal")
    val result = sparkSession.sql(
          s"""
            |select
            |count(remote_addr) as pvCount
            |from kpi
            |where createtime between "${formatStartTime}" and "${formatStopTime}"
            |
          """.stripMargin)
      //
      //
      // val result = sparkSession.sql(
//      s"""
//         |select
//         |count(remote_addr) as pvCount,count(distinct remote_addr) as uvCount
//         |from kpi
//         |
//    """.stripMargin)
    result.show()
//    result.write.format("org.apache.phoenix.spark").mode(SaveMode.Overwrite).option("table", outputTable).option("zkUrl","192.168.2.235:2181").save()

    sparkSession.close()
  }

  def articlePVCount()={
    val df = sparkSession.read.jdbc(url,inputTable._1,properties)
    df.printSchema()
    df.createOrReplaceTempView("articlePV")
    val result = sparkSession.sql(
      """
        |select count(remote_addr) as articlePvCount from articlePV
        |where request like '%/article%'
      """.stripMargin)

    result.show()

    sparkSession.close()
  }


  def main(args: Array[String]): Unit = {
    pvUvCount()
//    articlePVCount()
  }

}
