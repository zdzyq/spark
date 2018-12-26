package com.zhongdakeji

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark com.personaltest
  *
  * Create by zyq on 2018/11/28 0028
  *
  */
object SparkSqlMysqlReadAndWrite {
  //创建conf
  val conf = new SparkConf().setMaster("local[*]").setAppName("sparksql_read_and_write_mysql_test")
  // 创建sc
  val  sc = SparkContext.getOrCreate(conf)
  val sparkSession = SparkSession.builder().getOrCreate()
  //加载驱动
  Class.forName("com.mysql.jdbc.Driver")
  //构建jdbc连接
  val url = "jdbc:mysql://localhost:3306/xxl-job?serverTimezone=UTC&characterEncoding=utf-8"
  val properties = new Properties()
  properties.setProperty("user","root")
  properties.setProperty("password","123456")

  val inputTable = "Student"
  val outputTable = "person"
  val getConnection=()=>{
    DriverManager.getConnection(url,properties)
  }
//未测试成功的方法
  def readDataFromMysql()={
    val sql = "select * from Student where s_id>=?and s_id<=?"
    val mysqlRdd = new JdbcRDD(sc,getConnection,sql,1,6,2,rs=>{
      (rs.getString("s_id"),rs.getString("s_name"),rs.getString("s_birth"),rs.getString("s_sex"))
    })
    mysqlRdd.foreach(println)
    sc.stop()
  }
  def sparkSqlReadDataFromMysql()={
    val df = sparkSession.read.jdbc(url,inputTable,properties)
    df.printSchema()
    df.createOrReplaceTempView("b")
    val result = sparkSession.sql(
      """
        |
        |select * from b
        |where s_id>03
        |
      """.stripMargin)
    result.show()
    sc.stop()
  }
  def sparkSqlWriteDataToMysql()={
    val df = sparkSession.read.jdbc(url,inputTable,properties)
    df.printSchema()
    df.createOrReplaceTempView("p")

    val result = sparkSession.sql(
      """
        |select * from p
        |where s_birth>'1990-05-19'
        |order by s_birth
        |
      """.stripMargin)
    result.show()
    result.write.mode(SaveMode.Append).jdbc(url,outputTable,properties)
    sc.stop()
  }

  def main(args: Array[String]): Unit = {
//    readDataFromMysql()

    sparkSqlReadDataFromMysql()
//    sparkSqlWriteDataToMysql()
  }

}
