package com.personaltest

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark com.personaltest
  *
  * Create by zyq on 2018/12/12 0012
  *
  */
object jdbcTest {
  val conf = new SparkConf().setMaster("local[*]").setAppName("jdbc")
  val sc = SparkContext.getOrCreate(conf)

  def main(args: Array[String]): Unit = {
    val driver = "org.apache.phoenix.jdbc.PhoenixDriver"
    val url = "jdbc:phoenix:bigdata01:2181"
    val username = "root"
    val password = "123456"
    Class.forName(driver)
    val connection = DriverManager.getConnection(url,username,password)
    val sql = "upsert into kpitotal(ID) values (?)"
    val statment = connection.prepareStatement(sql)
    statment.setString(1,"112")
    statment.executeUpdate()

    connection.commit()
    connection.close()

  }


}
