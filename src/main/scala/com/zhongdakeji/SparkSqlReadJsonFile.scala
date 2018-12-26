package com.zhongdakeji

import org.apache.spark.sql.SparkSession

/**
  * spark com.personaltest
  *
  * Create by zyq on 2018/11/28 0028
  *
  */
object SparkSqlReadJsonFile {
  val sparkSession = SparkSession.builder().appName("sparksql_read_json").master("local[*]").getOrCreate()

  def main(args: Array[String]): Unit = {
    val df = sparkSession.read.json("file:///C:\\Users\\Administrator\\Desktop\\a.json")
    df.printSchema()
    df.createOrReplaceTempView("a")
    val result = sparkSession.sql(
      """
        |
        |select * from a
        |
      """.stripMargin)
    result.show()
  }
}
