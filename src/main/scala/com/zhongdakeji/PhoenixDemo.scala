package com.zhongdakeji

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.phoenix.spark._

/**
  * spark com.zhongdakeji
  *
  * Create by zyq on 2018/11/27 0027
  *
  */
object PhoenixDemo {
  val sparkSession = SparkSession.builder().appName("phoenix-test").master("local[*]").getOrCreate()
  val sqlContext = sparkSession.sqlContext
  val sc = SparkContext.getOrCreate()
  def Test1()={
    val df = sqlContext.load(
      "org.apache.phoenix.spark",
      Map("table" -> "TABLE1", "zkUrl" -> "bigdata01:2181")
    )
    df
      .filter(df("COL1") === "test_row_1" && df("ID") === 1L)
      .select(df("ID"))
      .show
  }
  def Test2()={
    val configuration = new Configuration()
    val df = sqlContext.phoenixTableAsDataFrame(
      "TABLE1", Array("ID", "COL1"), conf = configuration
    )
    df.show
  }
  def Test3()={
    val rdd: RDD[Map[String, AnyRef]] = sc.phoenixTableAsRDD(
      "TABLE1", Seq("ID", "COL1"), zkUrl = Some("bigdata01:2181")
    )
    rdd.count()

    val firstId = rdd.first()("ID").asInstanceOf[Long]
    val firstCol = rdd.first()("COL1").asInstanceOf[String]
  }
  def saveRDDTest()={

    val dataSet = List((1L, "1", 1), (2L, "2", 2), (3L, "3", 3))

    sc
      .parallelize(dataSet)
      .saveToPhoenix(
        "OUTPUT_TEST_TABLE",
        Seq("ID","COL1","COL2"),
        zkUrl = Some("bigdata01:2181")
      )
  }
  def saveDFTest()={
    val df = sqlContext.load("org.apache.phoenix.spark", Map("table" -> "INPUT_TABLE",
      "zkUrl" -> "bigdata01:2181"))

    //    // Save to OUTPUT_TABLE
    //    df.saveToPhoenix(Map("table" -> "OUTPUT_TABLE", "zkUrl" -> "bigdata01:2181"))
    //
    //    or

    df.write
      .format("org.apache.phoenix.spark")
      .mode("overwrite")
      .option("table", "OUTPUT_TABLE")
      .option("zkUrl", "bigdata01:2181")
      .save()
  }
  def pageRankTest()={
    val rdd = sc.phoenixTableAsRDD("EMAIL_ENRON", Seq("MAIL_FROM", "MAIL_TO"), zkUrl=Some("bigdata01"))           // load from phoenix
    val rawEdges = rdd.map{ e => (e("MAIL_FROM").asInstanceOf[VertexId], e("MAIL_TO").asInstanceOf[VertexId]) }   // map to vertexids
    val graph = Graph.fromEdgeTuples(rawEdges, 1.0)                                                               // create a graph
    val pr = graph.pageRank(0.001)                                                                                // run pagerank
    pr.vertices.saveToPhoenix("EMAIL_ENRON_PAGERANK", Seq("ID", "RANK"), zkUrl = Some("bigdata01"))               // save to phoenix

  }


  def main(args: Array[String]): Unit = {
    //    Test1()
    //    Test2()
    //    Test3()
    //    saveRDDTest()
    //    saveDFTest()
    pageRankTest()

  }

}
