package com.zhongdakeji

import java.util.UUID

import com.learning.Transaction
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark com.learning
  *
  * Create by zyq on 2018/12/26 0026
  *
  */
object HBaseWriteTest {
  val conf = new SparkConf().setAppName("hbasetest").setMaster("local[*]")
  val sc = SparkContext.getOrCreate(conf)
  val tableName = "transaction"

  def getTransactionRdd() = {
    val regex = "(.*)#(.*)#(\\d+)#(\\d+)#(\\d+)#(.*)".r
    val transactionRdd = sc.textFile("/transaction/ch04_data_transactions.txt")
      .map(x=>{
        x match{
          case regex(c1,c2,c3,c4,c5,c6) => Transaction(c1,c2,c3,c4,c5.toInt,c6.toDouble)
          case _ => null
        }
      })
    transactionRdd
  }
  def writeToHBase()={
    val transactionRdd = getTransactionRdd()
    //rowkey  uuid-customerId-productId
    val rddForHBase = transactionRdd.map(x=>{
  val rowkey = s"${UUID.randomUUID()}-${x.customerId}-${x.productId}"
  val put = new Put(rowkey.getBytes)
  put.addColumn("i".getBytes(),"date".getBytes(),x.date.getBytes())
  put.addColumn("i".getBytes(),"time".getBytes(),x.time.getBytes())
  put.addColumn("i".getBytes(),"cid".getBytes(),x.customerId.getBytes())
  put.addColumn("i".getBytes(),"pid".getBytes(),x.productId.getBytes())
  put.addColumn("i".getBytes(),"quantity".getBytes(),x.quantity.toString.getBytes())
  put.addColumn("i".getBytes(),"price".getBytes(),x.price.toString.getBytes())

  (new ImmutableBytesWritable(),put)
})
    val configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum","bigdata01")
    configuration.set("hbase.zookeeper.property.clientPort","2181")

    val jobConf = new JobConf(configuration)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
    rddForHBase.saveAsHadoopDataset(jobConf)
    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    writeToHBase()
  }

}
