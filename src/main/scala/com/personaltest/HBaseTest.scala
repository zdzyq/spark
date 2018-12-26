package com.personaltest

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan, Table}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark com.personaltest
  *
  * Create by zyq on 2018/11/27 0027
  *
  */
object HBaseTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafka-spark-demo7")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val sc = ssc.sparkContext
    val conf: Configuration = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(conf);
    //    val table: Table = connection.getTable(TableName.valueOf("pv:t_pageview"))
    val table: Table = connection.getTable(TableName.valueOf("person"))
    //    conf.set("hbase.zookeeper.quorum","192.168.2.235,192.168.2.236")
    //    conf.set("hbase.zookeeper.property.clientPort","2181")
    //    conf.set("hbase.master", "192.168.2.235")
    //    conf.set(TableInputFormat.INPUT_TABLE,"pv:t_pageview")
    conf.set(TableInputFormat.INPUT_TABLE,"person")
    val scan = new Scan()
    //    val date = new Date()
    //    val dateTime = date.getTime
    //    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    //    val dateRe = df.format(date).substring(0,15) + "0"
    //    val dfr = df.parse(dateRe)
    //    val startTime = (dfr.getTime - 600000).toString + "10001"
    //    val endTime = dfr.getTime.toString + "99999"
    //    println(startTime)
    //    println(endTime)
    //    scan.setStartRow(Bytes.toBytes(startTime.toString))
    //    scan.setStopRow(Bytes.toBytes(endTime.toString))
    scan.withStartRow(Bytes.toBytes("1"))
    scan.withStopRow(Bytes.toBytes("7"))
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    val count = hBaseRDD.count()
    println(count)
    hBaseRDD.foreach{
      case (_,result) => {
        // 获取行键
        val rowKey = Bytes.toString(result.getRow)
        val name = Bytes.toString(result.getValue("i".getBytes, "name".getBytes))
        val age = Bytes.toString(result.getValue("i".getBytes(),"age".getBytes()))
        println(rowKey+name+age)
      }
    }

    val scanner = table.getScanner(scan)
    var result = scanner.next()
    //数据不为空时输出数据
    while (result!=null){
      while (result.advance()) {
        val cell = result.current()
        val row = CellUtil.cloneRow(cell)
        val family = CellUtil.cloneFamily(cell)
        val qualify = CellUtil.cloneQualifier(cell)
        val value = CellUtil.cloneValue(cell)

        println(
          "timestamp:" + cell.getTimestamp +
            ",rowkey:" + Bytes.toString(row) +
            ",family:" + Bytes.toString(family) +
            ",qualify:" + Bytes.toString(qualify) +
            ",value:" + Bytes.toString(value)
        )
      }
      result=scanner.next()
    }
    //通过scan取完数据后，记得要关闭ResultScanner，否则RegionServer可能会出现问题(对应的Server资源无法释放)
    scanner.close()

//    ssc.start() // 真正启动程序
//
//    ssc.awaitTermination() //阻塞等待
  }
  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

}
