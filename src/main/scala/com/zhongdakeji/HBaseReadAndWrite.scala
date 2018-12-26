package com.zhongdakeji

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark com.zhongdakeji
  *
  * Create by zyq on 2018/11/27 0027
  *
  */
object HBaseReadAndWrite {
  //读取person表
  val tableName="person"
  val conf = new SparkConf().setAppName("HbaseReadAndWrite").setMaster("local[*]")
  val sc = SparkContext.getOrCreate(conf)

  def readFromHbase()={
    val configuration = HBaseConfiguration.create()
    //    直接从hbase中读取数据，使用configuration配置参数
    configuration.set("hbase.zookeeper.quorum","bigdata01")
    configuration.set("hbase.zookeeper.property.clientPort","2181")
    configuration.set(TableInputFormat.INPUT_TABLE,tableName)

    //    先构建job
    //    val jobConf = new JobConf(configuration)
    ////    使用行键过滤器
    //    val startRowKey = "1"
    //    val endRowKey = "5"
    //
    //    val filters = new FilterList(FilterList.Operator.MUST_PASS_ALL)
    //    val startFilter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(startRowKey.getBytes()))
    //    filters.addFilter(startFilter)
    //    val endFilter = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(endRowKey.getBytes()))
    //    filters.addFilter(endFilter)
    ////    创建scan对象
    //    val scan = new Scan()
    //    将过滤器添加到scan对象
    //    scan.setFilter(filters)
    //    scan.withStartRow("1".getBytes())
    //    scan.withStopRow("3".getBytes())

    //    def convertScanToString(scan: Scan) = {
    //      val proto = ProtobufUtil.toScan(scan)
    //      Base64.encodeBytes(proto.toByteArray)
    //    }
    //    configuration.set(TableInputFormat.SCAN,convertScanToString(scan))

    val kvrdd = sc.newAPIHadoopRDD(configuration,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    kvrdd.foreach(x=>{
      val rowKey = x._1
      val result = x._2
      println(
        s"key:${Bytes.toString(rowKey.get())}"+
          s"  i:name:${Bytes.toString(result.getValue("i".getBytes(),"name".getBytes()))}," +
          s" i:age:${Bytes.toString(result.getValue("i".getBytes(),"age".getBytes()))}"
      )
    })
    sc.stop()
  }
  def writeToHBase()={
    val arr = Array("1,abc,23","2,def,44","3,hig,15")
    val inputRdd = sc.parallelize(arr)
    val contentRdd = inputRdd.map(x=>x.split(","))
    val rdd = contentRdd.map(arr=>{
      val put = new Put(Bytes.toBytes(arr(0)))
      put.addColumn(Bytes.toBytes("i"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.addColumn(Bytes.toBytes("i"),Bytes.toBytes("age"),Bytes.toBytes(arr(2)))
      (new ImmutableBytesWritable(),put)
    })
    val configuration = HBaseConfiguration.create()
    configuration.set("hbase.zookeeper.quorum","bigdata01")
    configuration.set("hbase.zookeeper.property.clientPort","2181")
    val jobConf = new JobConf(configuration)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
    rdd.saveAsHadoopDataset(jobConf)
    sc.stop()
  }
  def main(args: Array[String]): Unit = {
//        readFromHbase()
    writeToHBase()
  }

}
