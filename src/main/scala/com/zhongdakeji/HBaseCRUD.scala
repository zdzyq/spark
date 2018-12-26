package com.zhongdakeji

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/**
  * spark com.zhongdakeji
  *
  * Create by zyq on 2018/11/27 0027
  *
  */
object HBaseCRUD {
  val tableNameValue = "aa"
  val tableName = TableName.valueOf(tableNameValue)
  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum","bigdata01")
  conf.set("hbase.zookeeper.property.clientPort","2181")
  val connection = ConnectionFactory.createConnection(conf)
  val key = "1"
  val family = "i"
  val column01 = "i:name"
  val value01 = "zhangsan"
  val column02 = "i:age"
  val value02 = 23
  val admin = connection.getAdmin
  val table = connection.getTable(tableName)


  def createTable()={
    if (admin.tableExists(tableName)){
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
      println("删除已有表成功")
    }
    val tableDesc = new HTableDescriptor(tableName)
    tableDesc.addFamily(new HColumnDescriptor(family.getBytes()))
    if(!admin.tableExists(tableName)){
      admin.createTable(tableDesc)
      println("创建表成功")
    }
    connection.close()
  }
  def upsertTable()={
    val putIn = new Put(key.getBytes())
    putIn.addColumn(family.getBytes(),column01.getBytes(),value01.getBytes())
    putIn.addColumn(family.getBytes(),column02.getBytes(),value02.toString.getBytes())
    table.put(putIn)
    println("插入表数据成功")
    connection.close()
  }
  def deleteData()={
    val delete = new Delete(key.getBytes())
    //    delete.addFamily("i".getBytes())
    delete.addColumn(family.getBytes(),column01.getBytes())
    delete.addColumn(family.getBytes(),column02.getBytes())
    table.delete(delete)
    println("删除表数据成功")
    connection.close()
  }
  def getData()={
    val getData = new Get(key.getBytes())
    val result = table.get(getData)
    val name = Bytes.toString(result.getValue(family.getBytes(),column01.getBytes()))
    val age = Bytes.toString(result.getValue(family.getBytes(),column02.getBytes()))
    println(s"获取的表数据为：name:$name--age:$age")
    connection.close()
  }
  def scanData()={
    val scanData = new Scan()
    val resultScanner = table.getScanner(scanData)
    val result = resultScanner.next()
    while (result!=null){
      while (result.advance()){
        val cell = result.current()
        val row = Bytes.toString(CellUtil.cloneRow(cell))
        val family = Bytes.toString(CellUtil.cloneFamily(cell))
        val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
        val timeStamp = cell.getTimestamp
        val value = Bytes.toString(CellUtil.cloneValue(cell))
        println(s"扫描的表结果为：row:$row--timeStamp:$timeStamp--family:$family--qualifier:$qualifier--value:$value")
      }
      resultScanner.next()
    }
    connection.close()
  }



  def main(args: Array[String]): Unit = {
    //    createTable()
    //    upsertTable()
    //    deleteData()
    //    getData()
    scanData()
  }

}
