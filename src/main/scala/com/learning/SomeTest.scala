package com.learning
import com.learning.PairRDDApiTest.sc
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.io.{Source, StdIn}

/**
  * spark com.personaltest
  *
  * Create by zyq on 2018/11/29 0029
  *
  */
//case class Transaction(date:String,time:String,customerId:String,productId:String,quantity:Int,price:Double)
object SomeTest {
  val conf = new SparkConf().setAppName("test").setMaster("local[*]")
  val sc = SparkContext.getOrCreate(conf)

  def reduceByKey()={
    val map = List((1,"a"),(2,"b"),(3,"c"),(3,"d"))
    val kvrdd = sc.parallelize(map)
    val result = kvrdd.reduceByKey((v1,v2)=>s"$v1,$v2")
    result.foreach(println)
  }

  def caseClassMatchTest()={
    val transactionFile = sc.textFile("/transaction/ch04_data_transactions.txt",3)
    val transactionRdd = transactionFile.map(x=>{
      val regex = "(.*)#(.*)#(\\d+)#(\\d+)#(\\d+)#(.*)".r
      x match {
        case regex(c1,c2,c3,c4,c5,c6)=>Transaction(c1,c2,c3,c4,c5.toInt,c6.toDouble)
        case _=>null
      }
    })
    transactionRdd.foreach(println)
    println(transactionRdd.count())
    transactionRdd
  }
  def stdInTest()={
    val line = StdIn.readLine()
    println("您输入的是："+line)
  }
def sourceTest()={
  val line = Source.fromFile("C:\\Users\\Administrator\\Desktop\\nginx日志文件字段说明.txt")

  line.foreach(print)
}
  def regexTest()={
    val pattern = "Scala".r
    val str = "Scala is scalable and Scala cool"
    println(pattern.findAllIn(str).mkString("---"))
  }
  def accumulatorTest()={
    val file = sc.textFile("file:///C:\\Users\\Administrator\\Desktop\\b.txt",1)
    val blankLines = sc.longAccumulator
    file.foreach(x=>{
      println(x)
      blankLines.add(1L)
      println(blankLines)
      println(blankLines.value)

    })
  }
  def rePartitionTest()={
    val kvRdd1 = sc.parallelize(List((1,"a"),(2,"b"),(3,"c"),(4,"d"),(5,"d"),(6,"e"),(3,"e")),3)
    val repartition = kvRdd1.partitionBy(new Partitioner {
      override def getPartition(key: Any): Int = key.asInstanceOf[Int]%2

      override def numPartitions: Int = 2
    }).saveAsTextFile("/repartition3")
  }
  def topNTest()={
    val transactionRdd = PairRDDApiTest.getTransactionRdd()
    val top1 = transactionRdd.map(x=>x.price).top(3)
    println(top1.mkString(","))
    val top2 = transactionRdd.map(x=>x.price).takeOrdered(3)
    println(top2.mkString(","))
  }

  class MyAccumulator extends AccumulatorV2 {
    override def isZero: Boolean = ???

    override def copy(): AccumulatorV2[Nothing, Nothing] = ???

    override def reset(): Unit = ???

    override def add(v: Nothing): Unit = ???

    override def merge(other: AccumulatorV2[Nothing, Nothing]): Unit = ???

    override def value: Nothing = ???
  }

  def allTopNTest()={
    val transactionRdd = PairRDDApiTest.getProductionRdd()
    val allTop1= transactionRdd.top(3)(new Ordering[Product]{
      override def compare(x: Product, y: Product): Int = x.price.compare(y.price)
    })
    println(allTop1.mkString(","))

  }
  def listTest()={
    val list = List("1","2","3","4","5")
    if(list.contains("1")){
      println("111")
    }
  }

  def main(args: Array[String]): Unit = {
//    caseClassMatchTest()
//    stdInTest()
//    sourceTest()
//    regexTest()
//    accumulatorTest()
//    rePartitionTest()
    topNTest()
//    allTopNTest()
//    listTest()

  }
}
