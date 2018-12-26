package com.learning

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark com.learning
  *
  * Create by zyq on 2018/12/3 0003
  *
  */
//case  class Transaction(date:String,time:String,customId:String,productId:String,quantity:Int,price:Double)
object RDDApiTest {
  val conf = new SparkConf().setAppName("rddtest").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
  val fileTransaction = sc.textFile("/transaction/ch04_data_transactions.txt", 3)

  def mapTest()={
    //rdd[String]------>rdd[Transaction]
    println(fileTransaction.count())//得到fileTransaction的记录数
    //每一条记录都会被执行一遍map的算子
    val transaction = fileTransaction.map(x=>{
      val reg = "(.*)#(.*)#(\\d+)#(\\d+)#(\\d+)#(.*)".r   //rdd中每个元素在转换时都会重新创建一个reg对象
      x match {
        case reg(c1,c2,c3,c4,c5,c6)=>Transaction(c1,c2,c3,c4,c5.toInt,c6.toDouble)
        case _ => null
      }
    })
    transaction.foreach(println)
    println(transaction.count())
  }
  def mapPartitionTest()={
    //一个rdd的每个partition上只执行一次mapPartion的算子
    val transaction = fileTransaction.mapPartitions(x=>{
      val reg = "(.*)#(.*)#(\\d+)#(\\d+)#(\\d+)#(.*)".r  //一个分区共用同一个reg对象
      for (item<-x) yield {
        item match {
          case reg(c1,c2,c3,c4,c5,c6) => Transaction(c1,c2,c3,c4,c5.toInt,c6.toDouble)
          case _ => null
        }
      }
    })
    transaction
  }

  def flatMapTest()={
    //姓名，学科，成绩
    val list = List("张三 语文:80,数学:90,英语:70","李四 语文:33,数学:96","王五 语文:55,数学:22,生物:44")
    val rdd = sc.parallelize(list)
    val result = rdd.flatMap(x=>{
      val infos = x.split(" ")
      val classAndScore = infos(1)
      for (cas<-classAndScore.split(","))yield {
        s"${infos(0)},${cas.replace(":",",")}"
      }
    })
    result.foreach(println)

  }

  //排序，根据交易信息中的购买数量对交易信息进行排序
  def sortbyTest()={
    val transaction = mapPartitionTest()
    val result = transaction.sortBy(x=>x.quantity)
//        result.foreach(println)
    //按照交易金额排序
    val result1 = transaction.sortBy(x=>x.quantity*x.price)
//    result1.foreach(println)

    result.mapPartitionsWithIndex((index,pis)=>{
      for (item <-pis)yield {
        (index,item)
      }
    }).foreach(println)
    //二次排序 交易的数量和价格排序
    val result2 = transaction.sortBy(x=>(x.quantity,x.price))
//    result2.saveAsTextFile("/testsortby")
  }
  //求价格最高的topN 5
  //价格最低的topN 5
  def topNTest()={
    val transaction = mapPartitionTest()
    val topn1 = transaction.map(x=>x.price).top(5)
    val topn2 = transaction.map(x=>x.price).takeOrdered(5)
    println(topn1.mkString(","))
    println(topn2.mkString(","))
    //全信息topN
    val topn11 = transaction.top(5)(new Ordering[Transaction] {
      override def compare(x: Transaction, y: Transaction): Int = x.price.compare(y.price)
    })
    println(topn11.mkString(","))

    val topn22 = transaction.takeOrdered(5)(new Ordering[Transaction] {
      override def compare(x: Transaction, y: Transaction): Int = x.price.compare(y.price)
    })
    println(topn22.mkString(","))
  }
  //集合操作
  def collectTest()={
    val rdd1 = sc.parallelize(List(1,3,5,7,9))
    val rdd2 = sc.parallelize(List(2,4,6,8,10))
    val rdd3 = sc.parallelize(List("a","b","c","d"))
    //交集
    val rdd4 = rdd1.intersection(rdd2)
    rdd4.foreach(print)

    //合集
    val rdd5 = rdd1.union(rdd2)
    val rdd55 = rdd1++rdd2
    rdd5.foreach(print)
    rdd55.foreach(print)
    // 减集
    val rdd6 = rdd1.subtract(rdd2)
    rdd6.foreach(print)
    //笛卡尔乘积
    val rdd7 = rdd1.cartesian(rdd3)
    rdd7.foreach(print)

  }
  //缓存
  def cacheTest()= {
    val transaction = mapPartitionTest()
    //缓存transaction
    //    transaction.cache()
    //    transaction.persist()//等同于cache，缓存在内存中
    transaction.persist(StorageLevel.MEMORY_AND_DISK_2)
    //使用MEMORY_AND_DISK级别缓存rdd
    //多次使用transaction
    val startTime = System.currentTimeMillis()
    transaction.foreach(println)
    println(transaction.count())
    println(System.currentTimeMillis() - startTime)
    //取消缓存
    transaction.unpersist()
  }
  //重新分区
  def repartitionTest()={
    val rdd = sc.parallelize(List("a","b","c","d","e","f","g","h"),4)
    rdd.saveAsTextFile("/sparkapitest/rep0")
    val coalesce = rdd.coalesce(2)
    coalesce.saveAsTextFile("/sparkapitest/rep1")
    val repartition = rdd.repartition(2)
    repartition.saveAsTextFile("/sparkapitest/rep2")
    val repartition1 = rdd.repartition(6)
    repartition1.saveAsTextFile("/sparkapitest/rep3")
  }

  def countByValue()={

    val rdd = sc.parallelize(List(1,2,3,5,6,1,3,5,3))
    rdd.foreach(println)
    val result = rdd.countByValue()
    val result1 = rdd.max()
    val result2 = rdd.min()
    val result3 = rdd.count()


    result.foreach(println)
    println(result1+"---"+result2+"---"+result3)
  }
  def practiceTest()={
    //1.计算transaction文件中的总交易额
    val transaction = mapPartitionTest()
    val sumtradeprice = transaction.map(x=>(x.price*x.quantity))
    //    sumtradeprice.foreach(println)
    val sumprice = sumtradeprice.reduce((x1,x2)=>x1+x2)
    //    println(sumprice)
    //2.计算transaction最大交易金额
    val maxtradeprice = sumtradeprice.max()
    //    println(maxtradeprice)
    //3.计算出交易发生的最小的时间
    val time = transaction.map(x=>x.date+" "+x.time)
        time.foreach(println)

    val dateformat = new SimpleDateFormat("yyyy-MM-dd hh:mm a",Locale.ENGLISH)
    val datetime = time.map(x=>dateformat.parse(x)).min()
        println(datetime)

    //4.把购买过productId=72的用户id合并成一个字符串：customerId0,customerId1,...
    val rdd1 = transaction.filter(x=>x.productId=="72").map(x=>s"${x.productId}:customerId:${x.customerId}")
    rdd1.foreach(println)
    //5.把customerId=58的用户的订单信息合并成(productId0:amount0,productId1:amount1,...)

    val rdd2 = transaction.filter(x=>x.customerId=="58").map(x=>s"${x.customerId}:productId:${x.productId},amount:${x.price*x.quantity}")
    //    rdd2.foreach(println)
  }


  def main(args: Array[String]): Unit = {
    //    mapTest()
//        flatMapTest()
//        sortbyTest()
//          topNTest()
          collectTest()
//        cacheTest()
//        repartitionTest()
//        countByValue()
//    practiceTest()

  }

}
