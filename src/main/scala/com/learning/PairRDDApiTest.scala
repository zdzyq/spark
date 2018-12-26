package com.learning

//import com.learning.SharedVariables.sc
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * spark com.learning
  *
  * Create by zyq on 2018/11/29 0029
  *
  */
case  class Transaction(date:String,time:String,customerId:String,productId:String,quantity:Int,price:Double)
case class Product(id:Int,name:String,price:Double,category:Int)
object PairRDDApiTest {
  val conf = new SparkConf().setAppName("PairRDDApiTest").setMaster("local[*]")
  val sc = SparkContext.getOrCreate(conf)

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

  def getProductionRdd()={
    val productfileRdd = sc.textFile("/product/ch04_data_products.txt")
    val productRdd = productfileRdd.map(x=>{
      val rex = "(\\d+)#(.*)#(.*)#(\\d+)".r
      x match {
        case rex(c1,c2,c3,c4)=>Product(c1.toInt,c2,c3.toDouble,c4.toInt)
        case _ =>null
      }
    })
      productRdd
  }

  //kvrdd
  def mapTest()={
    val kvRdd1 = sc.parallelize(List((1,"a"),(2,"b"),(3,"c"),(4,"d"),(5,"d"),(6,"e"),(3,"e")))
    val rdd1 = sc.parallelize(List(1,2,3,4,5))
    val rdd2 = sc.parallelize(List("a,f,k","b,g","c,x,z","d,u","e,i"))
    val rddString =sc.parallelize(List(("a",1,8),("b",3,6),("c",3,7)))
    //拉链操作，集合合并
    val kvRdd2 = rdd1.zip(rdd2)
    kvRdd2.foreach(println)
    //取出所有key值
    val keys1 = kvRdd1.keys
    keys1.foreach(println)
    //集合中第一个元素为字符串类型，将取出来的内容加载原来的元素之前
    val keyby = rddString.keyBy(x=>(x._2,x._1,x._3))
    keyby.foreach(println)
    val keyby1 = kvRdd1.keyBy(x=>x._2)
    keyby1.foreach(println)
    //取出所有value值
    val values  = kvRdd2.values
    values.foreach(println)
    //对所有value进行map操作
    val map1 = kvRdd1.mapValues(x=>x.toUpperCase)
    map1.foreach(println)
    //对所有value进行展平操作
    val map2 = kvRdd2.flatMapValues(x=>x.split(","))
    map2.foreach(println)
    //根据key值查找value值
    val lookup = kvRdd1.lookup(3)
    lookup.foreach(println)
  }
  //根据key做交集和减集
  def substractTest()={
    val kvRdd1 = sc.parallelize(List((1,"a"),(2,"b"),(3,"c"),(4,"d"),(5,"d"),(6,"e"),(3,"e")))
    val kvRdd2 = sc.parallelize(List(2->"f",3->"g",4->"d"))
    //减集
    val subRdd = kvRdd1.subtractByKey(kvRdd2)
    println("substractByKey减集操作")
    subRdd.foreach(println)
    //交集
    val intersect = kvRdd1.intersection(kvRdd2)
    println("intersect交集操作")
    intersect.foreach(println)
  }
  //重分区
  def partitionByTest()={
    val kvRdd1 = sc.parallelize(List((1,"a"),(2,"b"),(3,"c"),(4,"d"),(5,"d"),(6,"e"),(3,"e")),3)
    val kvRdd2 = sc.parallelize(List(("a",1),("b",2),("c",3),("d",4),("d",5),("e",6),("e",7)),4)
    val repartition = kvRdd2.partitionBy(new Partitioner {
      //      override def getPartition(key: Any): Int = key.asInstanceOf[Int]%2   //如何分区
      override def getPartition(key: Any): Int =if (key=="c"||key=="a") 0 else 1 //如何分区
      override def numPartitions: Int = 2//分区数
    })
          repartition.saveAsTextFile("/sparkapitest/rep4")
  }
  //按照产品类型分组
  def groupByTest()={
    val productRdd = getProductionRdd().filter(x=>x!=null).keyBy(_.category)
    productRdd.foreach(println)
    //以key作分组聚合，同一个key下的value聚合为一个Iterable对象
    val grpByKey = productRdd.groupByKey()
    grpByKey.foreach(println)

    grpByKey.foreach(x=>{
      println(s"${x._1}------------")
      for (product<-x._2) println(s"    ${product}")
    })

//  使用groupby计算出，每个品类下的商品数
    val categoryCount = grpByKey.mapValues(x=>x.size)//对value进行各种操作（保留key）
    categoryCount.foreach(println)
  }

  //先分组再关联：cogroup等同于groupWith
  def cogroupTest()={
    val kvRdd1 = sc.parallelize(List((1,"a"),(1,"b"),(2,"c"),(2,"d"),(3,"a"),(3,"f"),(4,"e")),1)
    val kvRdd2 = sc.parallelize(List((1,"A"),(1,"C"),(2,"R"),(2,"T"),(3,"I"),(3,"U"),(5,"X")),1)
    val result = kvRdd1.cogroup(kvRdd2)
        result.foreach(println)//cogroup 关联不上的key会相互的补空
//    result.foreach(x=>{
//      println(s"${x._1}--------------")
//      println(s"${x._2}")
//    })
  }
  def joinTest()={
    val kvRdd1 = sc.parallelize(List((1,"a"),(1,"b"),(2,"c"),(2,"d"),(3,"a"),(3,"f"),(4,"e")),1)
    val kvRdd2 = sc.parallelize(List((1,"A"),(1,"C"),(2,"R"),(2,"T"),(3,"I"),(3,"U"),(5,"X")),1)
    //innerjoin内连接
    val innerjoin = kvRdd1.join(kvRdd2) //等同于kvRdd2.join(kvRdd1)，因为两个数据集的地位是平等的
        innerjoin.foreach(print)
    //lefeOuterJoin  rightOuterJion左外/右外连接
    val leftjoin = kvRdd1.leftOuterJoin(kvRdd2)//用kvRdd1的key去过滤kvRdd2中的key，2有1没有的不会出现在结果集中，1有2没有的会在结果集中
        leftjoin.foreach(println)
    val rightjoin = kvRdd1.rightOuterJoin(kvRdd2)//用kvRdd2的key去过滤kvRdd1中的key，2有1没有的会出现在结果集中，1有2没有的不会在结果集中
    //    rightjoin.foreach(println)
    //fullOuterJoin
    val fullouterjoin = kvRdd1.fullOuterJoin(kvRdd2)
    fullouterjoin.foreach(println)
  }
  //计算每个品类下商品的个数

  //统计出每个客户的销售总金额
  def reduceByKeyTest()={
    val transaction = getTransactionRdd()
    val result = transaction.map(x=>(x.customerId,x.quantity*x.price))
      .foldByKey(0.00)((v1,v2)=>v1+v2)
    result.foreach(println)
  }
  def saveAsTest()={
    val kvRdd1 = sc.parallelize(List((1,"a"),(1,"b"),(2,"c"),(2,"d"),(3,"a"),(3,"f"),(4,"e")),1).map(x=>(new IntWritable(x._1),new Text(x._2)))
    kvRdd1.saveAsNewAPIHadoopFile("/sparktest/saveas",classOf[IntWritable],classOf[Text],classOf[SequenceFileOutputFormat[IntWritable,Text]],sc.hadoopConfiguration)

    //      val readRDD = sc.newAPIHadoopFile("/sparktest/saveas",)
  }

  def main(args: Array[String]): Unit = {
//    mapTest()
//    substractTest()
//    partitionByTest()
    groupByTest()
//    cogroupTest()
//    joinTest()
//    reduceByKeyTest()
//    saveAsTest()
//    getProductionRdd().foreach(println)

  }

}
