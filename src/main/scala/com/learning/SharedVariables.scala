package com.learning

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import java.lang.{Double=>jDouble}

/**
  * spark com.learning
  *
  * Create by zyq on 2018/11/29 0029
  *
  */
object SharedVariables {
  val conf = new SparkConf().setAppName("SharedVariablesTest").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    def readTransaction() = {
      val regex = "(.*)#(.*)#(\\d+)#(\\d+)#(\\d+)#(.*)".r
      val rdd = sc.textFile("/transaction/ch04_data_transactions.txt")
        .map(x=>{
          x match{
            case regex(c1,c2,c3,c4,c5,c6) => Transaction(c1,c2,c3,c4,c5.toInt,c6.toDouble)
            case _ => null
          }
        })
      rdd
    }
    def readTransactionOptimization() = {
      val regex = "(.*)#(.*)#(\\d+)#(\\d+)#(\\d+)#(.*)".r
      val broadcastRegex = sc.broadcast(regex)
      val rdd = sc.textFile("/transaction/ch04_data_transactions.txt")
        .map(x=>{
          val reg = broadcastRegex.value
          x match{
            case reg(c1,c2,c3,c4,c5,c6) => Transaction(c1,c2,c3,c4,c5.toInt,c6.toDouble)
            case _ => null
          }
        })
      rdd
    }
    //大表小表关联----》使用广播变量，广播小表的数据
    //统计每个品类商品的销售总额
    //同时统计出交易金额大于10000的交易订单个数，小于10000的交易订单个数，
    // 交易订单最大的交易金额是多少，productId为18的交易订单数，客户id为94的交易总金额
  def mapSideJionTest()={
      val transaction = readTransactionOptimization()
      val product = PairRDDApiTest.getProductionRdd()
      //product小表，广播
      val productCategory = product.map(x=>(x.id,x.category)).collectAsMap()
      val broadcastProductCategory = sc.broadcast(productCategory)  //广播
      //map端关联可以对大表使用map方法就可以实现
      val joinResult = transaction.map(x=>{
        (x,broadcastProductCategory.value.get(x.productId.toInt))  //相当于leftJoin
      })
      val result = joinResult.map(x=>(x._2,x._1.price*x._1.quantity)).combineByKey(
        (v:Double) => v,
        (c:Double,v:Double) => c+v,
        (c1:Double,c2:Double) => c1 +c2
      )
      result.foreach(println)
    }
    //大表小表关联----》使用广播变量，广播小表的数据
    //统计每个品类商品的销售总额
    //同时统计出交易金额大于10000的交易订单个数，小于10000的交易订单个数
    // ，交易订单最大的交易金额是多少 ----不太适合使用累加器
    // ，productId为18的交易订单数，客户id为94的交易总金额
    def mapSideJionTestOptimization()={
      val transaction = readTransactionOptimization()
      val product = PairRDDApiTest.getProductionRdd()
      //product小表，广播
      val productCategory = product.map(x=>(x.id,x.category)).collectAsMap()
      val broadcastProductCategory = sc.broadcast(productCategory)  //广播

      //声明累加器
      val a1 = sc.longAccumulator("a1") //交易金额大于10000的交易订单个数
      val a2 = sc.longAccumulator("a2") //小于10000的交易订单个数
      val a3 = sc.longAccumulator("a3") //productId为18的交易订单数
      val a4 = sc.doubleAccumulator("a4") //客户id为94的交易总金额
      val a5 = new MyMaxAccumulator()   //实例化累加器变量
      sc.register(a5,"a5")             //注册累加器
      //map端关联可以对大表使用map方法就可以实现
      val joinResult = transaction.map(x=>{
        if(x.price*x.quantity>10000) a1.add(1) else a2.add(1)
        if(x.productId == "18") a3.add(1)
        if(x.customerId == "94") a4.add(x.price*x.quantity)
        a5.add(x.price*x.quantity)
        (x,broadcastProductCategory.value.get(x.productId.toInt))  //相当于leftJoin
      })
      val result = joinResult.map(x=>(x._2,x._1.price*x._1.quantity)).combineByKey(
        (v:Double) => v,
        (c:Double,v:Double) => c+v,
        (c1:Double,c2:Double) => c1 +c2
      )
      result.foreach(println)
      println(s"大于10000的订单数：${a1.value}")
      println(s"小于10000的订单数：${a2.value}")
      println(s"productId为18的交易订单数：${a3.value}")
      println(s"客户id为94的交易总金额：${a4.value}")
      println(s"最大交易金额：${a5.value}")
    }
    //自定义accumulator实现求最大值
    class MyMaxAccumulator extends AccumulatorV2[jDouble,jDouble]{
      var _maxValue:jDouble = null
      override def isZero: Boolean = if(_maxValue==null) true else false
      //做一个对象的拷贝
      override def copy(): AccumulatorV2[jDouble, jDouble] = {
        val cp = new MyMaxAccumulator()
        cp._maxValue = this._maxValue
        cp
      }
      override def reset(): Unit = this._maxValue = null
      override def add(v: jDouble): Unit = {
        if(_maxValue==null){
          _maxValue = v
        }else{
          if(_maxValue<v) _maxValue = v
        }
      }
      override def merge(other: AccumulatorV2[jDouble, jDouble]): Unit = {
        if(_maxValue == null) _maxValue = other.value
        if(other!=null && _maxValue<other.value){
          _maxValue = other.value
        }
      }
      override def value: jDouble = _maxValue
    }

  def main(args: Array[String]): Unit = {
//    mapSideJionTest()
    mapSideJionTestOptimization()
  }
}
