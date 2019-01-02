package com.zhongdakeji

import java.util.Properties

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * spark com.zhongdakeji
  *
  * Create by zyq on 2018/12/28 0028
  *
  */
object DayReviewCount {
  //创建sparksession（如果提交spark集群运行，需将master设置为集群主节点地址或在提交时指定）
  val sparkSession = SparkSession.builder().appName("day_review_count").master("local[*]").getOrCreate()
  //创建sc
  val sc = sparkSession.sparkContext
  //需要使用的输入表和输出表
  val inputTable1 = "authdb.t_user_inner"
  val inputTable2 = "authdb.t_user_read_user_info"
  val inputTable3 = "content.t_article_review_users"
  val outputTable = "t_internal_users_day_review"

  def readHBaseToMysql() = {
    //构建HBaseConfiguration
    val configuration01 = HBaseConfiguration.create()
    //使用configuration配置参数（zookeeper的节点名称和端口号）
    configuration01.set("hbase.zookeeper.quorum", "node5,node6,node7")
    configuration01.set("hbase.zookeeper.property.clientPort", "2181")
    configuration01.set(TableInputFormat.INPUT_TABLE, inputTable1)
    val kvrdd01 = sc.newAPIHadoopRDD(configuration01, classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
      .map(x => {
        //获取字段
        val rowKey = Bytes.toString(x._1.get())
        val authorName = Bytes.toString(x._2.getValue("tui".getBytes(), "AUTHORNAME".getBytes()))
        val userId = Bytes.toString(x._2.getValue("tui".getBytes(), "USERID".getBytes()))
        val phone = Bytes.toString(x._2.getValue("tui".getBytes(), "PHONE".getBytes()))
        val tuiType = Bytes.toString(x._2.getValue("tui".getBytes(), "TYPE".getBytes()))
        val createTime = Bytes.toString(x._2.getValue("tui".getBytes(), "CREATE_TIME".getBytes()))
        (authorName, userId, phone, tuiType, createTime)
      })

    val configuration02 = HBaseConfiguration.create()
    configuration02.set("hbase.zookeeper.quorum", "node5,node6,node7")
    configuration02.set("hbase.zookeeper.property.clientPort", "2181")
    configuration02.set(TableInputFormat.INPUT_TABLE, inputTable2)
    val kvRdd02 = sc.newAPIHadoopRDD(configuration02, classOf[TableInputFormat], classOf[ImmutableBytesWritable],classOf[Result])
      .map(x => {
        val rowKey = Bytes.toString(x._1.get())
        val userId = Bytes.toString(x._2.getValue("tur".getBytes(), "USER_ID".getBytes()))
        val active = Bytes.toString(x._2.getValue("tur".getBytes(), "ACTIVE".getBytes()))
        val createTime = Bytes.toString(x._2.getValue("tur".getBytes(), "CREATE_TIME".getBytes()))
        val delMark = Bytes.toString(x._2.getValue("tur".getBytes(), "DEL_MARK".getBytes()))
        val isAudit = Bytes.toString(x._2.getValue("tur".getBytes(), "IS_AUDIT".getBytes()))
        val linkPhone = Bytes.toString(x._2.getValue("tur".getBytes(), "LINK_PHONE".getBytes()))
        val nickName = Bytes.toString(x._2.getValue("tur".getBytes(), "NICK_NAME".getBytes()))
        val password = Bytes.toString(x._2.getValue("tur".getBytes(), "PASSWORD".getBytes()))
        val photo = Bytes.toString(x._2.getValue("tur".getBytes(), "PHOTO".getBytes()))
        (userId, active, createTime, delMark, isAudit, linkPhone, nickName, password, photo)
      })

    val configuration03 = HBaseConfiguration.create()
    configuration03.set("hbase.zookeeper.quorum", "node5,node6,node7")
    configuration03.set("hbase.zookeeper.property.clientPort", "2181")
    configuration03.set(TableInputFormat.INPUT_TABLE, inputTable3)
    val kvRdd03 = sc.newAPIHadoopRDD(configuration03, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      .map(x => {
        val rowKey = Bytes.toString(x._1.get())
        val id = Bytes.toString(x._2.getValue("tar".getBytes(), "ID".getBytes()))
        val status = Bytes.toString(x._2.getValue("tar".getBytes(),"STATUS".getBytes()))
        val userId = Bytes.toString(x._2.getValue("tar".getBytes(),"USER_ID".getBytes()))
        val articleId = Bytes.toString(x._2.getValue("tar".getBytes(),"ARTICLE_ID".getBytes()))
        val articleTitle = Bytes.toString(x._2.getValue("tar".getBytes(),"ARTICLE_TITLE".getBytes()))
        val articelType = Bytes.toString(x._2.getValue("tar".getBytes(),"ARTICLE_TYPE".getBytes()))
        val authorId = Bytes.toString(x._2.getValue("tar".getBytes(),"AUTHOR_ID".getBytes()))
        val examinedTime = Bytes.toString(x._2.getValue("tar".getBytes(),"EXAMINED_TIME".getBytes()))
        (id,status,userId,articleId,articleTitle,articelType,authorId,examinedTime)
      })
    //隐式转换：将rdd转换为dataframe
    import sparkSession.implicits._
    //添加字段名称
    val df1 = kvrdd01.toDF("AUTHOR_NAME", "USER_ID", "PHONE", "TYPE", "CREATE_TIME")
    val df2 = kvRdd02.toDF("USER_ID", "ACTIVE", "CREATE_TIME", "DEL_MARK", "IS_AUDIT", "LINK_PHONE", "NICK_NAME", "PASSWORD", "PHOTO")
    val df3 = kvRdd03.toDF("ID","STATUS","USER_ID","ARTICLE_ID","ARTICLE_TITLE","ARTICLE_TYPE","AUTHOR_ID","EXAMINED_TIME")
    //创建临时表，并设置表名称
    df1.createOrReplaceTempView("t_user_inner")
    df2.createOrReplaceTempView("t_user_read_user_info")
    df3.createOrReplaceTempView("t_article_review_users")
    //加载驱动
    Class.forName("com.mysql.jdbc.Driver")
    //构建jdbc连接
//    val url = "jdbc:mysql://192.168.8.33:8066/contentdb?serverTimezone=UTC&characterEncoding=utf-8"
    val url = "jdbc:mysql://localhost:3306/contentdb?serverTimezone=UTC&characterEncoding=utf-8"
    val properties = new Properties()
    //设置登录用户名
    properties.setProperty("user", "root")
    //设置登录密码
    properties.setProperty("password", "123456")
    //执行sql
    val result1 = sparkSession.sql(
      """
        |
        |select
        |tar.USER_ID,count(0) as TOTAL_ARTICLE_REVIEW,date_format(tar.EXAMINED_TIME,'yyyy-MM-dd') as STATISTICAL_TIME,
        |tui.AUTHOR_NAME,tui.PHONE,tui.TYPE
        |from
        |t_user_inner tui,
        |t_article_review_users tar
        |where tui.USER_ID = tar.USER_ID
        |and tui.TYPE = 5
        |and date_format(tar.EXAMINED_TIME,'yyyy-MM-dd')=date_format(date_sub(now(), 20), 'yyyy-MM-dd')
        |group by
        |date_format(tar.EXAMINED_TIME,'yyyy-MM-dd'),tar.USER_ID,tui.AUTHOR_NAME,tui.PHONE,tui.TYPE
        |order by date_format(tar.EXAMINED_TIME,'yyyy-MM-dd')
        |
          """.stripMargin)
    val result2 = sparkSession.sql(
      """
        |
        |select
        |tar.USER_ID,count(0) as TOTAL_ARTICLE_REVIEW,date_format(tar.EXAMINED_TIME,'yyyy-MM-dd') as STATISTICAL_TIME,
        |tur.NICK_NAME as AUTHOR_NAME,tur.LINK_PHONE as PHONE,'-1' TYPE
        |from
        |t_user_read_user_info tur,
        |t_article_review_users tar
        |where tur.USER_ID = tar.USER_ID
        |and date_format(tar.EXAMINED_TIME,'yyyy-MM-dd')=date_format(date_sub(now(), 20), 'yyyy-MM-dd')
        |group by
        |date_format(tar.EXAMINED_TIME,'yyyy-MM-dd'),tar.USER_ID,tur.NICK_NAME,tur.LINK_PHONE,TYPE
        |order by date_format(tar.EXAMINED_TIME,'yyyy-MM-dd')
        |
        |
      """.stripMargin)
    //将sql结果进行uinon操作
    val result = result1.union(result2)
    //将最终结果打印到控制台
//    result.show()
    //将最终结果写入mysql数据库对应的表中
    result.write.mode(SaveMode.Append).jdbc(url,outputTable,properties)
    //关闭sparksession
    sparkSession.close()
    //关闭sc
    sc.stop()
  }
  def main(args: Array[String]): Unit = {
    readHBaseToMysql()
  }
}
