package com.xiaopeng.bi.crontal

import java.sql.{PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import com.xiaopeng.bi.utils.{FileUtil, JdbcUtil}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by 苏孟虎 on 2016/8/24.
 * 对游戏发布数据报表统计-基本指标
 */
object TbVsHdfs {

  val logger = Logger.getLogger(TbVsHdfs.getClass)


  /**
    * 获取数据中的账号数
    *
    * @param jg30
    * @param jg60
    * @return
    */

  def getDbAccouts(jg30: String, jg60: String) :Double={
    val conn=JdbcUtil.getXiaopeng2Conn();
    var jg=0.0
    var stmt: PreparedStatement = null
    val sql: String =" select count(distinct orderid) ods from orders  where state=4 and  erectime<='jg30' and erectime>='jg60'".replace("jg30",jg30).replace("jg60",jg60)
    stmt=conn.prepareStatement(sql)
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next) {
      jg=rs.getString("ods").toDouble;
    }
    stmt.close()
    return jg;
  }

  /**
    * 获取数据中的订单数
    *
    * @param jg30
    * @param jg60
    * @return
    */

  def getDbOrders(jg30: String, jg60: String) :Double={
    val conn=JdbcUtil.getXiaopeng2Conn();
    var jg=0.0
    var stmt: PreparedStatement = null
    val sql: String =" select count(distinct orderid) ods from orders  where state=4 and  erectime<='jg30' and erectime>='jg60'".replace("jg30",jg30).replace("jg60",jg60)
    println(sql)
    stmt=conn.prepareStatement(sql)
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next) {
      jg=rs.getString("ods").toDouble;
    }
    stmt.close()
    return jg;
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$",""))
      .set("spark.sql.shuffle.partitions","60")
    val sc = new SparkContext(sparkConf)
    val sqlContext=new SQLContext(sc)
    //注册成表
    getOrderData(sc,sqlContext)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val currentTime = System.currentTimeMillis() ;
    //30分钟前
    val currentTime30 =currentTime-25*60*1000;
    val jg30=dateFormat.format(new Date(currentTime30));

    //60分钟前
    val  currentTime60 =currentTime-75*60*1000;
    val jg60=dateFormat.format(new Date(currentTime60));
    //从库中获取
    val dbods: Double =getDbOrders(jg30,jg60);
    println(dbods)
    //hdfs中获取
    val result = sqlContext.sql("select count(distinct order_no) from orders_tmp where order_no!='' and order_status='4' and order_time<='jg30' and order_time>='jg60' limit 1".replace("jg30",jg30).replace("jg60",jg60))
   // println("select count(distinct order_no) from orders_tmp  where order_no!=''  and order_time<='jg30' and order_time>='jg60'".replace("jg30",jg30).replace("jg60",jg60))
    result.show()
    var jgrz: Double =0.0
    jgrz=result.take(1).apply(0).get(0).toString.toDouble
    println(jgrz)
    println((jgrz/dbods).toString)
    //比较
    if(dbods>5.0)
      {
        FileUtil.delfile("/home/hduser/projs/logs/.TbVsHdfs.data")
        FileUtil.apppendTofile("/home/hduser/projs/logs/.TbVsHdfs.data",if(jgrz/dbods<=0.8) {(jgrz/dbods).toString} else "1");
      }

  }

  /**
    * 获取表里面的订单数
    *
    * @param sc
    * @param sqlContext
    */
  def getOrderData(sc:SparkContext,sqlContext: SQLContext): Unit ={
    val df = new SimpleDateFormat("yyyyMMdd");//设置日期格式
    val currentDay=df.format(new Date());
    val regiRdd = sc.newAPIHadoopFile("/user/hive/warehouse/yyft.db/order/*"+currentDay+"*",
      classOf[CombineTextInputFormat],
      classOf[LongWritable],
      classOf[Text]).map(line => {line._2.toString})

    val pubGameRdd = regiRdd.filter(line => line.contains("bi_order")).map(line => {
      try {
        val splited = line.split("\\|",-1)
        Row(splited(2),splited(6),splited(19))
      }catch {
        case ex: Exception => {
          Row("","","")
        }
      }
    })
    val pubGameStruct = (new StructType).add("order_no",StringType).add("order_time", StringType).add("order_status", StringType)
    val pubGameDF = sqlContext.createDataFrame(pubGameRdd, pubGameStruct);
    pubGameDF.registerTempTable("orders_tmp")
  }


}
