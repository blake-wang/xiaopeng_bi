package com.xiaopeng.bi.app

import java.sql.PreparedStatement

import com.xiaopeng.bi.utils.{ConfigurationUtil, Hadoop, JdbcUtil, JedisUtil}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by denglh on 2016/11/3.
  * function:实现最近在玩,累计逻辑,下载等实现
  */
object AppV2_2Install {
  def main(args: Array[String]) {
    Hadoop.hd
    val ssc= StreamingContext.getOrCreate(ConfigurationUtil.getProperty("checkpointdirinstall"), recently _)
    ssc.start()
    ssc.awaitTermination()
  }

 /**
    * 把数据加载到bi_app_phoneapps表，记录手机内应用安装情况
   *
   * @param appInstall time(0),deviceid(1),packname(2），appname（3），status（4）,os(5)
    * */
  def appPhoneApps(appInstall: DStream[(String, String, String, String, Int,String)]) = {
    val sql="insert into bi_app_phoneapps(device_id,pkg_name,app_name,status,opera_date,platform) values(?,?,?,?,?,?) on duplicate key update app_name=?,opera_date=?,status=?,platform=?"
    appInstall.foreachRDD(rows=>{
    println("-------->appPhoneApps开始处理")
      rows.foreachPartition(fp=>{
        val conn = JdbcUtil.getConn() //数据库连接
        val ps: PreparedStatement = conn.prepareStatement(sql)
        //获取redis
        val pool = JedisUtil.getJedisPool
        val jedis=pool.getResource
        jedis.select(10)
        fp.foreach(ds => {
          val pkg=jedis.hget(ds._3.toLowerCase,"package_name")
         if(pkg!=null) {
             println("---------------------------->"+pkg)
             ps.setString(1, ds._2)
             ps.setString(2, ds._3)
             ps.setString(3, ds._4)
             ps.setInt(4, if(ds._5==0){2} else 1)
             ps.setString(5, ds._1)
             ps.setString(6, ds._6)
             //update info
             ps.setString(7, ds._4)
             ps.setString(8, ds._1)
             ps.setInt(9, if(ds._5==0){2} else 1)
             ps.setString(10, ds._6)
             ps.executeUpdate()
           }
        })
        conn.close()
        //redis close
        pool.returnResource(jedis)
        pool.destroy()
      })
      println("-------->appPhoneApps处理结束")
    })
  }

  /**
    * 获取kafka streaming数据
    *
    * @return StreamingContext
    */
  def recently(): StreamingContext = {
    val Array(brokers, topics) = Array(ConfigurationUtil.getProperty("kafka.metadata.broker.list"),ConfigurationUtil.getProperty("topics2"))
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$",""))
    sparkConf.set("spark.streaming.backpressure.enabled","true")
    //sparkConf.set("spark.streaming.kafka.maxRatePerPartition","60")
    val ssc = new StreamingContext(new SparkContext(sparkConf), Seconds(100))//5 Seconds
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    //从kafka中获取所有游戏日志数据
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    //获取人tuple的values
    val valuesDstream = messages.map(_._2.toLowerCase)

    //手机内安装，time(0),deviceid(1),packname(2），appname（3），status（4）,platform(5),需要判断是否有6个长度
    val appInstall=valuesDstream.filter(x=>x.split("\\|")(0).contains("bi_install_app")).filter(_.split("\\|",-1).length>=6)
                                 .map(x=>{(x.split("\\|")(0).split(",")(0),x.split("\\|")(1),x.split("\\|")(2),x.split("\\|")(3),x.split("\\|",-1)(4).toInt,if(x.split("\\|",-1)(5).equals("")||x.split("\\|",-1)(5).equals("1")) {"android"} else {"ios"})}).cache()

    //手机安装
    try {appPhoneApps(appInstall); }    catch {case e: Exception => e.printStackTrace}

     ssc.checkpoint(ConfigurationUtil.getProperty("checkpointdirinstall"))
     ssc
  }

}


