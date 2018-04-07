package com.xiaopeng.bi.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{FloatType, StringType, StructType}
import redis.clients.jedis.JedisPool

/**
  * Created by bigdata on 18-3-7.
  */
object GamePublicNewAdOrderUtils {


  def loadOrderInfo(rdd: RDD[String], hiveContext: HiveContext): Unit = {
    val orderRDD = rdd.filter(line => {
      val fields = line.split("\\|", -1)
      fields(0).contains("bi_order") && fields.length >= 26 && (!fields(2).equals("")) && fields(22).contains("6") && fields(19).contains("4")
    }).map(line => {
      val fields = line.split("\\|", -1)
      //游戏账号（5），订单号（2），订单日期（6），游戏id（7）,充值流水（10）+代金券，imei(24)
      Row(fields(5).trim.toLowerCase, fields(2), fields(6), fields(7).toInt, Commons.getNullTo0(fields(10)) + Commons.getNullTo0(fields(13)), fields(24))
    })
    val struct = new StructType()
      .add("game_account", StringType)
      .add("order_id", StringType)
      .add("order_time", StringType)
      .add("game_id", StringType)
      .add("pay_money", FloatType)
      .add("imei", StringType)
    hiveContext.createDataFrame(orderRDD, struct).registerTempTable("ods_order_cache")

    val sql = "select distinct game_account,order_id,order_time,game_id,pay_money,imei from ods_order_cache oz on join lastPubGame on oz.game_id = lastPubGame.game_id"
    val order_df = hiveContext.sql(sql)
    foreachOrderDF(order_df)
  }

  def foreachOrderDF(order_df: DataFrame): Unit = {
    order_df.foreachPartition(iter => {
      //创建jedis客户端
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis0 = pool.getResource
      jedis0.select(0)
      val jedis12 = pool.getResource
      jedis12.select(12)

      val conn = JdbcUtil.getConn()
      val connFx = JdbcUtil.getXiaopeng2FXConn()
      val stmt = conn.createStatement

      iter.foreach(line => {
        val game_account = line.getAs[String]("game_account")
        val order_id = line.getAs[String]("order_id")
        val order_time = line.getAs[String]("order_time")
        val game_id = line.getAs[Int]("game_id")
        val pay_money = line.getAs[Float]("pay_money")
        val imei = line.getAs[String]("imei")


      })


    })

  }

}
