package com.xiaopeng.bi.utils


import java.util.Date

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


/**
  * Created by bigdata on 17-9-16.
  * 用来更新bi_adv_momo_click 历史数据
  */
object UpdateTableOnlyOnce {


  def main(args: Array[String]): Unit = {
    var currentday = ""
    if (args.length > 0) {
      currentday = args(0)
    }

    //09-16
    //更新历史imei_md5_upper
    //updateImeiMd5Upper(currentday)

    //12-04
    //更新广告监测报表 新增注册设备数 字段
    updateNewRegiDevNum

  }


  def updateNewRegiDevNum(): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.memory.storageFraction", ConfigurationUtil.getProperty("spark.memory.storageFraction"))
      .set("spark.sql.shuffle.partitions", ConfigurationUtil.getProperty("spark.sql.shuffle.partitions"))
    SparkUtils.setMaster(sparkConf)
    val sparkContext = new SparkContext(sparkConf)
    val hiveContext = HiveContextSingleton.getInstance(sparkContext)
    hiveContext.sql("use yyft")

    //处理ios数据
    val ios_sql = "select \nfilter_regi.reg_time publish_date,\nfilter_regi.game_id game_id,\nfilter_regi.pkg_id pkg_id,\ncount(distinct filter_regi.imei) new_regi_dev_num\nfrom\n(select regi.reg_time reg_time,regi.game_id game_id,regi.imei imei,active.pkg_id pkg_id \nfrom\n(select to_date(active_time) active_time,game_id,pkg_id,imei from bi_ad_active_o_detail where to_date(active_time) >= '2017-09-15' and to_date(active_time) <= '2017-12-01' and os=2) active \njoin \n(select to_date(reg_time) reg_time,game_id,imei from ods_regi_rz where to_date(reg_time)>='2017-09-15' and to_date(reg_time)<='2017-12-01' and imei is not null) regi\non active.active_time=regi.reg_time and active.game_id=regi.game_id and active.imei=regi.imei) filter_regi group by filter_regi.reg_time,filter_regi.game_id,filter_regi.pkg_id"
    println(ios_sql)
    val iosDF = hiveContext.sql(ios_sql)
    updateNewRegiDevNumHistory(iosDF)


    //处理andorid数据
    val android_sql = "select \nfilter_regi.reg_time publish_date,\nfilter_regi.game_id game_id,\nfilter_regi.pkg_id pkg_id,\ncount(distinct filter_regi.imei) new_regi_dev_num\nfrom\n(select regi.reg_time reg_time,regi.game_id game_id,regi.imei imei,active.pkg_id pkg_id \nfrom\n(select to_date(active_time) active_time,game_id,pkg_id,imei from bi_ad_active_o_detail where to_date(active_time) >= '2017-09-15' and to_date(active_time) <= '2017-12-03' and os=1) active \njoin \n(select to_date(reg_time) reg_time,game_id,split(imei,'&')[0] imei from ods_regi_rz where to_date(reg_time)>='2017-09-15' and to_date(reg_time)<='2017-12-03' and imei is not null) regi\non active.active_time=regi.reg_time and active.game_id=regi.game_id and active.imei=regi.imei) filter_regi group by filter_regi.reg_time,filter_regi.game_id,filter_regi.pkg_id"
    println(android_sql)
    val androidDF = hiveContext.sql(android_sql)
    updateNewRegiDevNumHistory(androidDF)

  }

  def updateNewRegiDevNumHistory(iosDF: DataFrame): Unit = {
    iosDF.foreachPartition(iter => {
      val conn = JdbcUtil.getConn()
      val update_sql = "update bi_ad_channel_stats set new_regi_dev_num=? where publish_date=? and game_id=? and pkg_id=?"
      val arrBuffer = ArrayBuffer[Array[Any]]()
      for (row <- iter) {
        val publish_date = row.getAs[Date]("publish_date")
        val game_id = row.getAs[Int]("game_id")
        val pkg_id = row.getAs[String]("pkg_id")
        val newRegiDevNum = row.getAs[Long]("new_regi_dev_num")

        arrBuffer.+=(Array(newRegiDevNum, publish_date, game_id, pkg_id))
      }
      try {
        JdbcUtil.doBatch(update_sql, arrBuffer, conn)
      } finally {
        conn.close()
      }
    })
  }

  def updateImeiMd5Upper(currentday: String) = {
    val conn = JdbcUtil.getConn()
    val selectSql = "select pkg_id,game_id,imei,imei_md5_upper,os,ts,adv_name from bi_ad_momo_click where date(ts) = '" + currentday + "'"
    val stat = conn.createStatement()
    val resultSet = stat.executeQuery(selectSql)
    var buffer = ArrayBuffer[Array[String]]()
    while (resultSet.next()) {
      val pkg_code = resultSet.getString("pkg_id")
      val game_id = resultSet.getString("game_id")
      val imei = resultSet.getString("imei")
      val imei_md5_upper = resultSet.getString("imei_md5_upper")
      val os = resultSet.getString("os")
      val ts = resultSet.getString("ts")
      val adv_name = resultSet.getString("adv_name")
      println("查询 ： " + pkg_code + " - " + game_id + " - " + imei + " - " + imei_md5_upper + " - " + os + " - " + ts + " - " + adv_name)
      buffer.+=(Array[String](pkg_code, game_id, imei, imei_md5_upper, os, ts, adv_name))
    }
    stat.close()

    for (arr <- buffer) {
      val pkg_code = arr(0)
      val game_id = arr(1)
      val imei = arr(2)
      var imei_md5_upper = arr(3)
      val os = arr(4)
      val ts = arr(5).toString.split("\\.", -1)(0)
      val adv_name = arr(6).toInt

      if (os.equals("ios")) {
        if (adv_name == 5 || adv_name == 6) {
          imei_md5_upper = imei.toUpperCase
        } else {
          imei_md5_upper = MD5Util.string2MD5(imei.toString).toUpperCase
        }

      } else if (os.equals("android")) {
        imei_md5_upper = imei.toString.toUpperCase
      }
      println("更新 ： " + pkg_code + " - " + game_id + " - " + imei + " - " + imei_md5_upper + " - " + os + " - " + ts + " - " + adv_name)
      val updateSql = "update bi_ad_momo_click set imei_md5_upper=? where pkg_id=? and game_id=? and imei=? and os=? and ts=? and adv_name=?"
      val pstat = conn.prepareStatement(updateSql)
      pstat.setString(1, imei_md5_upper)
      pstat.setString(2, pkg_code)
      pstat.setString(3, game_id)
      pstat.setString(4, imei)
      pstat.setString(5, os)
      pstat.setString(6, ts)
      pstat.setInt(7, adv_name)
      val i = pstat.executeUpdate()
      println("更新了： " + i + " 行 ")
      pstat.close()
    }
    conn.close()
  }


}
