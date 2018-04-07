package com.xiaopeng.bi.gamepublish

import java.sql.PreparedStatement

import com.xiaopeng.bi.utils.{Commons, Hadoop, JdbcUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object GamePublishe3Mau {

  def main(args: Array[String]): Unit = {
      Hadoop.hd
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
      val currentday = args(0)
      val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      val sc = new SparkContext(sparkConf)
      val sqlContext = new HiveContext(sc)
      sparkConf.set("spark.storage.memoryFraction","0.7").set("spark.sql.shuffle.partitions","60")
      sqlContext.sql("use yyft")
      /*hivesql*/
      val hivesql= "select rs.dt as publish_date,rs.game_id child_game_id,rs.mau_device_num,rs.mau_account_num,sdk.game_id parent_game_id,system_type os ,group_id from \n(\nselect 'currentday' as dt,game_id,count(distinct imei) mau_device_num,count(distinct lg.game_account) mau_account_num  from yyft.ods_login lg \nwhere to_date(login_time)<='currentday' and to_date(login_time)>=date_add('currentday',-30)\ngroup by lg.game_id\n) rs \njoin yyft.game_sdk sdk on sdk.old_game_id=rs.game_id".replace("currentday",currentday)
      val df=sqlContext.sql(hivesql)
      val istSql="insert into bi_gamepublic_base_opera_kpi(publish_date,child_game_id,mau_device_num,mau_account_num,parent_game_id,os,group_id) values(?,?,?,?,?,?,?)" +
        " on duplicate key update mau_device_num=?,mau_account_num=?,os=?,group_id=?"
      Commons.processDbFct(df,istSql)
      System.clearProperty("spark.driver.port")
      sc.stop()
      sc.clearCallSite()
    }


  }

