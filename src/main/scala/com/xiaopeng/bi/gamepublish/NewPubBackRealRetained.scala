package com.xiaopeng.bi.gamepublish

import com.xiaopeng.bi.utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kequan
  * 运营日报 实时留存 修复
  */
object NewPubBackRealRetained {

  def main(args: Array[String]): Unit = {

    //跑数日期
    val today = args(0)
    val yesterday = DateUtils.getYesterDayDate()
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.memory.storageFraction", ConfigurationUtil.getProperty("spark.memory.storageFraction"))
      .set("spark.sql.shuffle.partitions", ConfigurationUtil.getProperty("spark.sql.shuffle.partitions")) //.setMaster("local[4]"
    SparkUtils.setMaster(sparkConf)

    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    sqlContext.sql("use yyft")
    //----------运营日报 实时留存修复  到游戏维度----------------------------
    val retainedexecSql = "select  rs.reg_time publish_date, rs.game_id game_id,rs.parent_game_id ,rs.system_type os,rs.group_id group_id,\nsum(CASE rs.dur WHEN 1 THEN 1 ELSE 0 END ) as retained_1day, \nsum(CASE rs.dur WHEN 2 THEN 1 ELSE 0 END ) as retained_2day,  \nsum(CASE rs.dur WHEN 6 THEN 1 ELSE 0 END ) as retained_6day  \nFROM \n(\nselect distinct rz.reg_time,rz.game_id,gs.game_id parent_game_id,gs.system_type system_type,gs.group_id group_id, datediff(odsl.login_time,rz.reg_time) dur,rz.game_account from    \n(select distinct to_date(reg_time) reg_time,game_id,expand_channel,lower(trim(game_account)) game_account from ods_regi_rz  where to_date(reg_time) in (date_add('today',-1),date_add('today',-2),date_add('today',-6)) and game_id is not null and game_account is not null)\nrz\njoin (select  distinct game_id  ,old_game_id,system_type,group_id from game_sdk  where state=0) gs on gs.old_game_id=rz.game_id\njoin (select distinct to_date(login_time) login_time,lower(trim(game_account)) game_account from ods_login) odsl on rz.game_account = odsl.game_account   where to_date(odsl.login_time) >= rz.reg_time and to_date(odsl.login_time)<='today' and datediff(odsl.login_time,rz.reg_time) in(1,2,6) \n) rs\ngroup BY rs.reg_time,rs.game_id,rs.parent_game_id,rs.system_type,rs.group_id"
      .replace("today", today)
    val operaRetainedDf = sqlContext.sql(retainedexecSql)
    operaRetainedForeachPartition(operaRetainedDf)
    System.clearProperty("spark.driver.port")
    sc.stop()
  }



  private def operaRetainedForeachPartition(deviceRetainedDf: DataFrame) = {
    deviceRetainedDf.foreachPartition(rows => {

      val conn = JdbcUtil.getConn()
      val statement = conn.createStatement
      val sqlText = "insert into bi_gamepublic_base_opera_kpi(\npublish_date,\nchild_game_id,\nparent_game_id,\nos,\ngroup_id,\nretained_1day,\nretained_3day,\nretained_7day)\nvalues(?,?,?,?,?,?,?,?)\non duplicate key update \nparent_game_id=VALUES(parent_game_id),\nos=VALUES(os),\ngroup_id=VALUES(group_id),\nretained_1day=VALUES(retained_1day),\nretained_3day=VALUES(retained_3day),\nretained_7day=VALUES(retained_7day)"
     val params = new ArrayBuffer[Array[Any]]()
      for (insertedRow <- rows) {
        params.+=(Array[Any](insertedRow.get(0), insertedRow.get(1), insertedRow.get(2),insertedRow.get(3), insertedRow.get(4), insertedRow.get(5), insertedRow.get(6), insertedRow.get(7)
        ))
      }
      try {
        JdbcUtil.doBatch(sqlText, params, conn)
      } finally {
        statement.close()
        conn.close
      }
    })
  }


}

