package com.xiaopeng.bi.utils

import java.sql.{Connection, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kequan on 2/26/18.
  */
object GamePublicAllianceActiveUtil {


  def loadActiveInfo(rdd: RDD[String], hiveContext: HiveContext) = {
    // 一.数据准备   将日志转化为表
    val activeRdd = rdd.filter(t => {
      val arr = t.split("\\|", -1);
      val b = arr.length
      arr(0).contains("bi_active") && arr.length >= 15 && (!arr(14).equals("")) && arr(5).length >= 13
    }).map(t => {
      val arr: Array[String] = t.split("\\|", -1);
      // game_id     alliance_bag_id  active_time  imei
      val a= Row(arr(1).toInt, arr(14), arr(5), arr(8))
      Row(arr(1).toInt, arr(14), arr(5), arr(8))
    })

    if (!activeRdd.isEmpty()) {
      val activeStruct = (new StructType).add("game_id", IntegerType).add("alliance_bag_id", StringType).add("active_time", StringType).add("imei", StringType);
      val activeDF = hiveContext.createDataFrame(activeRdd, activeStruct);
      activeDF.registerTempTable("ods_active_cache")
      //二.按照业务逻辑去重和过滤
      hiveContext.sql("use yyft");
      val sql_bi_active = "select distinct \naz.active_time as active_time,\ngs.game_id parent_game_id,  \naz.game_id as child_game_id, \nif(ad.terrace_name is null,'',ad.terrace_name) as terrace_name, \nif(ad.terrace_type is null,1,ad.terrace_type) as terrace_type, \naz.alliance_bag_id as alliance_bag_id, \ngs.system_type as os, \nif(ad.head_people is null,'',ad.head_people)as head_people,   \nif(ad.terrace_auto_id is null,0,ad.terrace_auto_id) terrace_auto_id,\nif(ad.alliance_company_id is null,0,ad.alliance_company_id) alliance_company_id,\nif(ad.alliance_company_name is null,'',ad.alliance_company_name) alliance_company_name,\naz.imei as imei  \nfrom  \n(select distinct game_id,alliance_bag_id,min(active_time)  as active_time,imei from ods_active_cache group by game_id,alliance_bag_id,imei) az  \njoin (select distinct game_id,system_type,old_game_id from game_sdk where state=0) gs on az.game_id=gs.old_game_id -- 游戏信息 \nleft join (select child_game_id,terrace_name,terrace_type,alliance_bag_id,head_people,terrace_auto_id,alliance_company_id,alliance_company_name  from  alliance_dim) ad on az.alliance_bag_id=ad.alliance_bag_id -- 联运包ID信息"
      val df_bi_active: DataFrame = hiveContext.sql(sql_bi_active);
      foreachRealPartition(df_bi_active)
    }
  }


  def foreachRealPartition(df_bi_active: DataFrame) = {
    df_bi_active.foreachPartition(iter => {
      if (!iter.isEmpty) {
        //数据库链接
        val conn: Connection = JdbcUtil.getConn();
        val stmt = conn.createStatement();

        // 一.明细表
        val sql_deatil = "INSERT INTO bi_gamepublic_alliance_active_detail(game_id,alliance_bag_id,active_time,imei) VALUES (?,?,?,?) "
        val ps_deatil: PreparedStatement = conn.prepareStatement(sql_deatil);
        val params_deatil = new ArrayBuffer[Array[Any]]()

        val sql_deatil_update = "update bi_gamepublic_alliance_active_detail set active_time =?  WHERE game_id = ? and alliance_bag_id=? and  imei=?"
        val ps_deatil_update: PreparedStatement = conn.prepareStatement(sql_deatil_update);
        val params_deatil_update = new ArrayBuffer[Array[Any]]()

        // 二.更新天表
        val sql_day_active_num ="INSERT INTO bi_gamepublic_alliance_base_day_kpi(publish_date,parent_game_id,child_game_id,terrace_name,terrace_type,alliance_bag_id,os,head_people,terrace_auto_id,alliance_company_id,alliance_company_name,active_num) VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY update \nparent_game_id=values(parent_game_id),\nterrace_name=values(terrace_name),\nterrace_type=values(terrace_type),\nos=values(os),\nhead_people=values(head_people),\nterrace_auto_id=values(terrace_auto_id),\nalliance_company_id=values(alliance_company_id),\nalliance_company_name=values(alliance_company_name),\nactive_num=active_num + VALUES(active_num)"
        val ps_day_active_num: PreparedStatement = conn.prepareStatement(sql_day_active_num);
        val params_day_active = new ArrayBuffer[Array[Any]]()

        iter.foreach(row => {
          // 获取dataframe的数据
          val active_time = row.getAs[String]("active_time");
          val publish_date = active_time.substring(0, 10);
          val parent_game_id = row.getAs[Int]("parent_game_id");
          val child_game_id = row.getAs[Int]("child_game_id");
          val terrace_name = row.getAs[String]("terrace_name");
          val terrace_type = row.getAs[Int]("terrace_type");
          val alliance_bag_id = row.getAs[String]("alliance_bag_id");
          val os = row.getAs[Int]("os");
          val head_people = row.getAs[String]("head_people");
          val terrace_auto_id = row.getAs[Int]("terrace_auto_id");
          val alliance_company_id = row.getAs[Int]("alliance_company_id");
          val alliance_company_name = row.getAs[String]("alliance_company_name");
          val imei = row.getAs[String]("imei");

          val sqlselct_all = "select active_time from bi_gamepublic_alliance_active_detail WHERE game_id = '" + child_game_id + "'and alliance_bag_id='" + alliance_bag_id + "' and  imei='" + imei + "' ";
          val results_all = stmt.executeQuery(sqlselct_all);
          if (!results_all.next()) {
            // 明细表插入
            params_deatil.+=(Array[Any](child_game_id, alliance_bag_id, active_time, imei))
            params_day_active.+=(Array[Any](publish_date, parent_game_id, child_game_id, terrace_name, terrace_type, alliance_bag_id, os, head_people,terrace_auto_id,alliance_company_id,alliance_company_name,1))
          } else {
            val active_time_old = results_all.getString("active_time")
            if (DateUtils.beforeHour(active_time, active_time_old)) {
              // 明细表更新
              params_deatil_update.+=(Array[Any](active_time, child_game_id, alliance_bag_id, imei))
              params_day_active.+=(Array[Any](publish_date, parent_game_id, child_game_id, terrace_name, terrace_type, alliance_bag_id, os, head_people,terrace_auto_id,alliance_company_id,alliance_company_name, 1))
              params_day_active.+=(Array[Any](active_time_old.substring(0, 10), parent_game_id, child_game_id, terrace_name, terrace_type, alliance_bag_id, os, head_people,terrace_auto_id,alliance_company_id,alliance_company_name, -1))
            }
          }
          // 插入数据库
          JdbcUtil.executeUpdate(ps_deatil, params_deatil, conn)
          JdbcUtil.executeUpdate(ps_deatil_update, params_deatil_update, conn)
          JdbcUtil.executeUpdate(ps_day_active_num, params_day_active, conn)

        })

        //关闭数据库链接
        stmt.close()
        ps_deatil.close()
        ps_deatil_update.close()
        ps_day_active_num.close()
        conn.close

      }
    })

  }
}
