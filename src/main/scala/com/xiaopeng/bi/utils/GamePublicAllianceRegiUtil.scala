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
object GamePublicAllianceRegiUtil {


  def loadRegiInfo(rdd: RDD[String], hiveContext: HiveContext) = {
    // 一.数据准备   将日志转化为表
    val regiRdd = rdd.filter(t => {
      val arr: Array[String] = t.split("\\|", -1);
      arr(0).contains("bi_regi") && arr.length >= 19 && (!arr(18).equals("")) && StringUtils.isNumber(arr(4))
    }).map(t => {
      val arr: Array[String] = t.split("\\|", -1);
      // game_account  game_id     alliance_bag_id  reg_time  imei
      Row(arr(3), arr(4).toInt, arr(18), arr(5), arr(14))
    });
    if (!regiRdd.isEmpty()) {
      // 将数据转化为 dataframe
      val regiStruct = (new StructType)
        .add("game_account", StringType)
        .add("game_id", IntegerType)
        .add("alliance_bag_id", StringType)
        .add("reg_time", StringType)
        .add("imei", StringType);
      val regiDF = hiveContext.createDataFrame(regiRdd, regiStruct);
      regiDF.registerTempTable("ods_regi_rz_cache")

      //二按业务需求 去重和过滤
      hiveContext.sql("use yyft");
      val sql_regi = "select distinct\nrz.reg_time as regi_hour,\ngs.game_id parent_game_id,\nrz.game_id as child_game_id, \nif(ad.terrace_name is null,'',ad.terrace_name) as terrace_name, \nif(ad.terrace_type is null,1,ad.terrace_type) as terrace_type, \nrz.alliance_bag_id as alliance_bag_id,  \ngs.system_type as os, \nif(ad.head_people is null,'',ad.head_people)as head_people,     \nif(ad.terrace_auto_id is null,0,ad.terrace_auto_id) terrace_auto_id,\nif(ad.alliance_company_id is null,0,ad.alliance_company_id) alliance_company_id,\nif(ad.alliance_company_name is null,'',ad.alliance_company_name) alliance_company_name,\nrz.imei as imei,\nrz.count_acc as count_acc\nfrom\n(select game_id,alliance_bag_id,imei,count(distinct game_account) count_acc,substr(reg_time,0,13) as reg_time from ods_regi_rz_cache where game_account!='' group by game_id,alliance_bag_id,imei,substr(reg_time,0,13)) rz  \njoin (select distinct game_id,system_type,old_game_id from game_sdk where state=0) gs on rz.game_id=gs.old_game_id -- 游戏信息 \nleft join (select child_game_id,terrace_name,terrace_type,alliance_bag_id,head_people,terrace_auto_id,alliance_company_id,alliance_company_name  from  alliance_dim) ad on rz.alliance_bag_id=ad.alliance_bag_id -- 联运包ID信息"
      val df_regi: DataFrame = hiveContext.sql(sql_regi);
      foreachRealPartition(df_regi)
    }
  }

  /**
    * 将注册数据过滤去重存入 mysql
    *
    * @param df
    */
  def foreachRealPartition(df: DataFrame) = {

    df.foreachPartition(iter => {
      if (!iter.isEmpty) {
        //数据库链接
        val conn: Connection = JdbcUtil.getConn();
        val stmt = conn.createStatement();

        // 一.更新明细表  按小时去重
        val sql_deatil = "INSERT INTO bi_gamepublic_alliance_regi_detail(game_id,alliance_bag_id,regi_time,imei) VALUES (?,?,?,?) "
        val pstat_sql_deatil: PreparedStatement = conn.prepareStatement(sql_deatil);
        var ps_sql_deatil_params = new ArrayBuffer[Array[Any]]()

        // 二.更新 联运管理日报
        val sql_day_kpi = "INSERT INTO bi_gamepublic_alliance_base_day_kpi(publish_date,parent_game_id,child_game_id,terrace_name,terrace_type,alliance_bag_id,os,head_people,terrace_auto_id,alliance_company_id,alliance_company_name,regi_account_num,regi_device_num,new_regi_device_num,new_regi_account_num) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY update \nparent_game_id=values(parent_game_id),\nterrace_name=values(terrace_name),\nterrace_type=values(terrace_type),\nos=values(os),\nhead_people=values(head_people),\nterrace_auto_id=values(terrace_auto_id),\nalliance_company_id=values(alliance_company_id),\nalliance_company_name=values(alliance_company_name),\nregi_account_num=regi_account_num + VALUES(regi_account_num),\nregi_device_num=regi_device_num + VALUES(regi_device_num),\nnew_regi_device_num=new_regi_device_num + VALUES(new_regi_device_num),\nnew_regi_account_num=new_regi_account_num+ VALUES(new_regi_account_num)"
        val ps_day_kpi: PreparedStatement = conn.prepareStatement(sql_day_kpi);
        var params_day_kpi = new ArrayBuffer[Array[Any]]()

        iter.foreach(row => {
          // 获取dataframe的数据
          val regi_hour = row.getAs[String]("regi_hour");
          val publish_date = regi_hour.substring(0, 10);
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
          var count_game_account = row.getAs[Long]("count_acc").toInt;

          // 一.明细表 按小时去重
          val sql_hour_day = "select regi_time from bi_gamepublic_alliance_regi_detail WHERE game_id = '" + child_game_id + "'and alliance_bag_id= '" + alliance_bag_id + "' and  imei='" + imei + "' and regi_time='" + regi_hour + "'";
          val results_hour = stmt.executeQuery(sql_hour_day);
          if (!results_hour.next()) {
            //  明细表
            ps_sql_deatil_params.+=(Array[Any](child_game_id, alliance_bag_id, regi_hour, imei))
          }

          // 二. 联运管理日报相关字段
          // 注册账号数
          val regi_account_num = count_game_account;
          // 注册设备数
          var regi_device_num = 0;
          // 激活 且 注册的设备数 =>  新增注册设备数
          var new_regi_device_num = 0;
          // 激活 且 注册的设备数 注册的账户数  =>  新增注册账户数
          var new_regi_account_num = 0;
          val sql_day = "select regi_time from bi_gamepublic_alliance_regi_detail WHERE game_id = '" + child_game_id + "'and alliance_bag_id='" + alliance_bag_id + "' and  imei='" + imei + "' and date(regi_time)='" + publish_date + "'";
          val results_day = stmt.executeQuery(sql_day);
          // 今天不存在 联运包ID
          if (!results_day.next()) {
            // 注册设备数
            regi_device_num = 1;

            val sqlselct_acticve = "select active_time from bi_gamepublic_alliance_active_detail WHERE game_id = '" + child_game_id + "'and date(active_time)='" + publish_date + "'and alliance_bag_id='" + alliance_bag_id + "' and  imei='" + imei + "' ";
            val results_active = stmt.executeQuery(sqlselct_acticve);
            // 今天激活的
            if (results_active.next()) {
              //  新增注册设备数
              new_regi_device_num = 1;
              //  新增注册账户数
              new_regi_account_num = count_game_account;
            }
          } else {
            val sqlselct_acticve = "select active_time from bi_gamepublic_alliance_active_detail WHERE game_id = '" + child_game_id + "'and date(active_time)='" + publish_date + "'and alliance_bag_id='" + alliance_bag_id + "' and  imei='" + imei + "' ";
            val results_active = stmt.executeQuery(sqlselct_acticve);
            // 今天激活的
            if (results_active.next()) {
              // 新增注册账户数
              new_regi_account_num = count_game_account;
            }
          }

          params_day_kpi.+=(Array[Any](publish_date, parent_game_id, child_game_id, terrace_name, terrace_type, alliance_bag_id, os, head_people, terrace_auto_id, alliance_company_id, alliance_company_name, regi_account_num, regi_device_num, new_regi_device_num,new_regi_account_num))
          //插入数据库
          JdbcUtil.executeUpdate(pstat_sql_deatil, ps_sql_deatil_params, conn)
          JdbcUtil.executeUpdate(ps_day_kpi, params_day_kpi, conn)

        })
        //关闭数据库链接
        pstat_sql_deatil.close()
        ps_day_kpi.close()
        stmt.close()
        conn.close
      }
    })

  }
}
