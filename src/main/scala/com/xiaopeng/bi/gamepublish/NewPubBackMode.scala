package com.xiaopeng.bi.gamepublish

import java.sql.Connection

import com.xiaopeng.bi.utils.{DateUtils, HiveContextSingleton, JdbcUtil, SparkUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kequan on 1/11/18.
  * 发行报表 分模式 报表
  */
object NewPubBackMode {
  var startDay = "";
  var endDay = "";
  var tomorrow = "";

  def main(args: Array[String]): Unit = {

    startDay = args(0)
    endDay = args(1)
    tomorrow = DateUtils.addDay(endDay, 1)

    //创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.sql.shuffle.partitions", "60")
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf);
    val hiveContext: HiveContext = HiveContextSingleton.getInstance(sc)

    hiveContext.sql("use yyft")
    val sql_delete = "select id from modes_of_statistic where to_date(update_time)>='startDay' and  to_date(update_time)<='endDay'"
      .replace("startDay", startDay).replace("endDay", DateUtils.addDay(endDay, 1))
    val df_delete = hiveContext.sql(sql_delete)
    foreachModeDeletePartition(df_delete)

    val sql = "select \nto_date(oz.order_time) publish_date,rz.parent_game_id parent_game_id,\nrz.child_game_id child_game_id,\nrz.publish_mode publish_mode,\nrz.os os,\nsum(if(oz.ori_price_all is null,0,oz.ori_price_all)) pay_water,\nsum(if(ver.sandbox is null,0,oz.ori_price_all)) box_water,\nmax(rz.mode_id) mode_id\nfrom \n(\nselect gs.parent_game_id parent_game_id,rz.game_id child_game_id,gs.system_type os, rz.game_account game_account,if(ms.id is null,0,ms.id) mode_id,\ncase\nwhen  rz.reg_time>=ms.modifytime_2  and rz.reg_time<ms.modifytime_3  then  ms.mode_2\nwhen  rz.reg_time>=ms.modifytime_3  and rz.reg_time<ms.modifytime_4  then  ms.mode_3\nwhen  rz.reg_time>=ms.modifytime_4  and rz.reg_time<ms.modifytime_5  then  ms.mode_4\nwhen  rz.reg_time>=ms.modifytime_5  then  ms.mode_5\nelse  if(ms.mode is null,0,ms.mode) end as publish_mode\nfrom \n(select  distinct game_id,lower(trim(game_account)) game_account, to_date(reg_time) reg_time from ods_regi_rz where game_id is not null and game_account is not null and reg_time is not null)  rz\njoin (select  distinct game_id as parent_game_id,old_game_id ,system_type from game_sdk  where state=0) gs on rz.game_id=gs.old_game_id\nleft join (select id,game_id,mode,mode_2,to_date(if(to_date(modifytime_2)>'2016-01-01',modifytime_2,'tomorrow')) modifytime_2,mode_3,to_date(if(to_date(modifytime_3)>'2016-01-01',modifytime_3,'tomorrow')) modifytime_3,mode_4,to_date(if(to_date(modifytime_4)>'2016-01-01',modifytime_4,'tomorrow')) modifytime_4,mode_5, to_date(if(to_date(modifytime_5)>'2016-01-01',modifytime_5,'tomorrow')) modifytime_5 from  modes_of_statistic where status=0) ms on ms.game_id=rz.game_id\n) rz\njoin (select distinct order_no,order_time, lower(trim(game_account)) game_account,game_id,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as ori_price_all,payment_type,order_status from ods_order where order_status=4 and  to_date(order_time)>='startDay' and   to_date(order_time)<='endDay' and prod_type=6)oz on  oz.game_account=rz.game_account\nleft join  (select distinct order_sn,sandbox from pywsdk_apple_receipt_verify where sandbox=1 and state=3) ver on ver.order_sn=oz.order_no\ngroup by to_date(oz.order_time),rz.parent_game_id,rz.child_game_id,rz.publish_mode,rz.os"
      .replace("tomorrow", tomorrow).replace("startDay", "2016-09-01").replace("endDay", endDay)
    val df_mode = hiveContext.sql(sql)
    foreachModePartition(df_mode)

    sc.stop()
  }

  /**
    * 删除 录入删除的数据
    *
    * @param df_delete
    */
  def foreachModeDeletePartition(df_delete: DataFrame) = {
    df_delete.foreachPartition(iter => {
      if (!iter.isEmpty) {
        val conn: Connection = JdbcUtil.getConn();
        val sql = "delete from bi_publish_mode where mode_id=? or mode_id=0"
        val params = new ArrayBuffer[Array[Any]]()

        iter.foreach(row => {
          params.+=(Array[Any](row.get(0)))
        })

        JdbcUtil.doBatch(sql, params, conn)
        conn.close()
      }
    })
  }

  /**
    * 将结果存入数据库
    *
    * @param df_mode
    */
  def foreachModePartition(df_mode: DataFrame) = {
    df_mode.foreachPartition(iter => {
      val conn: Connection = JdbcUtil.getConn();
      val sql = "insert into bi_publish_mode(publish_date,parent_game_id,child_game_id,publish_mode,os,pay_water,box_water,mode_id) values(?,?,?,?,?,?,?,?) on duplicate key update \nparent_game_id=values(parent_game_id),\npublish_mode=values(publish_mode),\nos=values(os),\npay_water=values(pay_water),\nbox_water=values(box_water),\nmode_id=values(mode_id)"
      val params = new ArrayBuffer[Array[Any]]()

      iter.foreach(row => {
        params.+=(Array[Any](row.get(0), row.get(1), row.get(2), row.get(3), row.get(4), row.get(5), row.get(6), row.get(7)))
      })

      JdbcUtil.doBatch(sql, params, conn)
      conn.close()
    })
  }


}
