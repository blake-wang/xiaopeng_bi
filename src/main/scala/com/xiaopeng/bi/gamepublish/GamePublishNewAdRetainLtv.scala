package com.xiaopeng.bi.gamepublish

import com.xiaopeng.bi.udf.ImeiUDF
import com.xiaopeng.bi.utils.{ConfigurationUtil, DateUtils, JdbcUtil, SparkUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, types}
import org.apache.spark.sql.types.StringType
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 18-3-27.
  * 新广告监测的留存和ltv计算
  */
object GamePublishNewAdRetainLtv {




  def main(args: Array[String]): Unit = {
    var yesterday = ""
    if (args.length > 0) {
      yesterday = args(0)
    }
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.memory.storageFraction", ConfigurationUtil.getProperty("spark.memory.storageFraction"))
      .set("spark.sql.shuffle.partitions", ConfigurationUtil.getProperty("spark.sql.shuffle.partitions"))
    SparkUtils.setMaster(sparkConf)
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    hiveContext.udf.register("getimei", new ImeiUDF(), StringType)
    hiveContext.sql("use yyft")

    //1:按设备统计留存  ： 这里的设备指得的是新增注册设备
    val retain_sql = "select\nrs.regi_time,\nrs.game_id,\nrs.pkg_id,\nsum(case when rs.dur=1 then 1 else 0 end ) retained_1day,\nsum(case when rs.dur=2 then 1 else 0 end ) retained_3day,\nsum(case when rs.dur=6 then 1 else 0 end ) retained_7day\nfrom\n(select\noz.regi_time,\noz.game_id,\noz.pkg_id,\ndatediff(ol.login_time,oz.regi_time) dur\nfrom\n(\nselect game_id,pkg_id,imei,regi_time from \n(select distinct game_id,pkg_id,imei,substr(min(regi_time),1,10) regi_time from yyft.bi_ad_regi_o_detail group by game_id,pkg_id,imei) rz where rz.imei !='no_imei' and to_date(rz.regi_time)>=date_add('yesterday',-6) and to_date(rz.regi_time)<='yesterday' ) oz   -- 新增注册设备\njoin (select distinct game_id,split(channel_expand,'_')[2] pkg_id,getimei(imei) imei,to_date(login_time) login_time from yyft.ods_login where to_date(login_time)>=date_add('yesterday',-6) and to_date(login_time)<='yesterday' ) ol\non oz.game_id=ol.game_id and oz.pkg_id=ol.pkg_id and oz.imei=ol.imei  where ol.login_time>=oz.regi_time and datediff(ol.login_time,oz.regi_time) in (1,2,6)) rs \ngroup by rs.regi_time,rs.game_id,rs.pkg_id"
      .replace("yesterday", yesterday)
    val retainDF = hiveContext.sql(retain_sql)
    foreachADRetainDF(retainDF)

    //2:按设备计算ltv,累计充值金额，累计充值设备数   : 这里的设备指的是新增注册设备
    foreachADLTV()

    //3：计算广告监测分包的投放成本

    //4：按游戏维度计算 新增设备的留存
    val game_retain_sql = "select\nrs.regi_time,\nrs.game_id,\nsum(case when rs.dur=1 then 1 else 0 end ) retained_1day,\nsum(case when rs.dur=2 then 1 else 0 end ) retained_3day,\nsum(case when rs.dur=6 then 1 else 0 end ) retained_7day\nfrom\n(select\noz.regi_time,\noz.game_id,\ndatediff(ol.login_time,oz.regi_time) dur\nfrom\n(select game_id,imei,regi_time from \n(select distinct game_id,imei,substr(min(reg_time),1,10) regi_time from ods_regi_rz group by game_id,imei) rz where rz.imei is not null and rz.game_id is not null and to_date(rz.regi_time)>=date_add('yesterday',-6) and to_date(rz.regi_time)<='yesterday' ) oz   -- 新增注册设备\njoin (select distinct game_id,imei,to_date(login_time) login_time from ods_login where to_date(login_time)>=date_add('yesterday',-6) and to_date(login_time)<='yesterday' ) ol\non oz.game_id=ol.game_id and oz.imei=ol.imei  where ol.login_time>=oz.regi_time and datediff(ol.login_time,oz.regi_time) in (1,2,6)) rs \ngroup by rs.regi_time,rs.game_id"
      .replace("yesterday", yesterday)
    val gameRetainDF = hiveContext.sql(game_retain_sql)
    foreachGameRetainDF(gameRetainDF)

    //5：按游戏维度计算 新增设备的ltv，累计充值金额，累计充值设备数
    val game_ltv_sql = "select\noz.regi_time,\noz.game_id,\nsum(case when datediff(substr(oo.order_time,0,10),oz.regi_time)=0 then oo.ori_price else 0 end ) ltv_dev_1day,\nsum(case when datediff(substr(oo.order_time,0,10),oz.regi_time)<=1 then oo.ori_price else 0 end ) ltv_dev_2day,\nsum(case when datediff(substr(oo.order_time,0,10),oz.regi_time)<=2 then oo.ori_price else 0 end ) ltv_dev_3day,\nsum(case when datediff(substr(oo.order_time,0,10),oz.regi_time)<=6 then oo.ori_price else 0 end ) ltv_dev_7day,\nsum(oo.ori_price) lj_pay_price,\ncount(distinct oz.imei) lj_pay_dev_num\nfrom\n(select rz.regi_time,rz.game_id,rz.imei\nfrom \n(select distinct game_id,imei,substr(min(reg_time),1,10) regi_time from ods_regi_rz group by game_id,imei) rz where rz.imei is not null and rz.game_id is not null and to_date(rz.regi_time)<='yesterday' ) oz\njoin\n(select distinct order_no,order_time,game_id,imei,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as ori_price from ods_order where order_status=4 and prod_type=6) oo\non oz.regi_time=substr(oo.order_time,0,10) and oz.game_id=oo.game_id where substr(oo.order_time,0,10)>=oz.regi_time \ngroup by oz.regi_time,oz.game_id"
      .replace("yesterday", yesterday)
    val gameLtvDF = hiveContext.sql(game_ltv_sql)
    foreachGameLtvDF(gameLtvDF)

    //6：按游戏维度计算 投放成本

    //7：自然量的留存，ltv，累计充值金额，累计充值设备，投放成本

  }

  def foreachADRetainDF(retainDF: DataFrame): Unit = {
    retainDF.foreachPartition(iter => {
      if (!iter.isEmpty) {
        val conn = JdbcUtil.getConn()
        val insert_sql = "update bi_new_merge_ad_kpi set retained_1day=?,retained_3day=?,retained_7day=? where publish_date=? and child_game_id=? and pkg_id=?"
        val pstmt = conn.prepareStatement(insert_sql)
        val params = new ArrayBuffer[Array[Any]]()

        iter.foreach(row => {
          val regi_time = row.getAs[String]("regi_time")
          val game_id = row.getAs[Int]("game_id")
          val pkg_id = row.getAs[String]("pkg_id")
          val retained_1day = row.getAs[Long]("retained_1day")
          val retained_3day = row.getAs[Long]("retained_3day")
          val retained_7day = row.getAs[Long]("retained_7day")
          params.+=(Array(retained_1day, retained_3day, retained_7day, regi_time, game_id, pkg_id))
        })

        try {
          JdbcUtil.executeUpdate(pstmt, params, conn)
        } finally {
          pstmt.close()
          conn.close()
        }
      }
    })
  }

  def foreachADLTV(): Unit = {
    val conn = JdbcUtil.getConn()
    val ltv_sql = "select\nrz.regi_time,\nrz.game_id,\nrz.pkg_id,\nsum(case when datediff(oz.order_time,rz.regi_time)=0 then pay_price else 0 end ) ltv_dev_1day,\nsum(case when datediff(oz.order_time,rz.regi_time)<=1 then pay_price else 0 end ) ltv_dev_2day,\nsum(case when datediff(oz.order_time,rz.regi_time)<=2 then pay_price else 0 end ) ltv_dev_3day,\nsum(case when datediff(oz.order_time,rz.regi_time)<=6 then pay_price else 0 end ) ltv_dev_7day,\nsum(oz.pay_price) lj_pay_price,\ncount(distinct rz.imei) lj_pay_dev_num\nfrom\n(select distinct game_id,pkg_id,imei,substr(min(regi_time),1,10) regi_time from bi_ad_regi_o_detail where imei != 'no_imei' group by game_id,pkg_id,imei) rz \njoin\n(select distinct order_id,game_id,pkg_id,imei,substr(order_time,1,10) order_time,pay_price from bi_ad_order_o_detail where imei != 'no_imei') oz \non rz.game_id=oz.game_id and rz.pkg_id=oz.pkg_id and rz.imei=oz.imei  where oz.order_time>=rz.regi_time\ngroup by rz.regi_time,rz.game_id,rz.pkg_id"

    val insert_sql = "update bi_new_merge_ad_kpi set ltv_dev_1day=?,ltv_dev_2day=?,ltv_dev_3day=?,ltv_dev_7day=?,lj_pay_price=?,lj_pay_dev_num=? where publish_date=? and child_game_id=? and pkg_id=?"
    val params = new ArrayBuffer[Array[Any]]()
    val pstmt = conn.prepareStatement(insert_sql)
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(ltv_sql)
    while (rs.next()) {
      val regi_time = rs.getString("regi_time")
      val game_id = rs.getString("game_id")
      val pkg_id = rs.getString("pkg_id")
      val ltv_dev_1day = rs.getString("ltv_dev_1day")
      val ltv_dev_2day = rs.getString("ltv_dev_2day")
      val ltv_dev_3day = rs.getString("ltv_dev_3day")
      val ltv_dev_7day = rs.getString("ltv_dev_7day")
      val lj_pay_price = rs.getString("lj_pay_price")
      val lj_pay_dev_num = rs.getString("lj_pay_dev_num")

      params.+=(Array(ltv_dev_1day, ltv_dev_2day, ltv_dev_3day, ltv_dev_7day, lj_pay_price, lj_pay_dev_num, regi_time, game_id, pkg_id))
    }

    try {
      JdbcUtil.executeUpdate(pstmt, params, conn)
    } finally {
      rs.close()
      stmt.close()
      pstmt.close()
      conn.close()
    }
  }

  def foreachGameRetainDF(gameRetainDF: DataFrame): Unit = {
    gameRetainDF.foreachPartition(iter => {
      if (!iter.isEmpty) {
        val conn = JdbcUtil.getConn()
        val insert_sql = "update bi_gamepublic_opera_actions set retained_1day=?,retained_3day=?,retained_7day=? where publish_date=? and child_game_id=?"
        val pstmt = conn.prepareStatement(insert_sql)
        val params = new ArrayBuffer[Array[Any]]()

        iter.foreach(row => {
          val regi_time = row.getAs[String]("regi_time")
          val game_id = row.getAs[Int]("game_id")
          val retained_1day = row.getAs[Long]("retained_1day")
          val retained_3day = row.getAs[Long]("retained_3day")
          val retained_7day = row.getAs[Long]("retained_7day")

          params.+=(Array(retained_1day, retained_3day, retained_7day, regi_time, game_id))
        })

        try {
          JdbcUtil.executeUpdate(pstmt, params, conn)
        } finally {
          pstmt.close()
          conn.close()
        }
      }
    })

  }

  def foreachGameLtvDF(gameLtvDF: DataFrame): Unit = {
    val conn = JdbcUtil.getConn()
    val ltv_sql = "select\noz.regi_time,\noz.game_id,\nsum(case when datediff(substr(oo.order_time,0,10),oz.regi_time)=0 then oo.ori_price else 0 end ) ltv_dev_1day,\nsum(case when datediff(substr(oo.order_time,0,10),oz.regi_time)<=1 then oo.ori_price else 0 end ) ltv_dev_2day,\nsum(case when datediff(substr(oo.order_time,0,10),oz.regi_time)<=2 then oo.ori_price else 0 end ) ltv_dev_3day,\nsum(case when datediff(substr(oo.order_time,0,10),oz.regi_time)<=6 then oo.ori_price else 0 end ) ltv_dev_7day,\nsum(oo.ori_price) lj_pay_price,\ncount(distinct oz.imei) lj_pay_dev_num\nfrom\n(select rz.regi_time,rz.game_id,rz.imei\nfrom \n(select distinct game_id,imei,substr(min(reg_time),1,10) regi_time from ods_regi_rz group by game_id,imei) rz where rz.imei is not null and rz.game_id is not null and to_date(rz.regi_time)<='yesterday' ) oz\njoin\n(select distinct order_no,order_time,game_id,imei,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as ori_price from ods_order where order_status=4 and prod_type=6) oo\non oz.regi_time=substr(oo.order_time,0,10) and oz.game_id=oo.game_id where substr(oo.order_time,0,10)>=oz.regi_time \ngroup by oz.regi_time,oz.game_id"

    val insert_sql = "update bi_gamepublic_opera_actions set ltv_dev_1day=?,ltv_dev_2day=?,ltv_dev_3day=?,ltv_dev_7day=?,lj_pay_price=?,lj_pay_dev_num=? where publish_date=? and  child_game_id=?"
    val params = new ArrayBuffer[Array[Any]]()
    val pstmt = conn.prepareStatement(insert_sql)
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(ltv_sql)
    while (rs.next()) {
      val regi_time = rs.getString("regi_time")
      val game_id = rs.getString("game_id")
      val ltv_dev_1day = rs.getString("ltv_dev_1day")
      val ltv_dev_2day = rs.getString("ltv_dev_2day")
      val ltv_dev_3day = rs.getString("ltv_dev_3day")
      val ltv_dev_7day = rs.getString("ltv_dev_7day")
      val lj_pay_price = rs.getString("lj_pay_price")
      val lj_pay_dev_num = rs.getString("lj_pay_dev_num")

      params.+=(Array(ltv_dev_1day, ltv_dev_2day, ltv_dev_3day, ltv_dev_7day, lj_pay_price, lj_pay_dev_num, regi_time, game_id))
    }

    try {
      JdbcUtil.executeUpdate(pstmt, params, conn)
    } finally {
      rs.close()
      stmt.close()
      pstmt.close()
      conn.close()
    }

  }
}
