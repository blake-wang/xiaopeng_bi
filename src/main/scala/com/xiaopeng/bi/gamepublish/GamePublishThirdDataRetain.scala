package com.xiaopeng.bi.gamepublish

import java.util.Date

import com.xiaopeng.bi.utils.{ConfigurationUtil, DateUtils, JdbcUtil, SparkUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 17-9-19.
  * 计算统计第三方广告数据 离线留存，ltv
  */
object GamePublishThirdDataRetain {


  def main(args: Array[String]): Unit = {
    var yesterday = DateUtils.getYesterDayDate()
    if (args.length > 0) {
      yesterday = args(0)
    }
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.memory.storageFraction", ConfigurationUtil.getProperty("spark.memory.storageFraction"))
      .set("spark.sql.shuffle.partitions", ConfigurationUtil.getProperty("spark.sql.shuffle.partitions"))
    SparkUtils.setMaster(sparkConf)
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    //当有注册日志时，会将媒介信息都存入统计表中，离线任务这里，就不用再关联媒介信息了，只需要更新新添加字段
    //hiveContext.read.parquet(ConfigurationUtil.getProperty("fxdim.parquet")).registerTempTable("fxdim")

    hiveContext.sql("use yyft")

    //一：按帐号统计<留存>  广告监测报表 到分包维度
    val retained_Sql = "select \nrs.regi_time regi_time,\nrs.game_id game_id,\nrs.pkg_id pkg_id,\nsum(case when rs.dur=1 then 1 else 0 end) retained_2day,\nsum(case when rs.dur=2 then 1 else 0 end) retained_3day,\nsum(case when rs.dur=6 then 1 else 0 end) retained_7day,\nsum(case when rs.dur=29 then 1 else 0 end) retained_30day\nfrom \n(select rz.regi_time,rz.game_id,rz.pkg_id,datediff(odsl.login_time,rz.regi_time) dur \nfrom \n(select distinct to_date(regi_time) regi_time,game_account,pkg_id,game_id from bi_ad_regi_o_detail where to_date(regi_time)>=date_add('yesterday',-29) and to_date(regi_time)<='yesterday') rz \njoin \n(select distinct game_id,to_date(login_time) login_time,game_account from ods_login where to_date(login_time)>=date_add('yesterday',-29) and to_date(login_time)<='yesterday') odsl \non trim(lower(rz.game_account))=trim(lower(odsl.game_account)) \nwhere datediff(odsl.login_time,rz.regi_time) in (1,2,6,29)) rs \ngroup by rs.regi_time,rs.game_id,rs.pkg_id"
      .replace("yesterday", yesterday)
    println(retained_Sql)
    val adAccRetainedDF = hiveContext.sql(retained_Sql)
    adAccRetainedDFForeachPartition(adAccRetainedDF)

    //二：按帐号计算<用户质量>  广告监测报表 到分包维度
    val ltv_Sql = "select \nrs.regi_time regi_time,\nrs.game_id game_id,\nrs.pkg_id pkg_id,\nsum(case when rs.dur=0 then rs.pay_price else 0 end) as recharge_1day,\nsum(case when rs.dur<=6 then rs.pay_price else 0 end) as recharge_7day,\nsum(case when rs.dur<=29 then rs.pay_price else 0 end) as recharge_30day,\nsum(if(rs.pay_price is null,0,rs.pay_price)) as recharge_total,\ncount(distinct rs.game_account) as recharge_acc_total \nfrom\n(select\nto_date(rz.regi_time) regi_time,rz.game_id,rz.pkg_id,oz.game_account,oz.pay_price,datediff(to_date(oz.order_time),to_date(rz.regi_time)) dur   \nfrom \n(select distinct regi_time,lower(trim(game_account)) game_account,pkg_id,game_id from bi_ad_regi_o_detail where to_date(regi_time)>=date_add('yesterday',-29) and to_date(regi_time)<='yesterday') rz \nleft join \n(select distinct order_time,lower(trim(game_account)) game_account,pkg_id,game_id,pay_price from bi_ad_order_o_detail where to_date(order_time)>=date_add('yesterday',-29) and to_date(order_time)<='yesterday') oz \non rz.game_account=oz.game_account) rs \ngroup by rs.regi_time,rs.game_id,rs.pkg_id"
      .replace("yesterday", yesterday)
    val adLtvDF = hiveContext.sql(ltv_Sql)
    println(ltv_Sql)
    adLtvDFForeachPartition(adLtvDF)

    //三：自然量 <留存><用户质量> 广告监测报表  到分包维度

    //计算思路，获取到总的注册量，减去匹配到的注册量，剩余的就是自然量

    //1:处理ios游戏
    var game_id_set = getADGameId(2)
    if (!game_id_set.equals("no_game")) {
      updateADRetainedLtv(game_id_set, yesterday)
    }
    //2:处理android游戏
    game_id_set = getADGameId(1)
    if (!game_id_set.equals("no_game")) {
      updateADRetainedLtv(game_id_set, yesterday)
    }


  }

  //更新自然量的留存，ltv
  def updateADRetainedLtv(game_id_set: String, yesterday: String) = {
    val select_retained_ltv_sql = "select \nkpi.publish_date,\nkpi.game_id,group_id,\nkpi.acc_retained_2day-IFNULL(rs.retained_2day,0) as retained_2day,\nkpi.acc_retained_3day-IFNULL(rs.retained_3day,0) as retained_3day,\nkpi.acc_retained_7day-IFNULL(rs.retained_7day,0) as retained_7day,\nkpi.acc_retained_30day-IFNULL(rs.retained_30day,0) as retained_30day,\nkpi.recharge_lj_1-IFNULL(rs.recharge_1day,0) as recharge_1day,\nkpi.recharge_lj_7-IFNULL(rs.recharge_7day,0) as recharge_7day,\nkpi.recharge_lj_30-IFNULL(rs.recharge_30day,0) as recharge_30day,\nkpi.recharge_price-IFNULL(rs.recharge_total,0) as recharge_total,\nkpi.recharge_accounts-IFNULL(rs.recharge_acc_total,0) as recharge_acc_total \nfrom \n(select publish_date,child_game_id game_id,group_id,sum(acc_retained_2day) acc_retained_2day,sum(acc_retained_3day) acc_retained_3day,sum(acc_retained_7day) acc_retained_7day,sum(acc_retained_30day) acc_retained_30day,sum(recharge_lj_1) recharge_lj_1,sum(recharge_lj_7) recharge_lj_7,sum(recharge_lj_30) recharge_lj_30,sum(recharge_price) recharge_price,sum(recharge_accounts) recharge_accounts from bi_gamepublic_actions where publish_date>=date_add('yesterday',interval -29 day) and publish_date <= 'yesterday' and child_game_id in (game_id_set) group by publish_date,child_game_id,group_id ) kpi \nleft join \n(select publish_date,game_id,sum(retained_2day) retained_2day,sum(retained_3day) retained_3day,sum(retained_7day) retained_7day,sum(retained_30day) retained_30day,sum(recharge_1day) recharge_1day,sum(recharge_7day) recharge_7day,sum(recharge_30day) recharge_30day,sum(recharge_total) recharge_total,sum(recharge_acc_total) recharge_acc_total from bi_ad_channel_stats where pkg_id!='' and publish_date>=date_add('yesterday',interval -29 day) and publish_date<='yesterday' and game_id in(game_id_set) group by publish_date,game_id) rs \non kpi.publish_date=rs.publish_date and kpi.game_id=rs.game_id"
      .replace("yesterday", yesterday).replace("game_id_set", game_id_set)
    println(select_retained_ltv_sql)
    val update_retained_ltv_sql = "update bi_ad_channel_stats set retained_2day=?,retained_3day=?,retained_7day=?,retained_30day=?,recharge_1day=?,recharge_7day=?,recharge_30day=?,recharge_total=?,recharge_acc_total=? where publish_date=? and game_id=? and pkg_id=?"

    //BiHippo是数据库从库，只有查询权限，没有写入权限
    val connBiHippo = JdbcUtil.getBiHippoConn()
    val pstmtBiHippo = connBiHippo.prepareStatement(select_retained_ltv_sql)
    val resultSet = pstmtBiHippo.executeQuery()

    //主数据库
    val conn = JdbcUtil.getConn()
    val pstmt = conn.prepareStatement(update_retained_ltv_sql)

    while (resultSet.next()) {
      val retained_2day = resultSet.getString("retained_2day").toInt
      val retained_3day = resultSet.getString("retained_3day").toInt
      val retained_7day = resultSet.getString("retained_7day").toInt
      val retained_30day = resultSet.getString("retained_30day").toInt
      val recharge_1day = resultSet.getString("recharge_1day").toInt
      val recharge_7day = resultSet.getString("recharge_7day").toInt
      val recharge_30day = resultSet.getString("recharge_30day").toInt
      val recharge_total = resultSet.getString("recharge_total").toInt
      val recharge_acc_total = resultSet.getString("recharge_acc_total").toInt
      val publish_date = resultSet.getString("publish_date")
      val game_id = resultSet.getString("game_id").toInt
      //自然量的分包id是 ""
      val pkg_id = ""

      println(publish_date + " - " + game_id + " - " + retained_2day + " - " + retained_3day + " - " + retained_7day + " - " + retained_30day + " - " + recharge_1day + " - " + recharge_7day + " - " + recharge_30day + " - " + recharge_total + " - " + recharge_acc_total)

      pstmt.setInt(1, retained_2day)
      pstmt.setInt(2, retained_3day)
      pstmt.setInt(3, retained_7day)
      pstmt.setInt(4, retained_30day)
      pstmt.setInt(5, recharge_1day)
      pstmt.setInt(6, recharge_7day)
      pstmt.setInt(7, recharge_30day)
      pstmt.setInt(8, recharge_total)
      pstmt.setInt(9, recharge_acc_total)
      pstmt.setString(10, publish_date)
      pstmt.setInt(11, game_id)
      pstmt.setString(12, pkg_id)

      val i = pstmt.executeUpdate()
      println("更新了 ： " + i + " 行")
    }
    pstmtBiHippo.close()
    pstmt.close()
    connBiHippo.close()
    conn.close()

  }

  //获取媒介包中哪些游戏有做广告推广
  def getADGameId(os: Int): String = {
    var game_id_set = "no_game"
    val conn = JdbcUtil.getXiaopeng2FXConn()
    val select_game_sql = "select group_concat(distinct game_id) game_id_set from medium_package where feedbackurl!='' and os=? group by os limit 1"
    val pstmt = conn.prepareStatement(select_game_sql)
    //1：android | 2：ios
    pstmt.setInt(1, os)
    val resultSet = pstmt.executeQuery()
    while (resultSet.next()) {
      game_id_set = resultSet.getString("game_id_set")
    }
    pstmt.close()
    conn.close()
    return game_id_set
  }


  def adAccRetainedDFForeachPartition(adAccRetainedDF: DataFrame) = {
    adAccRetainedDF.foreachPartition(iter => {
      val conn = JdbcUtil.getConn
      val updateSql = "update bi_ad_channel_stats set retained_2day=?,retained_3day=?,retained_7day=?,retained_30day=? where publish_date=? and game_id=? and pkg_id=?"
      val params = new ArrayBuffer[Array[Any]]()

      for (row <- iter) {
        val retained_2day = row.getAs[Long]("retained_2day").toInt
        val retained_3day = row.getAs[Long]("retained_3day").toInt
        val retained_7day = row.getAs[Long]("retained_7day").toInt
        val retained_30day = row.getAs[Long]("retained_30day").toInt
        val regi_time = row.getAs[Date]("regi_time")
        val game_id = row.getAs[Int]("game_id")
        val pkg_id = row.getAs[String]("pkg_id")

        params.+=(Array[Any](retained_2day, retained_3day, retained_7day, retained_30day, regi_time, game_id, pkg_id))
      }
      try {
        JdbcUtil.doBatch(updateSql, params, conn)
      } finally {
        conn.close()
      }


    })
  }

  def adLtvDFForeachPartition(adLtvDF: DataFrame) = {
    adLtvDF.foreachPartition(iter => {
      val conn = JdbcUtil.getConn()
      val updateSql = "update bi_ad_channel_stats set recharge_1day=?,recharge_7day=?,recharge_30day=?,recharge_total=?,recharge_acc_total=? where publish_date=? and game_id=? and pkg_id=?"
      val params = new ArrayBuffer[Array[Any]]()
      for (row <- iter) {
        val recharge_1day = row.getAs[Long]("recharge_1day").toInt
        val recharge_7day = row.getAs[Long]("recharge_7day").toInt
        val recharge_30day = row.getAs[Long]("recharge_30day").toInt
        val recharge_total = row.getAs[Long]("recharge_total").toInt
        val recharge_acc_total = row.getAs[Long]("recharge_acc_total").toInt
        val regi_time = row.getAs[Date]("regi_time")
        val game_id = row.getAs[Int]("game_id")
        val pkg_id = row.getAs[String]("pkg_id")

        val array = Array[Any](recharge_1day, recharge_7day, recharge_30day, recharge_total, recharge_acc_total, regi_time, game_id, pkg_id)
        params.+=(array)
      }
      try {
        JdbcUtil.doBatch(updateSql, params, conn)
      } finally {
        conn.close()
      }

    })
  }

}
