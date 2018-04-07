package com.xiaopeng.bi.gamepublish

import java.sql.{Connection, ResultSet}

import com.xiaopeng.bi.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kequan on 12/25/17.
  * 刷帮累计充值金额离线,cp_cost,利润
  */
object NewPubBackRank {
  var startDay = "";
  var endDay = ""
  var mode = ""

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length == 1) {
      startDay = args(0)
      endDay = startDay
    } else if (args.length == 2) {
      startDay = args(0)
      endDay = startDay
      mode = args(1)
    } else if (args.length == 3) {
      startDay = args(0)
      endDay = args(1)
      mode = args(2)
    }

    // 创建上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.driver.allowMultipleContexts", "true") //.setMaster("local[*]")
    SparkUtils.setMaster(sparkConf)

    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use yyft")

    // 刷榜过的游戏 id
    val game_id_rank = getRankGameIds()
    // 刷榜信息导入 和 删除
    DimensionUtil.updateRankDayDim(hiveContext, startDay, DateUtils.addDay(endDay, 1))
    // 读取分层比例
    SparkUtils.readBiTable("bi_publish_back_divide", hiveContext)

    val toDay = DateUtils.getTodayDate()
    val sql_regi = "select distinct game_id,to_date(reg_time) reg_time,lower(trim(game_account)) game_account from ods_regi_rz where game_account is not null and  game_account!='' and to_date(reg_time)>='startDay' and to_date(reg_time)<='toDay'"
      .replace("startDay", "2017-01-01").replace("toDay", toDay)
    hiveContext.sql(sql_regi).persist().registerTempTable("rz_regi")

    if (mode.equals("pay")) {
      // 累计充值金额 和 账户
      val sql_rank = "select\nrz.reg_time publish_date,\nrz.game_id child_game_id,\nsum(oz.ori_price_all) recharge_price,\ncount(distinct oz.game_account) recharge_accounts\nfrom\n(select game_id,reg_time,game_account from rz_regi where to_date(reg_time)>'startDay') rz\njoin  (select  distinct game_id as parent_game_id , old_game_id  from game_sdk  where state=0) gs on rz.game_id=gs.old_game_id \njoin (select distinct order_no,order_time,lower(trim(game_account)) game_account,game_id,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as ori_price_all,payment_type,order_status from ods_order where order_status=4  and prod_type=6) oz on rz.game_account=oz.game_account\ngroup by rz.reg_time,rz.game_id"
        .replace("startDay", "2017-06-01")
      val df_rank: DataFrame = hiveContext.sql(sql_rank)
      foreachRankOrderPartition(df_rank);
    }

    // 注册信息 和支付信息 join
    val sql_regi_order = "select\nroz.publish_date publish_date,\nroz.order_month order_month,\nroz.child_game_id child_game_id,\nmax(cast(roz.recharge_price as double)) recharge_price,\nmax(roz.division_rule) division_rule\nfrom \n(\nselect\nrz.reg_time publish_date,\nto_date(concat(substr(oz.order_time,0,7),'-01')) order_month,\nrz.game_id child_game_id,\nsum(oz.ori_price_all) recharge_price,\nmax(if(bd.division_rule is null,0,bd.division_rule)) division_rule\nfrom\n(select distinct game_id,reg_time,lower(trim(game_account)) game_account from rz_regi where to_date(reg_time)>='startDay' and to_date(reg_time)<='endDay' and game_id in (game_id_rank) ) rz\njoin  (select  distinct game_id as parent_game_id , old_game_id  from game_sdk  where state=0) gs on rz.game_id=gs.old_game_id \njoin (select distinct order_no,order_time,lower(trim(game_account)) game_account,game_id,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as ori_price_all,payment_type,order_status from ods_order where order_status=4  and prod_type=6 and to_date(order_time)<='endDay') oz on rz.game_account=oz.game_account\nleft join (select distinct child_game_id,divide_month,division_rule from  bi_publish_back_divide) bd on bd.divide_month=concat(substr(oz.order_time,0,7),'-01') and bd.child_game_id=rz.game_id\ngroup by rz.game_id,rz.reg_time, to_date(concat(substr(oz.order_time,0,7),'-01'))\nunion  all\nselect to_date(start_time) publish_date,to_date(concat(substr(start_time,0,7),'-01')) order_month,game_id child_game_id, 0 recharge_price , 0  division_rule from ranking where status=0\n)roz group by roz.publish_date,roz.order_month,roz.child_game_id"
      .replace("startDay", "2017-01-01").replace("endDay", endDay).replace("game_id_rank", game_id_rank)
    val df_regi_order = hiveContext.sql(sql_regi_order).cache()
    df_regi_order.registerTempTable("regi_order_rz")

    //日报明细的 cp_cost 和 利润 cp_cost= 1月流水（1-1月我方分层比）+ 2月流水（1-2月我方分层比）+... ; 利润= 累计流水-累计CP成本-刷榜费用
    val sql_day_cp = "select \nroz.publish_date,\nroz.child_game_id child_game_id,\nround(sum(roz.recharge_price*(1-roz.division_rule/100))) cp_cost,\nround(sum(roz.recharge_price*(roz.division_rule/100)-if(roz.publish_date=rk.start_time and roz.order_month=concat(substr(rk.start_time,0,7),'-01'),rk.cost,0))) profit\nfrom \n(select * from regi_order_rz where to_date(publish_date)>'startDay') roz\njoin (select game_id,to_date(start_time) start_time,if(to_date(end_time)>='2016-06-01',end_time,'endDay') end_time,cost/100 cost  from ranking where status=0)rk  on roz.child_game_id=rk.game_id and to_date(roz.publish_date)>=to_date(rk.start_time) and  to_date(roz.publish_date)<=to_date(rk.end_time)\ngroup by roz.publish_date,roz.child_game_id"
      .replace("startDay", "2017-06-01").replace("endDay", endDay)
    val df_day_cp: DataFrame = hiveContext.sql(sql_day_cp)
    foreachRankCpDayPartition(df_day_cp);

    // 月报 刷榜 信息  导入 和删除
    DimensionUtil.updateRankMonthDim(hiveContext, startDay, DateUtils.addDay(endDay, 1))
    //月报的 cp_cost 和 利润
    val sql_month_cp = "select \nroz.order_month publish_month,\nroz.child_game_id child_game_id,\nrk.id rank_id,\nmax(rk.game_main_id) parent_game_id,\nmax(rk.type) rank_type,\nmax(rk.times) rank_times,\nmax(rk.position) rank_position,\nmax(rk.position_x) rank_position_x,\nmax(rk.partner) rank_partner,\nmax(if(roz.order_month=concat(substr(rk.start_time,0,7),'-01'),rk.cost,0)) rank_cost,\nmax(rk.status) rank_status,\nmax(rk.start_time) rank_start_time,\nsum(roz.recharge_price) recharge_price,\nround(sum(roz.recharge_price*(1-roz.division_rule/100))) cp_cost,\nround(sum(roz.recharge_price*(roz.division_rule/100)-if(roz.publish_date=rk.start_time and roz.order_month=concat(substr(rk.start_time,0,7),'-01'),rk.cost,0))) profit\nfrom \n(select * from regi_order_rz where to_date(publish_date)>'startDay') roz\njoin (select game_id,id,game_main_id,type,times,position,partner,cost/100 cost,status,to_date(start_time) start_time,if(to_date(end_time)>='2016-06-01',end_time,'endDay') end_time,position_x  from ranking where status=0)rk  on roz.child_game_id=rk.game_id and to_date(roz.publish_date)>=to_date(rk.start_time) and  to_date(roz.publish_date)<=to_date(rk.end_time)\ngroup by rk.id,roz.order_month,roz.child_game_id"
      .replace("startDay", "2017-01-01").replace("endDay", endDay)
    val df_month_cp: DataFrame = hiveContext.sql(sql_month_cp)
    foreachRankCpMonthPartition(df_month_cp);

    System.clearProperty("spark.driver.port")
    sc.stop()
  }

  /**
    *
    * 获取所有 刷榜游戏 的 game_id
    *
    * @return
    */
  def getRankGameIds(): String = {
    var game_id_rank = "0"
    val conn = JdbcUtil.getXiaopeng2FXConn();
    val sql: String = " select GROUP_CONCAT(DISTINCT game_id) game_id_rank  from ranking where status=0"
    val stmt = conn.createStatement();
    val rs: ResultSet = stmt.executeQuery(sql)
    while (rs.next) {
      game_id_rank = rs.getString("game_id_rank");
    }
    stmt.close()
    return game_id_rank;
  }


  /**
    * 刷榜累计支付账户和累计支付金额 存入数据库
    *
    * @param df_rank
    */
  def foreachRankOrderPartition(df_rank: DataFrame) = {
    df_rank.foreachPartition(iter => {
      val conn: Connection = JdbcUtil.getConn();
      val sql = "insert into bi_gamepublic_base_opera_kpi(publish_date,child_game_id,recharge_price,recharge_accounts) values(?,?,?,?) on duplicate key update \nrecharge_price=values(recharge_price),\nrecharge_accounts=values(recharge_accounts)"
      val params = new ArrayBuffer[Array[Any]]()

      iter.foreach(row => {
        params.+=(Array[Any](row.get(0), row.get(1), row.get(2), row.get(3)))
      })
      JdbcUtil.doBatch(sql, params, conn)
      conn.close()
    })
  }


  /**
    * 刷榜 日报 累计 cp_cost profit存入数据库
    *
    * @param df_day_cp
    */
  def foreachRankCpDayPartition(df_day_cp: DataFrame) = {
    df_day_cp.foreachPartition(iter => {
      val conn: Connection = JdbcUtil.getConn();
      val sql = "insert into bi_gamepublic_opera_rank_day(publish_date,child_game_id,cp_cost,profit) values(?,?,?,?) on duplicate key update\ncp_cost=values(cp_cost),\nprofit=values(profit)"
      val params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        params.+=(Array[Any](row.get(0), row.get(1), row.get(2), row.get(3)))
      })
      JdbcUtil.doBatch(sql, params, conn)
      conn.close()
    })
  }


  /**
    * 刷榜 月报 累计 cp_cost profit存入数据库
    *
    * @param df_month_cp
    */
  def foreachRankCpMonthPartition(df_month_cp: DataFrame) = {
    df_month_cp.foreachPartition(iter => {
      val conn: Connection = JdbcUtil.getConn();
      val sql = "insert into bi_gamepublic_opera_rank_month(publish_month,child_game_id,rank_id,parent_game_id,rank_type,rank_times,rank_position,rank_position_x,rank_partner,rank_cost,rank_status,rank_start_time,recharge_price,cp_cost,profit) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update \nparent_game_id=values(parent_game_id),\nrank_type=values(rank_type),\nrank_times=values(rank_times),\nrank_position=values(rank_position),\nrank_position_x=values(rank_position_x),\nrank_partner=values(rank_partner),\nrank_cost=values(rank_cost),\nrank_status=values(rank_status),\nrank_start_time=values(rank_start_time),\nrecharge_price=values(recharge_price),\ncp_cost=values(cp_cost),\nprofit=values(profit)"
      val params = new ArrayBuffer[Array[Any]]()

      iter.foreach(row => {
        params.+=(Array[Any](row.get(0), row.get(1), row.get(2), row.get(3), row.get(4), row.get(5), row.get(6), row.get(7), row.get(8), row.get(9), row.get(10), row.get(11), row.get(12), row.get(13), row.get(14)))
      })

      JdbcUtil.doBatch(sql, params, conn)
      conn.close()
    })
  }

}
