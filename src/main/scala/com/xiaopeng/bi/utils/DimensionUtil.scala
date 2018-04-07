package com.xiaopeng.bi.utils

import java.sql.{Connection, PreparedStatement, ResultSet}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer

/**
  * 更新维度信息
  * Created by kequan on 9/11/17.
  */
object DimensionUtil {

  /**
    * 加载维度表数 据
    *
    * @param sqlContext
    * @param currentday
    */
  def processDbDim(sqlContext: HiveContext, currentday: String) = {
    sqlContext.read.parquet("/tmp/hive/fxdim.parquet").registerTempTable("tb_tmp")
    val parDim: DataFrame = sqlContext.sql("select medium_account,promotion_channel,promotion_mode,head_people,pkg_code from tb_tmp where date(mstart_date)<='currentday' and date(mend_date)>='currentday'".replace("currentday", currentday))
    val sql2Mysql = "update bi_gamepublic_actions set medium_account=?,promotion_channel=?,promotion_mode=?,head_people=? where publish_date=? and pkg_code=? "
    parDim.foreachPartition((rows: Iterator[Row]) => {
      val conn = JdbcUtil.getConn()
      val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
      for (x <- rows) {
        ps.setString(1, x.get(0).toString)
        ps.setString(2, x.get(1).toString)
        ps.setString(3, x.get(2).toString)
        ps.setString(4, x.get(3).toString)
        ps.setString(5, currentday)
        ps.setString(6, x.get(4).toString)
        ps.executeUpdate()
      }
      ps.close()
      conn.close()
    })
  }

  /**
    * 加载维度表数 据
    *
    * @param sqlContext
    * @param startDay
    * @param endDay
    */
  def processDbDim(sqlContext: HiveContext, startDay: String, endDay: String) = {
    sqlContext.read.parquet("/tmp/hive/fxdim.parquet").registerTempTable("tb_tmp")

    val days: Int = DateUtils.compareToDay(endDay, startDay)
    for (x <- 0.to(days)) {
      val currentday = DateUtils.addDay(startDay, x)
      val parDim: DataFrame = sqlContext.sql("select medium_account,promotion_channel,promotion_mode,head_people,pkg_code from tb_tmp where date(mstart_date)<='currentday' and date(mend_date)>='currentday'".replace("currentday", currentday))
      parDim.foreachPartition((rows: Iterator[Row]) => {
        val conn = JdbcUtil.getConn()
        val sql1 = "update bi_gamepublic_actions set medium_account=?,promotion_channel=?,promotion_mode=?,head_people=? where publish_date=? and pkg_code=? "
        val ps1: PreparedStatement = conn.prepareStatement(sql1)
        val sql2 = "update bi_gamepublic_basekpi set medium_account=?,promotion_channel=?,promotion_mode=?,head_people=? where date(publish_time)=? and ad_label=?"
        val ps2: PreparedStatement = conn.prepareStatement(sql2)
        val sql3 = "update bi_gamepublic_base_day_kpi set medium_account=?,promotion_channel=?,promotion_mode=?,head_people=? where publish_date=? and pkg_code=?"
        val ps3: PreparedStatement = conn.prepareStatement(sql3)

        for (x <- rows) {
          ps1.setString(1, x.get(0).toString)
          ps1.setString(2, x.get(1).toString)
          ps1.setString(3, x.get(2).toString)
          ps1.setString(4, x.get(3).toString)
          ps1.setString(5, currentday)
          ps1.setString(6, x.get(4).toString)
          ps1.executeUpdate()

          ps2.setString(1, x.get(0).toString)
          ps2.setString(2, x.get(1).toString)
          ps2.setString(3, x.get(2).toString)
          ps2.setString(4, x.get(3).toString)
          ps2.setString(5, currentday)
          ps2.setString(6, x.get(4).toString)
          ps2.executeUpdate()

          ps3.setString(1, x.get(0).toString)
          ps3.setString(2, x.get(1).toString)
          ps3.setString(3, x.get(2).toString)
          ps3.setString(4, x.get(3).toString)
          ps3.setString(5, currentday)
          ps3.setString(6, x.get(4).toString)
          ps3.executeUpdate()
        }
        ps1.close()
        ps2.close()
        ps3.close()
        conn.close()
      })
    }
  }


  /**
    * 更新实时表游戏相关的维度信息
    *
    * @param startdday
    * @param currentday
    */
  def updateGameDim(startdday: String, currentday: String) = {
    // 统一更新为维度信息 ： parent_game_id,os,group_id
    val dimsql = "select  distinct game_id as parent_game_id , old_game_id  ,system_type,group_id from game_sdk  where state=0"
    val connFx: Connection = JdbcUtil.getXiaopeng2FXConn();
    val stmtFX = connFx.createStatement();
    val rz: ResultSet = stmtFX.executeQuery(dimsql)

    val connBi: Connection = JdbcUtil.getConn()
    val stmtBi = connBi.createStatement();
    while (rz.next()) {
      val parent_game_id = rz.getString("parent_game_id")
      val game_id = rz.getString("old_game_id")
      val os = rz.getString("system_type")
      val group_id = rz.getString("group_id")
      val sqlbasekpi = "update bi_gamepublic_basekpi set parent_game_id='" + parent_game_id + "',os='" + os + "',group_id='" + group_id + "' where game_id='" + game_id + "' and publish_time>='" + startdday + "' and publish_time<= '" + currentday + "'"
      stmtBi.execute(sqlbasekpi)

      val sqlDaykpi = "update bi_gamepublic_base_day_kpi set parent_game_id='" + parent_game_id + "',os='" + os + "',group_id='" + group_id + "' where child_game_id='" + game_id + "' and publish_date>='" + startdday + "' and publish_date<= '" + currentday + "'"
      stmtBi.execute(sqlDaykpi)

      val sqlOperHour = "update bi_gamepublic_base_opera_hour_kpi set parent_game_id='" + parent_game_id + "',os='" + os + "',group_id='" + group_id + "' where child_game_id='" + game_id + "' and publish_time>='" + startdday + "' and publish_time<= '" + currentday + "'"
      stmtBi.execute(sqlOperHour)

      val sqlOper = "update bi_gamepublic_base_opera_kpi set parent_game_id='" + parent_game_id + "',os='" + os + "',group_id='" + group_id + "' where child_game_id='" + game_id + "' and publish_date>='" + startdday + "' and publish_date<= '" + currentday + "'"
      stmtBi.execute(sqlOper)
    }
    stmtBi.close()
    stmtFX.close()
    connBi.close()
    connBi.close()
  }

  /**
    * 更新实时刷榜的维度信息
    *
    * @param hiveContext
    */
  def updateRankDayDim(hiveContext: HiveContext, startDay: String, endDay: String) = {
    SparkUtils.readXiaopeng2FxTableCache("ranking", hiveContext)

    var sql = "select game_id,id,game_main_id,type,times,position,partner,cost,status,start_time,if(to_date(end_time)>='2016-06-01',end_time,'toDay') end_time,position_x  from ranking where to_date(update_time)>='startDay' and to_date(update_time)<='endDay'"
      .replace("startDay", startDay).replace("endDay", endDay).replace("toDay", DateUtils.getTodayDate())

    if (DateUtils.getHour().toInt == 0) {
      //  0  点的时候全量更新  rank_day 字段
      sql = "select game_id,id,game_main_id,type,times,position,partner,cost,status,start_time,if(to_date(end_time)>='2016-06-01',end_time,'toDay') end_time,position_x  from ranking "
        .replace("toDay", DateUtils.getTodayDate())
    }

    val df = hiveContext.sql(sql)
    df.foreachPartition(it => {
      if (!it.isEmpty) {
        val connBi: Connection = JdbcUtil.getConn()
        val sql = "insert into bi_gamepublic_opera_rank_day(publish_date,child_game_id,rank_id,parent_game_id,rank_type,rank_times,rank_position,rank_partner,rank_cost,rank_status,rank_position_x,rank_day) values(?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update\nrank_id=values(rank_id),\nparent_game_id=values(parent_game_id),\nrank_type=values(rank_type),\nrank_times=values(rank_times),\nrank_position=values(rank_position),\nrank_partner=values(rank_partner),\nrank_cost=values(rank_cost),\nrank_status=values(rank_status),\nrank_position_x=values(rank_position_x),\nrank_day=values(rank_day)"
        val params = new ArrayBuffer[Array[Any]]()

        val sql_delete = "delete from bi_gamepublic_opera_rank_day where rank_id = ? and (publish_date<? or publish_date>?)"
        val params_delete = new ArrayBuffer[Array[Any]]()

        val sql_delete_rm = "delete from bi_gamepublic_opera_rank_day where rank_id=?"
        val params_delete_rm = new ArrayBuffer[Array[Any]]()

        val sql_delete_game = "delete from bi_gamepublic_opera_rank_day where rank_id=? and child_game_id !=?"
        val params_delete_game = new ArrayBuffer[Array[Any]]()
        it.foreach(row => {
          if ((row.get(9) != null) && (row.get(8) != null)) {
            val startTime = row.get(9).toString;
            val endTime = row.get(10).toString;
            var rank_status = row.get(8).toString.toInt;
            var rank_day = DateUtils.compareToDay(DateUtils.getTodayDate(), startTime) + 1
            //这里判断一次刷榜状态，如果状态为0，则表示正常，状态为-1，表示这个刷榜信息已经软删。
            if (rank_status == 0) {
              for (x <- 0 to DateUtils.compareToDay(endTime, startTime)) {
                //这里x==0的判断，是因为，刷榜成本这个字段的值，在ranking这个表中，只在第一天录入，因此这里只在第一天的时候存入刷榜成本row.get(7).toString.toInt / 100
                if (x == 0) {
                  params.+=(Array[Any](DateUtils.addDay(startTime, x), row.get(0), row.get(1), row.get(2), row.get(3), row.get(4), row.get(5), row.get(6), row.get(7).toString.toInt / 100, row.get(8), row.get(11), rank_day))
                } else {
                  params.+=(Array[Any](DateUtils.addDay(startTime, x), row.get(0), row.get(1), row.get(2), row.get(3), row.get(4), row.get(5), row.get(6), 0, row.get(8), row.get(11), rank_day))
                }
              }
              //  删除 刷榜日期范围变小的数据
              params_delete.+=(Array[Any](row.getInt(1), startTime, endTime))
              //  删除 游戏id 改变的数据
              params_delete_game += (Array[Any](row.getInt(1), row.getInt(0)))
            } else {
              // 删除录入时删除的数据
              params_delete_rm.+=(Array[Any](row.get(1)))
            }
          }
        })

        try {
          JdbcUtil.doBatch(sql_delete_rm, params_delete_rm, connBi)
          JdbcUtil.doBatch(sql_delete, params_delete, connBi)
          JdbcUtil.doBatch(sql_delete_game, params_delete_game, connBi)
          JdbcUtil.doBatch(sql, params, connBi)
        } finally {
          connBi.close()
        }
      }

    })

    hiveContext.uncacheTable("ranking")
  }

  /**
    * 更新刷榜月榜信息
    *
    * @param hiveContext
    */
  def updateRankMonthDim(hiveContext: HiveContext, startDay: String, endDay: String) = {
    SparkUtils.readXiaopeng2FxTable("ranking", hiveContext)
    val toDay = DateUtils.getTodayDate()
    val sql = "select id from ranking where to_date(update_time)>='startDay' and to_date(update_time)<='endDay'"
      .replace("startDay", startDay).replace("endDay", endDay)

    val df = hiveContext.sql(sql)
    df.foreachPartition(it => {
      if (!it.isEmpty) {
        val connBi: Connection = JdbcUtil.getConn()
        val sql_delete = "delete from bi_gamepublic_opera_rank_month  where  rank_id=?"
        val params_delete = new ArrayBuffer[Array[Any]]()
        it.foreach(row => {
          // 删除录入时 删除或者修改的 数据
          params_delete.+=(Array[Any](row.get(0)))
        })
        JdbcUtil.doBatch(sql_delete, params_delete, connBi)
        connBi.close()
      }
    })
  }

  /**
    * 读取 联运管理表维度信息表  注册成新表  alliance_dim
    *
    * @param hiveContext
    * @return
    */
  def createAllianceDim(hiveContext: HiveContext) = {
    SparkUtils.readXiaopeng2FxTableCache("alliance_bag", hiveContext)
    SparkUtils.readXiaopeng2FxTableCache("alliance_terrace", hiveContext)
    SparkUtils.readXiaopeng2FxTableCache("alliance_company", hiveContext)
    SparkUtils.readXiaopeng2FxTableCache("user", hiveContext)
    SparkUtils.readXiaopeng2FxTableCache("game_sdk", hiveContext)
    val sql ="select\nmax(ab.game_id)  child_game_id,\nmax(if(at.name is null,'',at.name)) terrace_name,\nmax(if(at.type is null,1,at.type)) terrace_type,\nab.alliance_bag_id alliance_bag_id,\nmax(if(us.name is null,'',us.name)) head_people,\nmax(if(at.id is null,0,at.id)) terrace_auto_id,\nmax(if(ac.id is null,0, ac.id)) alliance_company_id,\nmax(if(ac.name is null,'',ac.name)) alliance_company_name\nfrom \n(select  distinct alliance_bag_id ,game_id,user_id,terrace_id from  alliance_bag) ab -- 联运包ID \nleft join (select distinct id,terrace_id,type,name,ac_id from alliance_terrace where status=1) at on ab.terrace_id=at.terrace_id  -- 联运平台信息\nleft join (select id,name from alliance_company where status=1)ac on ac.id=at.ac_id  -- 联运商信息\nleft join (select distinct id,name from user)us on ab.user_id=us.id  -- 负责人\ngroup by  ab.alliance_bag_id"
    hiveContext.sql(sql).persist().registerTempTable("alliance_dim")
    hiveContext.cacheTable("alliance_dim")
  }

  /**
    * 读取 联运管理表维度信息表  注册成新表  alliance_dim
    *
    * @param hiveContext
    * @return
    */
  def uncacheAllianceDimTable(hiveContext: HiveContext) = {
    hiveContext.uncacheTable("alliance_dim")
    hiveContext.uncacheTable("alliance_bag")
    hiveContext.uncacheTable("alliance_terrace")
    hiveContext.uncacheTable("alliance_company")
    hiveContext.uncacheTable("user")
    hiveContext.uncacheTable("game_sdk")
  }

}
