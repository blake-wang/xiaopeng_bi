package com.xiaopeng.bi.utils

import java.sql.Connection

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import redis.clients.jedis.JedisPool

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 18-3-7.
  */
object GamePublicNewAdLoginUtils {


  def loadLoginInfo(rdd: RDD[String], hiveContext: HiveContext) = {
    val loginRDD = rdd.filter(line => {
      val fields = line.split("\\|", -1)
      fields(0).contains("bi_login") && fields.length >= 9 && StringUtils.isNumber(fields(8))
    }).map(line => {
      val fields = line.split("\\|", -1)
      //game_account(3),logintime(4),game_id(8)，expand_channel(6),imei(7)
      Row(fields(3).trim.toLowerCase, fields(4), fields(8).toInt, StringUtils.getArrayChannel(fields(6))(2), fields(7))
    })

    val struct = new StructType()
      .add("game_account", StringType)
      .add("login_time", StringType)
      .add("game_id", IntegerType)
      .add("pkg_id", StringType)
      .add("imei", StringType)
    hiveContext.createDataFrame(loginRDD, struct).registerTempTable("ods_login_cache")

    val login_sql = "select distinct game_account,login_time,game_id,pkg_id,imei from ods_login_cache ol join loasPubGame on ol.game_id=loasPubGame.game_id"
    val login_df = hiveContext.sql(login_sql)
    foreachLoginDF(login_df)
  }

  def foreachLoginDF(login_df: DataFrame) = {
    login_df.foreachPartition(iter => {
      //创建jedis客户端
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis0 = pool.getResource
      jedis0.select(0)
      val jedis12 = pool.getResource
      jedis12.select(12)

      val conn = JdbcUtil.getConn()
      val connFx = JdbcUtil.getXiaopeng2FXConn()
      val stmt = conn.createStatement

      val login_kpi_sql = "insert into bi_new_merge_ad_kpi (publish_date,parent_game_id,child_game_id,pkg_id,remark,os,medium_account,head_people,medium,new_active_dev_num,retained_1day,retained_3day,retained_7day) \non duplicate key update parent_game_id=values(parent_game_id),remark=values(remark),os=values(os),medium_account=values(medium_account),head_people=values(head_people),medium=values(medium),new_active_dev_num=values(new_active_dev_num)+new_active_dev_num,retained_1day=values(retained_1day)+retained_1day,retained_3day=values(retained_3day)+retained_3day,retained_7day=values(retained_7day)+retained_7day"
      val login_kpi_params = new ArrayBuffer[Array[Any]]
      val login_kpi_pstmt = conn.prepareStatement(login_kpi_sql)

      iter.foreach(line => {
        val game_account = line.getAs[String]("game_account")
        val login_time = line.getAs[String]("login_time")
        val game_id = line.getAs[Int]("game_id")
        val pkg_id = line.getAs[String]("pkg_id")
        val imei = line.getAs[String]("imei")

        //获取redis的数据
        val redisValue = JedisUtil.getRedisValue(game_id, pkg_id, login_time.substring(0, 10), jedis0)
        val parent_game_id = redisValue(0)
        val medium_account = redisValue(2)
        val head_people = redisValue(5)
        val os = 1 //只取android数据
        val medium = 0 //默认都是自然量

        //分包备注
        var remark = ""
        if (pkg_id.contains("M")) {
          remark = CommonsThirdData.getMediumRemark(pkg_id, connFx)
        } else if (pkg_id.contains("Q")) {
          remark = CommonsThirdData.getChannelRemark(pkg_id, connFx)
        }

        //新增活跃设备 ： 当天激活，当天注册，并且登录次数大于等于2次的设备
        var new_active_dev_num = 0
        //设备的第一次注册时间
        var imei_first_reg_time = ""
        //设备的登录次数
        var imei_login_num = 0

        val reg_time_sql = "select reg_time from bi_new_merge_regi_detail where imei='" + imei + "' and game_id='" + game_id + "' and pkg_id='" + pkg_id + "' order by reg_time asc limit 1"
        val regiRS = stmt.executeQuery(reg_time_sql)
        if (regiRS.next()) {
          imei_first_reg_time = regiRS.getString("reg_time")
        }

        //1:先判断这个设备是否当天第一次次注册
        if (imei_first_reg_time.equals(login_time.substring(0, 10))) {
          //2:判断这个设备今天登录了几次
          imei_login_num = GamePublicNewAdCommonUtils.getImeiTodayLoginNum(login_time.substring(0, 10), game_id, pkg_id, imei, jedis12)
          //3:当设个设备登录次数达到两次new_active_dev_num计算一次
          if (imei_login_num == 2) {
            new_active_dev_num = 1
          }
        }

        //只在每天第一次登录的时候进行统计
        //次日留存
        var retain_1day = 0
        //三日留存
        var retain_3day = 0
        //7日留存
        var retain_7day = 0

        if (imei_login_num == 1) {
          //计算留存
          val retainDay = GamePublicNewAdCommonUtils.getLoginRetainDayDiff(imei_first_reg_time, login_time)
          retain_1day = if (retainDay == 1) 1 else 0
          retain_3day = if (retainDay == 2) 1 else 0
          retain_7day = if (retainDay == 6) 1 else 0
        }


        //判断这个设备的retain,只在这个设备当天第一次登录的时候计算一次
        login_kpi_params.+=(Array(login_time.substring(0, 10), parent_game_id, game_id, pkg_id, remark, os, medium_account, head_people, medium, new_active_dev_num, retain_1day, retain_3day, retain_7day))
        JdbcUtil.executeUpdate(login_kpi_pstmt, login_kpi_params, conn)
      })

      stmt.close()
      login_kpi_pstmt.close()
      conn.close()
      connFx.close()
      pool.returnResource(jedis0)
      pool.destroy()


    })
  }

}
