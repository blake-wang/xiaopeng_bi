package com.xiaopeng.bi.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import redis.clients.jedis.JedisPool

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 18-3-7.
  */
object GamePublicNewAdRegiUtils {


  def loadRegiInfo(rdd: RDD[String], hiveContext: HiveContext) = {
    val regiRdd = rdd.filter(line => {
      val fields = line.split("\\|", -1)
      fields(0).contains("bi_regi") && fields.length >= 14 && StringUtils.isNumber(fields(4))
    }).map(line => {
      val fields = line.split("\\|", -1)
      // game_account game_id pkg_id reg_time  imei
      Row(fields(3), fields(4).toInt, StringUtils.getArrayChannel(fields(13))(2), fields(5), fields(14))
    })

    if (!regiRdd.isEmpty()) {
      val regiStruct = new StructType()
        .add("game_account", StringType)
        .add("game_id", IntegerType)
        .add("pkg_id", StringType)
        .add("reg_time", StringType)
        .add("imei", StringType);

      hiveContext.createDataFrame(regiRdd, regiStruct).registerTempTable("ods_regi_rz_cache")
      hiveContext.sql("use yyft")
      val regi_sql = "select game_account,game_id,pkg_id,reg_time,imei from ods_regi_rz_cache oz join lastPubGame on oz.game_id = lastPubGame.game_id"
      val regi_df = hiveContext.sql(regi_sql)
      foreachRegiDF(regi_df)
    }
  }

  def foreachRegiDF(regi_df: DataFrame) = {
    regi_df.foreachPartition(iter => {
      //创建jedis客户端
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis0 = pool.getResource

      val conn = JdbcUtil.getConn()
      val connFx = JdbcUtil.getXiaopeng2FXConn()
      val stmt = conn.createStatement

      //插入明细表
      val regi_detail_sql = "insert into bi_new_merge_regi_detail (game_id,pkg_id,reg_time,imei) values (?,?,?,?)"
      val regi_detail_params = new ArrayBuffer[Array[Any]]
      val regi_detail_pstmt = conn.prepareStatement(regi_detail_sql)

      //插入统计表
      val regi_detail_kpi = "insert into bi_new_merge_ad_kpi (publish_date,parent_game_id,child_game_id,pkg_id,remark,os,medium_account,head_people,medium,regi_dev_num,new_regi_dev_num,active_regi_dev_num) values (?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update parent_game_id=values(parent_game_id),remark=values(remark),os=values(os),medium_account=values(medium_account),head_people=values(head_people),medium=values(medium),regi_dev_num=values(regi_dev_num)+regi_dev_num,new_regi_dev_num=values(new_regi_dev_num)+new_regi_dev_num,active_regi_dev_num=values(active_regi_dev_num)"
      val regi_kpi_params = new ArrayBuffer[Array[Any]]()
      val regi_kpi_pstmt = conn.prepareStatement(regi_detail_kpi)

      iter.foreach(line => {
        val reg_time = line.getAs[String]("reg_time")
        val game_id = line.getAs[Int]("game_id")
        val pkg_id = line.getAs[String]("pkg_id")
        val game_account = line.getAs[String]("game_account")
        val imei = line.getAs[String]("imei")

        //获取redis的数据
        val redisValue = JedisUtil.getRedisValue(game_id, pkg_id, reg_time.substring(0, 10), jedis0)
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

        //注册设备数
        var regi_dev_num = 0
        //新增注册设备数 : 当天激活，当天注册的设备
        var new_regi_dev_num = 0
        //激活注册设备数 ： 当天激活，并且注册的设备
        var active_regi_dev_num = 0

        val select_sql = "select reg_time from bi_new_merge_regi_detail where imei='" + imei + "' and game_id='" + game_id + "' and pkg_id='" + pkg_id + "' and date(reg_time)='" + reg_time.substring(0, 10) + "'"
        val rs = stmt.executeQuery(select_sql)
        if (!rs.next()) {
          //今天不存在
          regi_dev_num = 1
          //插入明细表 : 明细表存储的是设备在当天第一次注册的记录
          regi_detail_params.+=(Array(game_id, pkg_id, reg_time, imei))
          val select_before_sql = "select reg_time from bi_new_merge_regi_detail where imei='" + imei + "' and game_id='" + game_id + "' and pkg_id='" + pkg_id + "' and date(reg_time)<'" + reg_time.substring(0, 10) + "'"
          val rs = stmt.executeQuery(select_before_sql)
          if (!rs.next()) {
            //以前也不存在
            new_regi_dev_num = 1
            //再判断激活时间是否是今天
            val select_active_time_sql = "select active_time from bi_new_merge_active_detail where imei='" + imei + "' and game_id='" + game_id + "' and pkg_id='" + pkg_id + "' and date(active_time)='" + reg_time.substring(0, 10) + "'"
            val rs = stmt.executeQuery(select_active_time_sql)
            if (rs.next()) {
              active_regi_dev_num = 1
            }
          }
          //当今天不存在的时候，才会进行
          regi_kpi_params.+=(Array(reg_time.substring(0, 10), parent_game_id, game_id, pkg_id, remark, os, medium_account, head_people, medium, regi_dev_num, new_regi_dev_num, active_regi_dev_num))

          JdbcUtil.executeUpdate(regi_detail_pstmt, regi_detail_params, conn)
          JdbcUtil.executeUpdate(regi_kpi_pstmt, regi_kpi_params, conn)
        }
      })

      stmt.close()
      regi_detail_pstmt.close()
      regi_kpi_pstmt.close()
      conn.close()
      connFx.close()
      pool.returnResource(jedis0)
      pool.destroy()
    })
  }
}
