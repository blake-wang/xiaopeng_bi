package com.xiaopeng.bi.utils


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import redis.clients.jedis.{Jedis, JedisPool}
import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 18-3-7.
  */
object GamePublicNewAdActiveUtils {


  def loadActiveInfo(rdd: RDD[String], hiveContext: HiveContext): Unit = {
    val activeRDD = rdd.filter(line => {
      val fields = line.split("\\|", -1)
      fields(0).contains("bi_active") && fields.length >= 12 && fields(5).length >= 13 && fields(7).toLowerCase.contains("android") //只取android数据
    }).map(line => {
      val fields = line.split("\\|", -1)
      // game_id pkg_id active_time imei
      Row(fields(1).toInt, StringUtils.getArrayChannel(fields(4))(2), fields(5), fields(8))
    })


    if (!activeRDD.isEmpty()) {
      val struct = new StructType()
        .add("game_id", IntegerType)
        .add("pkg_id", StringType) //pkg_id 为 "" 或者为有效值
        .add("active_time", StringType)
        .add("imei", StringType)
      hiveContext.createDataFrame(activeRDD, struct).persist().registerTempTable("ods_active_cache")

      val sql_active = "select distinct game_id,pkg_id,active_time,imei from ods_active_cache oa join lastPubGame on oa.game_id = lastPubGame.game_id"
      val df_active = hiveContext.sql(sql_active)

      foreachActiveDF(df_active)

    }
  }

  def foreachActiveDF(df_active: DataFrame) = {
    df_active.foreachPartition(iter => {
      val conn = JdbcUtil.getConn()
      val connFx = JdbcUtil.getXiaopeng2FXConn()
      val stmt = conn.createStatement()
      // redis 链接
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis0: Jedis = pool.getResource;
      jedis0.select(0);

      //插入激活明细表
      val active_detail_sql = "insert into bi_new_merge_active_detail (game_id,pkg_id,active_time,imei) values (?,?,?,?)"
      val active_detail_params = new ArrayBuffer[Array[Any]]()
      val active_detail_pstmt = conn.prepareStatement(active_detail_sql)

      //更新激活明细表
      val active_detail_update_sql = "update bi_new_merge_active_detail set active_time=? where game_id=? and pkg_id=? and imei=?"
      val active_detail_update_params = new ArrayBuffer[Array[Any]]()
      val active_detail_update_pstmt = conn.prepareStatement(active_detail_update_sql)

      //统计表
      val active_kpi_sql = "insert into bi_new_merge_ad_kpi (publish_date,parent_game_id,child_game_id,pkg_id,remark,os,medium_account,head_people,medium,active_num) values (?,?,?,?,?,?,?,?,?,?) on duplicate key update parent_game_id=values(parent_game_id),remark=values(remark),os=values(os),medium_account=values(medium_account),head_people=values(head_people),medium=values(medium),active_num=values(active_num)+1"
      val active_kpi_params = new ArrayBuffer[Array[Any]]()
      val active_kpi_pstmt = conn.prepareStatement(active_kpi_sql)

      iter.foreach(line => {
        val game_id = line.getAs[Int]("game_id")
        val pkg_id = line.getAs[String]("pkg_id")
        val active_time = line.getAs[String]("active_time")
        val imei = line.getAs[String]("imei")

        //获取redis的数据
        val redisValue = JedisUtil.getRedisValue(game_id, pkg_id, active_time.substring(0, 10), jedis0)
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

        //激活数：设备在分包下第一次打开算一个激活，这里与需求文档里面写的有一点点不同
        val select_all = "select active_time from bi_new_merge_active_detail where game_id = '" + game_id + "' and pkg_id = '" + pkg_id + "' and imei ='" + imei + "'"
        val rs = stmt.executeQuery(select_all)
        if (!rs.next()) {
          active_detail_params.+=(Array(game_id, pkg_id, active_time, imei))
          active_kpi_params.+=(Array(active_time.substring(0, 10), parent_game_id, game_id, pkg_id, remark, os, medium_account, head_people, medium, 1))
        } else {
          val active_time_old = rs.getString("active_time")
          if (DateUtils.beforeTime(active_time, active_time_old)) {
            active_detail_update_params.+=(Array(active_time, game_id, pkg_id, imei))
            active_kpi_params.+=(Array(active_time.substring(0, 10), parent_game_id, game_id, pkg_id, remark, os, medium_account, head_people, medium, 1))
            active_kpi_params.+=(Array(active_time_old.substring(0, 10), parent_game_id, game_id, pkg_id, remark, os, medium_account, head_people, medium, -1))
          }
        }
        //插入明细表
        JdbcUtil.executeUpdate(active_detail_pstmt, active_detail_params, conn)
        //更新明细表
        JdbcUtil.executeUpdate(active_detail_update_pstmt, active_detail_update_params, conn)
        //统计表
        JdbcUtil.executeUpdate(active_kpi_pstmt, active_kpi_params, conn)
      })
      stmt.close()
      active_kpi_pstmt.close()
      active_detail_update_pstmt.close()
      active_detail_pstmt.close()
      conn.close()
      connFx.close()
      pool.returnResource(jedis0)
      pool.destroy()

    })


  }

}
