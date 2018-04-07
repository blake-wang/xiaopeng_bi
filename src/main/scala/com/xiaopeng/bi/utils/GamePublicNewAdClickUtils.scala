package com.xiaopeng.bi.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import redis.clients.jedis.JedisPool

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bigdata on 18-3-7.
  */
object GamePublicNewAdClickUtils {
  def loadClickInfo(rdd: RDD[String], hiveContext: HiveContext) = {
    //将channel里面的日志注册成表
    StreamingUtils.parseChannelToTmpTableNew(rdd, hiveContext)
    //将request里面的日志注册成表
    StreamingUtils.parseRequestToTmpTableNew(rdd, hiveContext)

    val clickDF = hiveContext.sql("select distinct click.game_id child_game_id,click.click_time click_time,click.type_id type_id,click.ip ip,gz.parent_game_id parent_game_id,\ngz.system_type os,\ngz.group_id group_id \nfrom \n(select * from request union select * from channel) click \njoin lastPubGame pg on click.game_id = pg.game_id \n join (select distinct game_id as parent_game_id,old_game_id,system_type,group_id from game_sdk where state=0 ) gz on click.game_id=gz.old_game_id \nwhere click.type_id in (2,4)")
    clickDF.show()
    clickDF.foreachPartition(rows => {
      //创建jedis客户端
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis0 = pool.getResource

      val conn = JdbcUtil.getConn()
      val connFx = JdbcUtil.getXiaopeng2FXConn()
      val stmt = conn.createStatement

      //明细表
      val click_detail_sql = "insert into bi_new_merge_click_detail (game_id,click_time,type_id,ip) values (?,?,?,?)"
      val click_detail_params = new ArrayBuffer[Array[Any]]()
      val click_detail_pstmt = conn.prepareStatement(click_detail_sql)

      //统计表
      val click_kpi_sql = "insert into bi_new_merge_base_day_kpi (publish_date,parent_game_id,child_game_id,os,group_id,request_click_num,request_click_uv,download_num,download_uv) values (?,?,?,?,?,?,?,?,?) on duplicate key update parent_game_id=values(parent_game_id),os=values(os),group_id=values(group_id),request_click_num=values(request_click_num)+request_click_num,request_click_uv=values(request_click_uv)+request_click_uv,download_num=values(download_num)+download_num,download_uv=values(download_uv)+download_uv"
      val click_kpi_params = new ArrayBuffer[Array[Any]]()
      val click_kpi_pstmt = conn.prepareStatement(click_kpi_sql)

      rows.foreach(row => {
        val child_game_id = row.getInt(0)
        val click_time = row.getString(1)
        val type_id = row.getInt(2) //只取了2和4，对应为活动页点击，下载
        val ip = row.get(3)
        val parent_game_id = row.get(4)
        val os = row.get(5)
        val group_id = row.get(6)

        //落地也点击数
        val request_click_num = if (type_id == 4) 1 else 0
        //下载数
        val download_num = if (type_id == 2) 1 else 0
        //落地也点击去重数
        var request_click_uv = 0
        //下载去重数
        var download_uv = 0

        val select_sql = "select ip from bi_new_merge_click_detail where game_id = '" + child_game_id + "' and type_id = '" + type_id + "' and ip = '" + ip + "' and date(click_time) = '" + click_time.substring(0, 10) + "'"

        val rs = stmt.executeQuery(select_sql)
        if (!rs.next()) {
          //落地也点击去重数
          if (type_id == 4) request_click_uv = 1
          //下载去重数
          if (type_id == 2) download_uv = 1
          //明细表  存储ip在当天第一次点击的时间
          click_detail_params.+=(Array(child_game_id, click_time, type_id, ip))
        }
        //统计表
        click_kpi_params.+=(Array(click_time.substring(0, 10), parent_game_id, child_game_id, os, group_id, request_click_num, request_click_uv, download_num, download_uv))


        JdbcUtil.executeUpdate(click_detail_pstmt, click_detail_params, conn)
        JdbcUtil.executeUpdate(click_kpi_pstmt, click_kpi_params, conn)
      })
      stmt.close()
      click_detail_pstmt.close()
      click_kpi_pstmt.close()
      conn.close()
      connFx.close()
      pool.returnResource(jedis0)
      pool.destroy()
    })
  }

}
