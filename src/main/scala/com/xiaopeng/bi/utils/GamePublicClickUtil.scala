package com.xiaopeng.bi.utils

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import redis.clients.jedis.JedisPool

import scala.collection.mutable.ArrayBuffer

/**
  * Created by root on 5/8/17.
  * -Xms1024m -Xmx1024m -XX:PermSize=512m -XX:MaxPermSize=512M
  */
object GamePublicClickUtil {
  val logger = Logger.getLogger(GamePublicClickUtil.getClass)
  var arg = "60"

  def loadClickInfo(rdd: RDD[String], sqlContext: SQLContext) = {
    //将channel里面的日志注册成表   expand_channel,game_id,publish_time,type,ip
    StreamingUtils.parseChannelToTmpTable(rdd, sqlContext)
    //将request里面的日志注册成表
    StreamingUtils.parseRequestToTmpTable(rdd, sqlContext)
    //这里，同一分包下的ip，按天去重了
    //    val clickDF = sqlContext.sql("select distinct click.* from (select * from request union select * from channel) click join lastPubGame pg on click.game_id = pg.game_id ")
    val clickDF = sqlContext.sql("select distinct click.expand_channel,click.game_id,click.publish_time,click.type_id,click.ip from (select * from request union select * from channel) click join lastPubGame pg on click.game_id = pg.game_id ")

    clickDF.foreachPartition(rows => {
      //创建jedis客户端
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis = pool.getResource

      val conn = JdbcUtil.getConn()
      val stmt = conn.createStatement


      //投放基础表
      val base_kpi_sql = "insert into bi_gamepublic_base_day_kpi(publish_date,child_game_id,medium_channel," +
        "ad_site_channel,pkg_code,adpage_click_uv,request_click_uv,show_uv,download_uv," +
        "parent_game_id,os,medium_account,promotion_channel,promotion_mode,head_people,group_id)" +
        " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
        " on duplicate key update adpage_click_uv=adpage_click_uv+?,request_click_uv=request_click_uv+?,show_uv=show_uv+?,download_uv=download_uv+?"
      val base_kpi_params = new ArrayBuffer[Array[Any]]()

      //点击明细表
      val sqlDetailText = "insert into bi_gamepublic_click_detail(publish_date,game_id,medium_channel,ad_site_channel,pkg_code,type,ip,create_time) values(?,?,?,?,?,?,?,?)"
      val click_detail_arams = new ArrayBuffer[Array[Any]]()

      //运营日报
      val base_opera_kpi_sql = "insert into bi_gamepublic_base_opera_kpi (publish_date,child_game_id,os,group_id,request_click_uv,download_uv) values (?,?,?,?,?,?) on duplicate key update os=values(os),group_id=values(group_id),request_click_uv=values(request_click_uv)+request_click_uv,download_uv=values(download_uv)+download_uv"
      val base_opera_kpi_params = new ArrayBuffer[Array[Any]]()


      rows.foreach(row => {
        val expand_channel = row.getString(0)
        val channelArray = StringUtils.getArrayChannel(expand_channel)
        val game_id = row.getInt(1)
        val pkg_code = channelArray(2)
        val click_date = row.getString(2)
        //点击类型
        val type_id = row.getInt(3)
        val ip = row.getString(4)
        val redisValue = JedisUtil.getRedisValue(game_id, pkg_code, click_date, jedis)
        val os = redisValue(1)
        val group_id = redisValue(6)
        val click_sql_pkg_id = "select ip from bi_gamepublic_click_detail where publish_date ='" + click_date + "' and game_id = " + game_id + "" +
          " and type = " + type_id + " and ip='" + ip + "'" + " and medium_channel = '" + channelArray(0) + "' and ad_site_channel = '" + channelArray(1) + "' and pkg_code='" + channelArray(2) + "'"
        val rs = stmt.executeQuery(click_sql_pkg_id)

        if (!rs.next()) {
          //1：展示数
          val adpage_click_uv = if (type_id == 1) 1 else 0
          //2：下载数
          val request_click_uv = if (type_id == 2) 1 else 0
          //3：广告展示
          val show_uv = if (type_id == 3) 1 else 0
          //4：活动页
          val download_uv = if (type_id == 4) 1 else 0

          //parent_game_id,os,medium_account,promotion_channel,promotion_mode,head_people,group_id
          if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 15) {
            base_kpi_params.+=(Array[Any](row.get(2), row.get(1), channelArray(0), channelArray(1), channelArray(2)
              , adpage_click_uv, request_click_uv, show_uv, download_uv,
              redisValue(0), redisValue(1), redisValue(2), redisValue(3), redisValue(4), redisValue(5), redisValue(6)
              , adpage_click_uv, request_click_uv, show_uv, download_uv))

            click_detail_arams.+=(Array[Any](row.get(2), row.get(1), channelArray(0), channelArray(1), channelArray(2), row.get(3), row.get(4), DateUtils.getTodayTime()))
          }
        }


        val click_sql_game_id = "select ip from bi_gamepublic_click_detail where publish_date ='" + row.getString(2) + "' and game_id = " + row.getInt(1) + " and type = " + row.getInt(3) + " and ip='" + row.getString(4) + "'"
        val res2 = stmt.executeQuery(click_sql_game_id)
        if (!res2.next()) {
          //2：下载数
          val request_click_uv = if (type_id == 2) 1 else 0
          //4：活动页
          val download_uv = if (type_id == 4) 1 else 0

          if (request_click_uv > 0 || download_uv > 0) {
            base_opera_kpi_params.+=(Array(click_date, game_id, os, group_id, request_click_uv, download_uv))
          }
        }

      })

      try {
        JdbcUtil.doBatch(sqlDetailText, click_detail_arams, conn)
        JdbcUtil.doBatch(base_kpi_sql, base_kpi_params, conn)
        JdbcUtil.doBatch(base_opera_kpi_sql, base_opera_kpi_params, conn)
      } catch {
        case ex: Exception => {
          logger.error("=========================插入click表异常：" + ex)
        }
      } finally {
        stmt.close()
        conn.close
      }

      pool.returnResource(jedis)
      pool.destroy()
    })
  }

}
