package com.xiaopeng.bi.utils


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kequan on 2/26/18.
  * 登录日志处理
  * 帐号，按小时去重
  */
object GamePublicAllianceLoginUtil {

  def loadLoginInfo(rdd: RDD[String], hiveContext: HiveContext) = {
    val loginRDD = rdd.filter(line => {
      val fields = line.split("\\|", -1)
      line.contains("bi_login") && fields.length >= 13 && (!fields(12).equals(""))
    }).map(line => {
      val fields = line.split("\\|", -1)
      //game_account(3),logintime(4),game_id(8),imei(7),alliance_id(12)
      Row(fields(3).trim().toLowerCase(), fields(4), fields(8).toInt, fields(7), fields(12))
    })

    if(!loginRDD.isEmpty()){
      val struct = new StructType()
        .add("game_account", StringType)
        .add("login_time", StringType)
        .add("game_id", IntegerType)
        .add("imei", StringType)
        .add("alliance_bag_id", StringType)
      hiveContext.createDataFrame(loginRDD, struct).registerTempTable("ods_login_cache")

      val sql = "select distinct \nlg.login_time,\nlg.game_id,\nlg.alliance_bag_id,\nif(gs.game_id is null,0,gs.game_id) parent_game_id,\nif(terrace_name is null,'',terrace_name) terrace_name,\nif(terrace_type is null,1,terrace_type) terrace_type,\nif(gs.system_type is null,0,gs.system_type) os,\nif(head_people is null,'',head_people) head_people,\nif(terrace_auto_id is null,0,terrace_auto_id)  terrace_auto_id,\nif(alliance_company_id is null,0,alliance_company_id) alliance_company_id,\nif(alliance_company_name is null,'',alliance_company_name) alliance_company_name,\nlg.game_account, \nlg.imei\nfrom ods_login_cache lg \njoin (select distinct game_id,old_game_id,system_type from game_sdk where state = 0) gs on lg.game_id=gs.old_game_id\nleft join alliance_dim ad on lg.alliance_bag_id=ad.alliance_bag_id "
      val loginDF = hiveContext.sql(sql)

      foreachLoginDF(loginDF)
    }

  }

  /**
    * 通过join的方式获取维度信息
    *
    * @param loginDF
    */
  def foreachLoginDF(loginDF: DataFrame) = {
    loginDF.foreachPartition(iter => {
      if (!iter.isEmpty) {
        val conn = JdbcUtil.getConn()
        val stat = conn.createStatement()

        val jedisPool = JedisUtil.getJedisPool()
        val jedis10 = jedisPool.getResource
        jedis10.select(10)
        val jedis0 = jedisPool.getResource

        //统计表 留存和ltv 分别插入
        val login_kpi_sql_dau = "insert into bi_gamepublic_alliance_base_day_kpi (publish_date,parent_game_id,child_game_id,terrace_name,terrace_type,alliance_bag_id,os,head_people,terrace_auto_id,alliance_company_id,alliance_company_name,dau_account_num,dau_device_num) values (?,?,?,?,?,?,?,?,?,?,?,?,?) \non duplicate key update \nterrace_name=values(terrace_name),terrace_type=values(terrace_type),os=values(os),head_people=values(head_people),terrace_auto_id=values(terrace_auto_id),alliance_company_id=values(alliance_company_id),alliance_company_name=values(alliance_company_name),dau_account_num=values(dau_account_num)+dau_account_num,dau_device_num=values(dau_device_num)+dau_device_num"
        val login_kpi_sql_dau_params = ArrayBuffer[Array[Any]]()
        val login_kpi_dau_pstat = conn.prepareStatement(login_kpi_sql_dau)

        val login_kpi_sql_retain = "insert into bi_gamepublic_alliance_base_day_kpi (publish_date,parent_game_id,child_game_id,terrace_name,terrace_type,alliance_bag_id,os,head_people,terrace_auto_id,alliance_company_id,alliance_company_name,retained_1day,retained_3day,retained_7day,retained_30day) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \non duplicate key update \nterrace_name=values(terrace_name),terrace_type=values(terrace_type),os=values(os),head_people=values(head_people),terrace_auto_id=values(terrace_auto_id),alliance_company_id=values(alliance_company_id),alliance_company_name=values(alliance_company_name),retained_1day=values(retained_1day)+retained_1day,retained_3day=values(retained_3day)+retained_3day,retained_7day=values(retained_7day)+retained_7day,retained_30day=values(retained_30day)+retained_30day"
        val login_kpi_sql_retain_params = ArrayBuffer[Array[Any]]()
        val login_kpi_retain_pstat = conn.prepareStatement(login_kpi_sql_retain)

        iter.foreach(line => {
          val login_time = line.getAs[String]("login_time")
          val game_id = line.getAs[Int]("game_id")
          val alliance_bag_id = line.getAs[String]("alliance_bag_id")
          val parent_game_id = line.getAs[Int]("parent_game_id")
          val terrace_name = line.getAs[String]("terrace_name")
          val terrace_type = line.getAs[Int]("terrace_type")
          val os = line.getAs[Int]("os")
          val head_people = line.getAs[String]("head_people")
          val terrace_auto_id = line.getAs[Int]("terrace_auto_id")
          val alliance_company_id = line.getAs[Int]("alliance_company_id")
          val alliance_company_name = line.getAs[String]("alliance_company_name")

          val imei = line.getAs[String]("imei")
          val game_account = line.getAs[String]("game_account")


          // DAU帐号数
          val dau_account_num = AllianceUtils.dauAccountNum(game_id, game_account, login_time, alliance_bag_id, jedis10)
          // DAU设备数
          val dau_device_num = AllianceUtils.dauDeviceNum(game_id, imei, login_time, alliance_bag_id, jedis10)

          //实时计算留存
          var retained_1day = 0
          var retained_3day = 0
          var retained_7day = 0
          var retained_30day = 0

          //留存这里的game_id和alliance_id要获取注册日志中的数据
          val accountInfo = jedis0.hgetAll(game_account)
          val reg_time = if (accountInfo.get("reg_time") == null) "0000-00-00" else accountInfo.get("reg_time")
          val reg_game_id = if (accountInfo.get("game_id") == null) "0" else accountInfo.get("game_id")
          val reg_alliance_bag_id = if (accountInfo.get("alliance_bag_id") == null) "0" else accountInfo.get("alliance_bag_id")

          //按帐号统计留存 : 只在这个帐号今天第一次登录的时候计算一次,并且将留存信息存入到注册那天的记录中
          if (dau_account_num == 1) {
            //通过帐号找出帐号的注册时间
            val retainedDay = AllianceUtils.getAllianceAccountDiffDay(login_time, reg_time)
            //次日留存
            retained_1day = if (retainedDay == 1) 1 else 0
            //3日留存
            retained_3day = if (retainedDay == 2) 1 else 0
            //7日留存
            retained_7day = if (retainedDay == 6) 1 else 0
            //30日留存
            retained_30day = if (retainedDay == 29) 1 else 0
          }


          //dau     用登录日志中的login_time,game_id和alliance_bag_id
          login_kpi_sql_dau_params.+=(Array(login_time.substring(0, 10), parent_game_id, game_id, terrace_name, terrace_type, alliance_bag_id, os, head_people, terrace_auto_id, alliance_company_id, alliance_company_name, dau_account_num, dau_device_num))
          //retain  用注册时的login_time,game_id和alliance_bag_id   这里要注意，留存要用注册时的账户信息
          if ((!reg_alliance_bag_id.equals("0") && (retained_1day == 1 || retained_3day == 1 || retained_7day == 1 || retained_30day == 1))) {
            login_kpi_sql_retain_params.+=(Array(reg_time.substring(0, 10), parent_game_id, reg_game_id, terrace_name, terrace_type, reg_alliance_bag_id, os, head_people, terrace_auto_id, alliance_company_id, alliance_company_name, retained_1day, retained_3day, retained_7day, retained_30day))
          }
          //更新统计表
          JdbcUtil.executeUpdate(login_kpi_dau_pstat, login_kpi_sql_dau_params, conn)
          JdbcUtil.executeUpdate(login_kpi_retain_pstat, login_kpi_sql_retain_params, conn)


          //缓存登录帐号到redis
          AllianceUtils.cacheAllianceTodayLoginAccount(game_id, game_account, login_time, alliance_bag_id, jedis10)
          //缓存登录设备到redis
          AllianceUtils.cacheAllianceTodayLoginDevice(game_id, imei, login_time, alliance_bag_id, jedis10)

        })

        login_kpi_dau_pstat.close()
        login_kpi_retain_pstat.close()
        stat.close()
        conn.close()
        jedisPool.returnResource(jedis10)
        jedisPool.returnResource(jedis0)
        jedisPool.destroy()
      }
    })
  }
}
