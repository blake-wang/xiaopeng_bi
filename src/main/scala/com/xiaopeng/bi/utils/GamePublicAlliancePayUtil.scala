package com.xiaopeng.bi.utils

import java.sql.{Connection, Statement}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kequan on 2/26/18.
  * 联运游戏  订单  处理
  */
object GamePublicAlliancePayUtil {


  def loadPayInfo(rdd: RDD[String], hiveContext: HiveContext) = {
    val orderRDD = rdd.filter(line => {
      val fields = line.split("\\|", -1)
      fields(0).contains("bi_order") && fields.length >= 30 && (!fields(29).equals("")) && (!fields(2).equals("")) && fields(22).contains("6") && fields(19).contains("4")
    }).map(line => {
      val fields = line.split("\\|", -1)
      //游戏账号（5），订单号（2），订单日期（6），游戏id（7）,充值流水（10）+代金券，imei(24),alliance_id(29)
      Row(fields(5).trim.toLowerCase, fields(2), fields(6), fields(7).toInt, Commons.getNullTo0(fields(10)) + Commons.getNullTo0(fields(13)), fields(24), fields(29))
    })

    if (!orderRDD.isEmpty()) {
      val struct = new StructType()
        .add("game_account", StringType)
        .add("order_id", StringType)
        .add("order_time", StringType)
        .add("game_id", IntegerType)
        .add("pay_money", FloatType)
        .add("imei", StringType)
        .add("alliance_bag_id", StringType)
      hiveContext.createDataFrame(orderRDD, struct).registerTempTable("ods_order_cache")

      val sql = "select distinct \noz.game_account,\noz.order_id,\noz.order_time,\noz.game_id,\noz.pay_money,\noz.imei,\noz.alliance_bag_id,\nif(gs.game_id is null,0,gs.game_id) parent_game_id,\nif(terrace_name is null,'',terrace_name) terrace_name,\nif(terrace_type is null,1,terrace_type) terrace_type,\nif(gs.system_type is null,0,gs.system_type) os,\nif(head_people is null,'',head_people) head_people,\nif(terrace_auto_id is null,0,terrace_auto_id)  terrace_auto_id,\nif(alliance_company_id is null,0,alliance_company_id) alliance_company_id,\nif(alliance_company_name is null,'',alliance_company_name) alliance_company_name\nfrom ods_order_cache oz \njoin (select distinct game_id,old_game_id,system_type from game_sdk where state = 0) gs on oz.game_id=gs.old_game_id\nleft join alliance_dim ad on oz.alliance_bag_id=ad.alliance_bag_id"
      val orderDF = hiveContext.sql(sql)

      foreachOrderDF(orderDF)
    }

  }

  def foreachOrderDF(orderDF: DataFrame) = {
    orderDF.foreachPartition(iter => {
      if (!iter.isEmpty) {
        val conn: Connection = JdbcUtil.getConn()
        val stmt: Statement = conn.createStatement()
        val connXP2: Connection = JdbcUtil.getXiaopeng2Conn()
        val stmtXP2: Statement = connXP2.createStatement()

        val jedisPool = JedisUtil.getJedisPool()
        val jedis0 = jedisPool.getResource
        val jedis10 = jedisPool.getResource
        jedis10.select(10)

        //统计表
        //1:插入 付费相关指标
        val order_kpi_sql = "insert into bi_gamepublic_alliance_base_day_kpi (publish_date,parent_game_id,child_game_id,terrace_name,terrace_type,alliance_bag_id,os,head_people,terrace_auto_id,alliance_company_id,alliance_company_name,pay_money,pay_account_num,pay_device_num,new_pay_money,new_pay_account_num,new_pay_device_num) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \non duplicate key update \nterrace_name=values(terrace_name),terrace_type=values(terrace_type),os=values(os),head_people=values(head_people),terrace_auto_id=values(terrace_auto_id),alliance_company_id=values(alliance_company_id),alliance_company_name=values(alliance_company_name),pay_money=values(pay_money)+pay_money,pay_account_num=values(pay_account_num)+pay_account_num,pay_device_num=values(pay_device_num)+pay_device_num,new_pay_money=values(new_pay_money)+new_pay_money,new_pay_account_num=values(new_pay_account_num)+new_pay_account_num,new_pay_device_num=values(new_pay_device_num)+new_pay_device_num"
        val order_kpi_params = ArrayBuffer[Array[Any]]()
        val order_kpi_pstmt = conn.prepareStatement(order_kpi_sql)
        //2:插入 ltv指标
        val order_kpi_sql_ltv = "insert into bi_gamepublic_alliance_base_day_kpi (publish_date,parent_game_id,child_game_id,terrace_name,terrace_type,alliance_bag_id,os,head_people,terrace_auto_id,alliance_company_id,alliance_company_name,ltv_1day_b,ltv_3day_b,ltv_7day_b,ltv_30day_b) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \non duplicate key update \nterrace_name=values(terrace_name),terrace_type=values(terrace_type),os=values(os),head_people=values(head_people),terrace_auto_id=values(terrace_auto_id),alliance_company_id=values(alliance_company_id),alliance_company_name=values(alliance_company_name),ltv_1day_b=values(ltv_1day_b)+ltv_1day_b,ltv_3day_b=values(ltv_3day_b)+ltv_3day_b,ltv_7day_b=values(ltv_7day_b)+ltv_7day_b,ltv_30day_b=values(ltv_30day_b)+ltv_30day_b"
        val order_kpi_sql_ltv_params = ArrayBuffer[Array[Any]]()
        val order_kpi_ltv_pstmt = conn.prepareStatement(order_kpi_sql_ltv)
        iter.foreach(line => {
          val order_id = line.getAs[String]("order_id")
          //判断是否沙河订单，如果是沙河订单，过滤掉
          val isSandBoxOrderId = AllianceUtils.isSandBoxOrderId(order_id, stmtXP2)
          if (!isSandBoxOrderId) {
            val game_account = line.getAs[String]("game_account")
            val order_time = line.getAs[String]("order_time")
            val game_id = line.getAs[Int]("game_id")
            //充值金额:当天成功充值总额
            val pay_money = line.getAs[Float]("pay_money")
            val imei = line.getAs[String]("imei")
            val alliance_bag_id = line.getAs[String]("alliance_bag_id")

            val parent_game_id = line.getAs[Int]("parent_game_id")
            val terrace_name = line.getAs[String]("terrace_name")
            val terrace_type = line.getAs[Int]("terrace_type")
            val os = line.getAs[Int]("os")
            val head_people = line.getAs[String]("head_people")
            val terrace_auto_id = line.getAs[Int]("terrace_auto_id")
            val alliance_company_id = line.getAs[Int]("alliance_company_id")
            val alliance_company_name = line.getAs[String]("alliance_company_name")

            //获取帐号的注册信息
            val accountInfo = jedis0.hgetAll(game_account)
            val reg_time = if (accountInfo.get("reg_time") == null) "0000-00-00" else accountInfo.get("reg_time")
            val reg_game_id = if (accountInfo.get("game_id") == null) "0" else accountInfo.get("game_id")
            val reg_alliance_bag_id = if (accountInfo.get("alliance_bag_id") == null) "0" else accountInfo.get("alliance_bag_id")

            //充值帐号数:当天成功充值记录数，按帐号去重
            val pay_account_num = AllianceUtils.payAccountNum(game_account, order_time, game_id, alliance_bag_id, jedis10)
            //充值设备数:当天成功充值记录数，按设备去重
            val pay_device_num = AllianceUtils.payDeviceNum(imei, order_time, game_id, alliance_bag_id, jedis10)
            //新增充值帐号数:第一次注册的帐号，且付费的帐号数,按帐号去重
            val new_pay_account_num = AllianceUtils.newPayAccountNum(pay_account_num, game_account, order_time, reg_time, game_id)
            //新增充值设备数:第一次注册的设备，且成功充值的设备数，按设备去重
            val new_pay_device_num = AllianceUtils.newPayDeviceNum(pay_device_num, imei, order_time, game_id, stmt)
            //新增充值金额:新增充值账号当天充值总额
            val new_pay_money = if (reg_time.substring(0, 10) == order_time.substring(0, 10)) pay_money else 0.0

            //ltv:当天账号注册日1天后累计充值金额,并将累计的金额存入到注册那天的记录
            val ltvDay = AllianceUtils.getAllianceAccountDiffDay(order_time, reg_time)
            val ltv_1day = if (ltvDay == 0) pay_money else 0.0
            val ltv_3day = if (ltvDay <= 2) pay_money else 0.0
            val ltv_7day = if (ltvDay <= 6) pay_money else 0.0
            val ltv_30day = if (ltvDay <= 29) pay_money else 0.0


            //支付基本指标   这里的game_id取的是订单日志中的
            order_kpi_params.+=(Array(order_time.substring(0, 10), parent_game_id, game_id, terrace_name, terrace_type, alliance_bag_id, os, head_people, terrace_auto_id, alliance_company_id, alliance_company_name, pay_money, pay_account_num, pay_device_num, new_pay_money, new_pay_account_num, new_pay_device_num))
            JdbcUtil.executeUpdate(order_kpi_pstmt, order_kpi_params, conn)

            //ltv  这里的publish_date,game_id和reg_alliance_bag_id取得是注册日志中的
            if ((!reg_alliance_bag_id.equals("0") && (ltv_1day > 0.0 || ltv_3day > 0.0 || ltv_7day > 0.0 || ltv_30day > 0.0))) {
              order_kpi_sql_ltv_params.+=(Array(reg_time.substring(0, 10), parent_game_id, reg_game_id, terrace_name, terrace_type, reg_alliance_bag_id, os, head_people, terrace_auto_id, alliance_company_id, alliance_company_name, ltv_1day, ltv_3day, ltv_7day, ltv_30day))
            }
            JdbcUtil.executeUpdate(order_kpi_ltv_pstmt, order_kpi_sql_ltv_params, conn)

            //缓存当天充值帐号到redis
            AllianceUtils.cacheAllianceTodayOrderAccount(game_id, alliance_bag_id, game_account, order_time, jedis10)
            //缓存当天充值设备到redis
            AllianceUtils.cacheAllianceTodayOrderDevice(game_id, alliance_bag_id, imei, order_time, jedis10)

          }

        })
        order_kpi_ltv_pstmt.close()
        order_kpi_pstmt.close()
        stmtXP2.close()
        stmt.close()
        connXP2.close()
        conn.close()
        jedisPool.returnResource(jedis10)
        jedisPool.returnResource(jedis0)
        jedisPool.destroy()
      }
    })

  }

}
