package com.xiaopeng.test

import com.xiaopeng.bi.utils.{ConfigurationUtil, FileUtil, SparkUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ltv: 用户价值分析，注册和支付
  */
object GamePublish2 {

  def main(args: Array[String]): Unit = {
    // 创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.sql.shuffle.partitions", ConfigurationUtil.getProperty("spark.sql.shuffle.partitions"))
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use yyft")

    val resultSql = "select\nreg.reg_time,\ncount(distinct ods_order.game_account) order_count_account,\ncount(case when datediff(ods_login.login_time,reg.reg_time)=0  then ods_login.game_account else null end) as ltv0_amount,\ncount(case when datediff(ods_login.login_time,reg.reg_time)=1  then ods_login.game_account else null end) as ltv1_amount,\ncount(case when datediff(ods_login.login_time,reg.reg_time)=3  then ods_login.game_account else null end) as ltv2_amount,\ncount(case when datediff(ods_login.login_time,reg.reg_time)=7  then ods_login.game_account else null end) as ltv3_amount,\ncount(case when datediff(ods_login.login_time,reg.reg_time)=15  then ods_login.game_account else null end) as ltv4_amount,\ncount(case when datediff(ods_login.login_time,reg.reg_time)=30  then ods_login.game_account else null end) as ltv5_amount\nfrom  \n(select lower(trim(game_account)) as game_account, to_date(max(reg_time)) reg_time from ods_regi_rz  where game_id in(3232,3234,3428,3625,3744,3745,3746,3747,3809,3893) group by lower(trim(game_account))) \nreg   -- 注册信息\nleft join  (select distinct  lower(trim(game_account)) game_account from ods_order where order_status in(4,8) and prod_type=6) ods_order on reg.game_account = ods_order.game_account  -- 支付信息\nleft join  (select  distinct lower(trim(game_account)) game_account,to_date(login_time) login_time from ods_login\nunion all\nselect  distinct lower(trim(game_account)) game_account,date(login_time) login_time from archive.ods_login_day\n) ods_login on ods_login.game_account=reg.game_account --登录信息\ngroup by reg.reg_time"
    val resultDf = hiveContext.sql(resultSql)

    FileUtil.apppendTofile("/home/hduser/projs/logs/android.csv", "日期,付费人数,0日登录数量,次日登录数量,3日登录数量,7日登录数量,15日登录数量,30日登录数量")
    FileUtil.apppendTofile("/home/hduser/projs/logs/ios.csv", "日期,付费人数,0日登录数量,次日登录数量,3日登录数量,7日登录数量,15日登录数量,30日登录数量")
    resultDf.foreachPartition(rows => {
      for (insertedRow <- rows) {
        FileUtil.apppendTofile("/home/hduser/projs/logs/android.csv", insertedRow.get(0) + "," + insertedRow.get(1) + "," + insertedRow.get(2) + "," + insertedRow.get(3) + "," + insertedRow.get(4) + "," + insertedRow.get(5) + "," + insertedRow.get(6) + "," + insertedRow.get(7))
      }
    })

    val resultSql2 = "select\nreg.reg_time,\ncount(distinct ods_order.game_account) order_count_account,\ncount(case when datediff(ods_login.login_time,reg.reg_time)=0  then ods_login.game_account else null end) as ltv0_amount,\ncount(case when datediff(ods_login.login_time,reg.reg_time)=1  then ods_login.game_account else null end) as ltv1_amount,\ncount(case when datediff(ods_login.login_time,reg.reg_time)=3  then ods_login.game_account else null end) as ltv2_amount,\ncount(case when datediff(ods_login.login_time,reg.reg_time)=7  then ods_login.game_account else null end) as ltv3_amount,\ncount(case when datediff(ods_login.login_time,reg.reg_time)=15  then ods_login.game_account else null end) as ltv4_amount,\ncount(case when datediff(ods_login.login_time,reg.reg_time)=30  then ods_login.game_account else null end) as ltv5_amount\nfrom  \n(select lower(trim(game_account)) as game_account, to_date(max(reg_time)) reg_time from ods_regi_rz where game_id in(3233,3235,3236,3510,3520,3519,3543,3555,3624,3702,3778,3973,4129,4187,4296) group by lower(trim(game_account))) \nreg   -- 注册信息\nleft join  (select distinct  lower(trim(game_account)) game_account from ods_order where order_status in(4,8) and prod_type=6) ods_order on reg.game_account = ods_order.game_account  -- 支付信息\nleft join  (select  distinct lower(trim(game_account)) game_account,date(login_time) login_time from ods_login\nunion all\nselect  distinct lower(trim(game_account)) game_account,date(login_time) login_time from archive.ods_login_day\n) ods_login on ods_login.game_account=reg.game_account --登录信息\ngroup by reg.reg_time"
    val resultDf2 = hiveContext.sql(resultSql2)

    resultDf2.foreachPartition(rows => {
      for (insertedRow <- rows) {
        FileUtil.apppendTofile("/home/hduser/projs/logs/ios.csv", insertedRow.get(0) + "," + insertedRow.get(1) + "," + insertedRow.get(2) + "," + insertedRow.get(3) + "," + insertedRow.get(4) + "," + insertedRow.get(5) + "," + insertedRow.get(6) + "," + insertedRow.get(7))
      }
    })
    sc.stop()
  }
}
