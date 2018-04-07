package com.xiaopeng.test

import com.xiaopeng.bi.utils.{FileUtil, SparkUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ltv: 用户价值分析，注册和支付
  */
object GamePublishCpUid {

  def main(args: Array[String]): Unit = {
    // 创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    val rdd: RDD[String] = sc.textFile("/cpuid.txt")
    val df = rdd.filter(t => {
      t.contains("__")
    }).map(t => {
      Row(t.split("__")(1))
    })

    val regiStruct = (new StructType).add("uid_cp", StringType);
    val regiDF = hiveContext.createDataFrame(df, regiStruct);
    regiDF.cache().registerTempTable("cp_uid")

    hiveContext.sql("use yyft")
    hiveContext.sql("select distinct accounts.cp_uid cp_uid,accounts.account account from (select lower(trim(account)) account,cp_uid from bgameaccount) accounts join (select distinct uid_cp from  cp_uid)cp_uid on  accounts.cp_uid= cp_uid.uid_cp").cache().registerTempTable("fifteraccount")
    hiveContext.sql("select ods_regi.game_account game_account,cp_account.cp_uid cp_uid,ods_regi.reg_time reg_time, ods_regi.expand_channel expand_channel from\n(select  lower(trim(game_account)) game_account, min(reg_time) reg_time, max(expand_channel) expand_channel from ods_regi_rz where game_account is not null group by lower(trim(game_account)))  \nods_regi \njoin (select distinct cp_uid,account from fifteraccount) cp_account on cp_account.account=ods_regi.game_account ").cache().registerTempTable("ods_regi")
    hiveContext.sql("select ods_order.order_no order_no,ods_order.order_time order_time,ods_order.game_account game_account,ods_order.game_id game_id,ods_order.ori_price_all ori_price_all,ods_order.payment_type payment_type,ods_order.order_status order_status from\n(select distinct order_no,order_time, lower(trim(game_account)) game_account,game_id,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as ori_price_all,payment_type,order_status from ods_order where order_status=4 and prod_type=6 ) ods_order\njoin (select distinct account from fifteraccount) cp_account on cp_account.account=ods_order.game_account ").cache().registerTempTable("ods_order")
    hiveContext.sql("select distinct \nlogin.game_account game_account,\nlogin.login_time login_time,\nlogin.channel_expand channel_expand,\nlogin.ip ip \nfrom \n( select  distinct lower(trim(game_account)) game_account,login_time,channel_expand,ip from yyft.ods_login union all select  distinct lower(trim(game_account)) game_account,login_time,channel_expand,ip from archive.ods_login_day ) login \njoin (select distinct account from fifteraccount) cp_account on cp_account.account=login.game_account ").cache().registerTempTable("ods_login_rz")
    val resultSql ="select\nods_regi.cp_uid,--cpuid   \nods_regi.game_account,--账户 \nods_regi.reg_time,--注册时间 \nif(ods_order_rz.amount is null,0,ods_order_rz.amount),--累计充值金额 \nif(login3.channel_expand is null,'',login3.channel_expand),--第一次登录 渠道 \nif(login4.ip is null,'0.0.0.0',login4.ip),-- 最后一次登录ip\nif(login4.login_time is null,'0000-00-00 00:00:00',login4.login_time)--最后一次登录时间\nfrom  \n(select distinct cp_uid,game_account,reg_time,expand_channel from ods_regi)  \nods_regi \nleft join (select  game_account,sum(pay.ori_price_all) amount from(select distinct order_no,order_time,game_account,game_id,ori_price_all,payment_type,order_status from ods_order) pay group by pay.game_account) ods_order_rz on ods_regi.game_account=ods_order_rz.game_account \nleft join (select distinct login1.game_account,login1.channel_expand from (select  distinct lower(trim(game_account)) game_account,login_time,channel_expand from  ods_login_rz) login1 join (select  game_account,min(login_time) login_time from ods_login_rz group by  game_account) login2 on  login1.game_account=login2.game_account and login1.login_time=login2.login_time) login3 on ods_regi.game_account=login3.game_account \nleft join (select distinct login1.game_account,login1.ip,login1.login_time from (select  distinct lower(trim(game_account)) game_account,login_time,ip from  ods_login_rz) login1 join (select  game_account,max(login_time) login_time from ods_login_rz group by  game_account) login2 on  login1.game_account=login2.game_account and login1.login_time=login2.login_time) login4 on ods_regi.game_account=login4.game_account"
    val resultDf = hiveContext.sql(resultSql)

    val header = "cp_uid,账户,注册时间,累计充值金额,第一次登录渠道,最后一次登录时间,最后一次登录ip"
    FileUtil.apppendTofile("/home/hduser/crontabFiles/ltv/cp_uid4.csv", header)
    resultDf.foreachPartition(rows => {
      for (insertedRow <- rows) {
        FileUtil.apppendTofile("/home/hduser/crontabFiles/ltv/cp_uid4.csv", insertedRow.get(0) + "," + insertedRow.get(1) + "," + insertedRow.get(2) + "," + insertedRow.get(3) + "," + insertedRow.get(4) + "," + insertedRow.get(5)+ "," + insertedRow.get(6))
      }
    })
    sc.stop()
  }
}
