package com.xiaopeng.test

import java.io.{BufferedWriter, File, FileWriter}

import com.xiaopeng.bi.udf.ChannelUDF
import com.xiaopeng.bi.utils._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ltv: 用户价值分析，注册和支付
  */
object GamePublishLtvV22 {

  def main(args: Array[String]): Unit = {
    val currentday = args(0)
    // 创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.sql.shuffle.partitions", ConfigurationUtil.getProperty("spark.sql.shuffle.partitions"))
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    hiveContext.udf.register("getchannel", new ChannelUDF(), DataTypes.StringType)
    hiveContext.sql("use yyft")
    //一.过滤出符合条件的注册帐号: 1,关联深度联运 2,时间范围当前日往前180天
    val filterRegiSql = "select\ndistinct \nto_date(reg.reg_time) reg_time,\nreg.game_id game_id,\nif(reg.expand_channel = '' or reg.expand_channel is null,'21',reg.expand_channel) expand_channel,\nlower(trim(reg.game_account)) game_account\nfrom \n(select  lower(trim(game_account)) game_account, min(reg_time) reg_time,game_id,max(expand_channel) expand_channel from ods_regi_rz where game_id in(4163,4151,4158)  group by lower(trim(game_account)),game_id) \nreg join (select distinct game_id from ods_publish_game) pg on reg.game_id = pg.game_id where to_date(reg.reg_time)<='currentday' and to_date (reg.reg_time) >= date_add('currentday',-180)"
      .replace("currentday", currentday)
    val filterRegiDF = hiveContext.sql(filterRegiSql)
    filterRegiDF.cache()
    filterRegiDF.registerTempTable("filter_regi")

    //二.获取注册人数
    val accountCountSql = "select reg.reg_time,reg.game_id,reg.expand_channel,count(distinct reg.game_account) account_count from filter_regi reg group by reg.reg_time,reg.game_id,reg.expand_channel"
    hiveContext.sql(accountCountSql).registerTempTable("account_count")

    //三.计算每个时间段内  充值信息    （循环）
    val ltvDayArray = Array[Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 30, 45, 60, 90, 180)
    for (ltvDay <- ltvDayArray) {
      val ltvDaySql = "select reg.reg_time,\nreg.game_id,\nreg.expand_channel,\nsum(if(o.order_status = 4,o.ori_price_all,-o.ori_price_all)) amount\nfrom \nfilter_regi reg \njoin \n(select distinct order_time,lower(trim(game_account)) game_account,game_id,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as ori_price_all,order_status from ods_order where order_status in(4,8)) o on reg.game_account = o.game_account and reg.game_id=o.game_id and to_date(o.order_time)>=reg.reg_time and to_date(o.order_time)<date_add(reg.reg_time,'" + ltvDay + "') group by reg.reg_time,reg.game_id,reg.expand_channel"
      hiveContext.sql(ltvDaySql).registerTempTable("ltv" + ltvDay + "_day")
    }

    //四.关联 注册 和 充值信息  和 一些基本维度信息
    val resultSql = "select\naccount.reg_time as publish_date,\nif(game_sdk_cache.parent_game_id is null,0,game_sdk_cache.parent_game_id) parent_game_id,\naccount.game_id,\naccount.expand_channel,\nif(game_sdk_cache.os is null,0,game_sdk_cache.os) os,\nif(game_sdk_cache.group_id is null,0,game_sdk_cache.group_id) group_id,\naccount.account_count as acc_add_regi,\nif(ltv1day.amount is null,0,ltv1day.amount*100) ltv1_amount,\nif(ltv2day.amount is null,0,ltv2day.amount*100) ltv2_amount,\nif(ltv3day.amount is null,0,ltv3day.amount*100) ltv3_amount,\nif(ltv4day.amount is null,0,ltv4day.amount*100) ltv4_amount,\nif(ltv5day.amount is null,0,ltv5day.amount*100) ltv5_amount,\nif(ltv6day.amount is null,0,ltv6day.amount*100) ltv6_amount,\nif(ltv7day.amount is null,0,ltv7day.amount*100) ltv7_amount,\nif(ltv8day.amount is null,0,ltv8day.amount*100) ltv8_amount,\nif(ltv9day.amount is null,0,ltv9day.amount*100) ltv9_amount,\nif(ltv10day.amount is null,0,ltv10day.amount*100) ltv10_amount,\nif(ltv11day.amount is null,0,ltv11day.amount*100) ltv11_amount,\nif(ltv12day.amount is null,0,ltv12day.amount*100) ltv12_amount,\nif(ltv13day.amount is null,0,ltv13day.amount*100) ltv13_amount,\nif(ltv14day.amount is null,0,ltv14day.amount*100) ltv14_amount,\nif(ltv15day.amount is null,0,ltv15day.amount*100) ltv15_amount,\nif(ltv30day.amount is null,0,ltv30day.amount*100) ltv30_amount,\nif(ltv45day.amount is null,0,ltv45day.amount*100) ltv45_amount, \nif(ltv60day.amount is null,0,ltv60day.amount*100) ltv60_amount,\nif(ltv90day.amount is null,0,ltv90day.amount*100) ltv90_amount, \nif(ltv180day.amount is null,0,ltv180day.amount*100) ltv180_amount\nfrom account_count account\nleft join  (select  distinct game_id as parent_game_id , old_game_id as child_game_id,system_type as os,group_id from game_sdk where game_id is not null) game_sdk_cache on account.game_id=game_sdk_cache.child_game_id --补全 parent_game_id,os,group_id\nleft join ltv1_day ltv1day on ltv1day.reg_time=account.reg_time and ltv1day.game_id=account.game_id and account.expand_channel=ltv1day.expand_channel\nleft join ltv2_day ltv2day on ltv2day.reg_time=account.reg_time and ltv2day.game_id=account.game_id and account.expand_channel=ltv2day.expand_channel\nleft join ltv3_day ltv3day on account.reg_time=ltv3day.reg_time and account.game_id=ltv3day.game_id and account.expand_channel=ltv3day.expand_channel \nleft join ltv4_day ltv4day on ltv4day.reg_time=account.reg_time and ltv4day.game_id=account.game_id and account.expand_channel=ltv4day.expand_channel \nleft join ltv5_day ltv5day on account.reg_time=ltv5day.reg_time and account.game_id=ltv5day.game_id and account.expand_channel=ltv5day.expand_channel \nleft join ltv6_day ltv6day on ltv6day.reg_time=account.reg_time and ltv6day.game_id=account.game_id and account.expand_channel=ltv6day.expand_channel\nleft join ltv7_day ltv7day on account.reg_time=ltv7day.reg_time and account.game_id=ltv7day.game_id and account.expand_channel=ltv7day.expand_channel\nleft join ltv8_day ltv8day on ltv8day.reg_time=account.reg_time and ltv8day.game_id=account.game_id and account.expand_channel=ltv8day.expand_channel\nleft join ltv9_day ltv9day on ltv9day.reg_time=account.reg_time and ltv9day.game_id=account.game_id and account.expand_channel=ltv9day.expand_channel\nleft join ltv10_day ltv10day on account.reg_time=ltv10day.reg_time and account.game_id=ltv10day.game_id and account.expand_channel=ltv10day.expand_channel \nleft join ltv11_day ltv11day on ltv11day.reg_time=account.reg_time and ltv11day.game_id=account.game_id and account.expand_channel=ltv11day.expand_channel \nleft join ltv12_day ltv12day on account.reg_time=ltv12day.reg_time and account.game_id=ltv12day.game_id and account.expand_channel=ltv12day.expand_channel \nleft join ltv13_day ltv13day on ltv13day.reg_time=account.reg_time and ltv13day.game_id=account.game_id and account.expand_channel=ltv13day.expand_channel\nleft join ltv14_day ltv14day on account.reg_time=ltv14day.reg_time and account.game_id=ltv14day.game_id and account.expand_channel=ltv14day.expand_channel \nleft join ltv15_day ltv15day on ltv15day.reg_time=account.reg_time and ltv15day.game_id=account.game_id and account.expand_channel=ltv15day.expand_channel\nleft join ltv30_day ltv30day on account.reg_time=ltv30day.reg_time and account.game_id=ltv30day.game_id and account.expand_channel=ltv30day.expand_channel\nleft join ltv45_day ltv45day on account.reg_time=ltv45day.reg_time and account.game_id=ltv45day.game_id and account.expand_channel=ltv45day.expand_channel  \nleft join ltv60_day ltv60day on ltv60day.reg_time=account.reg_time and ltv60day.game_id=account.game_id and account.expand_channel=ltv60day.expand_channel\nleft join ltv90_day ltv90day on ltv90day.reg_time=account.reg_time and ltv90day.game_id=account.game_id and account.expand_channel=ltv90day.expand_channel\nleft join ltv180_day ltv180day on ltv180day.reg_time=account.reg_time and ltv180day.game_id=account.game_id and account.expand_channel=ltv180day.expand_channel"
    val resultDf = hiveContext.sql(resultSql)

    //五.把结果存入mysql
    val header = "publish_date, parent_game_id,game_id,medium_channel,ad_site_channel,pkg_code,os,group_id, acc_add_regi,ltv1_amount,ltv2_amount,ltv3_amount,ltv4_amount,ltv5_amount,ltv6_amount,ltv7_amount,ltv8_amount,ltv9_amount,ltv10_amount,ltv11_amount,ltv12_amount,ltv13_amount,ltv14_amount,ltv15_amount,ltv30_amount,ltv45_amount, ltv60_amount,ltv90_amount,ltv180_amount"
    FileUtil.apppendTofile("/home/hduser/crontabFiles/ltv/ltv_"+currentday+".csv", header)
    resultDf.foreachPartition(rows => {
      val file = new File("/home/hduser/crontabFiles/ltv/ltv_"+currentday+".csv")
      val writer = new BufferedWriter(new FileWriter(file, true))
      for (insertedRow <- rows) {
        val channelArray = StringUtils.getArrayChannel(insertedRow.get(3).toString)
        if (channelArray(0).length <= 10 && channelArray(1).length <= 10 && channelArray(2).length <= 12) {
          writer.append(insertedRow.get(0) + "," +insertedRow.get(1) + "," + insertedRow.get(2) + "," + channelArray(0) + "," + channelArray(1) + "," + channelArray(2) + "," + insertedRow.get(4) + "," + insertedRow.get(5) + "," + insertedRow.get(6) + "," + insertedRow.get(7) + "," + insertedRow.get(8) + "," + insertedRow.get(9) + "," + insertedRow.get(10) + "," + insertedRow.get(11) + "," + insertedRow.get(12) + "," + insertedRow.get(13) + "," + insertedRow.get(14) + "," + insertedRow.get(15) + "," + insertedRow.get(16) + "," + insertedRow.get(17) + "," + insertedRow.get(18) + "," + insertedRow.get(19) + "," + insertedRow.get(20) + "," + insertedRow.get(21) + "," + insertedRow.get(22) + "," + insertedRow.get(23) + "," + insertedRow.get(24) + "," + insertedRow.get(25) + "," + insertedRow.get(26))
          writer.newLine()
        }
      }
      writer.flush()
      writer.close()
    })
    sc.stop()
  }
}
