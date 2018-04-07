package com.xiaopeng.test

import java.io.{BufferedWriter, File, FileWriter}
import java.text.DecimalFormat

import com.xiaopeng.bi.udf.ChannelUDF
import com.xiaopeng.bi.utils._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ltv: 用户价值分析，注册和支付
  */
object GamePublishLtvV24v2 {

  def main(args: Array[String]): Unit = {
    val startTime = args(0)
    val endTime = args(1)
    val orderEndTime = args(2)
    // 创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.sql.shuffle.partitions", "60")
      .set("spark.streaming.stopGracefullyOnShutdown", "true");
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    hiveContext.udf.register("getchannel", new ChannelUDF(), DataTypes.StringType)
    hiveContext.sql("use yyft")

    //一.过滤出符合条件的注册帐号: 1,关联深度联运 2,时间范围当前日往前180天
    val filterRegiSql ="select\ndistinct \nto_date(reg.reg_time) reg_time,\nreg.game_id game_id ,lower(trim(reg.game_account)) game_account\nfrom \n(select  distinct lower(trim(game_account)) game_account, reg_time,  game_id from ods_regi_rz where game_id in(4363,4315,4313,4303,4297,4296,4277,4189,4187,4129,4079,4078,3973,3832,3831,3828,3819,3789,3788,3786,3778,3777,3705,3704,3703,3702,3624,3623,3555,3543,3519,3510,3237,3236,3235,3233)  ) \nreg join (select distinct game_id from ods_publish_game) pg on reg.game_id = pg.game_id where to_date(reg.reg_time)<='endTime' and to_date (reg.reg_time) >= 'startTime'"
      .replace("startTime", startTime).replace("endTime", endTime)
    val filterRegiDF = hiveContext.sql(filterRegiSql)
    filterRegiDF.cache()
    filterRegiDF.registerTempTable("filter_regi")

    //二.获取注册人数
    val accountCountSql ="select reg.reg_time,reg.game_id,count(distinct reg.game_account) account_count from filter_regi reg group by reg.reg_time,reg.game_id"
    hiveContext.sql(accountCountSql).registerTempTable("account_count")

    //三.计算每个时间段内  充值信息    （循环）
    val ltvDayArray = Array[Int](30, 60, 180)
    for (ltvDay <- ltvDayArray) {
      val ltvDaySql = "select reg.reg_time,\nreg.game_id,\nsum(if(o.order_status = 4,o.ori_price_all,-o.ori_price_all)) amount\nfrom \nfilter_regi reg \njoin \n(select distinct order_no,order_time,lower(trim(game_account)) game_account,game_id,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as ori_price_all,order_status from ods_order where order_status in(4,8)) o on reg.game_account = o.game_account and reg.game_id=o.game_id and to_date(o.order_time)>=reg.reg_time and to_date(o.order_time)<date_add(reg.reg_time,'"+ltvDay+"') and to_date(o.order_time)<'orderEndTime' group by reg.reg_time,reg.game_id"
        .replace("orderEndTime", orderEndTime)
      hiveContext.sql(ltvDaySql).registerTempTable("ltv" + ltvDay + "_day")
    }

    //四.关联 注册 和 充值信息  和 一些基本维度信息
    val resultSql ="\nselect account.reg_time as publish_date, \nif(game_sdk_cach.game_child_name is null,'',game_sdk_cach.game_child_name) game_child_name, \naccount.account_count as acc_add_regi, \nif(ltv30day.amount is null,0, ltv30day.amount/account.account_count) ltv30, \nif(ltv30day.amount is null,0,ltv30day.amount) ltv30_amount, \nif(ltv60day.amount is null,0, ltv60day.amount/account.account_count) ltv60, \nif(ltv60day.amount is null,0,ltv60day.amount) ltv60_amount, \nif(ltv180day.amount is null,0,ltv180day.amount/account.account_count) ltv180, \nif(ltv180day.amount is null,0,ltv180day.amount) ltv180_amount \nfrom \naccount_count account \nleft join  (select  distinct game_child_name, \nold_game_id as child_game_id from game_sdk where game_id is not null) game_sdk_cach on account.game_id=game_sdk_cach.child_game_id \nleft join ltv30_day ltv30day on account.reg_time=ltv30day.reg_time and account.game_id=ltv30day.game_id \nleft join ltv60_day ltv60day on ltv60day.reg_time=account.reg_time and ltv60day.game_id=account.game_id\nleft join ltv180_day ltv180day on ltv180day.reg_time=account.reg_time and ltv180day.game_id=account.game_id"
    val resultDf = hiveContext.sql(resultSql)

    //五.把结果存入mysql
    val header = "日期,游戏名,父渠道,子渠道,广告标签,注册账号数,三十日LTV,三十日流水,六十日LTV,六十日流水,至今LTV,至今流水"
    FileUtil.apppendTofile("/home/hduser/crontabFiles/ltv/ltv2_" + endTime + ".csv", header)
    resultDf.foreachPartition(rows => {
      val file = new File("/home/hduser/crontabFiles/ltv/ltv2_" + endTime + ".csv")
      val writer = new BufferedWriter(new FileWriter(file, true))
      val  df: DecimalFormat = new DecimalFormat("#.00");
      for (insertedRow <- rows) {
          writer.append(insertedRow.get(0) + "," + insertedRow.get(1) + "," +insertedRow.get(2)+ "," + insertedRow.get(3) + "," +df.format(insertedRow.get(4)) + "," + insertedRow.get(5) + "," + df.format(insertedRow.get(6)) + "," + insertedRow.get(7) + "," + df.format(insertedRow.get(8)))
          writer.newLine()
      }
      writer.flush()
      writer.close()
    })
    sc.stop()
  }
}
