package com.xiaopeng.test

import com.xiaopeng.bi.utils.{ConfigurationUtil, FileUtil, SparkUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ltv: 用户价值分析，注册和支付
  */
object GamePublishLtvV23 {

  def main(args: Array[String]): Unit = {
    val startTime = args(0)
    val endTime = args(1)
    val orderStartTime = args(2)
    val orderEndTime = args(3)
    // 创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.sql.shuffle.partitions", ConfigurationUtil.getProperty("spark.sql.shuffle.partitions"))
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use yyft")
    //一.过滤出符合条件的注册帐号: 1,关联深度联运 2,时间范围当前日往前180天
    val filterRegiSql ="select distinct  \nto_date(reg.reg_time) reg_time, \ngame_sdk.parent_game_id parent_game_id, \nreg.game_id game_id, \nreg.game_account game_account, \nreg.imei imei \nfrom  \n(select lower(trim(game_account)) as game_account, min(reg_time) as reg_time,game_id,max(imei) as imei from ods_regi_rz where to_date(reg_time)>='startTime' and to_date (reg_time) <= 'endTime'  group by lower(trim(game_account)),game_id) \nreg  \njoin (select distinct game_id from ods_publish_game) pg on reg.game_id = pg.game_id  join (select  distinct game_id as parent_game_id , old_game_id as child_game_id from game_sdk) game_sdk on reg.game_id=game_sdk.child_game_id --补全 main_id"
      .replace("startTime", startTime).replace("endTime", endTime)
    val filterRegiDF = hiveContext.sql(filterRegiSql)
    filterRegiDF.cache()
    filterRegiDF.registerTempTable("filter_regi")

    // 二.关联 注册 和 充值信息  和 一些基本维度信息
    val resultSql ="select  \nreg.parent_game_id, \nreg.game_id, \nreg.game_account, \nreg.reg_time, reg.imei, \nsum(if(o.order_status = 4,if(o.ori_price_all is null,0, o.ori_price_all),-if(o.ori_price_all is null,0, o.ori_price_all))) amount \nfrom  filter_regi reg  \nleft join  \n(select distinct order_time,lower(trim(game_account)) game_account,game_id,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as ori_price_all,order_status from ods_order where order_status in(4,8)  and prod_type=6  and game_account is not null) o \non reg.game_account = o.game_account  and  reg.game_id=o.game_id and to_date(o.order_time)>='orderStartTime' and to_date(o.order_time)<='orderEndTime'   group by reg.reg_time,reg.parent_game_id,reg.game_id,reg.game_account,reg.imei"
      .replace("orderStartTime", orderStartTime).replace("orderEndTime", orderEndTime)
    val resultDf = hiveContext.sql(resultSql)

    //将数据存入文件
    val header = "主游戏id,子游戏id,账号,注册时间,设备号,总充值金额"
    FileUtil.apppendTofile("/home/hduser/crontabFiles/ltv/amount_"+endTime+".txt", header)

    resultDf.foreachPartition(rows => {
      for (insertedRow <- rows) {
        FileUtil.apppendTofile("/home/hduser/crontabFiles/ltv/amount_"+endTime+".txt", insertedRow.get(0) + "," + insertedRow.get(1) + "," + insertedRow.get(2) + "," + insertedRow.get(3) + "," + insertedRow.get(4)+ "," + insertedRow.get(5))
      }
    })
    sc.stop()
  }
}
