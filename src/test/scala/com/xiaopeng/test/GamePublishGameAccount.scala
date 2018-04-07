package com.xiaopeng.bi.gamepublish

import com.xiaopeng.bi.utils.{ConfigurationUtil, FileUtil, JdbcUtil, SparkUtils}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ltv: 用户价值分析，注册和支付
  */
object GamePublishGameAccount {

  def main(args: Array[String]): Unit = {
    val yesterday = args(0)
    val header = "主游戏id" + "\t" + "主游戏名" + "\t" + "子游戏ID" + "\t" + "子游戏名" + "\t" + "游戏账号" + "\t" + "注册时间" + "\t" + "首充时间" + "\t" + "累计充值金额" + "\t" + "累计充值天数" + "\t" + "最近充值" + "\t" + "最近一天充值金额" + "\t" + "最近登陆时间"
    FileUtil.apppendTofile("/home/hduser/crontabFiles/gameaccount/game29_detail_" + yesterday + ".txt", header)
    FileUtil.apppendTofile("/home/hduser/crontabFiles/gameaccount/game45_detail_" + yesterday + ".txt", header)

    //    val games=args(1)
    // 创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.sql.shuffle.partitions", ConfigurationUtil.getProperty("spark.sql.shuffle.partitions"))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.streaming.stopGracefullyOnShutdown", "true");

    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use yyft")

    val game_id_set: String = getGameId()
    val resultSql = "select  gb.id parent_game_id,gb.game_name parent_game_name,gs.old_game_id game_child_id,gs.game_child_name game_child_name,rz.game_account,rz.reg_time,\nif(oz.min_order_time is null,'',oz.min_order_time) min_order_time,\nif(oz.or_price_all  is null,'',oz.or_price_all) or_price_all,\nif(oz.count_pay_day  is null,0,oz.count_pay_day) count_pay_day,\nif(oz.max_order_time  is null,'',oz.max_order_time) max_order_time,\nif(oz.or_price_lastday  is null,0,oz.or_price_lastday) or_price_lastday,\nif(lz.max_login_time  is null,'',lz.max_login_time) max_login_time\nfrom \n(select  distinct game_account, game_id,reg_time from ods_regi_rz where game_account is not null and to_date(reg_time)<='yesterday' and game_id in (game_id_set)) rz \njoin  (select  distinct game_id as parent_game_id , old_game_id,game_child_name from game_sdk  where state=0) gs on rz.game_id=gs.old_game_id\njoin (select distinct id,game_name  from game_base where id in (29,45)) gb on gb.id=gs.parent_game_id\njoin  \n(\nselect oz.game_account game_account,max(oz.order_time) max_order_time ,min(oz.order_time) min_order_time,count(distinct to_date(oz.order_time)) count_pay_day,sum(oz.or_price_all) or_price_all,sum(if(to_date(oz.order_time)=to_date(oz2.max_order_time),oz.or_price_all,0)) or_price_lastday from\n(select distinct order_no,order_time,game_account,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as or_price_all from ods_order where order_status=4 and prod_type=6 and to_date(order_time)<='yesterday' and game_id in (game_id_set)) oz \njoin(select game_account,max(order_time) max_order_time from  ods_order where order_status=4 and prod_type=6 and to_date(order_time)<='yesterday' and game_id in (game_id_set) group by game_account) oz2 on oz.game_account=oz2.game_account group by oz.game_account) oz on oz.game_account=rz.game_account\nleft  join\n(\nselect lz.game_account game_account,max(lz.login_time) max_login_time from\n(select  distinct game_account, login_time  from ods_login where game_id in (game_id_set) union all select  distinct  game_account, login_time  from archive.ods_login_archive where game_id in (game_id_set)) lz \nwhere to_date(lz.login_time) <='yesterday'  group by lz.game_account\n)lz on  lz.game_account=rz.game_account \norder by gb.id,gs.old_game_id,rz.game_account"
      .replace("yesterday", yesterday).replace("game_id_set", game_id_set)

    val resultDf = hiveContext.sql(resultSql)
    resultDf.saveAsTable("tmp.game_acc_detail");

    sc.stop()
  }

  //获取媒介包中哪些游戏有做广告推广
  def getGameId(): String = {
    var game_id_set = "no_game"
    val conn = JdbcUtil.getXiaopeng2FXConn()
    val stmt = conn.createStatement();
    val sql = "select  group_concat(gs.old_game_id)  game_id_set from \n(select  distinct game_id as parent_game_id , old_game_id,game_child_name from game_sdk  where state=0) gs \njoin (select distinct id,game_name  from game_base where id in (29,45)) gb on gb.id=gs.parent_game_id"

    val resultSet = stmt.executeQuery(sql)
    while (resultSet.next()) {
      game_id_set = resultSet.getString("game_id_set")
    }

    stmt.close()
    conn.close()
    return game_id_set
  }


}
