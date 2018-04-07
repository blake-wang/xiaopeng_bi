package com.xiaopeng.bi.gamepublish

import com.xiaopeng.bi.utils.{JdbcUtil, SparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer

/**
  * Created by kequan on 11/20/17.
  *
  */
object NewPubBackKpiRegi {
  var startDay = "";
  var endDay = "";

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    if (args.length == 2) {
      startDay = args(0)
      endDay = args(1)
    } else if (args.length == 1) {
      startDay = args(0)
      endDay = startDay
    }
    // 创建上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.driver.allowMultipleContexts", "true") //.setMaster("local[*]")
    SparkUtils.setMaster(sparkConf)
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("use yyft")

    //DAU(登录账户) 注册来源
    val sql_dau ="select\nlz.login_time publish_date, \ngs.game_id parent_game_id,\nlz.game_id child_game_id, \ngs.system_type os,\ngs.group_id group_id,\ncount(distinct lz.game_account)  dau_accounts,\ncount(distinct (case when datediff(lz.login_time,rz.reg_time)=0  then lz.game_account else null end)) as dau_regi_1, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)=1  then lz.game_account else null end)) as dau_regi_2, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)=2  then lz.game_account else null end)) as dau_regi_3, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)=3  then lz.game_account else null end)) as dau_regi_4, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)=4  then lz.game_account else null end)) as dau_regi_5, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)=5  then lz.game_account else null end)) as dau_regi_6, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)=6  then lz.game_account else null end)) as dau_regi_7, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)=7  then lz.game_account else null end)) as dau_regi_8, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)=8  then lz.game_account else null end)) as dau_regi_9, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)=9  then lz.game_account else null end)) as dau_regi_10, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)=10  then lz.game_account else null end)) as dau_regi_11, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)=11  then lz.game_account else null end)) as dau_regi_12, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)=12  then lz.game_account else null end)) as dau_regi_13, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)=13  then lz.game_account else null end)) as dau_regi_14, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)=14  then lz.game_account else null end)) as dau_regi_15, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)<=29  and  datediff(to_date(lz.login_time),rz.reg_time)>=15  then lz.game_account else null end)) as dau_regi_16_30, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)<=44  and  datediff(to_date(lz.login_time),rz.reg_time)>=30  then lz.game_account else null end)) as dau_regi_31_45, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)<=59  and  datediff(to_date(lz.login_time),rz.reg_time)>=45  then lz.game_account else null end)) as dau_regi_46_60, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)<=89  and  datediff(to_date(lz.login_time),rz.reg_time)>=60  then lz.game_account else null end)) as dau_regi_61_90, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)<=119  and  datediff(to_date(lz.login_time),rz.reg_time)>=90  then lz.game_account else null end)) as dau_regi_91_120, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)<=149  and  datediff(to_date(lz.login_time),rz.reg_time)>=120  then lz.game_account else null end)) as dau_regi_121_150, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)<=179  and  datediff(to_date(lz.login_time),rz.reg_time)>=150  then lz.game_account else null end)) as dau_regi_151_180, \ncount(distinct (case when datediff(lz.login_time,rz.reg_time)>=180  then lz.game_account else null end)) as dau_regi_181, \ncount(distinct (case when rz.reg_time is null  then lz.game_account else null end)) as dau_regi_lost\nfrom \n(select  distinct game_id ,to_date(login_time) login_time,lower(trim(game_account)) game_account from ods_login where  to_date(login_time)>='startDay' and  to_date(login_time)<='endDay'  and game_account is not null and game_account!='') lz\njoin  (select  distinct game_id , old_game_id ,system_type,group_id from game_sdk ) gs on lz.game_id=gs.old_game_id\nleft join (select distinct lower(trim(game_account)) game_account,to_date(reg_time) reg_time from ods_regi_rz where game_id is not null) rz  on  rz.game_account = lz.game_account\ngroup by lz.game_id,lz.login_time,gs.game_id,gs.system_type,gs.group_id"
      .replace("startDay", startDay).replace("endDay", endDay)
    val df_dau: DataFrame = sqlContext.sql(sql_dau)
    foreachDauPartition(df_dau)

    //支付账户数注册来源
    val sql_pay_acc = "select\noz.order_time publish_date, \ngs.game_id parent_game_id,\noz.game_id child_game_id, \ngs.system_type os,\ngs.group_id group_id,\ncount(distinct (case when rz.reg_time is null  then null else oz.game_account end)) recharge_accounts,\ncount(distinct (case when datediff(oz.order_time,rz.reg_time)=0  then oz.game_account else null end)) as recharge_acc_regi_1, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)=1  then oz.game_account else null end)) as recharge_acc_regi_2, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)=2  then oz.game_account else null end)) as recharge_acc_regi_3, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)=3  then oz.game_account else null end)) as recharge_acc_regi_4, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)=4  then oz.game_account else null end)) as recharge_acc_regi_5, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)=5  then oz.game_account else null end)) as recharge_acc_regi_6, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)=6  then oz.game_account else null end)) as recharge_acc_regi_7, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)=7  then oz.game_account else null end)) as recharge_acc_regi_8, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)=8  then oz.game_account else null end)) as recharge_acc_regi_9, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)=9  then oz.game_account else null end)) as recharge_acc_regi_10, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)=10  then oz.game_account else null end)) as recharge_acc_regi_11, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)=11  then oz.game_account else null end)) as recharge_acc_regi_12, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)=12  then oz.game_account else null end)) as recharge_acc_regi_13, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)=13  then oz.game_account else null end)) as recharge_acc_regi_14, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)=14  then oz.game_account else null end)) as recharge_acc_regi_15, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)<=29  and  datediff(to_date(oz.order_time),rz.reg_time)>=15  then oz.game_account else null end)) as recharge_acc_regi_16_30, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)<=44  and  datediff(to_date(oz.order_time),rz.reg_time)>=30  then oz.game_account else null end)) as recharge_acc_regi_31_45, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)<=59  and  datediff(to_date(oz.order_time),rz.reg_time)>=45  then oz.game_account else null end)) as recharge_acc_regi_46_60, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)<=89  and  datediff(to_date(oz.order_time),rz.reg_time)>=60  then oz.game_account else null end)) as recharge_acc_regi_61_90, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)<=119  and  datediff(to_date(oz.order_time),rz.reg_time)>=90  then oz.game_account else null end)) as recharge_acc_regi_91_120, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)<=149  and  datediff(to_date(oz.order_time),rz.reg_time)>=120  then oz.game_account else null end)) as recharge_acc_regi_121_150, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)<=179  and  datediff(to_date(oz.order_time),rz.reg_time)>=150  then oz.game_account else null end)) as recharge_acc_regi_151_180, \ncount(distinct (case when datediff(oz.order_time,rz.reg_time)>=180  then oz.game_account else null end)) as recharge_acc_regi_181, \ncount(distinct (case when rz.reg_time is null  then oz.game_account else null end)) as recharge_acc_regi_lost\nfrom \n(select distinct game_id,to_date(order_time) order_time,lower(trim(game_account))  game_account from ods_order where order_status=4 and  to_date(order_time)>='startDay' and   to_date(order_time)<='endDay' and prod_type=6 and game_account is not null and game_account!='') oz\njoin  (select  distinct game_id , old_game_id ,system_type,group_id from game_sdk ) gs on oz.game_id=gs.old_game_id\nleft join (select distinct lower(trim(game_account)) game_account,to_date(reg_time) reg_time from ods_regi_rz where game_id is not null) rz  on  rz.game_account = oz.game_account\ngroup by oz.game_id,oz.order_time,gs.game_id,gs.system_type,gs.group_id"
      .replace("startDay", startDay).replace("endDay", endDay)
    val df_pay_acc: DataFrame = sqlContext.sql(sql_pay_acc)
    foreachPayAccPartition(df_pay_acc)

    //支付金额注册来源
    val sql_pay_money ="select\nto_date(oz.order_time) publish_date, \ngs.game_id parent_game_id,\noz.game_id child_game_id, \ngs.system_type os,\ngs.group_id group_id,\nsum(case when rz.reg_time is null  then 0 else oz.pay_all end)*100 recharge_price,\nsum(case when datediff(oz.order_time,rz.reg_time)=0  then oz.pay_all else 0 end)*100 as recharge_pri_regi_1, \nsum(case when datediff(oz.order_time,rz.reg_time)=1  then oz.pay_all else 0 end)*100 as recharge_pri_regi_2, \nsum(case when datediff(oz.order_time,rz.reg_time)=2  then oz.pay_all else 0 end)*100 as recharge_pri_regi_3, \nsum(case when datediff(oz.order_time,rz.reg_time)=3  then oz.pay_all else 0 end)*100 as recharge_pri_regi_4, \nsum(case when datediff(oz.order_time,rz.reg_time)=4  then oz.pay_all else 0 end)*100 as recharge_pri_regi_5, \nsum(case when datediff(oz.order_time,rz.reg_time)=5  then oz.pay_all else 0 end)*100 as recharge_pri_regi_6, \nsum(case when datediff(oz.order_time,rz.reg_time)=6  then oz.pay_all else 0 end)*100 as recharge_pri_regi_7, \nsum(case when datediff(oz.order_time,rz.reg_time)=7  then oz.pay_all else 0 end)*100 as recharge_pri_regi_8, \nsum(case when datediff(oz.order_time,rz.reg_time)=8  then oz.pay_all else 0 end)*100 as recharge_pri_regi_9, \nsum(case when datediff(oz.order_time,rz.reg_time)=9  then oz.pay_all else 0 end)*100 as recharge_pri_regi_10, \nsum(case when datediff(oz.order_time,rz.reg_time)=10  then oz.pay_all else 0 end)*100 as recharge_pri_regi_11, \nsum(case when datediff(oz.order_time,rz.reg_time)=11  then oz.pay_all else 0 end)*100 as recharge_pri_regi_12, \nsum(case when datediff(oz.order_time,rz.reg_time)=12  then oz.pay_all else 0 end)*100 as recharge_pri_regi_13, \nsum(case when datediff(oz.order_time,rz.reg_time)=13  then oz.pay_all else 0 end)*100 as recharge_pri_regi_14, \nsum(case when datediff(oz.order_time,rz.reg_time)=14  then oz.pay_all else 0 end)*100 as recharge_pri_regi_15, \nsum(case when datediff(oz.order_time,rz.reg_time)<=29  and  datediff(to_date(oz.order_time),rz.reg_time)>=15  then oz.pay_all else 0 end)*100 as recharge_pri_regi_16_30, \nsum(case when datediff(oz.order_time,rz.reg_time)<=44  and  datediff(to_date(oz.order_time),rz.reg_time)>=30  then oz.pay_all else 0 end)*100 as recharge_pri_regi_31_45, \nsum(case when datediff(oz.order_time,rz.reg_time)<=59  and  datediff(to_date(oz.order_time),rz.reg_time)>=45  then oz.pay_all else 0 end)*100 as recharge_pri_regi_46_60, \nsum(case when datediff(oz.order_time,rz.reg_time)<=89  and  datediff(to_date(oz.order_time),rz.reg_time)>=60  then oz.pay_all else 0 end)*100 as recharge_pri_regi_61_90, \nsum(case when datediff(oz.order_time,rz.reg_time)<=119  and  datediff(to_date(oz.order_time),rz.reg_time)>=90  then oz.pay_all else 0 end)*100 as recharge_pri_regi_91_120, \nsum(case when datediff(oz.order_time,rz.reg_time)<=149  and  datediff(to_date(oz.order_time),rz.reg_time)>=120  then oz.pay_all else 0 end)*100 as recharge_pri_regi_121_150, \nsum(case when datediff(oz.order_time,rz.reg_time)<=179  and  datediff(to_date(oz.order_time),rz.reg_time)>=150  then oz.pay_all else 0 end)*100 as recharge_pri_regi_151_180, \nsum(case when datediff(oz.order_time,rz.reg_time)>=180  then oz.pay_all else 0 end)*100 as recharge_pri_regi_181, \nsum(case when rz.reg_time is null  then oz.pay_all else 0 end)*100 as recharge_pri_regi_lost\nfrom \n(select distinct lower(trim(game_account)) game_account,order_no,order_time,game_id,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as pay_all from ods_order where order_status=4 and  to_date(order_time)>='startDay' and   to_date(order_time)<='endDay' and prod_type=6  and game_account is not null and game_account!='') oz\njoin  (select  distinct game_id , old_game_id ,system_type,group_id from game_sdk ) gs on oz.game_id=gs.old_game_id\nleft join (select distinct lower(trim(game_account)) game_account,to_date(reg_time) reg_time from ods_regi_rz where game_id is not null) rz  on  rz.game_account=oz.game_account \ngroup by oz.game_id,to_date(oz.order_time),gs.game_id,gs.system_type,gs.group_id"
      .replace("startDay", startDay).replace("endDay", endDay)
    val df_pay_money: DataFrame = sqlContext.sql(sql_pay_money)
    foreachPayMoneyPartition(df_pay_money)
    sc.stop()
  }


  def foreachDauPartition(df_dau: DataFrame) = {
    df_dau.foreachPartition(iter => {
      val conn = JdbcUtil.getConn()
      val statement = conn.createStatement
      val sqlText = "insert into bi_gamepublic_opera_regi_actions(publish_date,parent_game_id,child_game_id,os,group_id,dau_accounts,dau_regi_1,dau_regi_2,dau_regi_3,dau_regi_4,dau_regi_5,dau_regi_6,dau_regi_7,dau_regi_8,dau_regi_9,dau_regi_10,dau_regi_11,dau_regi_12,dau_regi_13,dau_regi_14,dau_regi_15,dau_regi_16_30,dau_regi_31_45,dau_regi_46_60,dau_regi_61_90,dau_regi_91_120,dau_regi_121_150,dau_regi_151_180,dau_regi_181,dau_regi_lost) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update \nos=values(os),\ngroup_id=values(group_id),\ndau_accounts=values(dau_accounts),\ndau_regi_1=values(dau_regi_1),\ndau_regi_2=values(dau_regi_2),\ndau_regi_3=values(dau_regi_3),\ndau_regi_4=values(dau_regi_4),\ndau_regi_5=values(dau_regi_5),\ndau_regi_6=values(dau_regi_6),\ndau_regi_7=values(dau_regi_7),\ndau_regi_8=values(dau_regi_8),\ndau_regi_9=values(dau_regi_9),\ndau_regi_10=values(dau_regi_10),\ndau_regi_11=values(dau_regi_11),\ndau_regi_12=values(dau_regi_12),\ndau_regi_13=values(dau_regi_13),\ndau_regi_14=values(dau_regi_14),\ndau_regi_15=values(dau_regi_15),\ndau_regi_16_30=values(dau_regi_16_30),\ndau_regi_31_45=values(dau_regi_31_45),\ndau_regi_46_60=values(dau_regi_46_60),\ndau_regi_61_90=values(dau_regi_61_90),\ndau_regi_91_120=values(dau_regi_91_120),\ndau_regi_121_150=values(dau_regi_121_150),\ndau_regi_151_180=values(dau_regi_151_180),\ndau_regi_181=values(dau_regi_181),\ndau_regi_lost=values(dau_regi_lost)"
      val params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        params.+=(Array[Any](row.get(0), row.get(1), row.get(2), row.get(3), row.get(4), row.get(5),
          row.get(6), row.get(7), row.get(8), row.get(9), row.get(10), row.get(11),
          row.get(12), row.get(13), row.get(14), row.get(15), row.get(16), row.get(17),
          row.get(18), row.get(19), row.get(20), row.get(21), row.get(22), row.get(23),
          row.get(24), row.get(25), row.get(26), row.get(27), row.get(28), row.get(29)))
      })
      JdbcUtil.doBatch(sqlText, params, conn)
      statement.close()
      conn.close
    })
  }


  def foreachPayAccPartition(df_pay_acc: DataFrame) = {
    df_pay_acc.foreachPartition(iter => {
      val conn = JdbcUtil.getConn()
      val statement = conn.createStatement
      val sqlText = "insert into bi_gamepublic_opera_regi_actions(publish_date,parent_game_id,child_game_id,os,group_id,recharge_accounts,recharge_acc_regi_1,recharge_acc_regi_2,recharge_acc_regi_3,recharge_acc_regi_4,recharge_acc_regi_5,recharge_acc_regi_6,recharge_acc_regi_7,recharge_acc_regi_8,recharge_acc_regi_9,recharge_acc_regi_10,recharge_acc_regi_11,recharge_acc_regi_12,recharge_acc_regi_13,recharge_acc_regi_14,recharge_acc_regi_15,recharge_acc_regi_16_30,recharge_acc_regi_31_45,recharge_acc_regi_46_60,recharge_acc_regi_61_90,recharge_acc_regi_91_120,recharge_acc_regi_121_150,recharge_acc_regi_151_180,recharge_acc_regi_181,recharge_acc_lost) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update \nos=values(os),\ngroup_id=values(group_id),\nrecharge_accounts=values(recharge_accounts),\nrecharge_acc_regi_1=values(recharge_acc_regi_1),\nrecharge_acc_regi_2=values(recharge_acc_regi_2),\nrecharge_acc_regi_3=values(recharge_acc_regi_3),\nrecharge_acc_regi_4=values(recharge_acc_regi_4),\nrecharge_acc_regi_5=values(recharge_acc_regi_5),\nrecharge_acc_regi_6=values(recharge_acc_regi_6),\nrecharge_acc_regi_7=values(recharge_acc_regi_7),\nrecharge_acc_regi_8=values(recharge_acc_regi_8),\nrecharge_acc_regi_9=values(recharge_acc_regi_9),\nrecharge_acc_regi_10=values(recharge_acc_regi_10),\nrecharge_acc_regi_11=values(recharge_acc_regi_11),\nrecharge_acc_regi_12=values(recharge_acc_regi_12),\nrecharge_acc_regi_13=values(recharge_acc_regi_13),\nrecharge_acc_regi_14=values(recharge_acc_regi_14),\nrecharge_acc_regi_15=values(recharge_acc_regi_15),\nrecharge_acc_regi_16_30=values(recharge_acc_regi_16_30),\nrecharge_acc_regi_31_45=values(recharge_acc_regi_31_45),\nrecharge_acc_regi_46_60=values(recharge_acc_regi_46_60),\nrecharge_acc_regi_61_90=values(recharge_acc_regi_61_90),\nrecharge_acc_regi_91_120=values(recharge_acc_regi_91_120),\nrecharge_acc_regi_121_150=values(recharge_acc_regi_121_150),\nrecharge_acc_regi_151_180=values(recharge_acc_regi_151_180),\nrecharge_acc_regi_181=values(recharge_acc_regi_181),\nrecharge_acc_lost=values(recharge_acc_lost)"
      val params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        params.+=(Array[Any](row.get(0), row.get(1), row.get(2), row.get(3), row.get(4), row.get(5),
          row.get(6), row.get(7), row.get(8), row.get(9), row.get(10), row.get(11),
          row.get(12), row.get(13), row.get(14), row.get(15), row.get(16), row.get(17),
          row.get(18), row.get(19), row.get(20), row.get(21), row.get(22), row.get(23),
          row.get(24), row.get(25), row.get(26), row.get(27), row.get(28), row.get(29)))
      })
      JdbcUtil.doBatch(sqlText, params, conn)
      statement.close()
      conn.close
    })
  }

  def foreachPayMoneyPartition(df_pay_money: DataFrame) = {
    df_pay_money.foreachPartition(iter => {
      val conn = JdbcUtil.getConn()
      val statement = conn.createStatement
      val sqlText = "insert into bi_gamepublic_opera_regi_actions(publish_date,parent_game_id,child_game_id,os,group_id,recharge_price,recharge_pri_regi_1,recharge_pri_regi_2,recharge_pri_regi_3,recharge_pri_regi_4,recharge_pri_regi_5,recharge_pri_regi_6,recharge_pri_regi_7,recharge_pri_regi_8,recharge_pri_regi_9,recharge_pri_regi_10,recharge_pri_regi_11,recharge_pri_regi_12,recharge_pri_regi_13,recharge_pri_regi_14,recharge_pri_regi_15,recharge_pri_regi_16_30,recharge_pri_regi_31_45,recharge_pri_regi_46_60,recharge_pri_regi_61_90,recharge_pri_regi_91_120,recharge_pri_regi_121_150,recharge_pri_regi_151_180,recharge_pri_regi_181,recharge_pri_lost) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update \nos=values(os),\ngroup_id=values(group_id),\nrecharge_price=values(recharge_price),\nrecharge_pri_regi_1=values(recharge_pri_regi_1),\nrecharge_pri_regi_2=values(recharge_pri_regi_2),\nrecharge_pri_regi_3=values(recharge_pri_regi_3),\nrecharge_pri_regi_4=values(recharge_pri_regi_4),\nrecharge_pri_regi_5=values(recharge_pri_regi_5),\nrecharge_pri_regi_6=values(recharge_pri_regi_6),\nrecharge_pri_regi_7=values(recharge_pri_regi_7),\nrecharge_pri_regi_8=values(recharge_pri_regi_8),\nrecharge_pri_regi_9=values(recharge_pri_regi_9),\nrecharge_pri_regi_10=values(recharge_pri_regi_10),\nrecharge_pri_regi_11=values(recharge_pri_regi_11),\nrecharge_pri_regi_12=values(recharge_pri_regi_12),\nrecharge_pri_regi_13=values(recharge_pri_regi_13),\nrecharge_pri_regi_14=values(recharge_pri_regi_14),\nrecharge_pri_regi_15=values(recharge_pri_regi_15),\nrecharge_pri_regi_16_30=values(recharge_pri_regi_16_30),\nrecharge_pri_regi_31_45=values(recharge_pri_regi_31_45),\nrecharge_pri_regi_46_60=values(recharge_pri_regi_46_60),\nrecharge_pri_regi_61_90=values(recharge_pri_regi_61_90),\nrecharge_pri_regi_91_120=values(recharge_pri_regi_91_120),\nrecharge_pri_regi_121_150=values(recharge_pri_regi_121_150),\nrecharge_pri_regi_151_180=values(recharge_pri_regi_151_180),\nrecharge_pri_regi_181=values(recharge_pri_regi_181),\nrecharge_pri_lost=values(recharge_pri_lost)"
      val params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        params.+=(Array[Any](row.get(0), row.get(1), row.get(2), row.get(3), row.get(4), row.get(5),
          row.get(6), row.get(7), row.get(8), row.get(9), row.get(10), row.get(11),
          row.get(12), row.get(13), row.get(14), row.get(15), row.get(16), row.get(17),
          row.get(18), row.get(19), row.get(20), row.get(21), row.get(22), row.get(23),
          row.get(24), row.get(25), row.get(26), row.get(27), row.get(28), row.get(29)))
      })
      JdbcUtil.doBatch(sqlText, params, conn)
      statement.close()
      conn.close
    })
  }

}
