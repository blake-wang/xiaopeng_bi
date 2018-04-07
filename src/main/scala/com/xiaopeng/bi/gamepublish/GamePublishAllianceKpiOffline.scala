package com.xiaopeng.bi.gamepublish

import java.sql.{Connection, PreparedStatement}

import com.xiaopeng.bi.udf.ChannelUDF
import com.xiaopeng.bi.utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kequan on 2/27/18.
  * 发行管理后台离线逻辑
  */
object GamePublishAllianceKpiOffline {
  var startTime = "";
  var endTime = "";
  var mode = ""


  def main(args: Array[String]): Unit = {
    startTime = args(0)
    endTime = args(1)
    mode = args(2)

    //创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.streaming.stopGracefullyOnShutdown", "true");
    SparkUtils.setMaster(sparkConf);
    val sc: SparkContext = new SparkContext(sparkConf);
    val hiveContext = new HiveContext(sc);
    hiveContext.udf.register("getchannel", new ChannelUDF(), DataTypes.StringType)
    hiveContext.sql("use yyft");

    // 联运管理维度信息表
    DimensionUtil.createAllianceDim(hiveContext)

    if (mode.equals("regi")) {
      //注册相关
      loadRegiInfoOffline(sc, hiveContext, startTime, endTime);
    } else if (mode.equals("active")) {
      //激活相关
      loadActiveInfoOffline(sc, hiveContext, startTime, endTime);
    } else if (mode.equals("order")) {
      // 支付相关 
      loadOrderInfoOffline(sc, hiveContext, startTime, endTime);
    } else if (mode.equals("login")) {
      // 登录相关
      loadLoginInfoOffline(sc, hiveContext, startTime, endTime);
    } else if (mode.equals("all")) {
      //  发行 联运管理后台 全部
      loadActiveInfoOffline(sc, hiveContext, startTime, endTime);
      loadRegiInfoOffline(sc, hiveContext, startTime, endTime);
      loadLoginInfoOffline(sc, hiveContext, startTime, endTime);
      loadOrderInfoOffline(sc, hiveContext, startTime, endTime);

    }

    // 联运管理维度信息表 内存消除
    DimensionUtil.uncacheAllianceDimTable(hiveContext)
    sc.stop();
  }


  /**
    * 注册离线
    *
    * @param sc
    * @param hiveContext
    * @param startTime
    * @param endTime
    */
  def loadRegiInfoOffline(sc: SparkContext, hiveContext: HiveContext, startTime: String, endTime: String) = {
    // 一.获取注册数据
    hiveContext.sql("use yyft");
    val sql_regi = "select \nrz.game_id game_id,\nrz.alliance_bag_id alliance_bag_id,\nrz.reg_time reg_time, \nrz.imei imei,\nrz.game_account game_account\nfrom\n(select distinct game_id,alliance_bag_id,substr(reg_time,0,13) reg_time, imei,game_account  from ods_regi_rz where to_date(reg_time)>='startTime' and to_date(reg_time)<='endTime' and imei is not null and alliance_bag_id is not null and alliance_bag_id !='') rz\njoin  (select  distinct game_id as parent_game_id , old_game_id  ,system_type,group_id from game_sdk  where state=0) gs on rz.game_id=gs.old_game_id"
      .replace("startTime", startTime).replace("endTime", endTime)
    hiveContext.sql(sql_regi).persist().registerTempTable("ods_regi_rz_cache")

    // 二.bi_gamepublic_alliance_base_day_kpi  天表 联运ID : 注册账户数 和 注册设备数
    val sql_regi_day = "select \nrz.reg_time as publish_date,\ngs.game_id parent_game_id,\nrz.game_id as child_game_id, \nif(ad.terrace_name is null,'',ad.terrace_name) as terrace_name, \nif(ad.terrace_type is null,1,ad.terrace_type) as terrace_type, \nrz.alliance_bag_id as alliance_bag_id,  \ngs.system_type as os, \nif(ad.head_people is null,'',ad.head_people)as head_people,     \nif(ad.terrace_auto_id is null,0,ad.terrace_auto_id) terrace_auto_id,\nif(ad.alliance_company_id is null,0,ad.alliance_company_id) alliance_company_id,\nif(ad.alliance_company_name is null,'',ad.alliance_company_name) alliance_company_name,\nrz.regi_device_num as regi_device_num,\nrz.regi_account_num as regi_account_num\nfrom\n(select game_id,alliance_bag_id,substr(reg_time,0,10) reg_time, count(distinct imei) regi_device_num,count(game_account) regi_account_num from   ods_regi_rz_cache group by game_id,alliance_bag_id,substr(reg_time,0,10)) rz \njoin (select distinct game_id,system_type,old_game_id from game_sdk where state=0) gs on rz.game_id=gs.old_game_id -- 游戏信息 \nleft join (select child_game_id,terrace_name,terrace_type,alliance_bag_id,head_people,terrace_auto_id,alliance_company_id,alliance_company_name  from  alliance_dim) ad on rz.alliance_bag_id=ad.alliance_bag_id -- 联运包ID信息"
    val regiDF: DataFrame = hiveContext.sql(sql_regi_day).persist()
    foreachRegiDayPartition(regiDF);

    // 三.明细表和 新增注册设备数
    // 1.明细表
    // 一.补全明细数据
    hiveContext.sql("use yyft");
    val sql_regi_detail = "select distinct game_id,alliance_bag_id,substr(reg_time,0,13) reg_time, imei from ods_regi_rz_cache "
    val regiDF_detail: DataFrame = hiveContext.sql(sql_regi_detail)
    foreachRegiDeatilPartition(regiDF_detail);

    // 2.新增注册设备数 新增注册账户数
    //  读取 激活明细表
    SparkUtils.readAllianceActiveDeatials(sc, hiveContext, startTime, endTime)
    val sql_day_bag = "select \nrz.reg_time as publish_date,\ngs.game_id parent_game_id,\nrz.game_id as child_game_id, \nif(ad.terrace_name is null,'',ad.terrace_name) as terrace_name, \nif(ad.terrace_type is null,1,ad.terrace_type) as terrace_type, \nrz.alliance_bag_id as alliance_bag_id,  \ngs.system_type as os, \nif(ad.head_people is null,'',ad.head_people)as head_people,\nrz.new_regi_device_num as new_regi_device_num,\nrz.new_regi_account_num as new_regi_account_num\nfrom\n(\n\tselect  rz.game_id game_id,rz.alliance_bag_id alliance_bag_id,substr(rz.reg_time,0,10) reg_time,count(distinct rz.imei) new_regi_device_num,count(rz.game_account) new_regi_account_num\n\tfrom \n\t(select distinct game_id,alliance_bag_id,substr(reg_time,0,10) reg_time,imei,game_account  from   ods_regi_rz_cache) rz \n\tjoin (select distinct game_id,alliance_bag_id,substr(active_time,0,10) active_time,imei from bi_gamepublic_alliance_active_detail) az on rz.imei=az.imei and rz.game_id=az.game_id  and rz.alliance_bag_id=az.alliance_bag_id   and rz.reg_time=az.active_time  \n\tgroup by rz.game_id,rz.alliance_bag_id,substr(rz.reg_time,0,10)\n) rz \njoin (select distinct game_id,system_type,old_game_id from game_sdk where state=0) gs on rz.game_id=gs.old_game_id -- 游戏信息 \nleft join (select child_game_id,terrace_name,terrace_type,alliance_bag_id,head_people  from  alliance_dim) ad on rz.alliance_bag_id=ad.alliance_bag_id -- 联运包ID信息"
    val df_day_bag: DataFrame = hiveContext.sql(sql_day_bag)
    foreachRegiDayBagPartition(df_day_bag);


  }

  /**
    * 激活离线
    *
    * @param sc
    * @param hiveContext
    * @param startTime
    * @param endTime
    */
  def loadActiveInfoOffline(sc: SparkContext, hiveContext: HiveContext, startTime: String, endTime: String) = {
    //一.补全明细
    hiveContext.sql("use yyft");
    val sql_bi_active = "select \naz.game_id child_game_id,\naz.alliance_bag_id alliance_bag_id,\naz.active_time  active_time,\naz.imei imei\nfrom \n(select  game_id,alliance_bag_id,imei,min(active_time) active_time from ods_active where to_date(active_time)>='startTime' and to_date(active_time)<='endTime' and imei is not null and alliance_bag_id is not null and alliance_bag_id !='' group by game_id,alliance_bag_id,imei) az \njoin  (select  distinct game_id as parent_game_id , old_game_id  ,system_type,group_id from game_sdk  where state=0) gs on az.game_id=gs.old_game_id"
      .replace("startTime", startTime).replace("endTime", endTime)
    val df_bi_active: DataFrame = hiveContext.sql(sql_bi_active);
    foreachActiveDeatilPartition(df_bi_active)

    //二.读取明细表
    SparkUtils.readAllianceActiveDeatials(sc, hiveContext, startTime, endTime)

    //三.根据明细表 更新 bi_gamepublic_alliance_base_day_kpi active_num 天 联运包 ID
    val sql_day = "select distinct \naz.active_time as publish_date,\ngs.game_id parent_game_id,  \naz.game_id as child_game_id, \nif(ad.terrace_name is null,'',ad.terrace_name) as terrace_name, \nif(ad.terrace_type is null,1,ad.terrace_type) as terrace_type, \naz.alliance_bag_id as alliance_bag_id, \ngs.system_type as os, \nif(ad.head_people is null,'',ad.head_people)as head_people,\nif(ad.terrace_auto_id is null,0,ad.terrace_auto_id) terrace_auto_id,\nif(ad.alliance_company_id is null,0,ad.alliance_company_id) alliance_company_id,\nif(ad.alliance_company_name is null,'',ad.alliance_company_name) alliance_company_name,\naz.active_num as active_num  \nfrom  \n(select game_id,alliance_bag_id,substr(active_time,0,10) active_time,count(distinct imei) active_num from bi_gamepublic_alliance_active_detail group by game_id,alliance_bag_id,substr(active_time,0,10)) az  \njoin (select distinct game_id,system_type,old_game_id from game_sdk where state=0) gs on az.game_id=gs.old_game_id -- 游戏信息 \nleft join (select child_game_id,terrace_name,terrace_type,alliance_bag_id,head_people,terrace_auto_id,alliance_company_id,alliance_company_name  from  alliance_dim) ad on az.alliance_bag_id=ad.alliance_bag_id -- 联运包ID信息"
    val df_day: DataFrame = hiveContext.sql(sql_day);
    foreachActiveDayPartition(df_day)

  }


  def foreachPayMoneyDF(pay_money_df: DataFrame) = {
    pay_money_df.foreachPartition(iter => {
      val conn = JdbcUtil.getConn()

      val pay_money_sql = "insert into bi_gamepublic_alliance_base_day_kpi (publish_date,parent_game_id,child_game_id,terrace_name,terrace_type,alliance_bag_id,os,head_people,terrace_auto_id,alliance_company_id,alliance_company_name,pay_money,pay_account_num,pay_device_num) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update terrace_name=values(terrace_name),terrace_type=values(terrace_type),os=values(os),head_people=values(head_people),terrace_auto_id=values(terrace_auto_id),alliance_company_id=values(alliance_company_id),alliance_company_name=values(alliance_company_name),pay_money=values(pay_money),pay_account_num=values(pay_account_num),pay_device_num=values(pay_device_num)"
      val pay_money_params = new ArrayBuffer[Array[Any]]()
      val pay_money_pstmt = conn.prepareStatement(pay_money_sql)

      iter.foreach(line => {
        val publish_date = line.getAs[String]("order_time")
        val parent_game_id = line.getAs[Int]("parent_game_id")
        val child_game_id = line.getAs[Int]("game_id")
        val terrace_name = line.getAs[String]("terrace_name")
        val terrace_type = line.getAs[Int]("terrace_type")
        val alliance_bag_id = line.getAs[String]("alliance_bag_id")
        val os = line.getAs[Int]("os")
        val head_people = line.getAs[String]("head_people")
        val terrace_auto_id = line.getAs[Int]("terrace_auto_id")
        val alliance_company_id = line.getAs[Int]("alliance_company_id")
        val alliance_company_name = line.getAs[String]("alliance_company_name")
        val pay_money = line.getAs[java.math.BigDecimal]("pay_money")
        val pay_account_num = line.getAs[Long]("pay_account_num")
        val pay_device_num = line.getAs[Long]("pay_device_num")

        pay_money_params.+=(Array(publish_date, parent_game_id, child_game_id, terrace_name, terrace_type, alliance_bag_id, os, head_people, terrace_auto_id, alliance_company_id, alliance_company_name, pay_money, pay_account_num, pay_device_num))
        JdbcUtil.executeUpdate(pay_money_pstmt, pay_money_params, conn)
      })
      pay_money_pstmt.close()
      conn.close()
    })
  }

  def foreachNewPayMoneyDF(pay_account_num_df: DataFrame) = {
    pay_account_num_df.foreachPartition(iter => {
      val conn = JdbcUtil.getConn()

      val pay_money_sql = "insert into bi_gamepublic_alliance_base_day_kpi (publish_date,child_game_id,alliance_bag_id,new_pay_account_num,new_pay_money) values (?,?,?,?,?) on duplicate key update new_pay_account_num=values(new_pay_account_num),new_pay_money=values(new_pay_money)"
      val pay_money_params = new ArrayBuffer[Array[Any]]()
      val pay_money_pstmt = conn.prepareStatement(pay_money_sql)

      iter.foreach(line => {
        val publish_date = line.getAs[String]("order_time")
        val child_game_id = line.getAs[Int]("game_id")
        val alliance_bag_id = line.getAs[String]("alliance_bag_id")
        val new_pay_account_num = line.getAs[Long]("new_pay_account_num")
        val new_pay_money = line.getAs[java.math.BigDecimal]("new_pay_money")

        pay_money_params.+=(Array(publish_date, child_game_id, alliance_bag_id, new_pay_account_num, new_pay_money))
        JdbcUtil.executeUpdate(pay_money_pstmt, pay_money_params, conn)
      })
      pay_money_pstmt.close()
      conn.close()
    })
  }

  def foreachNewPayDeviceDF(new_pay_device_df: DataFrame) = {
    new_pay_device_df.foreachPartition(iter => {
      val conn = JdbcUtil.getConn()

      val new_pay_device_sql = "insert into bi_gamepublic_alliance_base_day_kpi (publish_date,child_game_id,alliance_bag_id,new_pay_device_num) values (?,?,?,?) on duplicate key update new_pay_device_num=values(new_pay_device_num)"
      val new_pay_device_params = new ArrayBuffer[Array[Any]]()
      val new_pay_device_pstmt = conn.prepareStatement(new_pay_device_sql)

      iter.foreach(line => {
        val publish_date = line.getAs[String]("order_time")
        val child_game_id = line.getAs[Int]("game_id")
        val alliance_bag_id = line.getAs[String]("alliance_bag_id")
        val new_pay_device_num = line.getAs[Long]("new_pay_device_num")


        new_pay_device_params.+=(Array(publish_date, child_game_id, alliance_bag_id, new_pay_device_num))
        JdbcUtil.executeUpdate(new_pay_device_pstmt, new_pay_device_params, conn)
      })
      new_pay_device_pstmt.close()
      conn.close()
    })
  }

  def foreachLtvDF(ltv_df: DataFrame) = {
    ltv_df.foreachPartition(iter => {
      val conn = JdbcUtil.getConn()
      val ltv_sql = "insert into bi_gamepublic_alliance_base_day_kpi (publish_date,child_game_id,alliance_bag_id,ltv_1day_b,ltv_3day_b,ltv_7day_b,ltv_30day_b) values (?,?,?,?,?,?,?) on duplicate key update ltv_1day_b=values(ltv_1day_b),ltv_3day_b=values(ltv_3day_b),ltv_7day_b=values(ltv_7day_b),ltv_30day_b=values(ltv_30day_b)"
      val ltv_params = new ArrayBuffer[Array[Any]]()
      val ltv_pstmt = conn.prepareStatement(ltv_sql)

      iter.foreach(line => {
        val reg_time = line.getAs[String]("reg_time")
        val reg_child_game_id = line.getAs[Int]("child_game_id")
        val reg_alliance_bag_id = line.getAs[String]("alliance_bag_id")
        val ltv_1day_b = line.getAs[java.math.BigDecimal]("ltv_1day_b")
        val ltv_3day_b = line.getAs[java.math.BigDecimal]("ltv_3day_b")
        val ltv_7day_b = line.getAs[java.math.BigDecimal]("ltv_7day_b")
        val ltv_30day_b = line.getAs[java.math.BigDecimal]("ltv_30day_b")

        ltv_params.+=(Array(reg_time, reg_child_game_id, reg_alliance_bag_id, ltv_1day_b, ltv_3day_b, ltv_7day_b, ltv_30day_b))
        JdbcUtil.executeUpdate(ltv_pstmt, ltv_params, conn)
      })
      ltv_pstmt.close()
      conn.close()
    })
  }

  /**
    * 支付离线
    *
    * @param sc
    * @param hiveContext
    * @param startTime
    * @param endTime
    */
  def loadOrderInfoOffline(sc: SparkContext, hiveContext: HiveContext, startTime: String, endTime: String) = {
    hiveContext.sql("use yyft")

    val regi_sql = "select distinct substr(reg_time,0,10) reg_time,game_id,alliance_bag_id,imei,game_account from ods_regi_rz where to_date(reg_time)>=date_add('startTime',-29) and to_date(reg_time)<='endTime' and alliance_bag_id is not null and alliance_bag_id != '' and game_account is not null and imei is not null"
      .replace("startTime", startTime).replace("endTime", endTime)
    hiveContext.sql(regi_sql).persist().registerTempTable("ods_regi_cache")

    //    SparkUtils.readXiaopeng2Table("common_sdk_order_pay", hiveContext)
    //    SparkUtils.readXiaopeng2Table("common_sdk_order_tran", hiveContext)
    SparkUtils.readCommonSdkOrderPay(sc, hiveContext, startTime, endTime)
    SparkUtils.readCommonSdkOrderTran(sc, hiveContext, startTime, endTime)


    val order_sql = "select distinct order_no,substr(order_time,0,10) order_time,imei,lower(trim(game_account)) as game_account,game_id,alliance_bag_id,ori_price_all,total_amt,payment_type,order_status \nfrom\n(select distinct order_no,substr(order_time,0,10) order_time,imei,lower(trim(game_account)) as game_account,game_id,alliance_bag_id,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as ori_price_all,total_amt,payment_type,order_status from ods_order where order_status=4 and prod_type=6 and to_date(order_time)>=date_add('startTime',-29) and to_date(order_time)<='entTime' and alliance_bag_id is not null and alliance_bag_id != '') oz \nleft join common_sdk_order_tran csot on oz.order_no =csot.pyw_order_sn\nleft join common_sdk_order_pay csop on csot.channel_order_sn=csop.channel_order_sn \nwhere csop.is_sandbox_pay = 0 or csop.is_sandbox_pay is null"
      .replace("startTime", startTime).replace("endTime", endTime)
    hiveContext.sql(order_sql).persist().registerTempTable("ods_order_cache")

    //充值金额 & 充值帐号数 & 充值设备数
    val pay_money_sql = "select\noz.order_time,\noz.game_id,\noz.alliance_bag_id,\ngs.game_id parent_game_id,\ngs.system_type as os,\nif(ad.terrace_name is null,'',ad.terrace_name) terrace_name,\nif(ad.terrace_type is null,1,ad.terrace_type) terrace_type,\nif(ad.head_people is null,'',ad.head_people) head_people,\nif(terrace_auto_id is null,0,terrace_auto_id)  terrace_auto_id,\nif(alliance_company_id is null,0,alliance_company_id) alliance_company_id,\nif(alliance_company_name is null,'',alliance_company_name) alliance_company_name,\noz.pay_money,\noz.pay_account_num,\noz.pay_device_num\nfrom\n(select order_time,game_id,alliance_bag_id,sum(ori_price_all) pay_money,count(distinct game_account) pay_account_num,count(distinct imei) pay_device_num from ods_order_cache group by order_time,game_id,alliance_bag_id) oz \njoin (select distinct game_id,old_game_id,system_type from game_sdk where state=0) gs on oz.game_id=gs.old_game_id\nleft join (select child_game_id,terrace_name,terrace_type,alliance_bag_id,head_people,terrace_auto_id,alliance_company_id,alliance_company_name from alliance_dim) ad on oz.alliance_bag_id=ad.alliance_bag_id"
    val pay_money_df = hiveContext.sql(pay_money_sql)
    foreachPayMoneyDF(pay_money_df)

    //新增充值帐号数 & 新增充值金额
    val new_pay_money_sql = "select oz.order_time,oz.game_id,oz.alliance_bag_id,count(distinct oz.game_account) new_pay_account_num,sum(oz.ori_price_all) new_pay_money from ods_regi_cache rz join ods_order_cache oz on rz.reg_time=oz.order_time and rz.game_account=oz.game_account group by oz.order_time,oz.game_id,oz.alliance_bag_id"
    val new_pay_money_df = hiveContext.sql(new_pay_money_sql)
    foreachNewPayMoneyDF(new_pay_money_df)

    //新增充值设备数
    val new_pay_device_sql = "select oz.order_time,oz.game_id,oz.alliance_bag_id,count(distinct oz.imei) new_pay_device_num from ods_regi_cache rz join ods_order_cache oz on rz.game_id=oz.game_id and rz.reg_time=oz.order_time and rz.alliance_bag_id=oz.alliance_bag_id and rz.imei=oz.imei group by oz.order_time,oz.game_id,oz.alliance_bag_id  "
    val new_pay_device_df = hiveContext.sql(new_pay_device_sql)
    foreachNewPayDeviceDF(new_pay_device_df)

    //用户质量
    val ltv_sql = "select \nodsr.reg_time  reg_time,\nodsr.game_id child_game_id,\nodsr.alliance_bag_id,\nodsr.ltv_1day_b,\nodsr.ltv_3day_b,\nodsr.ltv_7day_b,\nodsr.ltv_30day_b\nfrom\n(select \nrz.reg_time,\nrz.game_id,\nrz.alliance_bag_id,\nsum(case when datediff(oz.order_time,rz.reg_time)=0 and ori_price_all is not null then oz.ori_price_all else 0 end ) as ltv_1day_b,\nsum(case when datediff(oz.order_time,rz.reg_time)<=2 and ori_price_all is not null then oz.ori_price_all else 0 end ) as ltv_3day_b,\nsum(case when datediff(oz.order_time,rz.reg_time)<=6 and ori_price_all is not null then oz.ori_price_all else 0 end ) as ltv_7day_b,\nsum(case when datediff(oz.order_time,rz.reg_time)<=29 and ori_price_all is not null then oz.ori_price_all else 0 end ) as ltv_30day_b\nfrom ods_regi_cache rz \nleft join ods_order_cache oz on rz.game_account=oz.game_account \ngroup by rz.reg_time,rz.game_id,rz.alliance_bag_id) odsr\njoin (select distinct game_id,system_type,old_game_id from game_sdk where state=0) gs on odsr.game_id=gs.old_game_id"
    val ltv_df = hiveContext.sql(ltv_sql)
    foreachLtvDF(ltv_df)


  }

  def foreachLoginDauPartition(deviceDF: DataFrame) = {
    deviceDF.foreachPartition(iter => {
      val conn = JdbcUtil.getConn()

      val dau_num_sql = "insert into bi_gamepublic_alliance_base_day_kpi (publish_date,parent_game_id,child_game_id,terrace_name,terrace_type,alliance_bag_id,os,head_people,terrace_auto_id,alliance_company_id,alliance_company_name,dau_device_num,dau_account_num) values (?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update terrace_name=values(terrace_name),terrace_type=values(terrace_type),os=values(os),head_people=values(head_people),terrace_auto_id=values(terrace_auto_id),alliance_company_id=values(alliance_company_id),alliance_company_name=values(alliance_company_name),dau_device_num=values(dau_device_num),dau_account_num=values(dau_account_num)"
      val dau_num_params = new ArrayBuffer[Array[Any]]()
      val dau_num_pstmt = conn.prepareStatement(dau_num_sql)

      iter.foreach(line => {
        val login_date = line.getAs[String]("login_time")
        val child_game_id = line.getAs[Int]("game_id")
        val parent_game_id = line.getAs[Int]("parent_game_id")
        val terrace_name = line.getAs[String]("terrace_name")
        val terrace_type = line.getAs[Int]("terrace_type")
        val alliance_bag_id = line.getAs[String]("alliance_bag_id")
        val os = line.getAs[Int]("os")
        val head_people = line.getAs[String]("head_people")
        val terrace_auto_id = line.getAs[Int]("terrace_auto_id")
        val alliance_company_id = line.getAs[Int]("alliance_company_id")
        val alliance_company_name = line.getAs[String]("alliance_company_name")
        val dau_device_num = line.getAs[Long]("dau_device_num")
        val dau_account_num = line.getAs[Long]("dau_account_num")

        dau_num_params.+=(Array(login_date, parent_game_id, child_game_id, terrace_name, terrace_type, alliance_bag_id, os, head_people, terrace_auto_id, alliance_company_id, alliance_company_name, dau_device_num, dau_account_num))

        JdbcUtil.executeUpdate(dau_num_pstmt, dau_num_params, conn)

      })

      dau_num_pstmt.close()
      conn.close()


    })

  }


  def foreachRetainDF(retainDF: DataFrame) = {
    retainDF.foreachPartition(iter => {
      val conn = JdbcUtil.getConn()

      val retain_sql = "insert into bi_gamepublic_alliance_base_day_kpi (publish_date,child_game_id,alliance_bag_id,retained_1day,retained_3day,retained_7day,retained_30day) values (?,?,?,?,?,?,?) on duplicate key update retained_1day=values(retained_1day),retained_3day=values(retained_3day),retained_7day=values(retained_7day),retained_30day=values(retained_30day)"
      val retain_params = new ArrayBuffer[Array[Any]]()
      val retain_pstmt = conn.prepareStatement(retain_sql)

      iter.foreach(line => {
        val reg_date = line.getAs[String]("reg_time")
        val reg_game_id = line.getAs[Int]("game_id")
        val reg_alliance_bag_id = line.getAs[String]("alliance_bag_id")
        val retained_1day = line.getAs[Long]("retained_1day")
        val retained_3day = line.getAs[Long]("retained_3day")
        val retained_7day = line.getAs[Long]("retained_7day")
        val retained_30day = line.getAs[Long]("retained_30day")


        retain_params.+=(Array(reg_date, reg_game_id, reg_alliance_bag_id, retained_1day, retained_3day, retained_7day, retained_30day))

        JdbcUtil.executeUpdate(retain_pstmt, retain_params, conn)

      })
      retain_pstmt.close()
      conn.close()


    })

  }

  /**
    * 登录离线
    *
    * @param sc
    * @param hiveContext
    * @param startTime
    * @param endTime
    */
  def loadLoginInfoOffline(sc: SparkContext, hiveContext: HiveContext, startTime: String, endTime: String) = {
    hiveContext.sql("use yyft")
    val login_sql = "select distinct substr(login_time,0,10) login_time,game_id,alliance_bag_id,imei,game_account from ods_login where to_date(login_time)>='startTime' and to_date(login_time)<='endTime' and alliance_bag_id is not null and alliance_bag_id != '' and game_account is not null and imei is not null"
      .replace("startTime", startTime).replace("endTime", endTime)
    val loginLogDF = hiveContext.sql(login_sql)
    loginLogDF.registerTempTable("ods_login_cache")

    //dau_device_num & dau_account_num
    val dau_num_sql = "select \nlg.login_time,\nlg.game_id,\nlg.alliance_bag_id,\ngs.game_id parent_game_id,\ngs.system_type as os,\nif(ad.terrace_name is null,'',ad.terrace_name) terrace_name,\nif(ad.terrace_type is null,1,ad.terrace_type) terrace_type,\nif(ad.head_people is null,'',ad.head_people) head_people,\nif(terrace_auto_id is null,0,terrace_auto_id)  terrace_auto_id,\nif(alliance_company_id is null,0,alliance_company_id) alliance_company_id,\nif(alliance_company_name is null,'',alliance_company_name) alliance_company_name,\nlg.dau_device_num,\nlg.dau_account_num\nfrom\n(select login_time,game_id,alliance_bag_id,count(distinct imei) dau_device_num,count(distinct game_account) dau_account_num from ods_login_cache group by login_time,game_id,alliance_bag_id) lg  \njoin (select game_id,system_type,old_game_id from game_sdk where state=0) gs on lg.game_id=gs.old_game_id \nleft join (select child_game_id,terrace_name,terrace_type,alliance_bag_id,head_people,terrace_auto_id,alliance_company_id,alliance_company_name from alliance_dim ) ad on lg.alliance_bag_id=ad.alliance_bag_id"
    val dauDF = hiveContext.sql(dau_num_sql)
    foreachLoginDauPartition(dauDF)

    //留存  按帐号统计
    val regi_sql = "select distinct substr(reg_time,0,10) reg_time,game_id,alliance_bag_id,imei,game_account from ods_regi_rz where to_date(reg_time)>=date_add('startTime',-29) and to_date(reg_time)<='endTime' and alliance_bag_id is not null and alliance_bag_id != '' and game_account is not null and imei is not null"
      .replace("startTime", startTime).replace("endTime", endTime)
    val regiLogDF = hiveContext.sql(regi_sql)
    regiLogDF.registerTempTable("ods_regi_cache")

    val retain_sql = "select\nrs.reg_time,\nrs.game_id,\nrs.alliance_bag_id,\nsum(case rs.dau when 1 then 1 else 0 end) retained_1day,\nsum(case rs.dau when 2 then 1 else 0 end) retained_3day,\nsum(case rs.dau when 6 then 1 else 0 end) retained_7day,\nsum(case rs.dau when 29 then 1 else 0 end) retained_30day\nfrom\n(select \nrz.reg_time,\nrz.game_id,\nrz.alliance_bag_id,\nrz.game_account,\ndatediff(lg.login_time,rz.reg_time) dau \nfrom\n(select distinct reg_time,game_id,alliance_bag_id,game_account from ods_regi_cache ) rz \njoin (select distinct login_time,game_id,alliance_bag_id,game_account from ods_login_cache) lg on rz.game_account = lg.game_account \nwhere datediff(lg.login_time,rz.reg_time) in (1,2,6,29) ) rs group by rs.reg_time,rs.game_id,rs.alliance_bag_id"
    val retainDF = hiveContext.sql(retain_sql)
    foreachRetainDF(retainDF)


  }


  def foreachActiveDeatilPartition(df_bi_active: DataFrame) = {
    df_bi_active.foreachPartition(it => {
      //数据库链接
      val conn: Connection = JdbcUtil.getConn();
      val stmt = conn.createStatement();
      // 一.明细表
      val sql_deatil = "INSERT INTO bi_gamepublic_alliance_active_detail(game_id,alliance_bag_id,active_time,imei) VALUES (?,?,?,?) "
      val pstat_sql_deatil: PreparedStatement = conn.prepareStatement(sql_deatil);
      val ps_sql_deatil_params = new ArrayBuffer[Array[Any]]()

      val sql_deatil_update = "update bi_gamepublic_alliance_active_detail set active_time =?  WHERE game_id = ? and alliance_bag_id=? and  imei=?"
      val pstat_sql_deatil_update: PreparedStatement = conn.prepareStatement(sql_deatil_update);
      val ps_sql_deatil_update_params = new ArrayBuffer[Array[Any]]()

      it.foreach(row => {
        val child_game_id = row.getAs[Int]("child_game_id");
        val alliance_bag_id = row.getAs[String]("alliance_bag_id");
        val active_time = row.getAs[String]("active_time");
        val imei = row.getAs[String]("imei");

        val sqlselct_all = "select active_time from bi_gamepublic_alliance_active_detail WHERE game_id = '" + child_game_id + "'and alliance_bag_id='" + alliance_bag_id + "' and  imei='" + imei + "' ";
        val results_all = stmt.executeQuery(sqlselct_all);
        if (!results_all.next()) {
          ps_sql_deatil_params.+=(Array[Any](child_game_id, alliance_bag_id, active_time, imei))
        } else {
          val active_time_old = results_all.getString("active_time")
          if (DateUtils.beforeHour(active_time, active_time_old)) {
            // 新数据的激活时间靠前
            ps_sql_deatil_update_params.+=(Array[Any](active_time, child_game_id, alliance_bag_id, imei))
          }
        }
        // 插入数据库
        JdbcUtil.executeUpdate(pstat_sql_deatil, ps_sql_deatil_params, conn)
        JdbcUtil.executeUpdate(pstat_sql_deatil_update, ps_sql_deatil_update_params, conn)
      })

      //关闭数据库链接
      stmt.close()
      pstat_sql_deatil.close()
      pstat_sql_deatil_update.close()
      conn.close
    })

  }

  def foreachActiveDayPartition(df_day: DataFrame) = {
    df_day.foreachPartition(iter => {
      //数据库链接
      val conn: Connection = JdbcUtil.getConn();
      val stmt = conn.createStatement();
      // 更新天表
      val sql_day_active_num = "INSERT INTO bi_gamepublic_alliance_base_day_kpi(publish_date,parent_game_id,child_game_id,terrace_name,terrace_type,alliance_bag_id,os,head_people,terrace_auto_id,alliance_company_id,alliance_company_name,active_num) VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY update \nparent_game_id=values(parent_game_id),\nterrace_name=values(terrace_name),\nterrace_type=values(terrace_type),\nos=values(os),\nhead_people=values(head_people),\nterrace_auto_id=values(terrace_auto_id),\nalliance_company_id=values(alliance_company_id),\nalliance_company_name=values(alliance_company_name),\nactive_num=VALUES(active_num)"
      val ps_day_active_num: PreparedStatement = conn.prepareStatement(sql_day_active_num);
      val params_day_active = new ArrayBuffer[Array[Any]]()

      iter.foreach(row => {
        // 获取dataframe的数据
        val publish_date = row.getAs[String]("publish_date");
        val parent_game_id = row.getAs[Int]("parent_game_id");
        val child_game_id = row.getAs[Int]("child_game_id");
        val terrace_name = row.getAs[String]("terrace_name");
        val terrace_type = row.getAs[Int]("terrace_type");
        val alliance_bag_id = row.getAs[String]("alliance_bag_id");
        val os = row.getAs[Int]("os");
        val head_people = row.getAs[String]("head_people");
        val terrace_auto_id = row.getAs[Int]("terrace_auto_id");
        val alliance_company_id = row.getAs[Int]("alliance_company_id");
        val alliance_company_name = row.getAs[String]("alliance_company_name");
        val active_num = row.getAs[Long]("active_num").toInt;

        params_day_active.+=(Array[Any](publish_date, parent_game_id, child_game_id, terrace_name, terrace_type, alliance_bag_id, os, head_people, terrace_auto_id, alliance_company_id, alliance_company_name, active_num))
        // 插入数据库
        JdbcUtil.executeUpdate(ps_day_active_num, params_day_active, conn)
      })

      ps_day_active_num.close()
      conn.close


    })
  }


  def foreachRegiDayPartition(df: DataFrame) = {

    df.foreachPartition(iter => {
      if (!iter.isEmpty) {
        //数据库链接
        val conn: Connection = JdbcUtil.getConn();
        val stmt = conn.createStatement();

        // 联运管理日报  regi_device_num   regi_account_num
        val sql_day_kpi = "INSERT INTO bi_gamepublic_alliance_base_day_kpi(publish_date,parent_game_id,child_game_id,terrace_name,terrace_type,alliance_bag_id,os,head_people,terrace_auto_id,alliance_company_id,alliance_company_name,regi_account_num,regi_device_num) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY update \nparent_game_id=values(parent_game_id),\nterrace_name=values(terrace_name),\nterrace_type=values(terrace_type),\nos=values(os),\nhead_people=values(head_people),\nterrace_auto_id=values(terrace_auto_id),\nalliance_company_id=values(alliance_company_id),\nalliance_company_name=values(alliance_company_name),\nregi_account_num=values(regi_account_num),\nregi_device_num=values(regi_device_num)"
        val ps_day_kpi: PreparedStatement = conn.prepareStatement(sql_day_kpi);
        var params_day_kpi = new ArrayBuffer[Array[Any]]()

        iter.foreach(row => {
          // 获取dataframe的数据
          val publish_date = row.getAs[String]("publish_date");
          val parent_game_id = row.getAs[Int]("parent_game_id");
          val child_game_id = row.getAs[Int]("child_game_id");
          val terrace_name = row.getAs[String]("terrace_name");
          val terrace_type = row.getAs[Int]("terrace_type");
          val alliance_bag_id = row.getAs[String]("alliance_bag_id");
          val os = row.getAs[Int]("os");
          val head_people = row.getAs[String]("head_people");
          val terrace_auto_id = row.getAs[Int]("terrace_auto_id");
          val alliance_company_id = row.getAs[Int]("alliance_company_id");
          val alliance_company_name = row.getAs[String]("alliance_company_name");
          val regi_device_num = row.getAs[Long]("regi_device_num").toInt;
          var regi_account_num = row.getAs[Long]("regi_account_num").toInt;

          params_day_kpi.+=(Array[Any](publish_date, parent_game_id, child_game_id, terrace_name, terrace_type, alliance_bag_id, os, head_people, terrace_auto_id, alliance_company_id, alliance_company_name, regi_account_num, regi_device_num))
          //插入数据库
          JdbcUtil.executeUpdate(ps_day_kpi, params_day_kpi, conn)

        })
        //关闭数据库链接
        ps_day_kpi.close()
        stmt.close()
        conn.close
      }
    })

  }

  def foreachRegiDeatilPartition(regiDF: DataFrame) = {
    regiDF.foreachPartition(iter => {
      //数据库链接
      val conn: Connection = JdbcUtil.getConn();
      val stmt = conn.createStatement();

      // 更新明细表  按小时去重
      val sql = "INSERT INTO bi_gamepublic_alliance_regi_detail(game_id,alliance_bag_id,regi_time,imei) VALUES (?,?,?,?) "
      val ps = conn.prepareStatement(sql)
      var params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        val game_id = row.getAs[Int]("game_id");
        val alliance_bag_id = row.getAs[String]("alliance_bag_id");
        val regi_time = row.getAs[String]("reg_time");
        val imei = row.getAs[String]("imei");
        val sqlselct_day = "select regi_time from bi_gamepublic_alliance_regi_detail WHERE game_id = '" + game_id + "'and alliance_bag_id= '" + alliance_bag_id + "' and  imei='" + imei + "' and regi_time='" + regi_time + "'";
        val results_day = stmt.executeQuery(sqlselct_day);
        if (!results_day.next()) {
          params.+=(Array[Any](game_id, alliance_bag_id, regi_time, imei))
          JdbcUtil.executeUpdate(ps, params, conn)
        }
      })

      stmt.close()
      conn.close

    })
  }

  def foreachRegiDayBagPartition(df: DataFrame) = {
    df.foreachPartition(iter => {
      if (!iter.isEmpty) {
        //数据库链接
        val conn: Connection = JdbcUtil.getConn();
        val stmt = conn.createStatement();

        // 联运管理日报  regi_device_num   regi_account_num
        val sql_day_kpi = "INSERT INTO bi_gamepublic_alliance_base_day_kpi(publish_date,parent_game_id,child_game_id,terrace_name,terrace_type,alliance_bag_id,os,head_people,new_regi_device_num,new_regi_account_num) VALUES (?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY update \nparent_game_id=values(parent_game_id),\nterrace_name=values(terrace_name),\nterrace_type=values(terrace_type),\nos=values(os),\nhead_people=values(head_people),\nnew_regi_device_num=values(new_regi_device_num),\nnew_regi_account_num=values(new_regi_account_num)"
        val ps_day_kpi: PreparedStatement = conn.prepareStatement(sql_day_kpi);
        var params_day_kpi = new ArrayBuffer[Array[Any]]()

        iter.foreach(row => {
          // 获取dataframe的数据
          val publish_date = row.getAs[String]("publish_date");
          val parent_game_id = row.getAs[Int]("parent_game_id");
          val child_game_id = row.getAs[Int]("child_game_id");
          val terrace_name = row.getAs[String]("terrace_name");
          val terrace_type = row.getAs[Int]("terrace_type");
          val alliance_bag_id = row.getAs[String]("alliance_bag_id");
          val os = row.getAs[Int]("os");
          val head_people = row.getAs[String]("head_people");
          val new_regi_device_num = row.getAs[Long]("new_regi_device_num").toInt;
          val new_regi_account_num = row.getAs[Long]("new_regi_account_num").toInt;

          params_day_kpi.+=(Array[Any](publish_date, parent_game_id, child_game_id, terrace_name, terrace_type, alliance_bag_id, os, head_people, new_regi_device_num, new_regi_account_num))
          //插入数据库
          JdbcUtil.executeUpdate(ps_day_kpi, params_day_kpi, conn)

        })
        //关闭数据库链接
        ps_day_kpi.close()
        stmt.close()
        conn.close
      }
    })

  }
}
