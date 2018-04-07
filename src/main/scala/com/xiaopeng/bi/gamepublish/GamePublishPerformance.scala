package com.xiaopeng.bi.gamepublish

import java.sql.{Connection, Date, ResultSet}

import com.xiaopeng.bi.udaf.ContactDivideUDAF
import com.xiaopeng.bi.udf.{ChannelUDF, GameDivideLadderUDF}
import com.xiaopeng.bi.utils.{DateUtils, JdbcUtil, SparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kequan on 4/6/17.
  */
object GamePublishPerformance {

  var startDay = "";
  var endDay = ""
  var mode = ""

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    if (args.length == 3) {
      startDay = args(0)
      endDay = args(1)
      mode = args(2)
    } else if (args.length == 2) {
      startDay = args(0)
      endDay = startDay
      mode = args(1)
    }
    //创建各种上下文
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      .set("spark.default.parallelism", "60")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.sql.shuffle.partitions", "60");
    SparkUtils.setMaster(sparkConf);
    val sc = new SparkContext(sparkConf);
    val hiveContext = new HiveContext(sc);

    hiveContext.sql("use yyft")
    // 渠道分类的UDF
    hiveContext.udf.register("getchannel", new ChannelUDF(), DataTypes.StringType)
    // 获取分层比例的UDF
    hiveContext.udf.register("getdivideladder", new GameDivideLadderUDF(), DataTypes.StringType)
    // 连接分层金额和分层比例的UDAF
    hiveContext.udf.register("contactdivideladder", new ContactDivideUDAF())

    if (mode.equals("first")) {

      //第一步 全部信息不变的信息： main_id 注册信息 支付信息
      // 1.全部信息
      val sql_result_rgi_order = "select\ngc.performance_time as performance_time ,\ngs.parent_game_id as parent_game_id,\ngc.game_id as child_game_id,\ngs.system_type as system_type,\ngs.group_id as group_id,\ngetchannel(if(rz.expand_channel is null or rz.expand_channel='','21',rz.expand_channel),'0')  as  medium_channel,\ngetchannel(if(rz.expand_channel is null or rz.expand_channel='','21',rz.expand_channel),'1') as ad_site_channel,\ngetchannel(if(rz.expand_channel is null or rz.expand_channel='','21',rz.expand_channel),'2')  as pkg_code,\ncase when oz.payment_type is  null  then 0 else oz.payment_type end as pay_type,--0代表未支付\ncast(sum(if(oz.ori_price_all is null,0,oz.ori_price_all))*100 as int) as pay_water,\ncast(sum(if(ver.sandbox is null,0,oz.ori_price_all))*100 as int) as box_water,\ncount(distinct case when to_date(rz.reg_time)=gc.performance_time then gc.game_account else null end) as regi_num  from\n(select distinct  performance_time,game_account,game_id  from   (SELECT distinct to_date(ods_regi_rz.reg_time) as performance_time, lower(trim(game_account)) game_account ,game_id FROM ods_regi_rz where  to_date(ods_regi_rz.reg_time)>='startDay' and  to_date(ods_regi_rz.reg_time)<='endDay' and game_account is not null and game_id is not null  UNION all SELECT distinct to_date(ods_order.order_time) as performance_time,lower(trim(game_account)) game_account,game_id FROM ods_order where  order_status in(4,8) and  to_date(order_time)>='startDay'  and  to_date(order_time)<='endDay' and prod_type=6 and game_account is not null and game_id is not null) gc_per)\ngc\njoin  (select  distinct game_id as parent_game_id , old_game_id  ,system_type,group_id from game_sdk  where state=0) gs on gc.game_id=gs.old_game_id --补全 parent_game_id\njoin  (select  distinct lower(trim(game_account)) game_account, to_date(reg_time) reg_time,expand_channel from ods_regi_rz where game_id is not null and game_account is not null and game_account!='' and reg_time is not null)  rz  on rz.game_account=gc.game_account--注册信息\nleft join  (select distinct order_no,order_time,lower(trim(game_account)) game_account,game_id,(if(ori_price is null,0,ori_price)+if(total_amt is null,0,total_amt)) as ori_price_all,payment_type,order_status from ods_order where order_status=4 and  to_date(order_time)>='startDay' and   to_date(order_time)<='endDay' and prod_type=6) oz on  oz.game_id=gc.game_id and  oz.game_account=gc.game_account  and gc.performance_time=to_date(oz.order_time) --支付信息\nleft join  (select distinct order_sn,sandbox from pywsdk_apple_receipt_verify where sandbox=1 and state=3) ver on ver.order_sn=oz.order_no\ngroup by gc.performance_time,gs.parent_game_id,gc.game_id,gs.system_type,gs.group_id,if(rz.expand_channel is null or rz.expand_channel='','21',rz.expand_channel),oz.payment_type"
        .replace("startDay", startDay).replace("endDay", endDay);
      val df_result_rgi_order = hiveContext.sql(sql_result_rgi_order).persist()
      df_result_rgi_order.registerTempTable("result_rgi_order")

      //第二步  获取关联信息
      // bi_publish_back_cost 由 pkg_code 决定的字段 1个(main_man)
      val sql_result_pkg_code_pre = "select distinct\nresult_rgi_order_cache.performance_time as performance_time,\nresult_rgi_order_cache.pkg_code as pkg_code,\ncase \nwhen users_channel.name is not null  then users_channel.name\nwhen users_medium.name is not null  then users_medium.name \nelse '' end as main_man\nfrom \n(select distinct pkg_code,performance_time from  result_rgi_order) \nresult_rgi_order_cache \nleft join  (select distinct pkg_code , manager from channel_pkg)  channel_pkg  on result_rgi_order_cache.pkg_code=channel_pkg.pkg_code --渠道负责人1\nleft join  (select distinct id ,name from user) users_channel on users_channel.id=channel_pkg.manager--渠道负责人2\nleft join  (select distinct subpackage_id , user_id from medium_package) medium_package on result_rgi_order_cache.pkg_code=medium_package.subpackage_id--媒介账号负责人1\nleft join  (select  distinct id ,name from user) users_medium on users_medium.id=medium_package.user_id--媒介账号负责人2"
      hiveContext.sql(sql_result_pkg_code_pre).registerTempTable("result_pkg_code")
      val sql_result_pkg_code = "select distinct performance_time,pkg_code,main_man from result_pkg_code where main_man!=''"
      val df_result_pkg_code = hiveContext.sql(sql_result_pkg_code).registerTempTable("result_pkg_code")

      // 第三步 关联信息 并 存入数据库
      resetData()
      // 重置数据
      //1.存入 bi_publish_back_performance
      val bi_publish_back_performance = "select \nresult_rgi_order_cache.performance_time performance_time,\nresult_rgi_order_cache.medium_channel medium_channel,\nresult_rgi_order_cache.ad_site_channel ad_site_channel,\nresult_rgi_order_cache.pkg_code pkg_code,\nresult_rgi_order_cache.parent_game_id parent_game_id,\nresult_rgi_order_cache.child_game_id child_game_id,\nresult_rgi_order_cache.pay_water pay_water,\nresult_rgi_order_cache.box_water box_water,\nresult_rgi_order_cache.regi_num regi_num,\nresult_rgi_order_cache.pay_type pay_type,\nif(result_pkg_code_cache.main_man is null,'',result_pkg_code_cache.main_man) main_man\nfrom\n(select distinct performance_time,medium_channel,ad_site_channel,pkg_code,parent_game_id,box_water,child_game_id,pay_water,regi_num,pay_type from  result_rgi_order)result_rgi_order_cache\nleft join (select  performance_time,pkg_code,max(main_man) main_man from result_pkg_code group by  performance_time,pkg_code ) result_pkg_code_cache on result_pkg_code_cache.performance_time=result_rgi_order_cache.performance_time and result_pkg_code_cache.pkg_code=result_rgi_order_cache.pkg_code"
      val df_bi_publish_back_performance = hiveContext.sql(bi_publish_back_performance)
      updateToMysql(df_bi_publish_back_performance, "result_rgi_order_performance")

      //2.存入 bi_publish_back_cost
      val bi_publish_back_cost = "select distinct\nresult_rgi_order_cache.performance_time,\nresult_rgi_order_cache.parent_game_id,\nresult_rgi_order_cache.child_game_id,\nresult_rgi_order_cache.system_type system_type,\nresult_rgi_order_cache.group_id group_id,\nif(corporation_finance.bill_type is null,0,corporation_finance.bill_type) bill_type,\nif(game_base.corporation_id is null,0,game_base.corporation_id) corporation_id\nfrom\n(select distinct performance_time,parent_game_id,child_game_id,system_type,group_id from  result_rgi_order) result_rgi_order_cache\nleft join (select distinct corporation_id,id from game_base) game_base on  result_rgi_order_cache.parent_game_id = game_base.id  --cp名称id ,发票类型 1 \nleft join (select distinct bill_type,corporation_base_id from corporation_finance) corporation_finance on  corporation_finance.corporation_base_id = game_base.corporation_id --cp名称,发票类型2 \n"
      val df_bi_publish_back_cost = hiveContext.sql(bi_publish_back_cost)
      updateToMysql(df_bi_publish_back_cost, "result_rgi_order_cost")

    } else if (mode.equals("second")) {
      //分层比例 动态影响因素： 1.流水 2. 代冲成本
      //cp_cost 动态影响因素 分层比例 ===> 1.流水 2. 代冲成本
      //第四步.更新我方分层比 流水因素
      // 读取流水 取4到5个月的流水
      startDay = DateUtils.LessDay(startDay, 160).substring(0, 7) + "-01"

      SparkUtils.readPerformance(sc, hiveContext, startDay, endDay)
      val df_pay_water_day: DataFrame = hiveContext.sql("select performance_time, parent_game_id, child_game_id, sum(pay_water) pay_water, sum(if(pay_type=5,pay_water,0)) apple_water from bi_publish_back_performance group by performance_time,parent_game_id,child_game_id")
      df_pay_water_day.registerTempTable("pay_water_day")

      // 1.跟随主游戏
      val sql_order_month_parent = "select distinct\norder_month_cache.performance_month as performance_month,\norder_month_cache.parent_game_id as  parent_game_id,\norder_month_cache.pay_water pay_water,\nif(fx_publish_result_base.apple_proxy_recharge is null,0,fx_publish_result_base.apple_proxy_recharge)  apple_proxy_recharge,\nif(game_divide.slotting_fee is null,0,game_divide.slotting_fee) slotting_fee,\nif(game_divide.division_rule = 1,game_divide.my_rate,getdivideladder(CONCAT(\"0\",\"=\",game_divide.my_rate,game_divide_ladder_rz_cache.ladder_price_ratios),cast((order_month_cache.pay_water-if(fx_publish_result_base.apple_proxy_recharge is null,0,fx_publish_result_base.apple_proxy_recharge)) as String))) division_rule \nfrom\n(select substr(performance_time,0,7) as performance_month,parent_game_id,sum(pay_water) as  pay_water from pay_water_day where to_date(performance_time)>= to_date(concat(substr('startDay',0,7),'-01'))and to_date(performance_time)<='endDay' group by  substr(performance_time,0,7),parent_game_id) \norder_month_cache\njoin (select distinct id,slotting_fee,division_rule,game_id,my_rate,audit_state,state,is_adjust,start_month,end_month from game_divide where  game_divide.audit_state =1 and  game_divide.state=0 and  is_adjust=0 and game_sdk_id=0 and follow_main=0) game_divide  on  order_month_cache.parent_game_id = game_divide.game_id  and to_date(concat(order_month_cache.performance_month,'-01')) >= to_date(game_divide.start_month)  and  to_date(concat(order_month_cache.performance_month,'-01')) < to_date(game_divide.end_month) --分层比例1\nleft join (SELECT  divide,contactdivideladder(ladder_price_ratio) as ladder_price_ratios from (select distinct divide ,CONCAT(ladder_price,'=',ladder_ratio) as ladder_price_ratio from game_divide_ladder) game_divide_ladder_cache GROUP BY game_divide_ladder_cache.divide) game_divide_ladder_rz_cache on  game_divide_ladder_rz_cache.divide=game_divide.id--分层比例2\nleft join (select  game_parent_id,substr(cost_date,0,7) as cost_date,sum(apple_proxy_recharge) as apple_proxy_recharge  from fx_publish_result_base where to_date(cost_date)>= to_date(concat(substr('startDay',0,7),'-01')) and to_date(cost_date)<='endDay' group by game_parent_id,substr(cost_date,0,7)) fx_publish_result_base on order_month_cache.performance_month=fx_publish_result_base.cost_date and order_month_cache.parent_game_id=fx_publish_result_base.game_parent_id--分层比例3"
        .replace("startDay", startDay).replace("endDay", endDay);
      val df_order_month_parent = hiveContext.sql(sql_order_month_parent)
      df_order_month_parent.registerTempTable("divide_parent_game_id")

      // 2.不跟随主游戏
      val sql_order_month_child = "select distinct\norder_month_cache.performance_month as performance_month,\norder_month_cache.child_game_id as  child_game_id,\ngame_divide.follow_main follow_main,\norder_month_cache.pay_water pay_water,\nif(fx_publish_result_base.apple_proxy_recharge is null,0,fx_publish_result_base.apple_proxy_recharge) apple_proxy_recharge,\nif(game_divide.slotting_fee is null,0,game_divide.slotting_fee) slotting_fee,\nif(game_divide.division_rule = 1,game_divide.my_rate,getdivideladder(CONCAT(\"0\",\"=\",game_divide.my_rate,game_divide_ladder_rz_cache.ladder_price_ratios),cast((order_month_cache.pay_water-if(fx_publish_result_base.apple_proxy_recharge is null,0,fx_publish_result_base.apple_proxy_recharge)) as String))) division_rule\nfrom \n(select substr(performance_time,0,7) as performance_month,child_game_id,sum(pay_water) as  pay_water from pay_water_day where to_date(performance_time)>= to_date(concat(substr('startDay',0,7),'-01'))and to_date(performance_time)<='endDay' group by  substr(performance_time,0,7),child_game_id) \norder_month_cache\njoin (select  distinct id,old_game_id from game_sdk  where state=0) game_sdk on order_month_cache.child_game_id=game_sdk.old_game_id\njoin (select distinct id ,slotting_fee,division_rule,game_sdk_id,my_rate,audit_state,state,is_adjust,start_month,end_month,follow_main from game_divide where  game_divide.audit_state =1 and  game_divide.state=0 and  is_adjust=0 and game_sdk_id!=0 and follow_main=1) game_divide  on  game_sdk.id = game_divide.game_sdk_id  and to_date(concat(order_month_cache.performance_month,'-01')) >= to_date(game_divide.start_month)  and  to_date(concat(order_month_cache.performance_month,'-01')) < to_date(game_divide.end_month) --子游戏分层比例 1\nleft join (SELECT  divide,contactdivideladder(ladder_price_ratio) as ladder_price_ratios from (select distinct divide ,CONCAT(ladder_price,'=',ladder_ratio) as ladder_price_ratio from game_divide_ladder) game_divide_ladder_cache GROUP BY game_divide_ladder_cache.divide) game_divide_ladder_rz_cache on  game_divide_ladder_rz_cache.divide=game_divide.id --子游戏分层比例  2\nleft join (select  game_sub_id,substr(cost_date,0,7) as cost_date,sum(apple_proxy_recharge) as apple_proxy_recharge  from fx_publish_result_base where to_date(cost_date)>= to_date(concat(substr('startDay',0,7),'-01')) and to_date(cost_date)<='endDay' group by game_sub_id,substr(cost_date,0,7)) fx_publish_result_base on order_month_cache.performance_month=fx_publish_result_base.cost_date and order_month_cache.child_game_id=fx_publish_result_base.game_sub_id--子游戏分层比例 3"
        .replace("startDay", startDay).replace("endDay", endDay);
      val df_order_month_child = hiveContext.sql(sql_order_month_child)
      df_order_month_child.registerTempTable("divide_child_game_id")

      // 3.汇总
      val sql_divide = "select distinct \ngame_id_all.performance_month performance_month,\ngame_id_all.parent_game_id parent_game_id,\ngame_id_all.child_game_id child_game_id,\nif(divide_child_game_id.follow_main=1,divide_child_game_id.slotting_fee,if(divide_parent_game_id.slotting_fee is null,0,divide_parent_game_id.slotting_fee))slotting_fee,\nif(divide_child_game_id.follow_main=1,divide_child_game_id.division_rule,if(divide_parent_game_id.division_rule is null,0,divide_parent_game_id.division_rule))division_rule\nfrom \n(select distinct substr(performance_time,0,7) as performance_month,parent_game_id,child_game_id from pay_water_day where to_date(performance_time)>= to_date(concat(substr('startDay',0,7),'-01'))and to_date(performance_time)<='endDay') game_id_all\nleft join (select performance_month,parent_game_id,slotting_fee,division_rule from divide_parent_game_id)divide_parent_game_id on game_id_all.performance_month=divide_parent_game_id.performance_month and  game_id_all.parent_game_id=divide_parent_game_id.parent_game_id\nleft join (select performance_month,child_game_id,slotting_fee,division_rule,follow_main from divide_child_game_id)divide_child_game_id on game_id_all.performance_month=divide_child_game_id.performance_month and  game_id_all.child_game_id=divide_child_game_id.child_game_id"
        .replace("startDay", startDay).replace("endDay", endDay);
      val df_divide = hiveContext.sql(sql_divide)
      updateToMysql(df_divide, "result_divide_all")
      df_divide.registerTempTable("divide_all")

      // 第五步 更新 cp_cost 和 profit 利润 （同时也取出了 behalf_water-代充流水,behalf_cost-代充成本,publish_cost-投放成本）
      // cp_cost = （当日pay_water-当日代充流水）*(1-slotting_fee)*(1-division_rule)*(1-bill_percent)
      // profit=总自然流水 - cp成本 - 投放成本 -代充成本 - 支付通道费 - （苹果内购-支付方式为5-代充流水） * 0.3
      // 流水因素
      // 1.join 其他信息
      val sql_cost = "select distinct \npay_water_day.performance_time performance_time,\npay_water_day.parent_game_id parent_game_id,\npay_water_day.child_game_id child_game_id,\npay_water_day.pay_water pay_water,\npay_water_day.apple_water apple_water,\nif(fx_publish_result.behalf_water is null,0,fx_publish_result.behalf_water) behalf_water, --代充流水\ncase when corporation_finance.bill_type=3 then 0.0627  when corporation_finance.bill_type=2 then 0.0336 else 0 end as bill_percent,\ndivide_all.slotting_fee   slotting_fee, -- 支付通道费\ndivide_all.division_rule  division_rule, --分成比例\nif(fx_publish_result.behalf_cost is null,0,fx_publish_result.behalf_cost) behalf_cost, --代充成本\nif(fx_publish_result.publish_cost is null,0,fx_publish_result.publish_cost) publish_cost -- 投放成本\nfrom \n(select distinct performance_time,parent_game_id,child_game_id,pay_water,apple_water from pay_water_day where to_date(performance_time)>= to_date(concat(substr('startDay',0,7),'-01'))and to_date(performance_time)<='endDay') pay_water_day\nleft join (select distinct performance_month,child_game_id,slotting_fee,division_rule from divide_all) divide_all on divide_all.child_game_id= pay_water_day.child_game_id and  divide_all.performance_month=substr(pay_water_day.performance_time,0,7) --子游戏分成比 通道费 \nleft join (select distinct corporation_id,id from game_base) game_base on  pay_water_day.parent_game_id = game_base.id  --发票类型 1 \nleft join (select distinct bill_type,corporation_base_id from corporation_finance) corporation_finance on  corporation_finance.corporation_base_id = game_base.corporation_id --发票类型2 \nleft join \n(select  \nfx_publish_result_base.cost_date cost_date,\nfx_publish_result_base.game_sub_id game_sub_id,\nsum(if(fx_publish_result_base.apple_proxy_recharge is null,0,fx_publish_result_base.apple_proxy_recharge)) as behalf_water, --代充流水\nsum(if(fx_publish_result_count.proxy_cost is null,0,fx_publish_result_count.proxy_cost)) as behalf_cost, --代充成本\nsum(if(fx_publish_result_count.publish_cost is null,0,fx_publish_result_count.publish_cost)) as publish_cost  -- 投放成本\nfrom \n(select distinct id ,pkg_id,game_sub_id,apple_proxy_recharge,to_date(cost_date) as cost_date,create_time from fx_publish_result_base ) fx_publish_result_base \njoin (select result_base_id, proxy_cost,publish_cost,profit,update_time from fx_publish_result_count WHERE   count_status=3) fx_publish_result_count \non  fx_publish_result_count.result_base_id = fx_publish_result_base.id group by fx_publish_result_base.cost_date,fx_publish_result_base.game_sub_id\n)fx_publish_result on pay_water_day.performance_time=fx_publish_result.cost_date and pay_water_day.child_game_id=fx_publish_result.game_sub_id"
        .replace("startDay", startDay).replace("endDay", endDay);
      val df_cost = hiveContext.sql(sql_cost)
      df_cost.registerTempTable("cp_cost_cache")

      // 2.计算 cp_cost  和 profit
      val sql_cost_result = "select distinct \ncp_cost_cache.performance_time performance_time ,\ncp_cost_cache.child_game_id child_game_id,\ncp_cost_cache.pay_water pay_water,\ncp_cost_cache.behalf_water behalf_water,\ncp_cost_cache.behalf_cost behalf_cost,\ncp_cost_cache.publish_cost publish_cost,\n(cp_cost_cache.pay_water-cp_cost_cache.behalf_water)*(1-slotting_fee/10000)*(1-division_rule/10000)*(1-bill_percent) cp_cost,  -- cp成本\n((cp_cost_cache.pay_water-cp_cost_cache.behalf_water)*99/100-(cp_cost_cache.pay_water-cp_cost_cache.behalf_water)*(1-slotting_fee/10000)*(1-division_rule/10000)*(1-bill_percent)-cp_cost_cache.publish_cost-cp_cost_cache.behalf_cost-(cp_cost_cache.apple_water-cp_cost_cache.behalf_water)*0.3)  as profit --利润\nfrom\n(select distinct performance_time,child_game_id,pay_water,behalf_water,behalf_cost,publish_cost,bill_percent,slotting_fee,division_rule,apple_water from cp_cost_cache) cp_cost_cache"
      val df_cost_result = hiveContext.sql(sql_cost_result)
      updateToMysql(df_cost_result, "result_cost")
    }


    sc.stop();

  }

  /**
    * 重置数据
    */
  def resetData() = {
    val conn: Connection = JdbcUtil.getConn();
    val stmt = conn.createStatement();
    var sql1 = "delete from bi_publish_back_performance where performance_time>='startDay' and performance_time<='endDay'".replace("startDay", startDay).replace("endDay", endDay)
    var sql2 = "delete from bi_publish_back_cost where performance_time>='startDay' and performance_time<='endDay' ".replace("startDay", startDay).replace("endDay", endDay)
    stmt.execute(sql1)
    stmt.execute(sql2)
    stmt.close()
    conn.close();
  }

  def updateToMysql(df: DataFrame, reuslt: String) = {

    df.foreachPartition(iter => {
      //数据库链接
      val conn = JdbcUtil.getConn();
      val stmtBi = conn.createStatement();

      var sql = ""
      if (reuslt.equals("result_rgi_order_performance")) {
        sql = "INSERT INTO bi_publish_back_performance(performance_time,medium_channel,ad_site_channel,pkg_code,parent_game_id,child_game_id,pay_water,box_water,regi_num,pay_type,main_man) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
      } else if (reuslt.equals("result_rgi_order_cost")) {
        sql = "INSERT INTO bi_publish_back_cost(performance_time,parent_game_id,child_game_id,system_type,group_id,bill_type,corporation_id) VALUES (?,?,?,?,?,?,?)"
      } else if (reuslt.equals("result_divide_all")) {
        sql = "INSERT INTO bi_publish_back_divide(parent_game_id,child_game_id,divide_month,performance_date,division_rule) VALUES (?,?,?,?,?) ON DUPLICATE KEY update performance_date= VALUES(performance_date),division_rule= VALUES(division_rule)"
      } else if (reuslt.equals("result_cost")) {
        sql = "update bi_publish_back_cost set behalf_water=?,behalf_cost=?,publish_cost=?,cp_cost=?,profit=? where performance_time=? and child_game_id=?";
      }
      val yesterday = DateUtils.getYesterDayDate()

      val params = new ArrayBuffer[Array[Any]]()
      iter.foreach(t => {
        if (reuslt.equals("result_rgi_order_performance")) {
          if (t.getAs[String]("medium_channel").length <= 10 && t.getAs[String]("ad_site_channel").length <= 10 && t.getAs[String]("pkg_code").length <= 12) {
            var pay_type: String = t.getAs[String]("pay_type").trim()
            if (pay_type.equals("11")) {
              pay_type = "a"
            } else if (pay_type.equals("100")) {
              pay_type = "b"
            }
            params.+=(Array[Any](t.getAs[Date]("performance_time"), t.getAs[String]("medium_channel"), t.getAs[String]("ad_site_channel"), t.getAs[String]("pkg_code"), t.getAs[Int]("parent_game_id"), t.getAs[Int]("child_game_id"), t.getAs[Int]("pay_water"), t.getAs[Int]("box_water"), t.getAs[Long]("regi_num").toInt, pay_type, t.getAs[String]("main_man")));
          } else {
            Logger.getLogger(GamePublishPerformance.getClass).error("expand_channel format error::::: " + t.toString())
          }
        } else if (reuslt.equals("result_rgi_order_cost")) {
          params.+=(Array[Any](t.getAs[Date]("performance_time"), t.getAs[Int]("parent_game_id"), t.getAs[Int]("child_game_id"), t.getAs[Byte]("system_type"), t.getAs[Int]("group_id"), t.getAs[Byte]("bill_type"), t.getAs[Int]("corporation_id")));
        } else if (reuslt.equals("result_divide_all")) {
          val sql_divide = "update bi_publish_back_cost set slotting_fee='" + (t.getAs[Long]("slotting_fee")).toInt / 100 + "',division_rule='" + (t.getAs[String]("division_rule").toInt / 100).toByte + "' where  left(performance_time,7)='" + t.getAs[String]("performance_month") + "' and child_game_id='" + t.getAs[Int]("child_game_id") + "' and division_rule!='" + (t.getAs[String]("division_rule").toInt / 100).toByte + "'";
          val result_change: Int = stmtBi.executeUpdate(sql_divide);

          if (result_change >= 1) {

            val sql_divide_de = "SELECT distinct division_rule FROM bi_publish_back_divide WHERE child_game_id='" + t.getAs[Int]("child_game_id") + "' and divide_month='" + t.getAs[String]("performance_month") + "'"
            val rz_divide: ResultSet = stmtBi.executeQuery(sql_divide_de)
            if (rz_divide.next()) {
              val division_rule = rz_divide.getString("division_rule");
              if (division_rule != (t.getAs[String]("division_rule").toInt / 100).toByte) {
                // 分成比例发生改变
                params.+=(Array[Any](t.getAs[Int]("parent_game_id"), t.getAs[Int]("child_game_id"), t.getAs[String]("performance_month").toString.substring(0, 7) + "-01", yesterday, (t.getAs[String]("division_rule").toInt / 100).toByte))
              }
              while (rz_divide.next()) {
                if (division_rule != (t.getAs[String]("division_rule").toInt / 100).toByte) {
                  // 分成比例发生改变
                  params.+=(Array[Any](t.getAs[Int]("parent_game_id"), t.getAs[Int]("child_game_id"), t.getAs[String]("performance_month").toString.substring(0, 7) + "-01", yesterday, (t.getAs[String]("division_rule").toInt / 100).toByte))
                }
              }
            } else {
              // 分成比例发生改变
              params.+=(Array[Any](t.getAs[Int]("parent_game_id"), t.getAs[Int]("child_game_id"), t.getAs[String]("performance_month").toString.substring(0, 7) + "-01", yesterday, (t.getAs[String]("division_rule").toInt / 100).toByte))
            }
          }
        } else if (reuslt.equals("result_cost")) {
          params.+=(Array[Any](t.getAs[Int]("behalf_water"), t.getAs[Int]("behalf_cost"), t.getAs[Int]("publish_cost"), Math.round(t.getAs[Double]("cp_cost")).toInt, Math.round(t.getAs[Double]("profit")).toInt, t.getAs[Date]("performance_time"), t.getAs[Int]("child_game_id")));
        }
      })
      JdbcUtil.executeBatch(sql, params)
    })
  }
}
