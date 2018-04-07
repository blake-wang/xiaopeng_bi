package com.xiaopeng.bi.centurioncard

import com.xiaopeng.bi.utils.{Commons, Hadoop}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object CenturionCommonSdkOffLine {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    if (args.length < 1) {
      System.err.println("Usage: <currentday> ")
      System.exit(1)
    }
    if (args.length > 1) {
      System.err.println("参数个数传入太多，固定为1个： <currentday>  ")
      System.exit(1)
    }
    val currentday = args(0)
    //Hadoop libariy
    Hadoop.hd
    //current
    val commSql="select game_id,channel_id,stata_date,sum(new_regi_account) new_regi_account\n,sum(active_num) active_num\n,sum(new_pay_account) new_pay_account\n,sum(new_pay_amount)*100 new_pay_amount\n,sum(total_pay_account) total_pay_account\n,sum(total_pay_amount)*100 total_pay_amount\nfrom \n(\nselect game_id,channel_id,to_date(create_time) stata_date,count(distinct account) new_regi_account,0 active_num,0 new_pay_account,0 new_pay_amount,0 total_pay_account,0 total_pay_amount\nfrom common_sdk_account_tran where to_date(create_time)='currentday'\ngroup by game_id,channel_id,to_date(create_time)\n\nunion all\n--活跃人数\n\nselect cml.game_id,cml.channel_id,login_date stata_date,0 new_regi_account,count(distinct cml.game_account) active_num,0 new_pay_account,0 new_pay_amount,0 total_pay_account,0 total_pay_amount\nfrom default.commlogin cml where login_date='currentday' group by game_id,cml.channel_id,login_date\n\nunion all\n--新增付费人数 新增付费金额\n\nselect  game_id,channel_id,to_date(order_time) stata_date,0 new_regi_account,0 active_num,count(distinct game_account) new_pay_account,sum(ori_price) new_pay_amount,0 total_pay_account,0 total_pay_amount\nfrom\n(\nselect cod.*,dense_rank() over(partition by game_account order by to_date(order_time) asc) rw from default.commorder cod\n) rs where rw=1 and to_date(order_time)='currentday'\ngroup by game_id,channel_id,to_date(order_time)\n\nunion all\n--总付费人数 总付费金额\n\nselect game_id,channel_id,'currentday' stata_date,0 new_regi_account,0 active_num,0 new_pay_account,0 new_pay_amount,count(distinct game_account) total_pay_account,sum(ori_price) total_pay_amount \nfrom default.commorder cod where to_date(order_time)='currentday' group by game_id,channel_id\n) rs  group by game_id,channel_id,stata_date".replace("currentday",currentday)
    val lcSql="select game_id,channel_id,reg_date stata_date,\ncount(distinct case when datediff(login_date,reg_date)=1 then lj.game_account else null end) as retained_2day,\ncount(distinct case when datediff(login_date,reg_date)=2 then lj.game_account else null end) as retained_3day,\ncount(distinct case when datediff(login_date,reg_date)=6 then lj.game_account else null end) as retained_7day,\ncount(distinct case when datediff(login_date,reg_date)=14 then lj.game_account else null end) as retained_15day,\ncount(distinct case when datediff(login_date,reg_date)=29 then lj.game_account else null end) as retained_30day\nfrom \ndefault.commlogin lj where reg_date<='currentday' and date_add('currentday',-30)<=reg_date and login_date<='currentday'\ngroup by game_id,reg_date,channel_id".replace("currentday",currentday)
    val commSqlServArea="select game_id,channel_id,'currentday' stata_date,if(service_area is null,'',service_area) service_area,count(distinct game_account) total_pay_account,sum(ori_price)*100 total_pay_amount \nfrom default.commorder cod where to_date(order_time)='currentday' group by game_id,channel_id,if(service_area is null,'',service_area)".replace("currentday",currentday)
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$","")).set("spark.sql.shuffle.partitions","30")
    val sc = new SparkContext(sparkConf)
    val sqlContext=new HiveContext(sc)

    sqlContext.sql("use yyft")

    sqlContext.sql("drop table default.commorder")
    sqlContext.sql("drop table default.commlogin")
    sqlContext.sql("create table default.commorder as \nselect distinct order_no,order_time,ori_price,game_account,od.game_id,sdk.channel_id,od.servarea service_area from ods_order od \njoin common_sdk_account_tran sdk on sdk.account=trim(lower(od.game_account))\nwhere od.order_status=4".replace("currentday",currentday))
    sqlContext.sql("create table default.commlogin as \nselect distinct lg.game_id,sdk.channel_id,to_date(lg.login_time) login_date,lg.game_account,to_date(sdk.create_time) reg_date from ods_login lg \njoin common_sdk_account_tran sdk on sdk.account=trim(lower(lg.game_account))\nwhere to_date(login_time)<=date_add('currentday',30) and date_add('currentday',-30)<=to_date(login_time) \nand to_date(sdk.create_time)>='2017-07-13' and to_date(login_time)>=to_date(sdk.create_time)".replace("currentday",currentday))

    val commDf: DataFrame =sqlContext.sql(commSql)
    val lcDf=sqlContext.sql(lcSql)
    val servAreaDf=sqlContext.sql(commSqlServArea)
    val istCommSql="insert into bi_centurion_commonsdk(game_id,channel_id,stata_date,new_regi_account,active_num,new_pay_account,new_pay_amount,total_pay_account,total_pay_amount)" +
                   " values(?,?,?,?,?,?,?,?,?) " +
                   " on duplicate key update new_regi_account=?,active_num=?,new_pay_account=?,new_pay_amount=?,total_pay_account=?,total_pay_amount=?"
    val istLcSql=" insert into bi_centurion_commonsdk(game_id,channel_id,stata_date,retained_2day,retained_3day,retained_7day,retained_15day,retained_30day)" +
                 " values(?,?,?,?,?,?,?,?)" +
                 " on duplicate key update retained_2day=?,retained_3day=?,retained_7day=?,retained_15day=?,retained_30day=?"
    val instCommSqlServArea = "insert into bi_centurion_commonsdk_serv(game_id,channel_id,stata_date,service_area,total_pay_account,total_pay_amount)" +
      " values(?,?,?,?,?,?)" +
      " on duplicate key update total_pay_account=?,total_pay_amount=?"
    /**推送数据到mysql*/
    Commons.processDbFct(commDf,istCommSql)
    Commons.processDbFct(lcDf,istLcSql)
    Commons.processDbFct(servAreaDf,instCommSqlServArea)
    System.clearProperty("spark.driver.port")
    sc.stop()
  }



}
