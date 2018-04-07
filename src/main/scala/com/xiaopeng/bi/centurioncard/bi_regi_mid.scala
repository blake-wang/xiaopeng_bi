package com.xiaopeng.bi.centurioncard

import java.io.File

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object bi_regi_mid {

  def main(args: Array[String]): Unit = {
    //日志输出警告
    val insertODSREGI="insert overwrite table yyft.ods_regi\nselect \ndistinct\n \"bi_regi\" regi\n,\"00\" requestid\n,IF(bind_member_id='',NULL,bind_member_id) as userid\n,regi.game_account\n,game_id\n,regi.reg_time\n,case when (prod_type in (1,3) ) then 2\n     when (regi.reg_resource=8) then 1\n     else 3 end reg_resource\n,channel_id\n,if(promo.owner_id is null,'0',promo.owner_id) owner_id\n,IF(bind_member_id='',NULL,bind_member_id) bind_member\n,regi.status status\n,if(reg_os_type is null or reg_os_type='','UNKNOW',reg_os_type) reg_os_type\n,expand_code\n,expand_channel\n,od.order_time as bind_uid_time\n,regi.firstlogintime\n,imei\n,promo.reg_time relate_time\n from \n(\nselect regi\n,requestid\n,userid\n,trim(game_account) game_account\n,game_id\n,reg_time\n,reg_resource\n,channel_id\n,owner_id\n,bind_member_id\n,rz.status\n,reg_os_type\n,expand_code\n,expand_channel\n,null as bind_uid_time\n,null as firstlogintime\n,imei\n,row_number() over(partition by game_account order by reg_time) rw \nfrom yyft.ods_regi_rz rz where game_id is not null\n) regi join default.promo promo\non trim(promo.game_account)=trim(regi.game_account) and regi.rw=1\nleft join\n(select order_time,prod_type,game_account,row_number() over(partition by lower(game_account) order by order_time asc) as rw from yyft.v_ods_order where order_status=4) od on trim(regi.game_account)=lower(od.game_account) and od.rw=1"
    val createODSRZ="create table default.ods_rz as select * from ods_regi_rz rz where reg_resource in(6,8) or (rz.owner_id is not null and rz.owner_id!=0)"
    val createPROMOMID="create table default.promo_mid as \nselect rz.owner_id owner_id,reg_time,game_account from default.ods_rz rz\nwhere (rz.owner_id is not null and rz.owner_id!=0)\nunion all\nselect if(rz.owner_id is null or rz.owner_id='0',pu.member_id,rz.owner_id) owner_id,reg_time,game_account from default.ods_rz rz join yyft.promo_user pu on pu.code=regexp_replace(rz.expand_code,'\\\\.','')\nwhere  reg_resource=8 and rz.expand_code is not null\nunion all\nselect if(rz.owner_id is null or rz.owner_id='0',pu.member_id,rz.owner_id) owner_id,reg_time,game_account from default.ods_rz rz join yyft.promo_user pu on pu.username=rz.bind_member_id\nwhere  reg_resource=6\nunion all\nselect if(rz.owner_id is null or rz.owner_id='0',pu.member_id,rz.owner_id) owner_id,reg_time,game_account from default.ods_rz rz join yyft.promo_user pu on pu.username=rz.bind_member_id\nwhere  reg_resource=8 and (expand_code is null or expand_code='') and (rz.owner_id=0 or rz.owner_id is null )"
    val createPROMO="create table default.promo as\nselect owner_id,min(reg_time) reg_time,game_account from default.promo_mid rs group by owner_id,game_account"

    //Hadoop libariy
    val path: String = new File(".").getCanonicalPath
    System.getProperties().put("hadoop.home.dir", path)
    new File("./bin").mkdirs()
    new File("./bin/winutils.exe").createNewFile()

    /********************hive库操作*******************/
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$",""))
    val sc = new SparkContext(sparkConf)
    val sqlContext=new HiveContext(sc)
    sqlContext.sql("use yyft")
    sqlContext.sql("drop table default.ods_rz")
    sqlContext.sql("drop table default.promo_mid")
    sqlContext.sql("drop table default.promo")

    sqlContext.sql(createODSRZ)
    sqlContext.sql(createPROMOMID)
    sqlContext.sql(createPROMO)
    sqlContext.sql(insertODSREGI)
    /********************数据库操作***************   ****/
    System.clearProperty("spark.driver.port")
    sc.stop()

  }

}