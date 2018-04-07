package com.xiaopeng.bi.centurioncard.rebate

import java.sql.PreparedStatement

import com.xiaopeng.bi.utils.{ConfigurationUtil, JdbcUtil}
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object bi_centurioncard_sub_rep {

  /**
    * 执行下级返点
    * @param rcid 年月id
    * @param syear 年
    * @param smonth 月
    */
  def sub_reb(rcid:Int,confType:Int,syear:String,smonth:String,sc:SparkContext): Unit = {

    val currentrcid =rcid.toString
    val year = syear
    val month=smonth
    val pi_yearmonth=syear.concat(smonth)
    /* 捕获异常开始 */
    try {
       var hivesql=""
       var ad_pu_role=""
       var ad_sub_role=""
       val mysqlsql = "insert into bi_centurioncard_sub_rebate(rcid,syearmonth,member_id,user_account,user_type,group_id,group_name,accu_ori_price,month_ori_price,sub_users,sub_month_ori_price,rebate_type,rebate_price,rebate_rate,status,conf_type)" +
        " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

      //排除渠道黑金卡累计流水
      val ad_centurioncard_reb_lj="insert overwrite table advance.ad_centurioncard_reb_lj partition(ym='pi_yearmonth')\nselect member_id,syearmonth,game_id,sum(lj_oriprice) lj_oriprice,syear,smonth,user_account\n from advance.ad_centurioncard_reb_detail_lj reb \nleft JOIN\n(\nselect channel_id,cf.year,cf.month from rebate_config_channel ch \nJOIN rebate_config cf on cf.id=ch.rcid\nwhere ch.status=1 and ch.rcid=currentrcid and cf.id=currentrcid and cf.status=4\n) pz on reb.syearmonth=concat(pz.year,pz.month) and reb.channel_id=pz.channel_id\nwhere reb.ym='pi_yearmonth' and pz.channel_id is null\ngroup by member_id,syearmonth,syear,smonth,game_id,user_account,user_type,recharge_type,discount".replace("pi_yearmonth",pi_yearmonth).replace("currentrcid",currentrcid)

      //黑金排除特殊游戏后的累计流水和流水(advance.ad_pu_price_exp_gamechannel),疯趣游戏统计不需要排除，当conftype参数为2时为疯趣一下
      var ad_pu_price_exp_gamechannel=""
      if(confType.toInt==1)
        {
          ad_pu_price_exp_gamechannel=" \ninsert overwrite table advance.ad_pu_price_exp_gamechannel \nselect mon.rcid,mon.syearmonth,lj.lj_oriprice,mon.oriprice,basciinfo.member_id,username,user_type,group_id,group_name from \n--basci info，获取黑金卡基本信息\n(\nselect pu.member_id,pu.username,pu.status+1 as user_type,pg.id group_id,pg.name as group_name from promo_user pu left join promo_user_group pg on pg.id=pu.group_id and pu.member_grade=1\ngroup by  pu.member_id,pu.username,pu.status,pg.id,pg.name\n) basciinfo\n\njoin\n--month oriprice，排除特殊游戏后月流水\n(\n  select member_id,syearmonth,currentrcid as rcid,sum(oriprice) oriprice from  \n    (select member_id,syearmonth,currentrcid as rcid,reb.game_id,reb.user_account,sum(oriprice) oriprice from advance.ad_centurioncard_reb reb \n     left join \n        (    \n          select distinct b.game_id,b.rcid,a.year,month from rebate_config a \n          join rebate_config_item_game b on  a.id=b.rcid and b.status=0 and a.status!=3  and b.type=1 and b.rcid=currentrcid\n         ) tsg on tsg.game_id=reb.game_id and tsg.year=reb.syear and tsg.month=reb.smonth\n     where ym='pi_yearmonth' and recharge_type='0,1,2' and tsg.game_id is null\n     group by member_id,syearmonth,reb.game_id,reb.user_account\n    ) rs left join\n    (\n      select distinct b.game_id,a.tel as user_account from rebate_config_item a \n      join rebate_config_item_game b on  a.rcid=b.rcid  and b.type=a.type and a.group_num=b.group_num and b.rcid=currentrcid\n      where  b.status=0 and a.status!=3 and a.type=2\n     ) tp on tp.game_id=rs.game_id and tp.user_account=rs.user_account\n    where tp.user_account is null group by  member_id,syearmonth,rcid\n) mon\non mon.member_id=basciinfo.member_id\n\njoin\n\n--to current day oriprice,累计流水\n(\n select member_id,syearmonth,currentrcid as rcid,sum(lj_oriprice) lj_oriprice from \n  (\n      select member_id,reb.game_id,reb.user_account,syearmonth,currentrcid rcid,sum(lj_oriprice) lj_oriprice from advance.ad_centurioncard_reb_lj reb \n      left join  --排除特殊游戏\n      (    \n      select distinct b.game_id,b.rcid,a.year,month from rebate_config a \n      join rebate_config_item_game b on  a.id=b.rcid and b.status=0 and a.status=4 and b.type=1 and b.rcid=currentrcid  ) tsg on tsg.game_id=reb.game_id and reb.syearmonth=concat(tsg.year,tsg.month)\n      where ym='pi_yearmonth'  and tsg.game_id is null\n      group by member_id,syearmonth,rcid,reb.game_id,user_account\n  ) rs left join --排除特殊审批游戏和黑金卡\n (\n   select distinct b.game_id,a.tel as user_account from rebate_config_item a \n   join rebate_config_item_game b on  a.rcid=b.rcid  and b.type=a.type and a.group_num=b.group_num and b.rcid=currentrcid\n   where  b.status=0 and a.status!=3 and a.type=2\n ) tp\n   on tp.game_id=rs.game_id and tp.user_account=rs.user_account where tp.user_account is null\n  group by  member_id,syearmonth,rcid\n) lj\non lj.member_id=basciinfo.member_id".replace("pi_yearmonth",pi_yearmonth).replace("currentrcid",currentrcid)
          hivesql = "select rcid,syearmonth,member_id,if(username is null,'',username) username,user_type,if(group_id is null,0,group_id) group_id,if(group_name is null,'',group_name) group_name,\n if(lj_oriprice is null,0,lj_oriprice)*100 lj_oriprice,if(oriprice is null,0,oriprice)*100 oriprice,sub_pus,if(sub_mon_price is null,0,sub_mon_price*100) sub_mon_price,\n rebate_type,case when rebate_price is null then 0 else rebate_price end rebate_price,rebate_rate,1 as status,confType as conf_type from advance.ad_pu_price_exp_gamechannel_3\n union\nselect rcid,syearmonth,member_id,if(username is null,'',username) username,user_type,if(group_id is null,0,group_id) group_id,if(group_name is null,'',group_name) group_name,\n if(lj_oriprice is null,0,lj_oriprice)*100 lj_oriprice,if(oriprice is null,0,oriprice)*100 oriprice,sub_pus,if(sub_mon_price is null,0,sub_mon_price*100) sub_mon_price,\n rebate_type,case when rebate_price is null then 0 else rebate_price end rebate_price,rebate_rate,1 as status,confType as conf_type from advance.ad_pu_price_exp_gamechannel_4"
          //黑金卡满足条件数据处理
          ad_pu_role="insert overwrite table  advance.ad_pu_price_exp_gamechannel_3 select pup.rcid,pup.syearmonth,pup.member_id,pup.username,pup.user_type,pup.group_id,pup.group_name,lj.lj_oriprice,reb.oriprice\n,count(subpu.member_id) sub_pus\n,if(sum(subpu.oriprice) is null,0,sum(subpu.oriprice)) sub_mon_price\n,3 as rebate_type\n,if(sum(subpu.oriprice) is null,0,rebate_scale*sum(subpu.oriprice)) rebate_price\n,rebate_scale*10000 as rebate_rate\nfrom advance.ad_pu_price_exp_gamechannel pup\njoin (select member_id,syearmonth,sum(oriprice) oriprice from advance.ad_centurioncard_reb where recharge_type='0,1,2' and ym='pi_yearmonth' group by member_id,syearmonth) reb on reb.member_id=pup.member_id and pup.syearmonth=reb.syearmonth \njoin (select member_id,syearmonth,sum(lj_oriprice) lj_oriprice from advance.ad_centurioncard_reb_lj where ym='pi_yearmonth' group by member_id,syearmonth) lj on lj.member_id=pup.member_id and pup.syearmonth=reb.syearmonth \njoin rebate_config_item item on pup.rcid=item.rcid  and item.type=3 and item.status=0  --返点类型为3，并且累计和月流水大于配置值\nleft join \n(select pu.member_id,owner_id,pr.oriprice,member_grade from  advance.ad_pu_price_exp_gamechannel pr join promo_user pu on pu.member_id=pr.member_id) subpu  --下级流水和下级数\non subpu.owner_id=pup.member_id \nwhere reb.oriprice>=month_number and lj.lj_oriprice>=accumulated_number\ngroup by\n pup.rcid,pup.syearmonth,pup.member_id,pup.username,pup.user_type,pup.group_id,pup.group_name,lj.lj_oriprice,reb.oriprice,rebate_scale".replace("pi_yearmonth",pi_yearmonth)
          //黑金卡下级满足条件规则数据处理
          ad_sub_role="insert overwrite table advance.ad_pu_price_exp_gamechannel_4  select pup.rcid,pup.syearmonth,pup.member_id,pup.username,pup.user_type,pup.group_id,pup.group_name,lj.lj_oriprice,reb.oriprice\n,count(subpu.member_id) sub_pus\n,if(sum(subpu.oriprice) is null,0,sum(subpu.oriprice)) sub_mon_price\n,4 as rebate_type\n,if(sum(subpu.oriprice) is null,0,rebate_scale*sum(subpu.oriprice)) rebate_price\n,rebate_scale*10000 as rebate_rate\nfrom advance.ad_pu_price_exp_gamechannel pup\njoin \n--计算下级数量和下级月流水，用来判断是否符合返点条件\n   ( select pu.owner_id as member_id,reb.syearmonth,reb.rcid,sum(case when reb.oriprice>=sub_month_number then 1 else 0 end) sub_users\n     from (select member_id,rcid,syearmonth,sum(oriprice) oriprice from advance.ad_centurioncard_reb where recharge_type='0,1,2' and ym='pi_yearmonth' group by member_id,syearmonth,rcid) reb join promo_user pu on pu.member_id=reb.member_id\n     join rebate_config_item item on reb.rcid=item.rcid  and item.type=4 and item.status=0\n     where pu.owner_id!=0 \n     group by pu.owner_id,reb.syearmonth,reb.rcid\n   ) subinfo on subinfo.member_id=pup.member_id and subinfo.syearmonth=pup.syearmonth\njoin rebate_config_item item on subinfo.rcid=item.rcid and  sub_users>=sub_number and item.type=4 and item.status=0  --类型4，并且符合人数和流水数\n--累计流水和月流水\njoin (select member_id,syearmonth,sum(oriprice) oriprice from advance.ad_centurioncard_reb where recharge_type='0,1,2' and ym='pi_yearmonth' group by member_id,syearmonth) reb on reb.member_id=pup.member_id and pup.syearmonth=reb.syearmonth \njoin (select member_id,syearmonth,sum(lj_oriprice) lj_oriprice from advance.ad_centurioncard_reb_lj where  ym='pi_yearmonth'  group by member_id,syearmonth) lj on lj.member_id=pup.member_id and pup.syearmonth=lj.syearmonth \n--对于已经符合条件的黑金卡返点，需要计算所有下级流水\nleft join (\n           select pu.member_id,owner_id,pr.oriprice,member_grade from  advance.ad_pu_price_exp_gamechannel pr \n           join promo_user pu on pu.member_id=pr.member_id\n          ) subpu \non subpu.owner_id=pup.member_id \n--排除优先级大的黑金卡返点规则\nleft join advance.ad_pu_price_exp_gamechannel_3 pu_3 on pu_3.member_id=pup.member_id and pu_3.rcid=pup.rcid\nwhere pu_3.member_id is null\ngroup by\n pup.rcid,pup.syearmonth,pup.member_id,pup.username,pup.user_type,pup.group_id,pup.group_name,lj.lj_oriprice,reb.oriprice,rebate_scale  ".replace("pi_yearmonth",pi_yearmonth)

        }else
        {
          println("-----------------------------执行疯趣返点-------------------------------")
          ad_pu_price_exp_gamechannel="  insert overwrite table advance.ad_pu_price_exp_gamechannel \nselect mon.rcid,mon.syearmonth,lj.lj_oriprice,mon.oriprice,basciinfo.member_id,username,user_type,group_id,group_name from \n--basci info，获取黑金卡基本信息\n(\nselect pu.member_id,pu.username,pu.status+1 as user_type,pg.id group_id,pg.name as group_name from promo_user pu left join promo_user_group pg on pg.id=pu.group_id and pu.member_grade=1\ngroup by  pu.member_id,pu.username,pu.status,pg.id,pg.name\n) basciinfo\n\njoin\n--month oriprice疯趣不用排除游戏\n(\n select member_id,syearmonth,currentrcid as rcid,reb.user_account,sum(oriprice) oriprice from advance.ad_centurioncard_reb reb \n   where ym='pi_yearmonth' and recharge_type='0,1,2'\n   group by member_id,syearmonth,reb.user_account\n) mon\non mon.member_id=basciinfo.member_id\n\njoin\n\n--to current day oriprice,累计流水\n(\n  select member_id,reb.user_account,syearmonth,currentrcid rcid,sum(lj_oriprice) lj_oriprice from advance.ad_centurioncard_reb_lj reb \n   where ym='pi_yearmonth' \n    group by member_id,syearmonth,user_account\n) lj\non lj.member_id=basciinfo.member_id".replace("pi_yearmonth",pi_yearmonth).replace("currentrcid",currentrcid)
          hivesql="select rcid,syearmonth,member_id,if(username is null,'',username) username,user_type,if(group_id is null,0,group_id) group_id,if(group_name is null,'',group_name) group_name,\n if(lj_oriprice is null,0,lj_oriprice)*100 lj_oriprice,if(oriprice is null,0,oriprice)*100 oriprice,sub_pus,if(sub_mon_price is null,0,sub_mon_price*100) sub_mon_price,\n rebate_type,case when rebate_price is null then 0 else rebate_price end rebate_price,rebate_rate,1 as status,confType as conf_type from advance.ad_pu_price_exp_gamechannel_4 "
          ad_sub_role=" insert overwrite table advance.ad_pu_price_exp_gamechannel_4  \n select pup.rcid,pup.syearmonth,pup.member_id,pup.username,pup.user_type,pup.group_id,pup.group_name,lj.lj_oriprice,reb.oriprice\n,count(subpu.member_id) sub_pus\n,if(sum(subpu.oriprice) is null,0,sum(subpu.oriprice)) sub_mon_price\n,3 as rebate_type\n,if(sum(subpu.oriprice) is null,0,rebate_scale*sum(subpu.oriprice)) rebate_price\n,rebate_scale*10000 as rebate_rate\nfrom advance.ad_pu_price_exp_gamechannel pup\njoin \n--计算下级数量和下级月流水，用来判断是否符合返点条件\n   ( select pu.owner_id as member_id,reb.syearmonth,reb.rcid,sum(case when reb.oriprice>=sub_month_number then 1 else 0 end) sub_users\n     from (select member_id,rcid,syearmonth,sum(oriprice) oriprice from advance.ad_centurioncard_reb where recharge_type='0,1,2' and ym='pi_yearmonth' group by member_id,syearmonth,rcid) reb join promo_user pu on pu.member_id=reb.member_id\n     join rebate_config_item item on reb.rcid=item.rcid  and item.type=4 and item.status=0\n     where pu.owner_id!=0 \n     group by pu.owner_id,reb.syearmonth,reb.rcid\n   ) subinfo on subinfo.member_id=pup.member_id and subinfo.syearmonth=pup.syearmonth\njoin rebate_config_item item on subinfo.rcid=item.rcid and  sub_users>=sub_number and item.type=4 and item.status=0  --类型4，并且符合人数和流水数\n--累计流水和月流水\njoin (select member_id,syearmonth,sum(oriprice) oriprice from advance.ad_centurioncard_reb where recharge_type='0,1,2' and ym='pi_yearmonth' group by member_id,syearmonth) reb on reb.member_id=pup.member_id and pup.syearmonth=reb.syearmonth \njoin (select member_id,syearmonth,sum(lj_oriprice) lj_oriprice from advance.ad_centurioncard_reb_lj where  ym='pi_yearmonth'  group by member_id,syearmonth) lj on lj.member_id=pup.member_id and pup.syearmonth=lj.syearmonth \n--对于已经符合条件的黑金卡返点，需要计算所有下级流水\nleft join (\n           select pu.member_id,owner_id,pr.oriprice,member_grade from  advance.ad_pu_price_exp_gamechannel pr \n           join promo_user pu on pu.member_id=pr.member_id\n          ) subpu \non subpu.owner_id=pup.member_id \ngroup by\n pup.rcid,pup.syearmonth,pup.member_id,pup.username,pup.user_type,pup.group_id,pup.group_name,lj.lj_oriprice,reb.oriprice,rebate_scale  ".replace("pi_yearmonth",pi_yearmonth)
        }
     //全部转为小写，后面好判断
      val execSql = hivesql.replace("currentrcid", currentrcid).replace("confType",confType.toString)//hive sql
      val sql2Mysql = mysqlsql.replace("|", " ").toLowerCase
      //获取values（）里面有多少个?参数，有利于后面的循环
      val startValuesIndex = sql2Mysql.indexOf("(?") + 1
      val endValuesIndex = sql2Mysql.indexOf("?)") + 1
      //values中的个数
      val valueArray: Array[String] = sql2Mysql.substring(startValuesIndex, endValuesIndex).split(",") //两个（？？）中间的值

      /********************hive库操作 *******************/
      val sqlContext = new HiveContext(sc)
      sqlContext.sql(ConfigurationUtil.getProperty("hivedb"))
      //黑金卡累计流水
      sqlContext.sql(ad_centurioncard_reb_lj).coalesce(1)
      //执行排除渠道排除特殊游戏后的累计流水和流水
      sqlContext.sql(ad_pu_price_exp_gamechannel).coalesce(1)
      //黑金卡满足条件规则
       if(confType.toInt==1) {
         sqlContext.sql(ad_pu_role).coalesce(1)
       }
      //下级满足条件规则
      sqlContext.sql(ad_sub_role).coalesce(1)
      //执行最后结果
      val dataf = sqlContext.sql(execSql).coalesce(2) //执行hive sql
      dataf.show()
      /********************数据库操作 *******************/
      dataf.foreachPartition(rows => {
        val conn = JdbcUtil.getConn()
        conn.setAutoCommit(false)
        val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
        ps.clearBatch()
        for (x <- rows) {
          //补充value值
          for (rs <- 0 to valueArray.length - 1) {
            ps.setString(rs.toInt + 1, x.get(rs).toString)
          }
          ps.addBatch()
        }
        ps.executeBatch()
        conn.commit()
        conn.close()
      }
      )
    }
    /**
      * 捕获异常结束
     */
    catch
      { case e: Exception => e.printStackTrace
        val conn = JdbcUtil.getConn()
        val ps: PreparedStatement = conn.prepareStatement("update bi_rebate_exec_status set status=2 where rcid=currentrcid".replace("currentrcid", currentrcid))
        ps.executeUpdate()
        conn.close()
      }
  }

  /**
    * 执行下级返点明细
    *
    * @param rcid 年月id
    * @param syear 年
    * @param smonth 月
    */

  def sub_reb_detail(rcid:Int,confType:Int,syear:String,smonth:String,sc:SparkContext): Unit = {
    val currentrcid =rcid.toString
    val year = syear
    val month=smonth
    val pi_yearmonth=syear.concat(smonth)
    /*
    捕获异常开始
     */
    try {
      //current
      val hivesql = " select \npu.rcid,\npu.syearmonth,\nif(basciinfo.member_id is null,0,basciinfo.member_id) as owner_id,\nif(basciinfo.username is null,'',basciinfo.username) username,\nif(user_type is null,1,user_type) user_type,\nif(group_id is null,0,group_id) group_id,\nif(group_name is null,'',group_name) group_name,\nif(pu.username is null,0,pu.username) sub_user_account,\nif(pu.oriprice is null,0,pu.oriprice)*100 as sub_month_ori_price,\nconfType as conf_type from \n--黑金卡基本信息\n(\n   select pu.member_id,pu.username,pu.status+1 as user_type,pg.id group_id,pg.name as group_name from promo_user pu left join promo_user_group pg on pg.id=pu.group_id and pu.member_grade=1\n   group by  pu.member_id,pu.username,pu.status,pg.id,pg.name\n) basciinfo\n join (select pu.member_id,pu.username,owner_id,pr.oriprice,member_grade,pr.rcid,pr.syearmonth from  advance.ad_pu_price_exp_gamechannel pr \n           join promo_user pu on pu.member_id=pr.member_id\n       ) pu on pu.owner_id=basciinfo.member_id  "
      val mysqlsql = " insert into bi_centurioncard_reb_sub_deta(rcid,syearmonth,member_id,user_account,user_type,group_id,group_name,sub_user_account,sub_month_ori_price,conf_type) "+
        " values(?,?,?,?,?,?,?,?,?,?)"

      //全部转为小写，后面好判断
      val execSql = hivesql.replace("currentrcid", currentrcid).replace("pi_yeramonth",pi_yearmonth).replace("confType",confType.toString) //hive sql
      val sql2Mysql = mysqlsql.replace("|", " ").toLowerCase
      //获取values（）里面有多少个?参数，有利于后面的循环
      val startValuesIndex = sql2Mysql.indexOf("(?") + 1
      val endValuesIndex = sql2Mysql.indexOf("?)") + 1
      //values中的个数
      val valueArray: Array[String] = sql2Mysql.substring(startValuesIndex, endValuesIndex).split(",") //两个（？？）中间的值

      /********************hive库操作 *******************/
      val sqlContext = new HiveContext(sc)
      sqlContext.sql(ConfigurationUtil.getProperty("hivedb"))
      //执行最后结果
      val dataf = sqlContext.sql(execSql) //执行hive sql
      dataf.show()
      /********************数据库操作 *******************/
      dataf.collect().foreach(x=>
        {
          val conn=JdbcUtil.getConn()
          val ps : PreparedStatement =conn.prepareStatement(sql2Mysql)
          //补充value值
          for(rs<-0 to valueArray.length-1)
          {
            ps.setString(rs.toInt+1,x.get(rs).toString)
          }
           ps.executeUpdate()
          conn.close()
        })
    }

    /**
      * 捕获异常结束
      */
    catch
      { case e: Exception => e.printStackTrace
        val conn = JdbcUtil.getConn()
        val ps: PreparedStatement = conn.prepareStatement("update bi_rebate_exec_status set status=2 where rcid=currentrcid".replace("currentrcid", currentrcid))
        ps.executeUpdate()
        conn.close()
      }


  }


}