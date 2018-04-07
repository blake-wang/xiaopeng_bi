package com.xiaopeng.bi.centurioncard.rebate

import java.sql.PreparedStatement

import com.xiaopeng.bi.utils.{ConfigurationUtil, Hadoop, JdbcUtil}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object bi_centurioncard_reb_deta {
  def main(args: Array[String]): Unit = {
    //Hadoop libariy
    Hadoop.hd

    if (args.length < 4) {
      System.err.println("Usage:  <rcid> <year> <month> <conftype> ")
      System.exit(1)
    }
    if (args.length > 4) {
      System.err.println("参数个数传入太多，固定为4个： <rcid><year><month><conftype>")
      System.exit(1)
    }
    val currentrcid = args(0)

    val year = args(1).toString
    val month=args(2).toString
    val confType=args(3)
    val pi_yearmonth=year.concat(month)
    /* 捕获异常开始 */
    try {
      val hivesql = "select ts.*,if(gp.id is null,0,gp.id) group_id,if(gp.name is null,'',gp.name) group_name,confType as conf_type  from advance.ad_tmp_centurioncard_reb_ts ts join promo_user pu on pu.member_id=ts.member_id left join promo_user_group gp on gp.id=pu.group_id where rcid=currentrcid\nunion all\nselect ts.*,if(gp.id is null,0,gp.id) id,if(gp.name is null,'',gp.name),confType as conf_type from advance.ad_tmp_centurioncard_reb_tp ts join promo_user pu on pu.member_id=ts.member_id left join promo_user_group gp on gp.id=pu.group_id where rcid=currentrcid\nunion all\nselect ts.*,if(gp.id is null,0,gp.id) id,if(gp.name is null,'',gp.name),confType as conf_type from advance.ad_tmp_centurioncard_reb_gd ts join promo_user pu on pu.member_id=ts.member_id left join promo_user_group gp on gp.id=pu.group_id where rcid=currentrcid\n"
      val mysqlsql = " insert into bi_centurioncard_reb_deta(syearmounth,member_id,user_account,user_type,game_id,game_name ,rebate_type,recharge_type,ori_price,rebate_price,rebate_rate,rcid,group_id,group_name,conf_type)" +
        " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
      //返点配置item加工处理
      val ad_rebate_item1="insert overwrite table advance.ad_rebate_item \nselect distinct a.tel,a.rcid,a.type,a.min_number,if(a1.max_number is null,999999999,a1.max_number),a.rebate_scale,a.top_up_type,a.group_num,exclude_discount\n      from (select tel,rcid,type,a.min_number,a.rebate_scale,a.top_up_type,group_num,exclude_discount,row_number() over(partition by rcid,top_up_type,group_num,type order by min_number asc) as rw from\n  rebate_config_item a where a.status=0 and a.type in(0,1,2) ) a left join\n            (select tel,rcid,type,a.min_number as  max_number,a.rebate_scale,a.top_up_type,group_num,row_number() over(partition by rcid,top_up_type,group_num,type order by min_number asc) as rw from\n            rebate_config_item a where  a.status=0 and a.type in(0,1,2) ) a1 \n             on a.rcid=a1.rcid and a.top_up_type=a1.top_up_type and a.type=a1.type and a.rw+1=a1.rw and a.group_num=a1.group_num\n             where a.type in(0,1) and a.rcid=currentrcid".replace("currentrcid",currentrcid)
      val ad_rebate_item2="insert into table  advance.ad_rebate_item   \nselect tel,rcid,type,a.min_number,a.max_number,a.rebate_scale,a.top_up_type,group_num,exclude_discount from \nrebate_config_item a where a.status=0 and a.type=2 and a.rcid=currentrcid".replace("currentrcid",currentrcid)
      //排除渠道，对流水数据根据排除渠道进行再加工
      val ad_centurioncard_reb=" insert overwrite table advance.ad_centurioncard_reb partition(ym='pi_yearmonth')\nselect member_id,syearmonth,syear,smonth,game_id,user_account,user_type,recharge_type,sum(oriprice) oriprice,0 as channel_id,discount,rcid\n from advance.ad_centurioncard_reb_detail reb \nleft JOIN\n(\nselect channel_id,cf.year,cf.month from rebate_config_channel ch \nJOIN rebate_config cf on cf.id=ch.rcid\nwhere ch.status=1 and ch.rcid=currentrcid and cf.id=currentrcid and cf.status=4\n) pz on reb.syear=pz.year and reb.smonth=pz.month and reb.channel_id=pz.channel_id\njoin (select distinct id as rcid,year,month from rebate_config a where a.status!=3 and type=confType and id=currentrcid) rcd on reb.syear=rcd.year and reb.smonth=rcd.month\nwhere reb.ym='pi_yearmonth' and pz.channel_id is null\ngroup by member_id,syearmonth,syear,smonth,game_id,user_account,user_type,recharge_type,discount,rcid".replace("pi_yearmonth",pi_yearmonth).replace("currentrcid",currentrcid).replace("confType",confType)
      //优先执行特批
      val ad_tmp_centurioncard_reb_tp="\ninsert overwrite table  advance.ad_tmp_centurioncard_reb_tp \nselect \n oprice.syearmonth\n,oprice.member_id\n,user_account\n,int(pu.status)+1 as user_type\n,tp.game_id\n,game_name\n,3 rebate_type\n,oprice.recharge_type\n--流水\n,oriprice\n--返点金额\n,rebate_rate*oriprice rebate_price\n--返点\n,tp.rebate_rate\n,tp.id rcid from \n (\n --先排除折扣\n  select syearmonth,member_id,user_account,user_type,recharge_type,game_id,sum(oriprice) oriprice from advance.ad_centurioncard_reb reb \n  left join advance.ad_rebate_item item on item.exclude_discount=reb.discount and item.type=2 where item.type is null\n  and ym='pi_yearmonth'\n  group by  syearmonth,member_id,user_account,user_type,recharge_type,game_id\n ) oprice  \njoin promo_user pu on pu.member_id=oprice.member_id and pu.member_grade=1\njoin\n--构成返点列表,特批申请单\n(\nselect distinct  \n tel\n,c.id\n,concat(c.year,LPAD(month,2,'0')) syearmonth\n,a.min_number\n,a.max_number\n,a.rebate_scale/100 rebate_rate\n,b.game_id\n,bg.name as game_name\n,b.type rebate_type\n,top_up_type recharge_type\nfrom \nadvance.ad_rebate_item  a \njoin rebate_config_item_game b\non a.group_num=b.group_num and a.rcid=b.rcid and a.type=b.type and b.status=0\njoin rebate_config c on c.id=b.rcid and c.status=4\njoin (select distinct mainid id,maingname as name from gameinfo) bg on bg.id=b.game_id\nwhere a.type=2 and c.id=currentrcid\n) tp\non tp.game_id=oprice.game_id and  tp.syearmonth=oprice.syearmonth and oriprice >= tp.min_number and oriprice<tp.max_number and trim(tp.tel)=trim(oprice.user_account) and oprice.recharge_type=tp.recharge_type".replace("pi_yearmonth",pi_yearmonth).replace("currentrcid",currentrcid)
      //再执行特殊
      val ad_tmp_centurioncard_reb_ts="insert overwrite table  advance.ad_tmp_centurioncard_reb_ts select \n oprice.syearmonth\n,oprice.member_id\n,oprice.user_account\n,int(oprice.putype)+1 user_type\n,ts.game_id\n,ts.game_name\n,2 rebate_type\n,oprice.recharge_type\n--流水\n,oprice.oriprice\n--返点金额\n,ts.rebate_rate*oprice.oriprice rebate_price\n--返点\n,ts.rebate_rate\n,ts.id rcid from \n( select distinct reb.*,sum(reb.oriprice) over(partition by reb.syearmonth,reb.member_id,reb.recharge_type,reb.game_id) as member_m_orice,pu.status putype from \n \n  (\n --先排除折扣\n  select syearmonth,member_id,user_account,user_type,recharge_type,game_id,sum(oriprice) oriprice from advance.ad_centurioncard_reb reb \n  left join advance.ad_rebate_item item on item.exclude_discount=reb.discount and item.type=1 where item.type is null\n  and ym='pi_yearmonth'\n  group by  syearmonth,member_id,user_account,user_type,recharge_type,game_id\n  ) reb\n  join promo_user pu on pu.member_id=reb.member_id\n  left join advance.ad_tmp_centurioncard_reb_tp tp on reb.member_id=tp.member_id and reb.game_id=tp.game_id and tp.rcid=currentrcid --排除特批\n -- left join rebate_config_item_game itemgame on reb.game_id=tp.game_id\n  where  tp.game_id is null \n -- and itemgame.game_id is null  \n  and pu.member_grade=1\n) oprice join \n--构成返点列表,特诉申请单\n(\nselect distinct  \nc.id\n,concat(c.year,LPAD(month,2,'0')) syearmonth\n,a.min_number\n,a.max_number\n,a.rebate_scale/100 rebate_rate\n,b.game_id\n,bg.name as game_name\n,top_up_type recharge_type\n,a.member_id\nfrom (select a.*,member_id from advance.ad_rebate_item a join (select distinct reap_uid member_id from ods_order) pu on 1=1 \n     )  a \njoin rebate_config_item_game b\non a.group_num=b.group_num and a.rcid=b.rcid and a.type=b.type and b.status=0\njoin rebate_config c on c.id=b.rcid and c.status=4 and c.id=currentrcid\njoin (select distinct mainid id,maingname as name from gameinfo) bg on bg.id=b.game_id \nwhere a.type=1\n  \n) ts\non ts.game_id=oprice.game_id and  ts.syearmonth=oprice.syearmonth and member_m_orice >= ts.min_number and member_m_orice< ts.max_number and ts.member_id=oprice.member_id and oprice.recharge_type=ts.recharge_type".replace("pi_yearmonth",pi_yearmonth).replace("currentrcid",currentrcid)
      //固定
      val ad_tmp_centurioncard_reb_gd="insert overwrite table  advance.ad_tmp_centurioncard_reb_gd \nselect \n oprice.syearmonth\n,oprice.member_id\n,oprice.user_account\n,int(oprice.putype)+1 user_type\n,gd.game_id\n,gd.name game_name\n,1 rebate_type\n,oprice.recharge_type\n--流水\n,oprice.oriprice\n--返点金额\n,gd.rebate_rate*oprice.oriprice rebate_price\n--返点\n,gd.rebate_rate\n,gd.id rcid from \n(\n  select distinct reb.*,sum(reb.oriprice) over(partition by reb.syearmonth,reb.member_id,reb.recharge_type) as member_m_orice,pu.status as putype from \n  (\n  --再次聚合\n  select syearmonth,member_id,user_account,user_type,recharge_type,game_id,sum(oriprice) oriprice from advance.ad_centurioncard_reb reb \n  where ym='pi_yearmonth'\n  group by  syearmonth,member_id,user_account,user_type,recharge_type,game_id\n  )\n  reb\n  join promo_user pu on pu.member_id=reb.member_id\n  left join advance.ad_tmp_centurioncard_reb_tp tp on reb.member_id=tp.member_id and reb.game_id=tp.game_id and tp.rcid=currentrcid\n  left join rebate_config_item_game ts on reb.game_id=ts.game_id and ts.type=1 and ts.rcid=currentrcid and ts.status=0 --把特殊的排除\n  where ts.game_id is null and tp.game_id is null and pu.member_grade=1\n) oprice join \n--构成返点列表,固定申请单\n(\nselect distinct  \nc.id\n,concat(c.year,LPAD(month,2,'0')) syearmonth\n,a.min_number\n,a.max_number\n,a.rebate_scale/100 rebate_rate\n,a.game_id\n,top_up_type recharge_type\n,a.member_id\n,bg.name\nfrom (\n      select distinct a.rcid,a.type,a.min_number,a.max_number,a.rebate_scale,a.top_up_type,pu.member_id,a.group_num,pu.game_id\n      from advance.ad_rebate_item a\n            join (select distinct mainid as game_id,reap_uid member_id from ods_order ord join gameinfo gif on gif.id=ord.game_id) pu on 1=1  \n     )  a \njoin rebate_config c on c.id=a.rcid and c.status in(4) and c.id=currentrcid\njoin (select distinct mainid id,maingname as name from gameinfo) bg on bg.id=a.game_id \n  where a.type=0\n) gd\non gd.game_id=oprice.game_id \nand  gd.syearmonth=oprice.syearmonth\nand member_m_orice >= gd.min_number and member_m_orice<gd.max_number \nand oprice.member_id=gd.member_id and oprice.recharge_type=gd.recharge_type".replace("pi_yearmonth",pi_yearmonth).replace("currentrcid",currentrcid)

    //全部转为小写，后面好判断
      val execSql = hivesql.replace("currentrcid", currentrcid).replace("confType", confType) //hive sql
      val sql2Mysql = mysqlsql.replace("|", " ").toLowerCase

      //获取values（）里面有多少个?参数，有利于后面的循环
      val startValuesIndex = sql2Mysql.indexOf("(?") + 1
      val endValuesIndex = sql2Mysql.indexOf("?)") + 1
      //values中的个数
      val valueArray: Array[String] = sql2Mysql.substring(startValuesIndex, endValuesIndex).split(",") //两个（？？）中间的值

      /********************hive库操作 *******************/
      val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$", ""))
      val sc = new SparkContext(sparkConf)
      val sqlContext = new HiveContext(sc)
      sqlContext.sql(ConfigurationUtil.getProperty("hivedb"))
      //执行子过程
      sqlContext.sql(ad_rebate_item1).repartition(1)
      sqlContext.sql(ad_rebate_item2).repartition(1)
      //对渠道进行排除
      sqlContext.sql(ad_centurioncard_reb).coalesce(1)
      sqlContext.sql(ad_tmp_centurioncard_reb_tp).coalesce(1)
      sqlContext.sql(ad_tmp_centurioncard_reb_ts).coalesce(1)
      sqlContext.sql(ad_tmp_centurioncard_reb_gd).coalesce(1)
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
      //调下级返点数据
      bi_centurioncard_sub_rep.sub_reb(currentrcid.toInt,confType.toInt,year,month,sc)
      //调下级返点明细
      bi_centurioncard_sub_rep.sub_reb_detail(currentrcid.toInt,confType.toInt,year,month,sc)
      /** ***********************汇总数据 ******************************/
      val conn = JdbcUtil.getConn()
      val ps: PreparedStatement = conn.prepareStatement("insert into bi_centurioncard_rebate(syearmounth,member_id,user_account,user_type,rcid,conf_type,ori_price,rebate_price,status,group_id,group_name)\nselect a.* from \n(\nselect syearmounth,member_id,user_account,user_type,rcid,conf_type,sum(ori_price) ori_price,sum(rebate_price) as  rebate_price,1 as status,group_id,group_name from bi_centurioncard_reb_deta a \nwhere rcid=? and conf_type=?\ngroup by syearmounth,member_id,user_account,user_type,rcid,group_id,group_name,conf_type\n) a")
      ps.setInt(1, currentrcid.toInt);
      ps.setInt(2, confType.toInt);
      ps.executeUpdate();
      //若执行都OK更新状态表为成功
      val psstatus: PreparedStatement = conn.prepareStatement("update bi_rebate_exec_status set status=1 where rcid=currentrcid".replace("currentrcid", currentrcid))
      psstatus.executeUpdate()
      conn.close()
      System.clearProperty("spark.driver.port")
      sc.stop()
    }
    /*
      捕获异常结束
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