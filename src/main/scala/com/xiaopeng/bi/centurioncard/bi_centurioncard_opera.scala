package com.xiaopeng.bi.centurioncard

import java.io.File
import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.xiaopeng.bi.utils.JdbcUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object bi_centurioncard_opera {

  def main(args: Array[String]): Unit = {


    if (args.length < 1) {
      System.err.println("Usage: <currentday> ")
      System.exit(1)
    }
    if (args.length > 1) {
      System.err.println("参数个数传入太多，固定为1个： <currentday>  ")
      System.exit(1)
    }
    //日志输出警告
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //跑数日期
    val currentday=args(0)
    //昨天
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date:Date  = dateFormat.parse(args(0))
    val cal:Calendar=Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE, -1)
    val yesterday=dateFormat.format(cal.getTime())

    //由于opera有点特殊，必须需要传入前天的值
    val dateFormat1: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date1:Date  = dateFormat1.parse(args(0))
    val cal1:Calendar=Calendar.getInstance()
    cal1.setTime(date1)
    cal1.add(Calendar.DATE, -2)
    val lasttwoday=dateFormat.format(cal1.getTime())

    //月第一天
    val monthfirstday=args(0).substring(0,7)+"-01"

    //2017-10-30 not need modify
    //记得历史数据加载后需要修改
    val hivesql="select \n'currentday' as statistics_date,\ncase when pu.username is null then '' else pu.username end user_account,\nif(subacc_month_amount is null,0,subacc_month_amount)  month_subacc_income,\ncase when subaccounts is null then 0 else subaccounts end sub_accounts,\ncase when rebate_amount is null then 0 else rebate_amount end balance,\ncase when subpkgs is null then 0 else subpkgs end subpackages,\ncase when subpkgs_regi is null then 0 else subpkgs_regi end subpackage_regs,\ncase when but_account is null then 0 else but_account end bought_gameaccs,\ncase when ori_price is null then 0 else ori_price end tocur_income,\ncase when rebate_amount is null then 0 else rebate_amount end tocur_amount,\nif(lj_accounts is null,0,lj_accounts) tocur_gameaccs,\nif(sub_ori_price is null,0,sub_ori_price)+if(ori_price is null,0,ori_price) tocur_includsub_income,\nif(sub_rebateamt is null,0,sub_rebateamt)+if(sub_rebateamt is null,0,sub_rebateamt) tocur_includsub_amount,\nif(subgcc is null,0,subgcc)+if(lj_accounts is null,0,lj_accounts) tocur_includsub_gameaccs,\nif(subaccou_subpkgs is null,0,subaccou_subpkgs)+if(subpkgs is null,0,subpkgs) tocur_includsub_pkgs,\nif(tocur_total_gameaccs is null,0,tocur_total_gameaccs) tocur_total_gameaccs,\nif(tocur_sub_total_gameaccs is null,0,tocur_sub_total_gameaccs)+if(tocur_total_gameaccs is null,0,tocur_total_gameaccs) tocur_incsub_total_gameaccs\n\nfrom promo_user pu   \n\n\n\n--子账号数\n left join \n  (\n    select a.owner_id as member_id ,count(a.member_id) as subaccounts from promo_user a where  to_date(create_time)<='currentday' group by a.owner_id\n  ) subaccounts\n  on subaccounts.member_id=pu.member_id\n\n--子账号月流水\nleft join\n  (\n     select b.owner_id member_id,sum(ori_price) as subacc_month_amount from \n     promo_user b    join v_ods_order od on od.reap_uid=b.member_id \n    where  to_date(order_time)<='currentday' and to_date(order_time)>='monthfirstday'   and od.order_status in(4,8)\n    group by b.owner_id\n   \n  ) subaccount_income\non  subaccount_income.member_id=pu.member_id\n\n--汇总数据-充值流水返利金额\nleft join\n (\n   select reap_uid member_id,sum(rebate_amount) rebate_amount ,sum(ori_price) ori_price from v_ods_order  \n   where to_date(order_time)<='currentday' and order_status in(4,8) group by reap_uid\n ) rebate\n on rebate.member_id=pu.member_id\n\n--分包数\n left join\n  (\n    select member_id,count(promo_file_name) subpkgs from  ods_subpkg  b where to_date(subpkg_time)<='currentday' group by member_id \n  ) subpkgs\n on subpkgs.member_id=pu.member_id\n\n--分包注册,1暂时代表分包注册;购买账号\nleft join\n  (\n    select owner_id member_id,\n    sum(case when reg_resource=1 then 1 else 0 end) subpkgs_regi,  --分包注册\n    sum(case when reg_resource=2 then 1 else 0 end) but_account,   --购买账号\n    count(distinct case when bind_uid_time is null then null when to_date(bind_uid_time)<='currentday' then game_account else null end) lj_accounts,  --累计交易账户数\n    count(game_account) tocur_total_gameaccs  --本账号游戏账号数，包括未激活\n    from ods_regi where to_date(reg_time)<='currentday' group by owner_id\n  )  regis\n on regis.member_id=pu.member_id\n\n\n--汇总数据-充值流水（以下为子账号）\nleft join\n (\n  select a.owner_id member_id,sum(ori_price) sub_ori_price,sum(rebate_amount)  sub_rebateamt from promo_user a   join v_ods_order b\n  on b.reap_uid=a.member_id  and to_date(order_time)<='currentday' and order_status in(4,8)  and a.member_grade=1\n  group by a.owner_id\n  )  subacc_sum_income\n on subacc_sum_income.member_id=pu.member_id\n \n\n--子账号累计分包数\nleft join\n (\n  select a.owner_id member_id,count(b.promo_file_name) as subaccou_subpkgs from promo_user a \n   join  ods_subpkg  b on a.member_id=b.member_id where to_date(subpkg_time)<='currentday'  and a.member_grade=1\n  group by owner_id\n  ) subaccou_subpkgs\non subaccou_subpkgs.member_id=pu.member_id\n\n--子账号游戏账号数，未包括本身，后面计算时加上本身即可，包括未激活;子账号累计账号数,产生订单\nleft join\n  (select pu.owner_id member_id,count(distinct bga.game_account) as  tocur_sub_total_gameaccs, --子账号游戏账号数，未包括本\n   count(distinct case when bind_uid_time is null then null when to_date(bind_uid_time)<='currentday' then game_account else null end) subgcc --子账号累计账号数\n   from ods_regi bga join promo_user pu\n   on bga.owner_id=pu.member_id \n   where to_date(reg_time)<='currentday'  and pu.member_grade=1\n   group by  pu.owner_id\n  ) tt_acc_sub\n    on tt_acc_sub.member_id=pu.member_id \n\nwhere  pu.member_grade=1\norder by user_account asc"

    val mysqlsql="insert into bi_centurioncard_opera(statistics_date,user_account,month_subacc_income,sub_accounts,balance,subpackages,subpackage_regs,bought_gameaccs,tocur_income,tocur_amount,tocur_gameaccs," +
                                                    " tocur_includsub_income,tocur_includsub_amount,tocur_includsub_gameaccs,tocur_includsub_pkgs,tocur_total_gameaccs,tocur_incsub_total_gameaccs) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) " +
                                                    " on duplicate key update month_subacc_income=?," +
                                                    " sub_accounts=?,balance=?,subpackages=?,subpackage_regs=?,bought_gameaccs=?,tocur_income=?,tocur_amount=?,tocur_gameaccs=?,tocur_includsub_income=?,tocur_includsub_amount=?,tocur_includsub_gameaccs=?,tocur_includsub_pkgs=?, tocur_total_gameaccs=?,tocur_incsub_total_gameaccs=?"
    //全部转为小写，后面好判断
    val execSql=hivesql.replace("currentday",currentday).replace("yesterday",yesterday).replace("monthfirstday",monthfirstday).replace("lasttwoday",lasttwoday)  //hive sql
    val sql2Mysql=mysqlsql.toLowerCase

    //Hadoop libariy
    val path: String = new File(".").getCanonicalPath
    System.getProperties().put("hadoop.home.dir", path)
    new File("./bin").mkdirs()
    new File("./bin/winutils.exe").createNewFile()

    //获取values（）里面有多少个?参数，有利于后面的循环
    val startValuesIndex=sql2Mysql.indexOf("(?")+1
    val endValuesIndex=sql2Mysql.indexOf("?)")+1
    //values中的个数
    val valueArray:Array[String]=sql2Mysql.substring(startValuesIndex,endValuesIndex).split(",")  //两个（？？）中间的值
    //条件中的参数个数
    val wh:Array[String]=sql2Mysql.substring(sql2Mysql.indexOf("update")+6).split(",")  //找update后面的字符串再判断
    //查找需要insert的字段
    val cols_ref=sql2Mysql.substring(0,sql2Mysql.lastIndexOf("(?"))  //获取（?特殊字符前的字符串，然后再找字段
    val cols:Array[String]=cols_ref.substring(cols_ref.lastIndexOf("(")+1,cols_ref.lastIndexOf(")")).split(",")

    /********************hive库操作*******************/
    val sparkConf = new SparkConf().setAppName(this.getClass.getName.replace("$",""))
    sparkConf.set("spark.memory.useLegacyMode","true").set("spark.shuffle.memoryFraction","0.6").set("spark.storage.memoryFraction","0.2")
    val sc = new SparkContext(sparkConf)
    val sqlContext=new HiveContext(sc)

    sqlContext.sql("use yyft")
    val dataf = sqlContext.sql(execSql)//执行hive sql
    dataf.coalesce(1)

    /********************数据库操作***************   ****/
    dataf.foreachPartition(rows=> {
      val conn = JdbcUtil.getConn()
      conn.setAutoCommit(false)
      val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
      ps.clearBatch()
      for (x <- rows) {
        //补充value值
        for (rs <- 0 to valueArray.length - 1) {
          ps.setString(rs.toInt + 1, x.get(rs).toString)
        }
        //补充条件
        for (i <- 0 to wh.length - 1) {
          val rs = wh(i).trim.substring(0, wh(i).trim.lastIndexOf("="))
          for (ii <- 0 to cols.length - 1) {
            if (cols(ii).trim.equals(rs)) {
              ps.setString(i.toInt + valueArray.length.toInt + 1, x.get(ii).toString)
            }
          }
        }
        ps.addBatch()
      }
      ps.executeBatch()
      conn.commit()
      conn.close()
    }
    )
    System.clearProperty("spark.driver.port")
    sc.stop()
  }
}
