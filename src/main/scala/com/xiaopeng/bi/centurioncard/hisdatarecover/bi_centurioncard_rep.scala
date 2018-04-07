package com.xiaopeng.bi.centurioncard.hisdatarecover

import java.io.File
import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.xiaopeng.bi.utils.JdbcUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object bi_centurioncard_rep {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: <currentday><startday> ")
      System.exit(1)
    }
    if (args.length > 2) {
      System.err.println("参数个数传入太多，固定为2个： <currentday><startday>  ")
      System.exit(1)
    }
    //跑数日期
    val currentday=args(0)
    //开始日期
    val startday=args(1)
    //昨天
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date:Date  = dateFormat.parse(args(0))
    val cal:Calendar=Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE, -1)
    val yesterday=dateFormat.format(cal.getTime())
    //月第一天
    val monthfirstday=args(0).substring(0,7)+"-01"
    //current
    val hivesql="select \nif(dt is null,'0000-00-00',dt) statistics_date,\ncase when info.member_id is null then 0 else info.member_id end member_id,\ncase when info.owner_id is null then 0 else info.owner_id end owner_id,\ncase when info.user_account is null then '' else info.user_account end user_account,\ncase when rs.game_id is null then 0 else rs.game_id end game_id,\ncase when rs.channel_id is null then 0 else rs.channel_id end channel_id,\ncase when bg.name is null then '' else bg.name end game_name,\ncase when bc.name is null then '' else bc.name end channel_name,\ncase when last_bind_time is null then '0000-00-00 00:00:00' else last_bind_time end last_bind_time,\ncase when expand_user_id is null then 0 else expand_user_id end expand_user_id,\ncase when expand_user is null then ' ' else expand_user end expand_user,\ncase when sum(first_exchg_income) is null then 0 else sum(first_exchg_income) end first_exchg_income,\nif(sum(conti_exchg_income) is null,0,sum(conti_exchg_income))-if(sum(gameacc_conti_income) is null,0,sum(gameacc_conti_income))  conti_exchg_income,  --续充要排除玩家续充\ncase when sum(direct_exchg_income) is null then 0 else sum(direct_exchg_income) end direct_exchg_income,\ncase when sum(direct_exchg_reb) is null then 0 else sum(direct_exchg_reb) end direct_exchg_reb,\ncase when sum(gameacc_conti_income) is null then 0 else sum(gameacc_conti_income) end gameacc_conti_income,\ncase when sum(gameacc_conti_reb) is null then 0 else sum(gameacc_conti_reb) end gameacc_conti_reb,\nsum(if(first_exchg_income is null,0,first_exchg_income)+if(conti_exchg_income is null,0,conti_exchg_income)+if(direct_exchg_income is null,0,direct_exchg_income)) as  income,\ncase when sum(subpkgs) is null then 0 else sum(subpkgs) end subpackages,\ncase when sum(add_users) is null then 0 else sum(add_users) end add_users,\ncase when info.create_time is null then '0000-00-00 00:00:00' else info.create_time end as vip_create_time\nfrom \n(\nselect \n  distinct\n  pu.member_id,\n  pu.owner_id,\n  pu.username user_account,\n  pu.last_bind_time,\n  pu.group_id expand_user_id,\n  gp.name expand_user,\n  pu.create_time\nfrom  promo_user pu \n left join promo_user_group gp on pu.group_id=gp.id and gp.status=0\n where pu.status in(0,1) and pu.member_grade=1 \n )info\n  join \n (\n   \n     --首充续充,13首充，24续充，6直充 --玩家续充,退单的情况\n    select to_date(order_time) as dt,game_id,channel_id,reap_uid as member_id,\n                     0 as add_users,\n                     sum(case when sxc.prod_type in(1,3) then sxc.ori_price else 0 end) first_exchg_income,\n                     sum(case when sxc.prod_type in(2,4) or sxc.prod_type is null then sxc.ori_price else 0 end) conti_exchg_income,   --2,4包括所有续充\n                     sum(case when sxc.prod_type=6 then sxc.ori_price else 0 end) direct_exchg_income,\n                     sum(case when sxc.prod_type=6 then sxc.rebate_amount else 0 end) direct_exchg_reb,\n                     sum(case when (sxc.reap_uid!=sxc.userid and sxc.prod_type in(2,4)  and reap_uid!=0 and userid!=0) then sxc.ori_price else 0 end) gameacc_conti_income,  --玩家续充：充值人与返利人不一致\n                     sum(case when (sxc.reap_uid!=sxc.userid and sxc.prod_type in(2,4)  and reap_uid!=0 and userid!=0) then sxc.rebate_amount else 0 end) gameacc_conti_reb,  --玩家续充：充值人与返利人不一致\n                     0 as subpkgs\n    from v_ods_order sxc \n    where order_status in(4,8) and  to_date(order_time)<='currentday' and to_date(order_time)>='startday'\n    group by game_id,channel_id,to_date(order_time),reap_uid\n\n   \n   --新增用户\n   union all\n   select to_date(bind_uid_time) as dt,game_id,channel_id,regi.owner_id as member_id,\n          count(regi.game_account) add_users,\n          0 first_exchg_income,\n          0 as conti_exchg_income,\n          0 as direct_exchg_income,\n          0 as direct_exchg_reb,\n          0 as gameacc_conti_income,\n          0 as gameacc_conti_reb,\n          0 as subpkgs\n   from \n    ods_regi regi \n    where  to_date(bind_uid_time)<='currentday' and to_date(bind_uid_time)>='startday'\n    group by game_id,channel_id,to_date(bind_uid_time),owner_id\n   \n\n    --分包\n    union all\n    select to_date(subpkg_time) as dt,game_id,channel_id,pu.member_id,\n           0 as add_users,\n           0 as first_exchg_income,\n           0 as conti_exchg_income,\n           0 as direct_exchg_income,\n           0 as direct_exchg_reb,\n           0 as gameacc_conti_income,\n           0 as gameacc_conti_reb,\n           count(pkg.promo_file_name) subpkgs\n    from  ods_subpkg  pkg join promo_user pu on pu.member_id=pkg.member_id and pu.status in(0,1) and member_grade=1\n     where   to_date(subpkg_time)<='currentday' and  to_date(subpkg_time)>='startday'\n    group by to_date(subpkg_time) ,game_id,channel_id ,pu.member_id\n     \n ) rs\n on info.member_id=rs.member_id \n join bgame bg on bg.id=rs.game_id \n join bchannel bc on bc.id=rs.channel_id \n group by\n info.member_id,\n info.owner_id,\n info.user_account,\n rs.game_id,\n rs.channel_id,\n bg.name,\n bc.name,\n last_bind_time,\n expand_user_id,\n expand_user,\n info.create_time,\n dt"

    val mysqlsql="insert into bi_centurioncard_rep(statistics_date,member_id,owner_id,user_account,game_id,channel_id,game_name,channel_name,last_binding_time,expand_user_id,expand_user," +
                                                  " first_exchg_income,conti_exchg_income,direct_exchg_income,direct_exchg_reb,gameacc_conti_income,gameacc_conti_reb,income,subpackages,add_users,vip_create_time)" +
                                                  " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
                                                  " on duplicate key update first_exchg_income=?,conti_exchg_income=?,direct_exchg_income=?,direct_exchg_reb=?,gameacc_conti_income=?,gameacc_conti_reb=?,income=?,subpackages=?,add_users=?"

    val execSql=hivesql.replace("currentday",currentday).replace("yesterday",yesterday).replace("monthfirstday",monthfirstday).replace("startday",startday)  //hive sql
    //全部转为小写，后面好判断
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
    val sc = new SparkContext(sparkConf)
    val sqlContext=new HiveContext(sc)
    sqlContext.sql("use yyft")
    val dataf = sqlContext.sql(execSql)//执行hive sql
    dataf.show()

    /********************数据库操作***************   ****/

    var js=0
    dataf.collect().foreach(x=>
    {
      val conn = JdbcUtil.getConn()
      val ps : PreparedStatement =conn.prepareStatement(sql2Mysql)
      //补充value值
      js=js+1
      for(rs<-0 to valueArray.length-1)
      {
        ps.setString(rs.toInt+1,x.get(rs).toString)
      }
      //补充条件
      for(i<-0 to wh.length-1)
      {
        val rs=wh(i).trim.substring(0,wh(i).trim.lastIndexOf("="))
        for(ii<-0 to cols.length-1)
        {
          if(cols(ii).trim.equals(rs))
          {
            ps.setString(i.toInt + valueArray.length.toInt + 1, x.get(ii).toString)
          }
        }
      }
      ps.executeUpdate()
      conn.close()
    }
    )
    System.clearProperty("spark.driver.port")
    sc.stop()

  }

}