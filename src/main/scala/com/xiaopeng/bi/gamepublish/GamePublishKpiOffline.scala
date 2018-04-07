package com.xiaopeng.bi.gamepublish

import java.sql.{Connection, Date, PreparedStatement}

import com.xiaopeng.bi.bean.MoneyMasterBean
import com.xiaopeng.bi.udf.ChannelUDF
import com.xiaopeng.bi.utils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.{Jedis, JedisPool}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kequan on 5/17/17.
  * 注册 激活 登录  支付 离线
  */
object GamePublishKpiOffline {
  var startTime = "";
  var endTime = "";
  var mode = ""


  def main(args: Array[String]): Unit = {
    startTime = args(0)
    endTime = args(1)
    if (args.length > 2) {
      mode = args(2)
    }

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
    if (mode.equals("regi")) {
      //注册相关
      loadRegiInfoOffline(sc, hiveContext, startTime, endTime);
    } else if (mode.equals("active")) {
      //激活相关
      loadActiveInfoOffline(sc, hiveContext, startTime, endTime);
    } else if (mode.equals("money")) {
      //钱大师
      loadMoneyInfoOffline(sc, hiveContext, startTime, endTime);
    } else if (mode.equals("all")) {
      // 全部
      loadRegiInfoOffline(sc, hiveContext, startTime, endTime);
      loadActiveInfoOffline(sc, hiveContext, startTime, endTime);
      loadMoneyInfoOffline(sc, hiveContext, startTime, endTime);
    }
    sc.stop();
  }


  /**
    * 离线处理
    *
    * @param hiveContext
    */
  def loadActiveInfoOffline(sc: SparkContext, hiveContext: HiveContext, startTime: String, endTime: String) = {
    //一.补全明细
    hiveContext.sql("use yyft");
    val sql_bi_active = "select \naz.game_id child_game_id,\naz.parent_channel parent_channel,\naz.child_channel child_channel,\naz.ad_label ad_label,\naz.active_time  active_hour,\naz.imei imei\nfrom \n(select  game_id,getchannel(expand_channel,'0') parent_channel,getchannel(expand_channel,'1') child_channel,getchannel(expand_channel,'2') ad_label,imei,substr(min(active_time),0,13) active_time from ods_active where to_date(active_time)>='startTime' and to_date(active_time)<='endTime' and imei is not null group by game_id,expand_channel,imei) az \njoin  (select  distinct game_id as parent_game_id , old_game_id  ,system_type,group_id from game_sdk  where state=0) gs on az.game_id=gs.old_game_id "
      .replace("startTime", startTime).replace("endTime", endTime)
    val df_bi_active: DataFrame = hiveContext.sql(sql_bi_active);
    foreachActiveDeatilPartition(df_bi_active)

    SparkUtils.readActiveDeatials(sc, hiveContext, startTime, endTime)
    //二.根据明细 更新 bi_gamepublic_basekpi active_num 小时  包
    val sql_base = "select gs.parent_game_id parent_game_id ,az.game_id child_game_id,az.parent_channel parent_channel,az.child_channel child_channel,az.ad_label ad_label,az.active_hour active_hour,count(distinct az.imei) active_num,gs.system_type os,gs.group_id group_id\nfrom\n(select game_id,parent_channel,child_channel,ad_label,imei,min(active_hour) active_hour from bi_gamepublic_active_detail where to_date(active_hour)>='startTime' and  to_date(active_hour)<='endTime' group by game_id,parent_channel,child_channel,ad_label,imei)az\njoin  (select  distinct game_id as parent_game_id , old_game_id  ,system_type,group_id from game_sdk  where state=0) gs on az.game_id=gs.old_game_id \ngroup by  gs.parent_game_id,az.game_id,az.parent_channel,az.child_channel,az.ad_label,az.active_hour,gs.system_type,gs.group_id"
      .replace("startTime", startTime).replace("endTime", endTime)
    val df_base: DataFrame = hiveContext.sql(sql_base).cache();
    foreachActiveBasePartition(df_base)
    df_base.registerTempTable("active_rz")

    //三.根据明细,更新维度到游戏的维度 bi_gamepublic_base_day_kpi active_num 天 包
    val sql_day = "select parent_game_id,child_game_id,parent_channel,child_channel,ad_label,to_date(active_hour) active_hour,sum(active_num) active_num,os,group_id from active_rz group by parent_game_id,child_game_id,parent_channel,child_channel,ad_label,to_date(active_hour),os,group_id"
    val df_day: DataFrame = hiveContext.sql(sql_day);
    foreachActiveDayPartition(df_day)

    //四.根据明细,更新维度到游戏的维度 bi_gamepublic_base_opera_hour_kpi active_num 小时 游戏
    val sql_hour_oper = "select gs.parent_game_id parent_game_id,az.game_id child_game_id,substring(active_hour,0,13) active_hour,count(distinct az.imei) active_num,gs.system_type os ,gs.group_id group_id\nfrom \n(select game_id,imei,min(active_hour) active_hour from bi_gamepublic_active_detail where to_date(active_hour)>='startTime' and  to_date(active_hour)<='endTime' group by game_id,imei)az\njoin  (select  distinct game_id as parent_game_id , old_game_id  ,system_type,group_id from game_sdk  where state=0) gs on az.game_id=gs.old_game_id \ngroup by  gs.parent_game_id,az.game_id,substring(active_hour,0,13),gs.system_type,gs.group_id"
      .replace("startTime", startTime).replace("endTime", endTime)
    val df_hour_oper: DataFrame = hiveContext.sql(sql_hour_oper);
    foreachActiveHourOperPartition(df_hour_oper)
    df_hour_oper.registerTempTable("active_oper_rz")

    //五.根据明细,更新维度到游戏的维度 bi_gamepublic_base_opera_kpi active_num 天 游戏
    val sql_oper = "select parent_game_id,child_game_id,to_date(active_hour) active_hour,sum(active_num) active_num,os,group_id from active_oper_rz group by parent_game_id,child_game_id,to_date(active_hour),os,group_id"
      .replace("startTime", startTime).replace("endTime", endTime)
    val df_oper: DataFrame = hiveContext.sql(sql_oper);
    foreachActiveOperPartition(df_oper)


  }

  def foreachActiveDeatilPartition(df_bi_active: DataFrame) = {
    df_bi_active.foreachPartition(it => {
      //数据库链接
      val conn: Connection = JdbcUtil.getConn();
      val stmt = conn.createStatement();
      // 一.明细表
      val sql_deatil = "INSERT INTO bi_gamepublic_active_detail(game_id,parent_channel,child_channel,ad_label,active_hour,imei) VALUES (?,?,?,?,?,?) "
      val pstat_sql_deatil: PreparedStatement = conn.prepareStatement(sql_deatil);
      val ps_sql_deatil_params = new ArrayBuffer[Array[Any]]()

      val sql_deatil_update = "update bi_gamepublic_active_detail set active_hour =?  WHERE game_id = ? and parent_channel=? and child_channel=? and ad_label=? and  imei=?"
      val pstat_sql_deatil_update: PreparedStatement = conn.prepareStatement(sql_deatil_update);
      val ps_sql_deatil_update_params = new ArrayBuffer[Array[Any]]()

      // redis 链接
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis: Jedis = pool.getResource;
      jedis.select(0);

      it.foreach(row => {
        val child_game_id = row.getAs[Int]("child_game_id");
        val parent_channel = row.getAs[String]("parent_channel");
        val child_channel = row.getAs[String]("child_channel");
        val ad_label = row.getAs[String]("ad_label");
        val publish_time = row.getAs[String]("active_hour");
        val imei = row.getAs[String]("imei");

        if (parent_channel.length <= 10 && child_channel.length <= 10 && ad_label.length <= 12) {
          val sqlselct_all = "select active_hour from bi_gamepublic_active_detail WHERE game_id = '" + child_game_id + "'and parent_channel='" + parent_channel + "' and child_channel='" + child_channel + "'and ad_label= '" + ad_label + "' and  imei='" + imei + "' ";
          val results_all = stmt.executeQuery(sqlselct_all);
          if (!results_all.next()) {
            ps_sql_deatil_params.+=(Array[Any](child_game_id, parent_channel, child_channel, ad_label, publish_time, imei))
          } else {
            val active_hour_old = results_all.getString("active_hour")
            if (DateUtils.beforeHour(publish_time, active_hour_old)) {
              // 新数据的激活时间靠前
              ps_sql_deatil_update_params.+=(Array[Any](publish_time, child_game_id, parent_channel, child_channel, ad_label, imei))
            }
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
      //关闭redis
      pool.returnResource(jedis)
      pool.destroy()

    })

  }

  def foreachActiveBasePartition(df_base: DataFrame) = {
    df_base.foreachPartition(iter => {
      //数据库链接
      val conn: Connection = JdbcUtil.getConn();
      val stmt = conn.createStatement();

      // redis 链接
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis: Jedis = pool.getResource;
      jedis.select(0);

      val sqlkpi_hour_active_num = "INSERT INTO bi_gamepublic_basekpi(parent_game_id,game_id,parent_channel,child_channel,ad_label,publish_time,active_num,os,group_id,medium_account,promotion_channel,promotion_mode,head_people) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY update os=values(os),group_id=values(group_id),medium_account=values(medium_account),promotion_channel=values(promotion_channel),promotion_mode=values(promotion_mode),head_people=values(head_people),active_num=VALUES(active_num)"
      val ps_hour_active_num: PreparedStatement = conn.prepareStatement(sqlkpi_hour_active_num);
      val parnms_hour_active = new ArrayBuffer[Array[Any]]()

      iter.foreach(row => {
        val parent_game_id = row.getAs[Int]("parent_game_id");
        val child_game_id = row.getAs[Int]("child_game_id");
        val parent_channel = row.getAs[String]("parent_channel");
        val child_channel = row.getAs[String]("child_channel");
        val ad_label = row.getAs[String]("ad_label");
        val publish_time = row.getAs[String]("active_hour");
        val os = row.getAs[Byte]("os");
        val group_id = row.getAs[Int]("group_id");
        val active_num = row.getAs[Long]("active_num").toInt;

        val redisValue = JedisUtil.getRedisValue(child_game_id, ad_label, publish_time.substring(0, 10), jedis)
        val medium_account = redisValue(2);
        val promotion_channel = redisValue(3);
        val promotion_mode = redisValue(4);
        val head_people = redisValue(5);

        parnms_hour_active.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label, publish_time, active_num, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))
        // 插入数据库
        JdbcUtil.executeUpdate(ps_hour_active_num, parnms_hour_active, conn)
      })

      ps_hour_active_num.close()
      conn.close

      pool.returnResource(jedis)
      pool.destroy()
    })
  }

  def foreachActiveDayPartition(df_day: DataFrame) = {
    df_day.foreachPartition(iter => {
      //数据库链接
      val conn: Connection = JdbcUtil.getConn();
      val stmt = conn.createStatement();

      // redis 链接
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis: Jedis = pool.getResource;
      jedis.select(0);

      // 更新投放天表
      val sql_day_active_num = "INSERT INTO bi_gamepublic_base_day_kpi(parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,active_num,os,group_id,medium_account,promotion_channel,promotion_mode,head_people) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY update os=values(os),group_id=values(group_id),medium_account=values(medium_account),promotion_channel=values(promotion_channel),promotion_mode=values(promotion_mode),head_people=values(head_people),active_num=VALUES(active_num)"
      val ps_day_active_num: PreparedStatement = conn.prepareStatement(sql_day_active_num);
      val params_day_active = new ArrayBuffer[Array[Any]]()


      iter.foreach(row => {
        val parent_game_id = row.getAs[Int]("parent_game_id");
        val child_game_id = row.getAs[Int]("child_game_id");
        val parent_channel = row.getAs[String]("parent_channel");
        val child_channel = row.getAs[String]("child_channel");
        val ad_label = row.getAs[String]("ad_label");
        val publish_time = row.getAs[Date]("active_hour").toString;
        val os = row.getAs[Byte]("os");
        val group_id = row.getAs[Int]("group_id");
        val active_num = row.getAs[Long]("active_num").toInt;

        val redisValue = JedisUtil.getRedisValue(child_game_id, ad_label, publish_time.substring(0, 10), jedis)
        val medium_account = redisValue(2);
        val promotion_channel = redisValue(3);
        val promotion_mode = redisValue(4);
        val head_people = redisValue(5);
        params_day_active.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label, publish_time, active_num, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))
        // 插入数据库
        JdbcUtil.executeUpdate(ps_day_active_num, params_day_active, conn)
      })

      ps_day_active_num.close()
      conn.close

      pool.returnResource(jedis)
      pool.destroy()
    })
  }

  def foreachActiveHourOperPartition(df_hour_oper: DataFrame) = {
    df_hour_oper.foreachPartition(iter => {
      val conn: Connection = JdbcUtil.getConn();
      val sql = "insert into bi_gamepublic_base_opera_hour_kpi(parent_game_id,child_game_id,publish_time,active_num,os,group_id) values(?,?,?,?,?,?) on duplicate key update os=VALUES(os),group_id=VALUES(group_id),active_num=VALUES(active_num)"
      val ps = conn.prepareStatement(sql)
      val params = new ArrayBuffer[Array[Any]]()

      iter.foreach(row => {
        params.+=(Array[Any](row.get(0), row.get(1), row.get(2), row.get(3), row.get(4), row.get(5)))
        // 插入数据库
        JdbcUtil.executeUpdate(ps, params, conn)
      })
      ps.close()
      conn.close()
    })
  }

  def foreachActiveOperPartition(df_oper: DataFrame) = {
    df_oper.foreachPartition(iter => {
      val conn: Connection = JdbcUtil.getConn();
      val sql = "insert into bi_gamepublic_base_opera_kpi(parent_game_id,child_game_id,publish_date,active_num,os,group_id) values(?,?,?,?,?,?) on duplicate key update os=VALUES(os),group_id=VALUES(group_id),active_num=VALUES(active_num)"
      val ps = conn.prepareStatement(sql)
      val params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        params.+=(Array[Any](row.get(0), row.get(1), row.get(2), row.get(3), row.get(4), row.get(5)))
        JdbcUtil.executeUpdate(ps, params, conn)
      })
      ps.close()
      conn.close()
    })
  }


  /**
    * 离线处理
    *
    * @param hiveContext
    * @param startTime
    * @param endTime
    */
  def loadRegiInfoOffline(sc: SparkContext, hiveContext: HiveContext, startTime: String, endTime: String) = {

    // 一.补全明细数据
    hiveContext.sql("use yyft");
    val sql_regi = "select \nrz.game_id child_game_id,\nrz.parent_channel parent_channel,\nrz.child_channel child_channel,\nrz.ad_label ad_label,\nrz.reg_time  reg_time,\nrz.imei imei\nfrom \n(select distinct game_id,getchannel(expand_channel,'0') parent_channel,getchannel(expand_channel,'1') child_channel,getchannel(expand_channel,'2') ad_label,substr(reg_time,0,13) reg_time, imei from ods_regi_rz where to_date(reg_time)>='startTime' and to_date(reg_time)<='endTime' and imei is not null) rz \njoin  (select  distinct game_id as parent_game_id , old_game_id  ,system_type,group_id from game_sdk  where state=0) gs on rz.game_id=gs.old_game_id"
      .replace("startTime", startTime).replace("endTime", endTime)
    val regiDF: DataFrame = hiveContext.sql(sql_regi)
    foreachRegiDeatilPartition(regiDF);

    // 二.新增注册设备数
    // 1. bi_gamepublic_basekpi  new_regi_device_num 包 小时
    var hour_new_regi_device_rows = new ArrayBuffer[Row]()
    var hour_new_regi_device_rowsBroadCast: Broadcast[ArrayBuffer[Row]] = sc.broadcast(hour_new_regi_device_rows);

    // 2. bi_gamepublic_base_day_kpi new_regi_device_num  包 天表
    var day_new_regi_device_rows = new ArrayBuffer[Row]()
    var day_new_regi_device_rowsBroadCast: Broadcast[ArrayBuffer[Row]] = sc.broadcast(day_new_regi_device_rows);

    // 3. bi_gamepublic_base_opera_hour_kpi  new_regi_device_num  游戏  小时
    var oper_hour_new_regi_device_rows = new ArrayBuffer[Row]()
    var oper_hour_new_regi_device_rowsBroadCast: Broadcast[ArrayBuffer[Row]] = sc.broadcast(oper_hour_new_regi_device_rows);

    // 4. bi_gamepublic_base_opera_kpi  new_regi_device_num  游戏  天
    var oper_new_regi_device_rows = new ArrayBuffer[Row]()
    var oper_new_regi_device_rowsBroadCast: Broadcast[ArrayBuffer[Row]] = sc.broadcast(oper_new_regi_device_rows);

    regiDF.foreachPartition(rdd => {
      val conn: Connection = JdbcUtil.getConn();
      val stmt = conn.createStatement();
      rdd.foreach(row => {
        val child_game_id = row.getAs[Int]("child_game_id");
        val parent_channel = row.getAs[String]("parent_channel");
        val child_channel = row.getAs[String]("child_channel");
        val ad_label = row.getAs[String]("ad_label");
        val publish_time = row.getAs[String]("reg_time");
        val publish_date =publish_time.substring(0, 10);
        val imei = row.getAs[String]("imei");

        //今天之前不存在  包
        val sql_beforToday = "select regi_hour from bi_gamepublic_regi_detail WHERE game_id = '" + child_game_id + "'and parent_channel='" + parent_channel + "' and child_channel='" + child_channel + "'and ad_label= '" + ad_label + "' and  imei='" + imei + "' and date(regi_hour)<'" + publish_time.substring(0, 10) + "'";
        val results_hour = stmt.executeQuery(sql_beforToday);
        if (!results_hour.next()) {
          hour_new_regi_device_rowsBroadCast.value.+=(row);
        }

        // 历史不存在  包
        val sq_all = "select left(regi_hour,10) as publish_time from bi_gamepublic_regi_detail WHERE game_id = '" + child_game_id + "'and parent_channel='" + parent_channel + "' and child_channel='" + child_channel + "'and ad_label= '" + ad_label + "' and  imei='" + imei + "' and TIMESTAMP(regi_hour)<'" + publish_time + "'";
        val results_all = stmt.executeQuery(sq_all);
        if (!results_all.next()) {
          day_new_regi_device_rowsBroadCast.value.+=(row);
        }

        val sql_oper_beforetoay = "select regi_hour from bi_gamepublic_regi_detail WHERE game_id = '" + child_game_id + "' and  imei='" + imei + "' and date(regi_hour)<'" + publish_time.substring(0, 10) + "'";
        val results_oper_beforetoay = stmt.executeQuery(sql_oper_beforetoay);
        // 今天以前不存在 包
        if (!results_oper_beforetoay.next()) {
          oper_hour_new_regi_device_rowsBroadCast.value.+=(row);
        }

        // 历史不存在  游戏
        val sql_select_oper_all = "select left(regi_hour,10) as publish_time from bi_gamepublic_regi_detail WHERE game_id = '" + child_game_id + "'and  imei='" + imei + "' and TIMESTAMP(regi_hour)<'" + publish_time + "'";
        val results_oper_all = stmt.executeQuery(sql_select_oper_all);
        if (!results_oper_all.next()) {
          oper_new_regi_device_rowsBroadCast.value.+=(row);
        }
      })
    })

    val schema = (new StructType).add("child_game_id", IntegerType).add("parent_channel", StringType).add("child_channel", StringType).add("ad_label", StringType).add("reg_time", StringType).add("imei", StringType)

    // 1. bi_gamepublic_basekpi  new_regi_device_num 包 小时
    val rdd_hour_pkg = sc.parallelize(hour_new_regi_device_rows);
    hiveContext.createDataFrame(rdd_hour_pkg, schema).persist().registerTempTable("regi_hour_pkg");
    val sql_hour_pkg = "select  child_game_id,parent_channel,child_channel,ad_label,reg_time,count(distinct imei) count_dev  from  regi_hour_pkg group by  child_game_id,parent_channel,child_channel,ad_label,reg_time"
    val df_hour_pkg: DataFrame = hiveContext.sql(sql_hour_pkg)
    foreachRegiHourPkgPartition(df_hour_pkg);

    // 2. bi_gamepublic_base_day_kpi new_regi_device_num  包 天表
    val rdd_day_pkg = sc.parallelize(day_new_regi_device_rows);
    hiveContext.createDataFrame(rdd_day_pkg, schema).persist().registerTempTable("regi_day_pkg");
    val sql_day_pkg = "select  child_game_id,parent_channel,child_channel,ad_label,to_date(reg_time) reg_time,count(distinct imei) count_dev from  regi_day_pkg group by  child_game_id,parent_channel,child_channel,ad_label,to_date(reg_time)"
    val df_day_pkg: DataFrame = hiveContext.sql(sql_day_pkg)
    foreachRegiDayPkgPartition(df_day_pkg);

    // 3. bi_gamepublic_base_opera_hour_kpi  new_regi_device_num  游戏  小时
    val rdd_hour_game = sc.parallelize(oper_hour_new_regi_device_rows);
    hiveContext.createDataFrame(rdd_hour_game, schema).persist().registerTempTable("regi_hour_game");
    val sql_hour_game = "select  child_game_id,substr(reg_time,0,13)  reg_time,count(distinct imei) count_dev from  regi_hour_game group by  child_game_id,substr(reg_time,0,13) "
    val df_hour_game: DataFrame = hiveContext.sql(sql_hour_game)
    foreachRegiHourGamePartition(df_hour_game);

    // 4. bi_gamepublic_base_opera_kpi  new_regi_device_num  游戏  天
    val rdd_day_game = sc.parallelize(oper_new_regi_device_rows);
    hiveContext.createDataFrame(rdd_day_game, schema).persist().registerTempTable("regi_day_game");
    val sql_day_game = "select  child_game_id,to_date(reg_time) reg_time,count( distinct imei) count_dev from  regi_day_game group by  child_game_id,to_date(reg_time)"
    val df_day_game: DataFrame = hiveContext.sql(sql_day_game)
    foreachRegiDayGamePartition(df_day_game);

    // 三.新增注册账户数
    // 1. bi_gamepublic_base_day_kpi new_regi_account_num 包 天
    val sql_day_pkg_acc = "select rzdp.child_game_id child_game_id,rzdp.parent_channel parent_channel,rzdp.child_channel child_channel,rzdp.ad_label ad_label,to_date(rzdp.reg_time) reg_time,count(distinct rz.game_account) count_acc from\n(select  distinct child_game_id,parent_channel,child_channel,ad_label,to_date(reg_time) reg_time,imei from  regi_day_pkg ) rzdp\njoin (select distinct game_id,getchannel(expand_channel,'0') parent_channel,getchannel(expand_channel,'1') child_channel,getchannel(expand_channel,'2') ad_label,to_date(reg_time) reg_time, imei,lower(trim(game_account)) game_account from ods_regi_rz where to_date(reg_time)>='startTime' and to_date(reg_time)<='endTime' and imei is not null) rz \non rzdp.child_game_id=rz.game_id and rzdp.parent_channel=rz.parent_channel and rzdp.child_channel=rz.child_channel and rzdp.ad_label=rz.ad_label and rzdp.reg_time=rz.reg_time and rzdp.imei=rz.imei\ngroup by  rzdp.child_game_id,rzdp.parent_channel,rzdp.child_channel,rzdp.ad_label,rzdp.reg_time"
      .replace("startTime", startTime).replace("endTime", endTime)
    val df_day_pkg_acc: DataFrame = hiveContext.sql(sql_day_pkg_acc)
    foreachRegiDayPkgAccPartition(df_day_pkg_acc);

    // 2. bi_gamepublic_base_opera_kpi  new_regi_account_num 游戏  天
    val sql_day_game_acc = "select rzdp.child_game_id child_game_id,to_date(rzdp.reg_time) reg_time,count(distinct rz.game_account) count_acc from\n(select distinct child_game_id,parent_channel,child_channel,ad_label,to_date(reg_time) reg_time,imei from  regi_day_game) rzdp\njoin (select distinct game_id,getchannel(expand_channel,'0') parent_channel,getchannel(expand_channel,'1') child_channel,getchannel(expand_channel,'2') ad_label,to_date(reg_time)reg_time, imei,lower(trim(game_account)) game_account from ods_regi_rz where to_date(reg_time)>='startTime' and to_date(reg_time)<='endTime' and imei is not null) rz \non rzdp.child_game_id=rz.game_id  and rzdp.reg_time=rz.reg_time and  rzdp.imei=rz.imei\ngroup by  rzdp.child_game_id,to_date(rzdp.reg_time)"
      .replace("startTime", startTime).replace("endTime", endTime)
    val df_day_game_acc: DataFrame = hiveContext.sql(sql_day_game_acc)
    foreachRegiDayGameAccPartition(df_day_game_acc);

  }

  def foreachRegiDeatilPartition(regiDF: DataFrame) = {
    regiDF.foreachPartition(iter => {
      //数据库链接
      val conn: Connection = JdbcUtil.getConn();
      val stmt = conn.createStatement();

      // 一.更新明细表  按小时去重
      val sql = "INSERT INTO bi_gamepublic_regi_detail(game_id,parent_channel,child_channel,ad_label,regi_hour,imei) VALUES (?,?,?,?,?,?) "
      val ps = conn.prepareStatement(sql)
      var params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {

        val child_game_id = row.getAs[Int]("child_game_id");
        val parent_channel = row.getAs[String]("parent_channel");
        val child_channel = row.getAs[String]("child_channel");
        val ad_label = row.getAs[String]("ad_label");
        val publish_time = row.getAs[String]("reg_time");
        val imei = row.getAs[String]("imei");
        val sqlselct_day = "select regi_hour from bi_gamepublic_regi_detail WHERE game_id = '" + child_game_id + "'and parent_channel='" + parent_channel + "' and child_channel='" + child_channel + "'and ad_label= '" + ad_label + "' and  imei='" + imei + "' and date(regi_hour)='" + publish_time.substring(0, 10) + "'";
        val results_day = stmt.executeQuery(sqlselct_day);
        if (!results_day.next()) {
          params.+=(Array[Any](child_game_id, parent_channel, child_channel, ad_label, publish_time, imei))
          JdbcUtil.executeUpdate(ps, params, conn)
        }
      })

      stmt.close()
      conn.close

    })
  }

  def foreachRegiHourPkgPartition(df_hour_pkg: DataFrame) = {
    df_hour_pkg.foreachPartition(iter => {
      // redis 链接
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis: Jedis = pool.getResource;
      jedis.select(0);
      val conn: Connection = JdbcUtil.getConn();
      val sql = "INSERT INTO bi_gamepublic_basekpi(parent_game_id,game_id,parent_channel,child_channel,ad_label,publish_time,new_regi_device_num,os,group_id,medium_account,promotion_channel,promotion_mode,head_people) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY update os=values(os),group_id=values(group_id),medium_account=values(medium_account),promotion_channel=values(promotion_channel),promotion_mode=values(promotion_mode),head_people=values(head_people),new_regi_device_num=VALUES(new_regi_device_num)"
      val ps = conn.prepareStatement(sql)
      var params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        val child_game_id = row.getAs[Int]("child_game_id");
        val parent_channel = row.getAs[String]("parent_channel");
        val child_channel = row.getAs[String]("child_channel");
        val ad_label = row.getAs[String]("ad_label");
        val publish_time = row.getAs[String]("reg_time");
        val count_dev = row.getAs[Long]("count_dev").toInt;
        // 获取redis的数据
        val redisValue = JedisUtil.getRedisValue(child_game_id, ad_label, publish_time.substring(0, 10), jedis)
        val parent_game_id = redisValue(0);
        val os = redisValue(1);
        val medium_account = redisValue(2);
        val promotion_channel = redisValue(3);
        val promotion_mode = redisValue(4);
        val head_people = redisValue(5);
        val group_id = redisValue(6);
        params.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label, publish_time + ":00:00", count_dev, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))
        JdbcUtil.executeUpdate(ps, params, conn)
      })
      ps.close()
      conn.close()
      pool.returnResource(jedis)
      pool.destroy()
    })
  }

  def foreachRegiDayPkgPartition(df_day_pkg: DataFrame) = {
    df_day_pkg.foreachPartition(iter => {

      // redis 链接
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis: Jedis = pool.getResource;
      jedis.select(0);

      val conn: Connection = JdbcUtil.getConn();
      val sql = "INSERT INTO bi_gamepublic_base_day_kpi(parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,new_regi_device_num,os,group_id,medium_account,promotion_channel,promotion_mode,head_people) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY update os=values(os),group_id=values(group_id),medium_account=values(medium_account),promotion_channel=values(promotion_channel),promotion_mode=values(promotion_mode),head_people=values(head_people),new_regi_device_num=VALUES(new_regi_device_num)"
      val ps = conn.prepareStatement(sql)
      var params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        val child_game_id = row.getAs[Int]("child_game_id");
        val parent_channel = row.getAs[String]("parent_channel");
        val child_channel = row.getAs[String]("child_channel");
        val ad_label = row.getAs[String]("ad_label");
        val publish_time = row.getAs[Date]("reg_time").toString;
        val count_dev = row.getAs[Long]("count_dev").toInt;
        // 获取redis的数据
        val redisValue = JedisUtil.getRedisValue(child_game_id, ad_label, publish_time.substring(0, 10), jedis)
        val parent_game_id = redisValue(0);
        val os = redisValue(1);
        val medium_account = redisValue(2);
        val promotion_channel = redisValue(3);
        val promotion_mode = redisValue(4);
        val head_people = redisValue(5);
        val group_id = redisValue(6);
        params.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label, publish_time + ":00:00", count_dev, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))
        JdbcUtil.executeUpdate(ps, params, conn)
      })
      ps.close()
      conn.close()
      pool.returnResource(jedis)
      pool.destroy()
    })
  }

  def foreachRegiHourGamePartition(df_hour_game: DataFrame) = {
    df_hour_game.foreachPartition(iter => {
      // redis 链接
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis: Jedis = pool.getResource;
      jedis.select(0);

      val conn: Connection = JdbcUtil.getConn();
      val sql = "INSERT INTO bi_gamepublic_base_opera_hour_kpi(parent_game_id,child_game_id,publish_time,new_regi_device_num,os,group_id) VALUES (?,?,?,?,?,?) ON DUPLICATE KEY update os=values(os),group_id=values(group_id),new_regi_device_num=VALUES(new_regi_device_num) "

      val ps = conn.prepareStatement(sql)
      var params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        val child_game_id = row.getAs[Int]("child_game_id");
        val publish_time = row.getAs[String]("reg_time").toString;
        val count_dev = row.getAs[Long]("count_dev").toInt;
        // 获取redis的数据
        val redisValue = JedisUtil.getRedisValue(child_game_id, "", publish_time, jedis)
        val parent_game_id = redisValue(0);
        val os = redisValue(1);
        val group_id = redisValue(6);
        params.+=(Array[Any](parent_game_id, child_game_id, publish_time.substring(0, 13) + ":00:00", count_dev, os, group_id))
        JdbcUtil.executeUpdate(ps, params, conn)
      })

      ps.close()
      conn.close()
      pool.returnResource(jedis)
      pool.destroy()
    })
  }

  def foreachRegiDayGamePartition(df_day_game: DataFrame) = {
    df_day_game.foreachPartition(iter => {
      // redis 链接
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis: Jedis = pool.getResource;
      jedis.select(0);

      val conn: Connection = JdbcUtil.getConn();
      val sql = "INSERT INTO bi_gamepublic_base_opera_kpi(parent_game_id,child_game_id,publish_date,new_regi_device_num,os,group_id) VALUES (?,?,?,?,?,?) ON DUPLICATE KEY update os=values(os),group_id=values(group_id),new_regi_device_num=VALUES(new_regi_device_num)"
      val ps = conn.prepareStatement(sql)
      var params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        val child_game_id = row.getAs[Int]("child_game_id");
        val publish_time = row.getAs[Date]("reg_time").toString;
        val count_dev = row.getAs[Long]("count_dev").toInt;
        // 获取redis的数据
        val redisValue = JedisUtil.getRedisValue(child_game_id, "", publish_time.substring(0, 10), jedis)
        val parent_game_id = redisValue(0);
        val os = redisValue(1);
        val group_id = redisValue(6);
        params.+=(Array[Any](parent_game_id, child_game_id, publish_time.substring(0, 10), count_dev, os, group_id))
        JdbcUtil.executeUpdate(ps, params, conn)
      })
      ps.close()
      conn.close()
      pool.returnResource(jedis)
      pool.destroy()
    })
  }


  def foreachRegiDayPkgAccPartition(df_day_pkg_acc: DataFrame) = {
    df_day_pkg_acc.foreachPartition(iter => {

      // redis 链接
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis: Jedis = pool.getResource;
      jedis.select(0);

      val conn: Connection = JdbcUtil.getConn();
      val sql = "INSERT INTO bi_gamepublic_base_day_kpi(parent_game_id,child_game_id,medium_channel,ad_site_channel,pkg_code,publish_date,new_regi_account_num,os,group_id,medium_account,promotion_channel,promotion_mode,head_people) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY update os=values(os),group_id=values(group_id),medium_account=values(medium_account),promotion_channel=values(promotion_channel),promotion_mode=values(promotion_mode),head_people=values(head_people),new_regi_account_num= VALUES(new_regi_account_num)"
      val ps = conn.prepareStatement(sql)
      var params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        val child_game_id = row.getAs[Int]("child_game_id");
        val parent_channel = row.getAs[String]("parent_channel");
        val child_channel = row.getAs[String]("child_channel");
        val ad_label = row.getAs[String]("ad_label");
        val publish_time = row.getAs[Date]("reg_time").toString;
        val count_acc = row.getAs[Long]("count_acc").toInt;
        // 获取redis的数据
        val redisValue = JedisUtil.getRedisValue(child_game_id, ad_label, publish_time.substring(0, 10), jedis)
        val parent_game_id = redisValue(0);
        val os = redisValue(1);
        val medium_account = redisValue(2);
        val promotion_channel = redisValue(3);
        val promotion_mode = redisValue(4);
        val head_people = redisValue(5);
        val group_id = redisValue(6);
        params.+=(Array[Any](parent_game_id, child_game_id, parent_channel, child_channel, ad_label, publish_time.substring(0, 10), count_acc, os, group_id, medium_account, promotion_channel, promotion_mode, head_people))
        JdbcUtil.executeUpdate(ps, params, conn)
      })
      ps.close()
      conn.close()
      pool.returnResource(jedis)
      pool.destroy()
    })
  }

  def foreachRegiDayGameAccPartition(df_day_pkg_acc: DataFrame) = {
    df_day_pkg_acc.foreachPartition(iter => {

      // redis 链接
      val pool: JedisPool = JedisUtil.getJedisPool;
      val jedis: Jedis = pool.getResource;
      jedis.select(0);
      val conn: Connection = JdbcUtil.getConn();
      val sql = "INSERT INTO bi_gamepublic_base_opera_kpi(parent_game_id,child_game_id,publish_date,new_regi_account_num,os,group_id) VALUES (?,?,?,?,?,?) ON DUPLICATE KEY update os=values(os),group_id=values(group_id),new_regi_account_num= VALUES(new_regi_account_num)"
      val ps = conn.prepareStatement(sql)
      var params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        val child_game_id = row.getAs[Int]("child_game_id");
        val publish_time = row.getAs[Date]("reg_time").toString;
        val count_acc = row.getAs[Long]("count_acc").toInt;
        // 获取redis的数据
        val redisValue = JedisUtil.getRedisValue(child_game_id, "", publish_time.substring(0, 10), jedis)
        val parent_game_id = redisValue(0);
        val os = redisValue(1);
        val group_id = redisValue(6);
        params.+=(Array[Any](parent_game_id, child_game_id, publish_time.substring(0, 10), count_acc, os, group_id))
        JdbcUtil.executeUpdate(ps, params, conn)
      })

      ps.close()
      conn.close()
      pool.returnResource(jedis)
      pool.destroy()
    })
  }


  /**
    * 钱大师离线逻辑
    *
    * @param sc
    * @param hiveContext
    * @param startTime
    * @param endTime
    */
  def loadMoneyInfoOffline(sc: SparkContext, hiveContext: HiveContext, startTime: String, endTime: String) = {

    hiveContext.sql("use yyft");
    val rddconf = sc.textFile(ConfigurationUtil.getProperty("gamepublish.offline.thirddata"))
      .filter(t => {
        var bean: MoneyMasterBean = null;
        if (t.indexOf("{") >= 0) {
          bean = AnalysisJsonUtil.AnalysisMoneyMasterData(t.substring(t.indexOf("{")));
        }

        (t.contains("bi_adv_money_click") || t.contains("bi_adv_money_active")) && (bean != null) && (StringUtils.isNumber(bean.getGame_id)) && (StringUtils.isNumber(bean.getApp_id))
      }).map(t => {
      val bean = AnalysisJsonUtil.AnalysisMoneyMasterData(t.substring(t.indexOf("{")))

      Row(bean.getGame_id, bean.getPkg_id, bean.getApp_id, bean.getTs, bean.getImei, bean.getIp, bean.getAdv_name)
    }).persist()

    val schema_rddconf = (new StructType).add("game_id", StringType).add("pkg_id", StringType).add("app_id", StringType).add("ts", StringType).add("imei", StringType).add("ip", StringType).add("adv_name", StringType)
    hiveContext.createDataFrame(rddconf, schema_rddconf).persist().registerTempTable("ods_thirddata");

    // 一. 点击相关
    val sql_money_click = "select  om.publish_date,om.child_game_id,count(distinct imei) click_dev_num,sum(click_num) click_num from \n(\nselect to_date(ts) publish_date,game_id child_game_id,imei,count(1) click_num from ods_thirddata where  adv_name='bi_adv_money_click' and to_date(ts)>='startTime' and  to_date(ts)<='endTime' group by to_date(ts),game_id,imei\n) om group by  om.publish_date,om.child_game_id"
      .replace("startTime", startTime).replace("endTime", endTime)
    val df_money_click: DataFrame = hiveContext.sql(sql_money_click);
    foreachMoneyClickPartition(df_money_click)

    // 二. 钱大师激活相关
    // 1.补全激活明细
    val sql_money_active_detail = "select game_id,pkg_id,app_id,min(TIMESTAMP(ts)) ts,imei from ods_thirddata where  adv_name='bi_adv_money_active' and to_date(ts)>='startTime' and  to_date(ts)<='endTime' group by  to_date(ts),game_id,pkg_id,app_id,imei"
      .replace("startTime", startTime).replace("endTime", endTime)
    val df_money_active_deatil: DataFrame = hiveContext.sql(sql_money_active_detail).persist();
    foreachMoneyActiveDetailPartition(df_money_active_deatil)
    df_money_active_deatil.registerTempTable("bi_ad_money_active_detail")

    // 2. mu_report_num(不去重),mu_active_num 激活数(媒体)
    // select game_id,to_date(active_time) active_time,count(distinct imei) from bi_ad_money_active_detail group by game_id, to_date(active_time)
    val sql_money_mu_ac_num = "select to_date(ts) ts,game_id,count(1),count(distinct imei) from ods_thirddata where  adv_name='bi_adv_money_active' and to_date(ts)>='startTime' and  to_date(ts)<='endTime'  group by game_id, to_date(ts)"
      .replace("startTime", startTime).replace("endTime", endTime)
    val df_money_mu_ac_num: DataFrame = hiveContext.sql(sql_money_mu_ac_num);
    foreachMoneyActiveMuAcNumPartition(df_money_mu_ac_num)

    // 二. 钱大师激活相关(去重)
    var repeat_active_num_rows = new ArrayBuffer[Row]()
    var repeat_active_num_rowsBroadCast: Broadcast[ArrayBuffer[Row]] = sc.broadcast(repeat_active_num_rows);

    var mu_repeat_active_num_rows = new ArrayBuffer[Row]()
    var mu_repeat_active_num_rowsBroadCast: Broadcast[ArrayBuffer[Row]] = sc.broadcast(mu_repeat_active_num_rows);

    var pyw_mu_active_num_rows = new ArrayBuffer[Row]()
    var pyw_mu_active_num_rowsBroadCast: Broadcast[ArrayBuffer[Row]] = sc.broadcast(pyw_mu_active_num_rows);

    var pyw_un_active_num_rows = new ArrayBuffer[Row]()
    var pyw_un_active_num_rowsBroadCast: Broadcast[ArrayBuffer[Row]] = sc.broadcast(pyw_un_active_num_rows);

    var regi_active_num_rows = new ArrayBuffer[Row]()
    var regi_active_num_rowsBroadCast: Broadcast[ArrayBuffer[Row]] = sc.broadcast(regi_active_num_rows);

    var new_mu_regi_dev_num_rows = new ArrayBuffer[Row]()
    var new_mu_regi_dev_num_rowsBroadCast: Broadcast[ArrayBuffer[Row]] = sc.broadcast(new_mu_regi_dev_num_rows);


    df_money_active_deatil.foreachPartition(iter => {
      //数据库链接
      val conn: Connection = JdbcUtil.getConn();
      val stmt = conn.createStatement();

      // 没有统计到的激活明细
      val sql_unactive_deatil = "INSERT INTO bi_ad_money_unactive_detail(game_id,pkg_code,app_id,active_time,imei) VALUES (?,?,?,?,?) "
      val ps_unactive_deatil = conn.prepareStatement(sql_unactive_deatil)
      var params_unactive_deatil = new ArrayBuffer[Array[Any]]()

      iter.foreach(row => {

        val game_id = row.get(0);
        val time = row.get(3).toString();
        val imei = row.get(4);

        val sql_active_pyw_day = "select imei from bi_gamepublic_active_detail where  game_id='" + game_id + "'  and date(active_hour)='" + time.substring(0, 10) + "' and imei='" + imei + "'";
        val rz_active_pyw_day = stmt.executeQuery(sql_active_pyw_day);

        if (rz_active_pyw_day.next()) {
          pyw_mu_active_num_rowsBroadCast.value.+=(row);
        } else {
          val sql_active_pyw_all = "select imei from bi_gamepublic_active_detail where  game_id='" + game_id + "'  and  date(active_hour)<='" + time.substring(0, 10) + "'and imei='" + imei + "'";
          val rz_active_pyw_all = stmt.executeQuery(sql_active_pyw_all);
          if (!rz_active_pyw_all.next()) {
            val sql_unactive_all = "select imei from bi_ad_money_unactive_detail where game_id='" + game_id + "'  and date(active_time)='" + time.substring(0, 10) + "' and imei='" + imei + "'";
            val rz_unactive_all = stmt.executeQuery(sql_unactive_all);
            if (!rz_unactive_all.next()) {
              pyw_un_active_num_rowsBroadCast.value.+=(row);
              params_unactive_deatil.+=(Array[Any](row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)))
              JdbcUtil.executeUpdate(ps_unactive_deatil, params_unactive_deatil, conn);
            }
          } else {
            val sql_active_beforetoday = "select imei from bi_ad_money_active_detail where  game_id='" + game_id + "'  and date(active_time)<'" + time.substring(0, 10) + "' and imei='" + imei + "'";
            val rz_active_beforetoday = stmt.executeQuery(sql_active_beforetoday);
            if (rz_active_beforetoday.next()) {
              mu_repeat_active_num_rowsBroadCast.value.+=(row);
            }

            val sql_active_pyw_beforetoday = "select imei from bi_gamepublic_active_detail where  game_id='" + game_id + "'  and  date(active_hour)<'" + time.substring(0, 10) + "' and imei='" + imei + "'";
            val rz_active_pyw_beforetoday = stmt.executeQuery(sql_active_pyw_beforetoday);
            if (rz_active_pyw_beforetoday.next()) {
              repeat_active_num_rowsBroadCast.value.+=(row);
            }
          }
        }

        val sql_regi_day = "select * from bi_gamepublic_regi_detail where game_id='" + game_id + "'  and date(regi_hour)= '" + time.substring(0, 10) + "'and imei='" + imei + "'"
        val rz_regi_day = stmt.executeQuery(sql_regi_day);
        if (rz_regi_day.next()) {
          regi_active_num_rowsBroadCast.value.+=(row);
          val sql_regi_beforetoday = "select * from bi_gamepublic_regi_detail where game_id='" + game_id + "'  and date(regi_hour) < '" + time.substring(0, 10) + "'and imei='" + imei + "'"
          val rz_regi_beforetoday = stmt.executeQuery(sql_regi_beforetoday);
          if (!rz_regi_beforetoday.next()) {
            new_mu_regi_dev_num_rowsBroadCast.value.+=(row);
          }
        }
      })
      ps_unactive_deatil.close()
      stmt.close()
      conn.close
    })

    // game_id,pkg_id,app_id,min(TIMESTAMP(ts)) active_time,imei
    val schema = (new StructType).add("game_id", StringType).add("pkg_id", StringType).add("app_id", StringType).add("ts", TimestampType).add("imei", StringType)

    // 1. repeat_active_num  重复激活数(重复朋友玩和媒体)
    val rdd_repeat_active_num = sc.parallelize(repeat_active_num_rows);
    hiveContext.createDataFrame(rdd_repeat_active_num, schema).persist().registerTempTable("repeat_active_num");
    val df_repeat_active_num: DataFrame = hiveContext.sql("select to_date(ts) ts , game_id,count(distinct imei)  from repeat_active_num group by to_date(ts) , game_id")
    foreachMoneyRACPartition(df_repeat_active_num)

    // 2. mu_repeat_active_num  重复激活数(重复媒体)
    val rdd_mu_repeat_active_num = sc.parallelize(mu_repeat_active_num_rows);
    hiveContext.createDataFrame(rdd_mu_repeat_active_num, schema).persist().registerTempTable("mu_repeat_active_num");
    val df_mu_repeat_active_num: DataFrame = hiveContext.sql("select to_date(ts) ts , game_id,count(distinct imei)  from mu_repeat_active_num group by to_date(ts) , game_id")
    foreachMoneyMRACPartition(df_mu_repeat_active_num)

    //3. pyw_mu_active_num  统计到激活数
    val rdd_pyw_mu_active_num = sc.parallelize(pyw_mu_active_num_rows);
    hiveContext.createDataFrame(rdd_pyw_mu_active_num, schema).persist().registerTempTable("pyw_mu_active_num");
    val df_pyw_mu_active_num: DataFrame = hiveContext.sql("select to_date(ts) ts , game_id,count(distinct imei)  from pyw_mu_active_num group by to_date(ts) , game_id")
    foreachMoneyPMANPartition(df_pyw_mu_active_num)

    //4. pyw_un_active_num  未统计到激活数
    val rdd_pyw_un_active_num = sc.parallelize(pyw_un_active_num_rows);
    hiveContext.createDataFrame(rdd_pyw_un_active_num, schema).persist().registerTempTable("pyw_un_active_num");
    val df_pyw_un_active_num: DataFrame = hiveContext.sql("select to_date(ts) ts , game_id,count(distinct imei)  from pyw_un_active_num group by to_date(ts) , game_id")
    foreachMoneyPUANPartition(df_pyw_un_active_num)

    //5.regi_active_num 朋友玩注册和钱大师激活重复的设备数
    val rdd_regi_active_num = sc.parallelize(regi_active_num_rows);
    hiveContext.createDataFrame(rdd_regi_active_num, schema).persist().registerTempTable("regi_active_num");
    val df_regi_active_num: DataFrame = hiveContext.sql("select to_date(ts) ts , game_id,count(distinct imei)  from regi_active_num group by to_date(ts), game_id")
    foreachMoneyRANPartition(df_regi_active_num)

    //6.new_mu_regi_dev_num 新增自然注册设备数
    val rdd_new_mu_regi_dev_num = sc.parallelize(new_mu_regi_dev_num_rows);
    hiveContext.createDataFrame(rdd_new_mu_regi_dev_num, schema).persist().registerTempTable("new_mu_regi_dev_num");
    val df_new_mu_regi_dev_num: DataFrame = hiveContext.sql("select to_date(ts) ts , game_id,count(distinct imei)  from new_mu_regi_dev_num group by to_date(ts) , game_id")
    foreachMoneyNMRDPartition(df_new_mu_regi_dev_num)

    // 三.曲线更新mysql
    loadMessFromMysql(startTime, endTime)
  }

  /**
    * 钱大师点击
    *
    * @param df_money_click
    */
  def foreachMoneyClickPartition(df_money_click: DataFrame) = {
    df_money_click.foreachPartition(iter => {
      //数据库链接
      val conn: Connection = JdbcUtil.getConn();
      val sql = "INSERT INTO bi_ad_money_base_day_kpi(publish_date,child_game_id,click_dev_num,click_num)\nVALUES(?,?,?,?)\nON DUPLICATE KEY update click_dev_num=values(click_dev_num),click_num=values(click_num)"
      val ps = conn.prepareStatement(sql)
      var params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        params.+=(Array[Any](row.get(0), row.get(1), row.get(2), row.get(3)))
        JdbcUtil.executeUpdate(ps, params, conn)
      })
      ps.close()
      conn.close()
    })
  }

  /**
    * 补全激活明细
    *
    * @param df_money_active_deatil
    */
  def foreachMoneyActiveDetailPartition(df_money_active_deatil: DataFrame) = {
    df_money_active_deatil.foreachPartition(iter => {
      //数据库链接
      val conn: Connection = JdbcUtil.getConn();
      val stmt = conn.createStatement();
      val sql_active_deatil = "INSERT INTO bi_ad_money_active_detail(game_id,pkg_code,app_id,active_time,imei) VALUES (?,?,?,?,?) "
      val ps = conn.prepareStatement(sql_active_deatil)
      var params = new ArrayBuffer[Array[Any]]()

      iter.foreach(row => {
        val sql_active_day = "select imei from bi_ad_money_active_detail where game_id='" + row.get(0) + "'  and date(active_time)='" + row.get(3).toString.substring(0, 10) + "' and imei='" + row.get(4).toString + "'";
        val rz_active_day = stmt.executeQuery(sql_active_day);
        if (!rz_active_day.next()) {
          params.+=(Array[Any](row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)))
          JdbcUtil.executeUpdate(ps, params, conn)
        }

      })

      ps.close()
      conn.close()
      stmt.close()
      conn.close

    })
  }

  /**
    * 钱大师 mu_active_num 激活数(媒体)
    *
    * @param df_money_mu_ac_num
    */
  def foreachMoneyActiveMuAcNumPartition(df_money_mu_ac_num: DataFrame) = {
    df_money_mu_ac_num.foreachPartition(iter => {
      //数据库链接
      val conn: Connection = JdbcUtil.getConn();
      val sql = "INSERT INTO bi_ad_money_base_day_kpi(publish_date,child_game_id,mu_report_num,mu_active_num)\nVALUES(?,?,?,?)\nON DUPLICATE KEY update mu_report_num=values(mu_report_num),mu_active_num=values(mu_active_num)"
      val ps = conn.prepareStatement(sql)
      var params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        params.+=(Array[Any](row.get(0), row.get(1), row.get(2), row.get(3)))
        JdbcUtil.executeUpdate(ps, params, conn)
      })
      ps.close()
      conn.close()
    })
  }

  /**
    * repeat_active_num_rows   重复激活数(重复朋友玩和媒体)
    *
    * @param df_repeat_active_num
    */
  def foreachMoneyRACPartition(df_repeat_active_num: DataFrame) = {
    df_repeat_active_num.foreachPartition(iter => {
      //数据库链接
      val conn: Connection = JdbcUtil.getConn();
      val sql = "INSERT INTO bi_ad_money_base_day_kpi(publish_date,child_game_id,repeat_active_num)\nVALUES(?,?,?)\nON DUPLICATE KEY update repeat_active_num=values(repeat_active_num)"
      val ps = conn.prepareStatement(sql)
      var params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        params.+=(Array[Any](row.get(0), row.get(1), row.get(2)))
        JdbcUtil.executeUpdate(ps, params, conn)
      })
      ps.close()
      conn.close()
    })
  }

  def foreachMoneyMRACPartition(df_mu_repeat_active_num: DataFrame) = {

    df_mu_repeat_active_num.foreachPartition(iter => {
      //数据库链接
      val conn: Connection = JdbcUtil.getConn();
      val sql = "INSERT INTO bi_ad_money_base_day_kpi(publish_date,child_game_id,mu_repeat_active_num)\nVALUES(?,?,?)\nON DUPLICATE KEY update mu_repeat_active_num=values(mu_repeat_active_num)"
      val ps = conn.prepareStatement(sql)
      var params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        params.+=(Array[Any](row.get(0), row.get(1), row.get(2)))
        JdbcUtil.executeUpdate(ps, params, conn)
      })
      ps.close()
      conn.close()
    })
  }

  def foreachMoneyPMANPartition(df_pyw_mu_active_num: DataFrame) = {
    df_pyw_mu_active_num.foreachPartition(iter => {
      //数据库链接
      val conn: Connection = JdbcUtil.getConn();
      val sql = "INSERT INTO bi_ad_money_base_day_kpi(publish_date,child_game_id,pyw_mu_active_num)\nVALUES(?,?,?)\nON DUPLICATE KEY update pyw_mu_active_num=values(pyw_mu_active_num)"
      val ps = conn.prepareStatement(sql)
      var params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        params.+=(Array[Any](row.get(0), row.get(1), row.get(2)))
        JdbcUtil.executeUpdate(ps, params, conn)
      })
      ps.close()
      conn.close()
    })
  }


  def foreachMoneyPUANPartition(df_pyw_un_active_num: DataFrame) = {
    df_pyw_un_active_num.foreachPartition(iter => {
      //数据库链接
      val conn: Connection = JdbcUtil.getConn();
      val sql = "INSERT INTO bi_ad_money_base_day_kpi(publish_date,child_game_id,pyw_un_active_num)\nVALUES(?,?,?)\nON DUPLICATE KEY update pyw_un_active_num=values(pyw_un_active_num)"
      val ps = conn.prepareStatement(sql)
      var params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        params.+=(Array[Any](row.get(0), row.get(1), row.get(2)))
        JdbcUtil.executeUpdate(ps, params, conn)
      })
      ps.close()
      conn.close()

    })
  }


  def foreachMoneyRANPartition(df_regi_active_num: DataFrame) = {
    df_regi_active_num.foreachPartition(iter => {
      //数据库链接
      val conn: Connection = JdbcUtil.getConn();
      val sql = "INSERT INTO bi_ad_money_base_day_kpi(publish_date,child_game_id,regi_active_num)\nVALUES(?,?,?)\nON DUPLICATE KEY update regi_active_num=values(regi_active_num)"
      val ps = conn.prepareStatement(sql)
      var params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        params.+=(Array[Any](row.get(0), row.get(1), row.get(2)))
        JdbcUtil.executeUpdate(ps, params, conn)
      })
      ps.close()
      conn.close()
    })
  }

  def foreachMoneyNMRDPartition(df_new_mu_regi_dev_num: DataFrame) = {
    df_new_mu_regi_dev_num.foreachPartition(iter => {
      //数据库链接
      val conn: Connection = JdbcUtil.getConn();
      val sql = "INSERT INTO bi_ad_money_base_day_kpi(publish_date,child_game_id,new_mu_regi_dev_num)\nVALUES(?,?,?)\nON DUPLICATE KEY update new_mu_regi_dev_num=values(new_mu_regi_dev_num)"
      val ps = conn.prepareStatement(sql)
      var params = new ArrayBuffer[Array[Any]]()
      iter.foreach(row => {
        params.+=(Array[Any](row.get(0), row.get(1), row.get(2)))
        JdbcUtil.executeUpdate(ps, params, conn)
      })
      ps.close()
      conn.close()
    })

  }

  /**
    * 把其他实时表的数据导入到这个表
    */
  def loadMessFromMysql(startday: String, endday: String) = {
    val conn: Connection = JdbcUtil.getConn();
    val stmt = conn.createStatement();

    val sql1 = "update bi_ad_money_base_day_kpi adkpi inner join \n(\nselect publish_date,child_game_id,sum(active_num) active_num,sum(regi_device_num) regi_device_num,sum(new_regi_device_num) new_regi_device_num from bi_gamepublic_base_opera_kpi \nwhere  publish_date>='startday' and publish_date<='endday' group by  publish_date,child_game_id\n) rs on adkpi.publish_date =rs.publish_date and adkpi.child_game_id =rs.child_game_id\nset adkpi.pyw_active_num= rs.active_num,adkpi.pyw_regi_dev_num=rs.regi_device_num,adkpi.new_regi_dev_num= rs.new_regi_device_num"
      .replace("startday", startday).replace("endday", endday);
    val sql2 = "update bi_ad_money_base_day_kpi adkpi set \nadkpi.pyw_repeat_active_num=if(adkpi.repeat_active_num-adkpi.mu_repeat_active_num>0,adkpi.repeat_active_num-adkpi.mu_repeat_active_num,0),\nadkpi.natural_regi_dev_num=if(adkpi.pyw_regi_dev_num-regi_active_num>0,adkpi.pyw_regi_dev_num-regi_active_num,0),\nadkpi.new_natural_regi_dev_num=if(adkpi.new_regi_dev_num-adkpi.new_mu_regi_dev_num>0,adkpi.new_regi_dev_num-adkpi.new_mu_regi_dev_num,0)  where publish_date>='startday' and publish_date<='endday'"
      .replace("startday", startday).replace("endday", endday);
    val sql3 = "update bi_ad_money_base_day_kpi adkpi inner join \n(\nselect date(active_time) publish_date,game_id child_game_id, count(distinct imei) pyw_un_active_num from  bi_ad_money_unactive_detail\nwhere  date(active_time)>='startday' and date(active_time)<='endday' group by  date(active_time),game_id\n) rs on adkpi.publish_date =rs.publish_date and adkpi.child_game_id =rs.child_game_id\nset adkpi.pyw_un_active_num= rs.pyw_un_active_num"
      .replace("startday", startday).replace("endday", endday);

    stmt.execute(sql1)
    stmt.execute(sql2)
    stmt.execute(sql3)
    stmt.close()
    conn.close();
  }
}
