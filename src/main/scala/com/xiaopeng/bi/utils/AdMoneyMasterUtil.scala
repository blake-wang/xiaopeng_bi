package com.xiaopeng.bi.utils

import java.sql.{Connection, PreparedStatement, Statement}

import com.xiaopeng.bi.bean.MoneyMasterBean
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kequan on 8/31/17.
  * 钱大师广告监控
  */
object AdMoneyMasterUtil {

  def loadMoneyMaterInfo(rdd: RDD[String], hiveContext: HiveContext) = {

    val moneyRdd: RDD[MoneyMasterBean] = rdd.filter(t => {
      var bean: MoneyMasterBean = null;
      if (t.indexOf("{") >= 0) {
        bean = AnalysisJsonUtil.AnalysisMoneyMasterData(t.substring(t.indexOf("{")));
      }
      (t.contains("bi_adv_money_click") || t.contains("bi_adv_money_active")) && (bean != null) && (StringUtils.isNumber(bean.getGame_id)) && (StringUtils.isNumber(bean.getApp_id))
    }).map(t => {
      AnalysisJsonUtil.AnalysisMoneyMasterData(t.substring(t.indexOf("{")))
    })

    loadMessFromMysql();
    foreachMoneyPartition(moneyRdd);
    updateMysql();
  }


  def foreachMoneyPartition(clickRdd: RDD[MoneyMasterBean]) = {

    clickRdd.foreachPartition(iter => {
      if (!iter.isEmpty) {
        //数据库链接
        val conn: Connection = JdbcUtil.getConn();
        val stmt: Statement = conn.createStatement();

        //  一.点击相关
        // 1.点击明细
        val sql_click_deatil = "INSERT INTO bi_ad_money_click_detail(game_id,pkg_code,app_id,click_time,imei) VALUES (?,?,?,?,?) "
        val ps_click_deatil: PreparedStatement = conn.prepareStatement(sql_click_deatil);
        var params_click_deatil = new ArrayBuffer[Array[Any]]()

        // 2.bi_ad_money_base_day_kpi
        val sql_click_base = "INSERT INTO bi_ad_money_base_day_kpi(publish_date,child_game_id,click_num,click_dev_num)\nVALUES(?,?,?,?)\n ON DUPLICATE KEY update\nclick_num=VALUES(click_num)+click_num,\nclick_dev_num=VALUES(click_dev_num)+click_dev_num"
        val ps_click_base: PreparedStatement = conn.prepareStatement(sql_click_base);
        var params_click_base = new ArrayBuffer[Array[Any]]()

        // 二.激活相关
        // 1.钱大师激活明细
        val sql_active_deatil = "INSERT INTO bi_ad_money_active_detail(game_id,pkg_code,app_id,active_time,imei) VALUES (?,?,?,?,?) "
        val ps_active_deatil: PreparedStatement = conn.prepareStatement(sql_active_deatil);
        var params_active_deatil = new ArrayBuffer[Array[Any]]()

        // 2.没有统计到的激活明细
        val sql_unactive_deatil = "INSERT INTO bi_ad_money_unactive_detail(game_id,pkg_code,app_id,active_time,imei) VALUES (?,?,?,?,?) "
        val ps_unactive_deatil: PreparedStatement = conn.prepareStatement(sql_unactive_deatil);
        var params_unactive_deatil = new ArrayBuffer[Array[Any]]()

        // 3.bi_ad_money_base_day_kpi
        val sql_active_base = "INSERT INTO bi_ad_money_base_day_kpi(publish_date,child_game_id,mu_report_num,mu_active_num,repeat_active_num,mu_repeat_active_num,pyw_mu_active_num,pyw_un_active_num,regi_active_num,new_mu_regi_dev_num)\nVALUES(?,?,?,?,?,?,?,?,?,?)\nON DUPLICATE KEY update mu_report_num=VALUES(mu_report_num)+mu_report_num,mu_active_num=VALUES(mu_active_num)+mu_active_num,\nrepeat_active_num=VALUES(repeat_active_num)+repeat_active_num,\nmu_repeat_active_num=VALUES(mu_repeat_active_num)+mu_repeat_active_num,\npyw_mu_active_num=VALUES(pyw_mu_active_num)+pyw_mu_active_num,\npyw_un_active_num=VALUES(pyw_un_active_num)+pyw_un_active_num,\nregi_active_num=VALUES(regi_active_num)+regi_active_num,\nnew_mu_regi_dev_num=VALUES(new_mu_regi_dev_num)+new_mu_regi_dev_num"
        val ps_active_base: PreparedStatement = conn.prepareStatement(sql_active_base);
        var params_active_base = new ArrayBuffer[Array[Any]]()

        iter.foreach(bean => {
          val adv_name = bean.getAdv_name
          val game_id = bean.getGame_id;
          val pkg_code = bean.getPkg_id;
          val app_id = bean.getApp_id;
          val time = bean.getTs;
          val imei = bean.getImei;

          if (adv_name.equals("bi_adv_money_click")) {

            val sql_click_day = "select imei from bi_ad_money_click_detail where  game_id='" + game_id + "'  and date(click_time)='" + time.substring(0, 10) + "' and imei='" + imei + "'";
            val rz_click_day = stmt.executeQuery(sql_click_day);
            var click_num = 0;
            var click_dev_num = 0;

            if (!rz_click_day.next()) {
              params_click_deatil.+=(Array[Any](game_id, pkg_code, app_id, time, imei))
              click_num = 1;
              click_dev_num = 1;
            } else {
              click_num = 1;
            }
            params_click_base.+=(Array[Any](time.substring(0, 10), game_id, click_num, click_dev_num))

            //插入数据库
            JdbcUtil.executeUpdate(ps_click_deatil, params_click_deatil, conn);
            JdbcUtil.executeUpdate(ps_click_base, params_click_base, conn);

          } else if (adv_name.equals("bi_adv_money_active")) {
            // 上报激活数,未去重
            var mu_report_num = 1;
            // 激活数(媒体)
            var mu_active_num = 0;
            // 重复激活数(重复朋友玩和媒体)
            var repeat_active_num = 0;
            // 重复激活数(重复媒体)
            var mu_repeat_active_num = 0;
            // 统计到激活数
            var pyw_mu_active_num = 0;
            // 未统计到激活数
            var pyw_un_active_num = 0;
            // 朋友玩注册和钱大师激活重复的设备数
            var regi_active_num = 0;
            // 钱大师新增自然注册设备数
            var new_mu_regi_dev_num = 0;

            val sql_active_day = "select imei from bi_ad_money_active_detail where  game_id='" + game_id + "'  and date(active_time)='" + time.substring(0, 10) + "' and imei='" + imei + "'";
            val rz_active_day = stmt.executeQuery(sql_active_day);
            if (!rz_active_day.next()) {
              params_active_deatil.+=(Array[Any](game_id, pkg_code, app_id, time, imei))
              mu_active_num = 1;

              val sql_active_pyw_day = "select imei from bi_gamepublic_active_detail where  game_id='" + game_id + "'  and date(active_hour)='" + time.substring(0, 10) + "' and imei='" + imei + "'";
              val rz_active_pyw_day = stmt.executeQuery(sql_active_pyw_day);
              if (rz_active_pyw_day.next()) {
                pyw_mu_active_num = 1;
              } else {
                val sql_active_pyw_all = "select imei from bi_gamepublic_active_detail where  game_id='" + game_id + "'  and  date(active_hour)<='" + time.substring(0, 10) + "'and imei='" + imei + "'";
                val rz_active_pyw_all = stmt.executeQuery(sql_active_pyw_all);
                if (!rz_active_pyw_all.next()) {
                  params_unactive_deatil.+=(Array[Any](game_id, pkg_code, app_id, time, imei))
                  pyw_un_active_num = 1;
                } else {
                  val sql_active_pyw_beforetoday = "select imei from bi_gamepublic_active_detail where  game_id='" + game_id + "'  and  date(active_hour)<'" + time.substring(0, 10) + "' and imei='" + imei + "'";
                  val rz_active_pyw_beforetoday = stmt.executeQuery(sql_active_pyw_beforetoday);
                  if (rz_active_pyw_beforetoday.next()) {
                    repeat_active_num = 1;
                    val sql_active_beforetoday = "select imei from bi_ad_money_active_detail where  game_id='" + game_id + "'  and date(active_time)<'" + time.substring(0, 10) + "' and imei='" + imei + "'";
                    val rz_active_beforetoday = stmt.executeQuery(sql_active_beforetoday);
                    if (rz_active_beforetoday.next()) {
                      mu_repeat_active_num = 1;
                    }
                  }
                }
              }

              val sql_regi_day = "select imei from bi_gamepublic_regi_detail where game_id='" + game_id + "'  and date(regi_hour)= '" + time.substring(0, 10) + "'and imei='" + imei + "'"
              val rz_regi_day = stmt.executeQuery(sql_regi_day);
              if (rz_regi_day.next()) {
                regi_active_num = 1;

                val sql_regi_beforetoday = "select imei from bi_gamepublic_regi_detail where game_id='" + game_id + "'  and date(regi_hour) < '" + time.substring(0, 10) + "'and imei='" + imei + "'"
                val rz_regi_beforetoday = stmt.executeQuery(sql_regi_beforetoday);
                if (!rz_regi_beforetoday.next()) {
                  new_mu_regi_dev_num = 1;
                }
              }
            }

            params_active_base.+=(Array[Any](time.substring(0, 10), game_id, mu_report_num, mu_active_num, repeat_active_num, mu_repeat_active_num, pyw_mu_active_num, pyw_un_active_num, regi_active_num, new_mu_regi_dev_num))

            //插入数据库
            JdbcUtil.executeUpdate(ps_active_deatil, params_active_deatil, conn);
            JdbcUtil.executeUpdate(ps_unactive_deatil, params_unactive_deatil, conn);
            JdbcUtil.executeUpdate(ps_active_base, params_active_base, conn);

          }
        })
        stmt.close()
        conn.close()
      }
    })
  }

  /**
    * 把其他实时表的数据导入到这个表
    */
  def loadMessFromMysql() = {
    val conn: Connection = JdbcUtil.getConn();
    val stmt = conn.createStatement();
    val startday = DateUtils.getCriticalDate;
    val endday = DateUtils.getTodayDate();

    val sql = "update bi_ad_money_base_day_kpi adkpi inner join \n(\nselect publish_date,child_game_id,sum(active_num) active_num,sum(regi_device_num) regi_device_num,sum(new_regi_device_num) new_regi_device_num from bi_gamepublic_base_opera_kpi \nwhere  publish_date>='startday' and publish_date<='endday' group by  publish_date,child_game_id\n) rs on adkpi.publish_date =rs.publish_date and adkpi.child_game_id =rs.child_game_id\nset adkpi.pyw_active_num= rs.active_num,adkpi.pyw_regi_dev_num=rs.regi_device_num,adkpi.new_regi_dev_num= rs.new_regi_device_num"
      .replace("startday", startday).replace("endday", endday);
    stmt.execute(sql)
    stmt.close()
    conn.close();
  }

  def updateMysql() = {
    val conn: Connection = JdbcUtil.getConn();
    val stmt = conn.createStatement();
    val startday = DateUtils.getCriticalDate;
    val endday = DateUtils.getTodayDate();
    val sql1 = "update bi_ad_money_base_day_kpi adkpi inner join \n(\nselect date(active_time) publish_date,game_id child_game_id, count(distinct imei) pyw_un_active_num from  bi_ad_money_unactive_detail\nwhere  date(active_time)>='startday' and date(active_time)<='endday' group by  date(active_time),game_id\n) rs on adkpi.publish_date =rs.publish_date and adkpi.child_game_id =rs.child_game_id\nset adkpi.pyw_un_active_num= rs.pyw_un_active_num"
      .replace("startday", startday).replace("endday", endday);

    val sql2 = "update bi_ad_money_base_day_kpi adkpi set \nadkpi.pyw_repeat_active_num=if(adkpi.repeat_active_num-adkpi.mu_repeat_active_num>0,adkpi.repeat_active_num-adkpi.mu_repeat_active_num,0),\nadkpi.natural_regi_dev_num=if(adkpi.pyw_regi_dev_num-regi_active_num>0,adkpi.pyw_regi_dev_num-regi_active_num,0),\nadkpi.new_natural_regi_dev_num=if(adkpi.new_regi_dev_num-adkpi.new_mu_regi_dev_num>0,adkpi.new_regi_dev_num-adkpi.new_mu_regi_dev_num,0)  where publish_date>='startday' and publish_date<='endday'"
      .replace("startday", startday).replace("endday", endday);
    stmt.execute(sql1)

    stmt.execute(sql2)
    stmt.close()
    conn.close();
  }

}
