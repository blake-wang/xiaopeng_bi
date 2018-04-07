package com.xiaopeng.bi.centurioncard.rebate

import java.sql.PreparedStatement

import com.xiaopeng.bi.utils.JdbcUtil

object bi_rebate_exec_status {
  def main(args: Array[String]): Unit = {

    if (args.length!=4) {
      System.err.println("参数个数有问题，固定为4个： <rcid><year><month><conf_type> ")
      System.exit(1)
    }
    val currentrcid = args(0).toInt
    val year = args(1).toString
    val month=args(2).toString
    val conf_type=args(3).toInt
    val pi_yearmonth=year.concat(month)
    val conn = JdbcUtil.getConn()
    val ps_execstatus: PreparedStatement = conn.prepareStatement("insert into bi_rebate_exec_status(rcid,status,model) values(?,0,'rebate') on duplicate key update status=0");
    val del_bi_centurioncard_reb_deta: PreparedStatement = conn.prepareStatement("delete from bi_centurioncard_reb_deta where syearmounth=? and conf_type=?");
    del_bi_centurioncard_reb_deta.setString(1, pi_yearmonth);
    del_bi_centurioncard_reb_deta.setInt(2, conf_type);
    val del_bi_centurioncard_rebate: PreparedStatement = conn.prepareStatement("delete from bi_centurioncard_rebate where syearmounth=? and conf_type=?")
    del_bi_centurioncard_rebate.setString(1, pi_yearmonth);
    del_bi_centurioncard_rebate.setInt(2, conf_type);
    //下级返点
    val del_bi_centurioncard_reb_sub_deta: PreparedStatement = conn.prepareStatement("delete from bi_centurioncard_reb_sub_deta where syearmonth=? and conf_type=?");
    del_bi_centurioncard_reb_sub_deta.setString(1, pi_yearmonth);
    del_bi_centurioncard_reb_sub_deta.setInt(2, conf_type);
    val del_bi_centurioncard_sub_rebate: PreparedStatement = conn.prepareStatement("delete from bi_centurioncard_sub_rebate where syearmonth=? and conf_type=?")
    del_bi_centurioncard_sub_rebate.setString(1,pi_yearmonth);
    del_bi_centurioncard_sub_rebate.setInt(2, conf_type);

    //exec
    del_bi_centurioncard_rebate.executeUpdate()
    del_bi_centurioncard_reb_deta.executeUpdate()
    //下级
    del_bi_centurioncard_sub_rebate.executeUpdate()
    del_bi_centurioncard_reb_sub_deta.executeUpdate()

    //执行状态表
    ps_execstatus.setInt(1,currentrcid.toInt)
    ps_execstatus.executeUpdate()
    conn.close()
  }


}