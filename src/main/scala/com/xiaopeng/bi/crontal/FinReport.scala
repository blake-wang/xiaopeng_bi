package com.xiaopeng.bi.crontal

import java.io._
import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.xiaopeng.bi.utils.{JdbcUtil, JedisUtil}
import redis.clients.jedis.Jedis


/**
 * Created by denglh on 2016/8/24.
 * 发行报表
 */
object FinReport {
  /**
    * 充值代金券 计划代金券 消费代金券
    *
    * @return
    */
  def doConponAdd(startday:String,endday:String){
    val connXp2=JdbcUtil.getXiaopeng2Conn();
    val sql: String = " select \ndate(tran_time) tran_date,\nsum(case when order_id>0 then amount_org else 0 end) recharge_conpon,\nsum(case when order_id<0 then amount_org else 0 end) plan_conpon\nfrom pyw_coupon_trans \nwhere tran_time>=? and tran_time<? and type=0\ngroup by date(tran_time)"
    val sql2Mysql = "insert into bi_fin_report" +
      "(fin_date,recharge_conpon,plan_conpon)" +
      " values(?,?,?) " +
      " on duplicate key update recharge_conpon=?,plan_conpon=?"
    val stmt: PreparedStatement =connXp2.prepareStatement(sql)
    stmt.setString(1,startday)
    stmt.setString(2,endday)
    val rs: ResultSet = stmt.executeQuery()
    val conn=JdbcUtil.getConn()
    val ps=conn.prepareStatement(sql2Mysql)
    while (rs.next) {
      if(rs.getString("tran_date")!=null) {
        ps.setString(1, rs.getString("tran_date"))
        ps.setFloat(2, getFloat(rs.getString("recharge_conpon")))
        ps.setFloat(3, getFloat(rs.getString("plan_conpon")))
        //update
        ps.setFloat(4, getFloat(rs.getString("recharge_conpon")))
        ps.setFloat(5, getFloat(rs.getString("plan_conpon")))
        ps.executeUpdate()
      }
    }
    stmt.close()
    connXp2.close()
    conn.close()
  }



  /**
    * 剩余代金券 && 剩余现金价值
    *
    * @return
    */
  def doConponRemaidVSCashValue(startday:String,endday:String){
    val connXp2=JdbcUtil.getXiaopeng2Conn();
   val sql:String="select ? tran_date,\nsum(amount) amount,\nFORMAT(SUM((tran.amount)*FORMAT((d.payprice+d.pay_credit+d.plat_coin/10)/d.productnum/d.oriprice,2)),2) remain_case_value\nfrom (\nselect ts.coupon_id,tsz.order_id,orders.payprice,orders.pay_credit,orders.productnum,orders.plat_coin,orders.oriprice,max(ts.tran_time) tran_time from pyw_coupon_trans ts \njoin pyw_coupon_trans tsz on tsz.coupon_code=ts.coupon_code and tsz.order_id>0 and tsz.type=0 \njoin orders orders on tsz.order_id=orders.id and orders.state in(4,11) and tsz.type=0\nwhere ts.tran_time<?\ngroup by ts.coupon_id,tsz.order_id,orders.payprice,orders.pay_credit,orders.productnum,orders.plat_coin,orders.oriprice\n) d join \npyw_coupon_trans tran on tran.coupon_id=d.coupon_id and d.tran_time=tran.tran_time"
   // val sql: String = "select ? tran_date,\nsum(amount) amount\nfrom (\nselect ts.coupon_id,max(ts.tran_time) tran_time from pyw_coupon_trans ts \njoin pyw_coupon_trans tsz on tsz.coupon_code=ts.coupon_code and tsz.order_id>0 and tsz.type=0 \njoin orders orders on tsz.order_id=orders.id and orders.state in(4,11) and tsz.type=0\nwhere ts.tran_time<?\ngroup by ts.coupon_id\n) mx join \npyw_coupon_trans tran on tran.coupon_id=mx.coupon_id and mx.tran_time=tran.tran_time"
    val sql2Mysql = "insert into bi_fin_report" +
      " (fin_date,remain_conpon,remain_case_value)" +
      " values(?,?,?) " +
      " on duplicate key update remain_conpon=?,remain_case_value=?"

    val stmt: PreparedStatement =connXp2.prepareStatement(sql)
    stmt.setString(1,startday)
    stmt.setString(2,endday)
    val rs: ResultSet = stmt.executeQuery()
    val conn=JdbcUtil.getConn()
    val ps=conn.prepareStatement(sql2Mysql)
    while (rs.next) {
      if(rs.getString("tran_date")!=null) {
        println("amount"+getFloat(rs.getString("amount")))
        println("casevalune:"+getFloat(rs.getString("remain_case_value")))
        ps.setString(1, rs.getString("tran_date"))
        ps.setFloat(2, getFloat(rs.getString("amount")))
        ps.setFloat(3, getFloat(rs.getString("remain_case_value")))
        //update
        ps.setFloat(4, getFloat(rs.getString("amount")))
        ps.setFloat(5, getFloat(rs.getString("remain_case_value")))
        ps.executeUpdate()
      }
    }
    stmt.close()
    connXp2.close()
    conn.close()
  }

  /**
    * #退代金券
    *
    * @return
    */
  def doConponfe(startday:String,endday:String){
    val connXp2=JdbcUtil.getXiaopeng2Conn();

    val sql: String = "select date(refund_time) tran_date,sum(amount_org) refund_conpon \nfrom   \npyw_coupon_trans cpt  \njoin orders ods on ods.id=cpt.order_id and ods.state=8 and cpt.type=0\nwhere  refund_time>=? and refund_time<?\ngroup by date(refund_time)"
    val sql2Mysql = "insert into bi_fin_report" +
      " (fin_date,refund_conpon)" +
      " values(?,?) " +
      " on duplicate key update refund_conpon=?"

    val stmt: PreparedStatement =connXp2.prepareStatement(sql)
    stmt.setString(1,startday)
    stmt.setString(2,endday)
    val rs: ResultSet = stmt.executeQuery()
    val conn=JdbcUtil.getConn()
    val ps=conn.prepareStatement(sql2Mysql)
    while (rs.next) {
      if(rs.getString("tran_date")!=null) {
        ps.setString(1, rs.getString("tran_date"))
        ps.setFloat(2, getFloat(rs.getString("refund_conpon")))
        //update
        ps.setFloat(3, getFloat(rs.getString("refund_conpon")))
        ps.executeUpdate()
      }
    }
    stmt.close()
    connXp2.close()
    conn.close()
  }


  /**
    * 充值现金价值
    *
    * @return
    */
  def doCashValueAdd(startday:String,endday:String){
    val connXp2=JdbcUtil.getXiaopeng2Conn();

    val sql: String = "select DATE(tran_time) tran_date,FORMAT(SUM((b.amount_org)*FORMAT((d.payprice+d.pay_credit+d.plat_coin/10)/d.productnum/d.oriprice,2)),2) recharge_cash_value\nfrom pyw_coupon_trans b\njoin orders d on b.order_id=d.id and d.state in(4,11,8)\nwhere tran_time>=? and tran_time<? and b.type=0 and order_id>0"
    val sql2Mysql = "insert into bi_fin_report" +
      " (fin_date,recharge_cash_value)" +
      " values(?,?) " +
      " on duplicate key update recharge_cash_value=?"

    val stmt: PreparedStatement =connXp2.prepareStatement(sql)
    stmt.setString(1,startday)
    stmt.setString(2,endday)
    val rs: ResultSet = stmt.executeQuery()
    val conn=JdbcUtil.getConn()
    val ps=conn.prepareStatement(sql2Mysql)
    while (rs.next) {
      if(rs.getString("tran_date")!=null) {
        ps.setString(1, rs.getString("tran_date"))
        ps.setFloat(2, getFloat(rs.getString("recharge_cash_value")))
        //update
        ps.setFloat(3, getFloat(rs.getString("recharge_cash_value")))
        ps.executeUpdate()
      }
    }
    stmt.close()
    connXp2.close()
    conn.close()
  }




  /**
    * 退款现金价值
    *
    * @return
    */
  def doCashValueFe(startday:String,endday:String){
    val connXp2=JdbcUtil.getXiaopeng2Conn();
    val sql: String = "select DATE(refund_time) tran_date,FORMAT(SUM((b.amount_org)*FORMAT((d.payprice+d.pay_credit+d.plat_coin/10)/d.productnum/d.oriprice,2)),2) refund_cash_value\nfrom pyw_coupon_trans b\njoin orders d on b.order_id=d.id and d.state=8 and b.type=0\nwhere  refund_time>=? and refund_time<? and order_id>0 "
    val sql2Mysql = "insert into bi_fin_report" +
      " (fin_date,refund_cash_value)" +
      " values(?,?) " +
      " on duplicate key update refund_cash_value=?"
    val stmt: PreparedStatement =connXp2.prepareStatement(sql)
    stmt.setString(1,startday)
    stmt.setString(2,endday)
    val rs: ResultSet = stmt.executeQuery()
    val conn=JdbcUtil.getConn()
    val ps=conn.prepareStatement(sql2Mysql)
    while (rs.next) {
      if(rs.getString("tran_date")!=null) {
        ps.setString(1, rs.getString("tran_date"))
        ps.setFloat(2, getFloat(rs.getString("refund_cash_value")))
        //update
        ps.setFloat(3, getFloat(rs.getString("refund_cash_value")))
        ps.executeUpdate()
      }
    }
    stmt.close()
    connXp2.close()
    conn.close()
  }




  /**
    * 充值金额
    *
    * @return
    */
  def doRecharge(startday:String,endday:String){
    val connXp2=JdbcUtil.getXiaopeng2Conn();
    //val sql: String = "select date(erectime) tran_date,sum(oriprice)*productnum ori_price,sum(price) pay_price,count(distinct orderid) pay_ordernum from orders where erectime>=? and erectime<? and state in(4,8)"
    //排除沙盒订单
    val sql:String="select date(erectime) tran_date,sum(oriprice)*productnum ori_price,sum(price) pay_price,count(distinct orderid) pay_ordernum \nfrom orders ods\nleft join \n(select distinct rep.order_id from pywsdk_apple_receipt_verify ver join pywsdk_cp_req rep on ver.order_sn=rep.order_no where ver.sandbox=1 and state=3 and rep.status=1) ver\non ver.order_id=ods.id\nwhere erectime>=? and erectime<? and state in(4,8) and ver.order_id is null"
    val sql2Mysql = "insert into bi_fin_report" +
      " (fin_date,ori_price,pay_price,pay_ordernum)" +
      " values(?,?,?,?) " +
      " on duplicate key update ori_price=?,pay_price=?,pay_ordernum=?"
    val stmt: PreparedStatement =connXp2.prepareStatement(sql)
    stmt.setString(1,startday)
    stmt.setString(2,endday)
    val rs: ResultSet = stmt.executeQuery()
    val conn=JdbcUtil.getConn()
    val ps=conn.prepareStatement(sql2Mysql)
    while (rs.next) {
      if(rs.getString("tran_date")!=null)
        {
          ps.setString(1,rs.getString("tran_date"))
          ps.setFloat(2,getFloat(rs.getString("ori_price")))
          ps.setFloat(3,getFloat(rs.getString("pay_price")))
          ps.setInt(4,rs.getString("pay_ordernum").toInt)
          //update
          ps.setFloat(5,getFloat(rs.getString("ori_price")))
          ps.setFloat(6,getFloat(rs.getString("pay_price")))
          ps.setInt(7,rs.getString("pay_ordernum").toInt)
          ps.executeUpdate()
        }

    }
    stmt.close()
    connXp2.close()
    conn.close()
  }


  /**
    * 退款金额
    *
    * @return
    */
  def doFebund(startday:String,endday:String){
    val connXp2=JdbcUtil.getXiaopeng2Conn();
    val sql: String = "select date(refund_time) tran_date,sum(oriprice)*productnum ori_refund_price,sum(price) pay_refund_price,count(distinct orderid) refund_ordernum \nfrom orders where refund_time>=? and refund_time<? and state=8"
    val sql2Mysql = "insert into bi_fin_report" +
      " (fin_date,ori_refund_price,pay_refund_price,refund_ordernum)" +
      " values(?,?,?,?) " +
      " on duplicate key update ori_refund_price=?,pay_refund_price=?,refund_ordernum=?"
    val stmt: PreparedStatement =connXp2.prepareStatement(sql)
    stmt.setString(1,startday)
    stmt.setString(2,endday)
    val rs: ResultSet = stmt.executeQuery()
    val conn=JdbcUtil.getConn()
    val ps=conn.prepareStatement(sql2Mysql)
    while (rs.next) {
      if(rs.getString("tran_date")!=null) {
        ps.setString(1, rs.getString("tran_date"))
        ps.setFloat(2, getFloat(rs.getString("ori_refund_price")))
        ps.setFloat(3, getFloat(rs.getString("pay_refund_price")))
        ps.setInt(4,rs.getString("refund_ordernum").toInt)
        //update
        ps.setFloat(5, getFloat(rs.getString("ori_refund_price")))
        ps.setFloat(6, getFloat(rs.getString("pay_refund_price")))
        ps.setInt(7,rs.getString("refund_ordernum").toInt)
        ps.executeUpdate()
      }
    }
    stmt.close()
    connXp2.close()
    conn.close()
  }


  /**
    * 下载财务概况报表明细
    */

  def doDownLoadFinDay(startday:String,endday:String,path:String)={
    val file: File = new File(path+"finn_" + startday.trim.replace("-", "") + ".csv1")
    file.delete
    val writerStream = new OutputStreamWriter(new FileOutputStream(file),"UTF-8");
    var writer: BufferedWriter =null
    val header: String = "订单号,所属游戏,游戏ID,充值类型,渠道名,支付方式,状态,支付时间,完成时间,充值金额,支付金额,余额支付,玩票支付,企业名称,部门"

    try {
      writer = new BufferedWriter(writerStream)
    }
    catch {
      case e: IOException => {
        e.printStackTrace
      }
    }
    //查看
    val conn: Connection =JdbcUtil.getXiaopeng2Conn()
    //排除沙盒订单
    //val sqlBi: String = "select distinct orderid,\nbg.name as bgname,\ngameid,\ncase when prod_type in (1,2,5) then '首充' \n     when prod_type in(2,4) then '续充' \n     else '直充' end as prod_type,\nbch.name as bchname,\npaytype,\n'订单完成' as state,\npaytime,\nerectime,\noriprice*productnum oriprice,\npayprice,\npay_credit,\nplat_coin,\nbg.corporation_name,\ncase when pgp.name is null then '运营组' else pgp.name end as gpname\nfrom orders ods\njoin bgame bg on ods.gameid=bg.id\njoin bchannel bch on bch.id=ods.channelid\nleft join publish_game pg on pg.game_id=ods.gameid and pg.status=1 and pg.channel_id=ods.channelid\nleft join publish_group pgp on pg.publish_group_id=pgp.id and pgp.status=1\nwhere erectime>=? and erectime<? and ods.state in(4,13,8)\n\nunion ALL\nselect distinct orderid,\nbg.name as bgname,\ngameid,\ncase when prod_type in (1,2,5) then '首充' \n     when prod_type in(2,4) then '续充' \n     else '直充' end as prod_type,\nbch.name as bchname,\npaytype,\n'退款完成' as state,\npaytime,\nerectime,\noriprice*productnum oriprice,\npayprice,\npay_credit,\nplat_coin,\nbg.corporation_name,\ncase when pgp.name is null then '运营组' else pgp.name end as gpname\nfrom orders ods \njoin bgame bg on ods.gameid=bg.id\njoin bchannel bch on bch.id=ods.channelid\nleft join publish_game pg on pg.game_id=ods.gameid and pg.status=1 and pg.channel_id=ods.channelid\nleft join publish_group pgp on pg.publish_group_id=pgp.id and pgp.status=1\nwhere refund_time>=? and refund_time<? and ods.state in(8)\n\n"
    val sqlBi:String="select distinct orderid,\nbg.name as bgname,\ngameid,\ncase when prod_type in (1,2,5) then '首充' \n     when prod_type in(2,4) then '续充' \n     else '直充' end as prod_type,\nbch.name as bchname,\npaytype,\n'订单完成' as state,\npaytime,\nerectime,\noriprice*productnum oriprice,\npayprice,\npay_credit,\nplat_coin,\nbg.corporation_name,\ncase when pgp.name is null then '运营组' else pgp.name end as gpname\nfrom orders ods\njoin bgame bg on ods.gameid=bg.id\njoin bchannel bch on bch.id=ods.channelid\nleft join publish_game pg on pg.game_id=ods.gameid and pg.status=1 and pg.channel_id=ods.channelid\nleft join publish_group pgp on pg.publish_group_id=pgp.id and pgp.status=1\nleft join \n(select distinct rep.order_id from pywsdk_apple_receipt_verify ver join pywsdk_cp_req rep on ver.order_sn=rep.order_no where ver.sandbox=1 and state=3 and rep.status=1) ver\non ver.order_id=ods.id\nwhere erectime>=? and erectime<? and ods.state in(4,13,8) and ver.order_id is null\n\nunion ALL\nselect distinct orderid,\nbg.name as bgname,\ngameid,\ncase when prod_type in (1,2,5) then '首充' \n     when prod_type in(2,4) then '续充' \n     else '直充' end as prod_type,\nbch.name as bchname,\npaytype,\n'退款完成' as state,\npaytime,\nerectime,\noriprice*productnum oriprice,\npayprice,\npay_credit,\nplat_coin,\nbg.corporation_name,\ncase when pgp.name is null then '运营组' else pgp.name end as gpname\nfrom orders ods \njoin bgame bg on ods.gameid=bg.id\njoin bchannel bch on bch.id=ods.channelid\nleft join publish_game pg on pg.game_id=ods.gameid and pg.status=1 and pg.channel_id=ods.channelid\nleft join publish_group pgp on pg.publish_group_id=pgp.id and pgp.status=1\nleft join \n(select distinct rep.order_id from pywsdk_apple_receipt_verify ver join pywsdk_cp_req rep on ver.order_sn=rep.order_no where ver.sandbox=1 and state=3 and rep.status=1) ver\non ver.order_id=ods.id\nwhere erectime>=? and erectime<? and ods.state in(8) and ver.order_id is null"
    val psBi: PreparedStatement = conn.prepareStatement(sqlBi)
    psBi.setString(1, startday)
    psBi.setString(2, endday)
    psBi.setString(3, startday)
    psBi.setString(4, endday)
    val rsBi: ResultSet = psBi.executeQuery
    //table header
    writer.append(header)
    writer.newLine
    val pool=JedisUtil.getJedisPool
    val jedis=pool.getResource
    jedis.select(7)
    while (rsBi.next) {
      {
        if(rsBi.getString("orderid")!=null) {
          try {
            val payType=rsBi.getString("paytype");
            val payTypeMd=getPayType(payType,jedis)
            writer.append(rsBi.getString("orderid") + ","
              + rsBi.getString("bgname").replace(",","") + ","
              + rsBi.getString("gameid") + ","
              + rsBi.getString("prod_type").replace(",","") + ","
              + rsBi.getString("bchname") + ","
              + payTypeMd.replace(",","") + ","
              + rsBi.getString("state") + ","
              + (if(rsBi.getString("paytime") ==null||rsBi.getString("paytime")=="null") startday+" 00:00:01" else rsBi.getString("paytime")).replace(",","") + ","
              + (if(rsBi.getString("erectime") ==null||rsBi.getString("erectime")=="null") startday+" 00:00:01" else rsBi.getString("erectime")).replace(",","") + ","
              + rsBi.getString("oriprice").replace(",","") + ","
              + rsBi.getString("payprice").replace(",","") + ","
              + rsBi.getString("pay_credit").replace(",","") + ","
              + rsBi.getString("plat_coin").replace(",","") + ","
              + rsBi.getString("corporation_name").replace(",","") + ","
              + rsBi.getString("gpname").replace(",","")
            )
            writer.newLine
          }
          catch {
            case e: Exception => {
              e.printStackTrace
              println(rsBi.getString("paytime"))
              println("异常数据"+rsBi.getString("orderid"))
            }
          }
        }
      }


    }
    pool.returnResource(jedis)
    pool.destroy()
    psBi.close()
    conn.close()
    if (null != writer) {
      try {
        writer.flush
        writer.close
      }
      catch {
        case e: IOException => {
          e.printStackTrace
        }
      }
    }
  }


  /**
    * 下载代金券充值报表明细
    */

  def doDownLoadConponDayAdd(startday:String,endday:String,path:String)={
    val file: File = new File(path+"conponadd_" + startday.trim.replace("-", "") + ".csv1")
    file.delete
    val writerStream = new OutputStreamWriter(new FileOutputStream(file),"UTF-8");
    var writer: BufferedWriter =null
    val header: String = "订单号,所属游戏,游戏ID,充值类型,渠道名,支付方式,状态,支付时间,完成时间,充值金额,支付金额,余额支付,玩票支付,企业名称,部门"

    try {
      writer = new BufferedWriter(writerStream)
    }
    catch {
      case e: IOException => {
        e.printStackTrace
      }
    }
    //查看
    val conn: Connection =JdbcUtil.getXiaopeng2Conn()
    val sqlBi: String = "select distinct orderid,\nbg.name as bgname,\ngameid,\ncase when prod_type in (1,2,5) then '首充' \n     when prod_type in(2,4) then '续充' \n     else '直充' end as prod_type,\nbch.name as bchname,\npaytype,\n'订单完成' as state,\npaytime,\nerectime,\noriprice*productnum oriprice,\npayprice,\npay_credit,\nplat_coin,\nbg.corporation_name,\ncase when pgp.name is null then '运营组' else pgp.name end as gpname\nfrom orders ods\njoin bgame bg on ods.gameid=bg.id\njoin bchannel bch on bch.id=ods.channelid\nleft join publish_game pg on pg.game_id=ods.gameid and pg.status=1 and pg.channel_id=ods.channelid\nleft join publish_group pgp on pg.publish_group_id=pgp.id and pgp.status=1\nwhere erectime>=? and erectime<? and ods.state in(4,13,8) and ods.prod_type in(1,3,2,4)\n\nunion ALL\nselect distinct orderid,\nbg.name as bgname,\ngameid,\ncase when prod_type in (1,2,5) then '首充' \n     when prod_type in(2,4) then '续充' \n     else '直充' end as prod_type,\nbch.name as bchname,\npaytype,\n'退款完成' as state,\npaytime,\nerectime,\noriprice*productnum oriprice,\npayprice,\npay_credit,\nplat_coin,\nbg.corporation_name,\ncase when pgp.name is null then '运营组' else pgp.name end as gpname\nfrom orders ods \njoin bgame bg on ods.gameid=bg.id\njoin bchannel bch on bch.id=ods.channelid\nleft join publish_game pg on pg.game_id=ods.gameid and pg.status=1 and pg.channel_id=ods.channelid\nleft join publish_group pgp on pg.publish_group_id=pgp.id and pgp.status=1\nwhere refund_time>=? and refund_time<? and ods.state in(8) and ods.prod_type in(1,3,2,4)"
    val psBi: PreparedStatement = conn.prepareStatement(sqlBi)
    psBi.setString(1, startday)
    psBi.setString(2, endday)
    psBi.setString(3, startday)
    psBi.setString(4, endday)
    val rsBi: ResultSet = psBi.executeQuery
    //table header
    writer.append(header)
    writer.newLine
    val pool=JedisUtil.getJedisPool
    val jedis=pool.getResource
    jedis.select(7)
    while (rsBi.next) {
      {
        if(rsBi.getString("orderid")!=null) {
          try {
            val payType=rsBi.getString("paytype");
            val payTypeMd=getPayType(payType,jedis)
            writer.append(rsBi.getString("orderid") + ","
              + rsBi.getString("bgname").replace(",","") + ","
              + rsBi.getString("gameid") + ","
              + rsBi.getString("prod_type") + ","
              + rsBi.getString("bchname") + ","
              + payTypeMd + ","
              + rsBi.getString("state") + ","
              + (if(rsBi.getString("paytime") ==null||rsBi.getString("paytime")=="null") startday+" 00:00:01" else rsBi.getString("paytime")).replace(",","") + ","
              + (if(rsBi.getString("erectime") ==null||rsBi.getString("erectime")=="null") startday+" 00:00:01" else rsBi.getString("erectime")).replace(",","") + ","
              + rsBi.getString("oriprice").replace(",","") + ","
              + rsBi.getString("payprice").replace(",","") + ","
              + rsBi.getString("pay_credit").replace(",","") + ","
              + rsBi.getString("plat_coin").replace(",","") + ","
              + rsBi.getString("corporation_name").replace(",","") + ","
              + rsBi.getString("gpname").replace(",","")
            )
            writer.newLine
          }
          catch {
            case e: Exception => {
              e.printStackTrace
              println(if(rsBi.getString("paytime") ==null||rsBi.getString("paytime")=="null") startday+" 00:00:01" else rsBi.getString("paytime"))
              println("erectime:"+rsBi.getString("erectime"))
              println("paytime:"+rsBi.getString("paytime"))
              println("异常数据"+rsBi.getString("orderid"))
            }
          }
        }
      }
    }
    pool.returnBrokenResource(jedis)
    pool.destroy()
    psBi.close()
    conn.close()
    if (null != writer) {
      try {
        writer.flush
        writer.close
      }
      catch {
        case e: IOException => {
          e.printStackTrace
        }
      }
    }
  }



  /**
    * 下载代金券消费报表明细
    */

  def doDownLoadConponDayDe(startday:String,endday:String,path:String)={
    val file: File = new File(path+"conpondel_" + startday.trim.replace("-", "") + ".csv1")
    file.delete
    val writerStream = new OutputStreamWriter(new FileOutputStream(file),"UTF-8");
    var writer: BufferedWriter =null
    val header: String = "订单号,所属游戏,游戏ID,状态,订单金额,支付方式,消耗代金券,消耗代金券现金价值,请求时间"
   // val header: String = "订单号,所属游戏,游戏ID,状态,消耗金额,支付方式,代金券支付金额,请求时间"
    try {
      writer = new BufferedWriter(writerStream)
    }
    catch {
      case e: IOException => {
        e.printStackTrace
      }
    }
    //查看
    val conn: Connection =JdbcUtil.getXiaopeng2Conn()
    val sqlBi="select \ndistinct\nod.orderid,\nbgm.name as bname,\nrq.game_id,\ncase when rq.status=1 then 'CP付款成功'  \n     when rq.status=0 then '未发送'   \n     when rq.status=2 then 'CP付款失败'\n     when rq.status=3 then '重试中'\n     when rq.status=4 then '重试失败'\nend as status,\nrq.amount amount,\n'纯代金券'  paytype,\nFORMAT((ts.amount_org-ts.amount)*FORMAT((od.payprice+od.pay_credit+od.plat_coin/10)/od.productnum/od.oriprice,2),2) AS consu_cash_value,\nts.amount_org-ts.amount as djq,\nts.tran_time req_time\nfrom pywsdk_cp_req rq\njoin pyw_coupon_trans ts on ts.cp_order_id=rq.cp_order_no and ts.type=1\njoin pyw_coupon_trans ts1 on ts.coupon_code=ts1.coupon_code and ts1.type=0\njoin orders od on od.id=ts1.order_id \njoin bgame bgm on bgm.id=rq.game_id\nwhere ts.tran_time>=? and ts.tran_time<? and rq.create_time>=? and rq.create_time<? and ts.type=1 and ts.order_id=0\n\nunion ALL\n\nselect \ndistinct\nod.orderid  orderid,\nbgm.name as bname,\nrq.game_id,\ncase when rq.status=1 then 'CP付款成功'  \n     when rq.status=0 then '未发送'   \n     when rq.status=2 then 'CP付款失败'\n     when rq.status=3 then '重试中'\n     when rq.status=4 then '重试失败'\nend as status,\nrq.amount amount,\n'混合支付'  paytype,\nFORMAT((ts.amount_org-ts.amount)*FORMAT((od.payprice+od.pay_credit+od.plat_coin/10)/od.productnum/od.oriprice,2),2) AS consu_cash_value,\nts.amount_org-ts.amount as djq,\nts.tran_time req_time\nfrom pywsdk_cp_req rq\njoin pyw_coupon_trans ts on ts.cp_order_id=rq.cp_order_no and ts.type=1\njoin pyw_coupon_trans tsz on tsz.coupon_id=ts.coupon_id and tsz.type=0 and tsz.order_id>0\njoin orders od on od.id=tsz.order_id\njoin bgame bgm on bgm.id=rq.game_id\nwhere ts.tran_time>=? and ts.tran_time<? and rq.create_time>=? and rq.create_time<? and ts.type=1 and ts.order_id>0"
    // val sqlBi: String = "select \ncase when rq.order_id =0  then od.orderid \n     when rq.order_no is null then '' \n     else rq.order_no end orderid,\nbgm.name as bname,\nrq.game_id,\ncase when rq.status=1 then 'CP付款成功'  \n     when rq.status=0 then '未发送'   \n     when rq.status=2 then 'CP付款失败'\n     when rq.status=3 then '重试中'\n     when rq.status=4 then '重试失败'\nend as status,\nrq.amount amount,\n'纯代金券'  paytype,\nts.amount_org-ts.amount as djq,\nts.tran_time req_time\nfrom pywsdk_cp_req rq\njoin pyw_coupon_trans ts on ts.cp_order_id=rq.cp_order_no \njoin pyw_coupon_trans ts1 on ts.coupon_code=ts1.coupon_code and ts1.type=0\njoin orders od on od.id=ts1.order_id\njoin bgame bgm on bgm.id=rq.game_id\nwhere ts.tran_time>=? and ts.tran_time<? and rq.create_time>=? and rq.create_time<? and ts.type=1 and ts.order_id=0\n\nunion ALL\n\nselect \nrq.order_no  orderid,\nbgm.name as bname,\nrq.game_id,\ncase when rq.status=1 then 'CP付款成功'  \n     when rq.status=0 then '未发送'   \n     when rq.status=2 then 'CP付款失败'\n     when rq.status=3 then '重试中'\n     when rq.status=4 then '重试失败'\nend as status,\nrq.amount amount,\n'混合支付'  paytype,\nts.amount_org-ts.amount as djq,\nts.tran_time req_time\nfrom pywsdk_cp_req rq\njoin pyw_coupon_trans ts on ts.cp_order_id=rq.cp_order_no \njoin bgame bgm on bgm.id=rq.game_id\nwhere ts.tran_time>=? and ts.tran_time<? and rq.create_time>=? and rq.create_time<? and ts.type=1 and ts.order_id>0"
    val psBi: PreparedStatement = conn.prepareStatement(sqlBi)
    psBi.setString(1, startday)
    psBi.setString(2, endday)
    psBi.setString(3, startday)
    psBi.setString(4, endday)
    psBi.setString(5, startday)
    psBi.setString(6, endday)
    psBi.setString(7, startday)
    psBi.setString(8, endday)
    val rsBi: ResultSet = psBi.executeQuery
    //table header
    writer.append(header)
    writer.newLine
    val pool=JedisUtil.getJedisPool
    val jedis=pool.getResource
    jedis.select(7)
    var cashValue: Float =0.00f
    var conponDes:Float=0.00f
    while (rsBi.next) {
        if(rsBi.getString("game_id")!=null) {
          try {
                val regTime=rsBi.getString("req_time");
                val payType=rsBi.getString("paytype")
                val payTypeMd=getPayType(payType,jedis)
                val cashv=rsBi.getString("consu_cash_value");
                val djq=rsBi.getString("djq")
                writer.append(rsBi.getString("orderid") + ","
                  + rsBi.getString("bname").replace(",","") + ","
                  + rsBi.getString("game_id") + ","
                  + rsBi.getString("status") + ","
                  + rsBi.getString("amount").replace(",","") + ","
                  + payType.replace(",","") + ","
                  + djq.replace(",","") + ","
                  + cashv.replace(",","") + ","
                  + rsBi.getString("req_time").replace(",","")
                )
                writer.newLine
                cashValue=cashValue+cashv.replace(",","").toFloat;
                conponDes=conponDes+djq.replace(",","").toFloat;
          }
          catch {
            case e: Exception => {
              println("异常数据"+rsBi.getString("orderid"))
              e.printStackTrace
            }
          }
        }
      }
      //价值现金价值和消费金额
    println("casevalue:"+cashValue*100)
    println("cconponDesasevalue:"+conponDes*100)
    loadConponDayDe(startday,cashValue*100,conponDes*100)
    pool.returnResource(jedis)
    pool.destroy()
    psBi.close()
    conn.close()
    if (null != writer) {
      try {
        writer.flush
        writer.close
      }
      catch {
        case e: IOException => {
          e.printStackTrace
        }
      }
    }

  }

  /**
    * 把消耗代金券和价值写入表
    *
    * @param startday
    * @param cashValue
    * @param conponDes
    */
  def loadConponDayDe(startday:String,cashValue: Float, conponDes: Float): Unit ={
    val conn=JdbcUtil.getConn()
    val sql2Mysql = "insert into bi_fin_report" +
      "(fin_date,consu_conpon,consu_cash_value)" +
      " values(?,?,?) " +
      " on duplicate key update consu_conpon=?,consu_cash_value=?"
    val stmt: PreparedStatement =conn.prepareStatement(sql2Mysql)
    stmt.setString(1,startday)
    stmt.setFloat(2,conponDes)
    stmt.setFloat(3,cashValue)
    stmt.setFloat(4,conponDes)
    stmt.setFloat(5,cashValue)
    stmt.executeUpdate();
    stmt.close()
    conn.close()
  }



  /**
    * 算总量
    *
    * @param startday
    * @param endday
    */
  def doTotal(startday: String, endday: String): Unit ={
    val conn: Connection =JdbcUtil.getConn()
    val sqlBi: String = "update bi_fin_report set ori_total_price=ori_price-ori_refund_price,pay_total_price=pay_price-pay_refund_price where fin_date>=? and fin_date<? "
    val psBi: PreparedStatement = conn.prepareStatement(sqlBi)
    psBi.setString(1, startday)
    psBi.setString(2, endday)
    psBi.executeUpdate()
    psBi.close()
    conn.close()

  }

  /**
    * 主执行
    *
    * @param args
    */

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage:currentday filepath ")
      System.exit(1)
    }
    if (args.length > 2) {
      System.err.println("Usage:currentday filepath  ")
      System.exit(1)
    }
  val currentday=args(0)
  val path=args(1)
  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val dateFormat1: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val date: Date = dateFormat.parse(currentday)
  val cal: Calendar = Calendar.getInstance()
  cal.setTime(date)
  cal.add(Calendar.DATE, -1)
  val startday = dateFormat.format(cal.getTime())
   //充值代金券 计划代金券 消费代金券

    System.out.println("doing doConponAdd......"+dateFormat1.format(new Date()))
    doConponAdd(startday,currentday);
    System.out.println("done doConponAdd。"+dateFormat1.format(new Date()))

    System.out.println("doing doConponRemaid......"+dateFormat1.format(new Date()))
    doConponRemaidVSCashValue(startday,currentday);
    System.out.println("done doConponRemaid。"+dateFormat1.format(new Date()))

    System.out.println("doing doConponfe......"+dateFormat1.format(new Date()))
    doConponfe(startday,currentday);
    System.out.println("done doConponfe。"+dateFormat1.format(new Date()))

    System.out.println("doing doCashValueAdd......"+dateFormat1.format(new Date()))
    doCashValueAdd(startday,currentday) ;
    System.out.println("done doCashValueAdd。"+dateFormat1.format(new Date()))

    System.out.println("doing doCashValueFe......"+dateFormat1.format(new Date()))
    doCashValueFe(startday,currentday);
    System.out.println("done doCashValueFe。"+dateFormat1.format(new Date()))

//    System.out.println("doing doCashValueRem......"+dateFormat1.format(new Date()))
//    doCashValueRem(startday,currentday);
//    System.out.println("done doCashValueRem。"+dateFormat1.format(new Date()))

    System.out.println("doing doRecharge......"+dateFormat1.format(new Date()))
    doRecharge(startday,currentday);
    System.out.println("done doRecharge。"+dateFormat1.format(new Date()))

    System.out.println("doing doFebund......"+dateFormat1.format(new Date()))
    doFebund(startday,currentday);
    System.out.println("done doFebund。"+dateFormat1.format(new Date()))

    System.out.println("doing doTotal......"+dateFormat1.format(new Date()))
    doTotal(startday,currentday);
    System.out.println("done doTotal。"+dateFormat1.format(new Date()))
    //download
    System.out.println("doing doDownLoadFinDay......"+dateFormat1.format(new Date()))
    doDownLoadFinDay(startday,currentday,path)
    System.out.println("done doDownLoadFinDay。"+dateFormat1.format(new Date()))

    System.out.println("doing doDownLoadConponDayAdd......"+dateFormat1.format(new Date()))
    doDownLoadConponDayAdd(startday,currentday,path)
    System.out.println("done doDownLoadConponDayAdd。"+dateFormat1.format(new Date()))

    System.out.println("doing doDownLoadConponDayDe......"+dateFormat1.format(new Date()))
    doDownLoadConponDayDe(startday,currentday,path)
    System.out.println("done doDownLoadConponDayDe。"+dateFormat1.format(new Date()))


  }

  /**
    * 获取字符数据，并转float
    */
  def getFloat(str:String):Float={
    var jg=0.0.toFloat

    if(str==null)
    {
      jg=0.0.toFloat
    }
    else jg=str.replace(",","").toFloat*100

    return jg
  }


  /**
    * 支付类型转换
    *
    * @param payType
    * @param jedis
    * @return
    */
  def getPayType(payType: String,jedis:Jedis) :String={
    var count=0
    val header="payType_"
    var payTypeMd=""
    if(payType.length>5)
    {
      payTypeMd=jedis.get(header+payType)

    }
    else {
      while(count<payType.length)
      {
        val jg: String =payType.substring(count,count+1)
        var payTypeMd1 =jedis.get(header+jg)
        if(payTypeMd1==null)
          {
            payTypeMd1="未知支付"
          }
        count=count+1
        payTypeMd=payTypeMd+"|"+payTypeMd1
      }
    }
    if(payTypeMd==null)
    {
      payTypeMd="未知支付"
    } else {
      payTypeMd = payTypeMd.substring(payTypeMd.indexOf("|") + 1, payTypeMd.length)
    }
    return payTypeMd
  }

  /**
    * 时间匹配
    *
    * @param regTime
    * @param createTime
    * @return
    */
  def isMatched(regTime: String, createTime: String): Boolean ={
    var jg=false
    val sdf = new   SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val regT=sdf.parse(regTime).getTime
    val createT=sdf.parse(createTime).getTime
    val endToStartSS =((regT-createT)/1000).abs;
    if(endToStartSS<3600)
      jg=true
    return jg
  }


}
