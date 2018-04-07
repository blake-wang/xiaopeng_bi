package com.xiaopeng.bi.utils

import java.sql._
import java.text.SimpleDateFormat
import java.util.regex.{Matcher, Pattern}
import java.util.{Calendar, Date}

import com.xiaopeng.bi.checkdata.MissInfo2Redis
import org.apache.spark.sql.{DataFrame, Row}
import redis.clients.jedis.Jedis

import scala.Array

/**
  * Created by Administrator on 2016/7/15.
  */
object Commons {
  def isCodeUniq(imei: String, pkgCode: String,conn: Connection):Boolean ={
    var jg=true
    var stmt: PreparedStatement = null
    val sql: String ="select pkg_code from bi_sub_centurion_devstatus where imei=? limit 1"
    stmt=conn.prepareStatement(sql)
    stmt.setString(1,imei)
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next) {
      if(!rs.getString("pkg_code").equals(pkgCode))
        {jg=false}
    }
    stmt.close()
    return jg
  }


  /**
    * 是否新登录账号
    *
    * @param gameAccount
    * @param loginDate
    * @param jedis
    */
  def getNewLoginAccount(gameAccount: String, loginDate: String, jedis: Jedis, jedis4: Jedis) :Int={
    var jg=0
    val regTime=jedis.hget(gameAccount,"reg_time")
    if(regTime==null)
    {
      jg=0
    }
     //今天注册并登录
    else if(regTime.contains(loginDate))
    {
      //只算一次
      if(!jedis4.exists(gameAccount+"|"+loginDate+"|"+"_NewLoginAccountByDay"))
      {
        jg=1
      } else jg=0
    }


    return  jg
  }


  /**
    * 一天充值账号只算一次
    *
    * @param gameAccount
    * @param orderDate
    * @param jedis
    */
  def getNewRechargeAccountByDay(gameAccount: String, orderDate: String, jedis: Jedis):Int={
    var jg=0
    if(!jedis.exists(gameAccount+"|"+orderDate+"|"+"_NewRechargeAccountByDay"))
    {
      jg=1
    }
    return  jg
  }


  /**
    * 是否新充值账号
 *
    * @param gameAccount
    * @param orderDate
    * @param pkgCode
    * @param gameId
    * @param jedis
    */
  def getNewRechargeAccount(gameAccount: String, orderDate: String, pkgCode: String, gameId: String, jedis: Jedis) :Int={
    var jg=0
    val regTime=jedis.hget(gameAccount,"reg_time")
    if(regTime==null)
      {
       jg=0
       }
    else
    if(regTime.contains(orderDate))
      {
        jg=1
      }
    return jg
  }

  def isSubCenturion(expandCode: String):Boolean= {
    var jg=false
    if(expandCode.contains("~"))
      {
        jg=true;
      }
    return jg

  }




  /**
    * 激活IP判断
 *
    * @param ip
    * @param activeDate
    * @param pkgCode
    * @param jedis
    */
  def getActiveIps(ip: String, activeDate: String, pkgCode: String,gameid:String, jedis: Jedis) :Int= {
    var jg=0
    if(!jedis.exists(ip+"|"+activeDate+"|"+pkgCode+"|"+gameid+"|"+"_SubCentActiveIps"))
    {
      jg=1
    }
    return  jg
  }


  /**
    * 激活设备数判断
 *
    * @param imei
    * @param activeDate
    * @param pkgCode
    */
  def getActiveDevs(imei: String, activeDate: String, pkgCode: String,gameid:String,jedis: Jedis) :Int= {
    var jg=0
    if(!jedis.exists(imei+"|"+activeDate+"|"+pkgCode+"|"+gameid+"|"+"_SubCentActiveDevs"))
    {
      jg=1
    }
    return  jg
  }


  /**
    * 日登陆账号判断
    *
    * @param gameAccount
    * @param loginDate
    * @param pkgCode
    * @param jedis
    * @return
    */
  def getLoginAccounts(gameAccount: String, loginDate: String, pkgCode: String,gameId:String,jedis: Jedis) :Int= {
    var jg=0
    if(!jedis.exists(gameAccount+"|"+loginDate+"|"+pkgCode+""+gameId+"|"+"_SubCentLoginAccounts"))
    {
      jg=1
    }
    return  jg

  }


  /**
    * 日充值设备数
    *
    * @param imei
    * @param orderDate
    * @param pkgCode
    * @param jedis
    * @return
    */
  def getRechargeDevs(imei: String, orderDate: String, pkgCode: String,gameId:String,jedis: Jedis) :Int= {
    var jg=0
    if(!jedis.exists(imei+"|"+orderDate+"|"+pkgCode+"|"+gameId+"|"+"_SubCentRechargeDevs"))
    {
      jg=1
    }
    return  jg
  }

  /**
    * 日充值账号数
    *
    * @param gameAccount
    * @param orderDate
    * @param pkgCode
    * @param jedis
    * @return
    */
  def getRechargeAccouts(gameAccount: String, orderDate: String, pkgCode: String,gameId:String,jedis: Jedis):Int ={
    var jg=0
    if(!jedis.exists(gameAccount+"|"+orderDate+"|"+pkgCode+"|"+gameId+"|"+"_SubCentRechargeAccounts"))
    {
      jg=1
    }
    return  jg

  }


  /**
    * 日注册新增设备
    *
    * @param imei
    * @param regiDate
    * @param pkgCode
    * @param jedis
    * @return
    */
  def getRegiDevs(imei: String, regiDate: String, pkgCode: String,gameId:String, jedis: Jedis) :Int={
    var jg=0
    if(!jedis.exists(imei+"|"+regiDate+"|"+pkgCode+"|"+gameId+"|"+"_SubCentNewRegiDev"))
      {
        jg=1
      }
      return  jg
  }


  /**
    * 通过账号找下级推广码
    *
    * @param gameAccount
    */
  def getUserCodeByOrder(gameAccount: String,jedis: Jedis):String = {
    var jg="0"
    if(jedis.exists(gameAccount))
      {
        val cd=jedis.hget(gameAccount,"expand_code_child") //下级不能有下划线
        if(cd==null||cd.contains("_"))
          {
            jg="0"
          } else jg=cd;
      } else {MissInfo2Redis.checkAccountSubCen(gameAccount)
      jg=jedis.hget(gameAccount,"expand_code_child")
    }
    return if(jg==null) "0" else jg

  }



  /**
    * 获取拓展人分组
    *
    * @param memberId
    * @param connXiappeng
    */
  def getGroupId(memberId: String, connXiappeng: Connection) :Int={
    var jg=0
    var stmt: PreparedStatement = null
    val sql: String =" select group_id from promo_user  where member_id=? limit 1"
    stmt=connXiappeng.prepareStatement(sql)
    stmt.setString(1,memberId)
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next) {
      jg=rs.getString("group_id").toInt
    }
    stmt.close()
    return jg
  }



  /**
    * 获取发行组&平台
    *
    * @param gameId
    * @param conn
    */
  def getPubGameGroupIdAndOs(gameId: Int, conn: Connection) : Array[String] ={
    var jg=Array[String]("0","1")
    var stmt: PreparedStatement = null
    val sql: String =" select distinct system_type os,group_id from game_sdk  where old_game_id=? limit 1"
    stmt=conn.prepareStatement(sql)
    stmt.setInt(1,gameId)
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next) {
      jg=Array[String](rs.getString("group_id"), rs.getString("os"))
    }
    stmt.close()
    return  jg
  }




  /**
    * 获取bind通行证
    *
    * @param gameAccount
    * @param jedis
    */
  def getBindMember(gameAccount: String, jedis: Jedis):String ={
    var jg=""
    var bindMember=""
    jedis.select(0)
    if(!jedis.exists(gameAccount))
      MissInfo2Redis.checkAccount(gameAccount)
    else {
      bindMember=jedis.hget(gameAccount,"bind_member_id")
    }
    jg=if(bindMember.length()>0) jedis.hget(bindMember+"_member","username") else ""
    return if(jg==null) "" else jg

  }


  /**
    * 获取member注册时间
    *
    * @param memberId
    */
  def getMemberRegiTime(memberId: String,jedis: Jedis):String ={
    var tm =jedis.hget(memberId+"_member","regtime")
    if(tm==null)
      tm="0000-00-00"
    return  tm
  }


  /**
    * 设备有效日期
    *
    * @param imei
    * @param idate
    */
  def getWorthinessDate(imei: String,pkgCode:String, idate: String,bt:Int,conn: Connection):String={
    val wDate=getday(bt,idate)
//    if(getDevRechareAmount(imei,pkgCode,conn)>=5000)
//      {
//       wDate="9999-01-01"
//      }
    return wDate
  }


  /**
    * 获取游戏设备累计充值金额
    */
  def getDevRechareAmount(imei:String,pkgCode:String,conn:Connection):Int={
    var jg=0
    var stmt: PreparedStatement = null
    val sql: String ="select recharge_amount from bi_sub_centurion_devstatus where imei=? limit 1"
    stmt=conn.prepareStatement(sql)
    stmt.setString(1,imei)
  //  stmt.setString(2,pkgCode)
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next) {
      jg=rs.getString("recharge_amount").toInt

    }
    stmt.close()
    return jg

  }

  /**
    * 15日后
    *
    * @param bt
    * @return
    */
  def getday(bt: Int,idate:String): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal: Calendar = Calendar.getInstance
    val date: Date = dateFormat.parse(idate)
    cal.setTime(date)
    cal.add(Calendar.DATE, bt)
    val dt = dateFormat.format(cal.getTime())
    return dt
  }

  /**
    * 获取通行证
    *
    * @param memberId
    */
  def getUserAccount(memberId: String,jedis: Jedis):String ={
    var userAccount=""
    if(jedis.exists(memberId+"_member"))
      {
        userAccount=jedis.hget(memberId.trim+"_member","username")
      }else
    {
      MissInfo2Redis.checkMember(memberId);
      userAccount=jedis.hget(memberId.trim+"_member","username")
    }
    return  if(userAccount==null) "0" else userAccount
  }

  /**
    * 获取通行证ID
    *
    * @param promoCode
    * @param jedis
    * @return
    */

  def getMemberId(promoCode: String,jedis:Jedis) :String={
    var memberId="0"
    if(jedis.exists(promoCode))
      {
        memberId=jedis.get(promoCode)
      }
    else if (isNumeric(promoCode))
    {
      MissInfo2Redis.checkPromoCode(promoCode)
      memberId=jedis.get(promoCode)
    }
    return if(memberId==null) "0" else memberId

  }

  /**
    * 判断推是否为数值
 *
    * @param str
    * @return
    */
  def  isNumeric(str:String):Boolean={
    val pattern  = Pattern.compile("[0-9]*");
    val isNum: Matcher = pattern.matcher(str);
    if( !isNum.matches() ){
      return false;
    }
    return true;
  }

  /**
    * Null转0
    *
    * @param ls
    * @return
    */
  def getNullTo0(ls:String):Float=
  {
    var rs=0.0.toFloat
    if(!ls.equals(""))
    {
      rs=ls.toFloat
    }
    return rs
  }



  /**
    * 通过日志的推广码进行拆分上级
    *
    * @param promo_code
    * @return
    */
  def getPromoCode(promo_code:String):String=
  {
    var rs=""
    if(promo_code.length>1)
    {
      rs=promo_code.split("~",-1)(0)
    }
    return rs
  }

  /**
    * 通过日志的推广码进行拆分下级
    *
    * @param promo_code
    * @return
    */
  def getUserCode(promo_code:String):String=
  {
    var rs=""
    if(promo_code.split("~",-1).length>1)
    {
      rs=promo_code.split("~",-1)(1)
    }
    return rs
  }

  /**
    * 通过日志的imei取真实imei
    *
    * @param imei
    * @return
    */
  def getImei(imei:String):String=
  {
    return imei.replace("&","|")
  }
  /**
    * 返回父渠道
    *
    * @return
    */
  def getParentChannl(channel:String):String={
    var rs=""
    rs=channel.split("_",-1)(0)
    return rs
  }

  /**
    * 返回父渠道
    *
    * @return
    */
  def getSubChannl(channel:String):String={
    var rs=""
    if (channel.split("_",-1).length > 1) {
      rs=channel.split("_",-1)(1)
    } else rs=""
    return rs
  }

  /**
    * 返回父渠道
    *
    * @return
    */
  def getAbChannl(channel:String):String={
    var rs=""
    if (channel.split("_",-1).length > 2) {
      rs=channel.split("_",-1)(2)
    } else rs=""
    return rs
  }

  //获取当前日期
  def getDate(): String ={
    val  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal:Calendar=Calendar.getInstance()
    var dt=""
      cal.add(Calendar.DATE,0)
      dt=dateFormat.format(cal.getTime())
    return  dt
  }
  /**
    * 推送data数据到数据库
    *
    * @param dataf
    * @param insertSql
    */
  def processDbFct(dataf: DataFrame,insertSql:String) = {
    //全部转为小写，后面好判断
    val sql2Mysql = insertSql.replace("|", " ").toLowerCase
    //获取values（）里面有多少个?参数，有利于后面的循环
    val startValuesIndex = sql2Mysql.indexOf("(?") + 1
    val endValuesIndex = sql2Mysql.indexOf("?)") + 1
    //values中的个数
    val valueArray:scala.Array[String]= sql2Mysql.substring(startValuesIndex, endValuesIndex).split(",") //两个（？？）中间的值
    //条件中的参数个数
    val wh :scala.Array[String]= sql2Mysql.substring(sql2Mysql.indexOf("update") + 6).split(",") //找update后面的字符串再判断
    //查找需要insert的字段
    val cols_ref = sql2Mysql.substring(0, sql2Mysql.lastIndexOf("(?")) //获取（?特殊字符前的字符串，然后再找字段
    val cols = cols_ref.substring(cols_ref.lastIndexOf("(") + 1, cols_ref.lastIndexOf(")")).split(",")

    /********************数据库操作 *******************/
    dataf.foreachPartition((rows: Iterator[Row]) => {
      val conn = JdbcUtil.getConn()
      val ps: PreparedStatement = conn.prepareStatement(sql2Mysql)
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
        ps.executeUpdate()
      }
      conn.close()
    }
    )
  }

}
