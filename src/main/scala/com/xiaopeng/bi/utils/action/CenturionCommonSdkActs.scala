package com.xiaopeng.bi.utils.action

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.xiaopeng.bi.utils.dao.CenturionCommonSdkDao
import com.xiaopeng.bi.utils.{JdbcUtil, JedisUtil}
import redis.clients.jedis.Jedis

/**
  * Created by Administrator on 2017/7/26.
  */
object CenturionCommonSdkActs {

  /**
    * 今天是否第一次充值
    *
    * @param gameAccount
    * @param orderDate
    */
  def getIsNewPayAccount(gameAccount: String, orderDate: String,connXiappeng:Connection):Int={
    var jg=0
    var stmt: PreparedStatement = null
    val sql: String =" select min(erectime) as erectime from orders where account=? and state=4 limit 1"
    stmt=connXiappeng.prepareStatement(sql)
    stmt.setString(1,gameAccount)
    val rs: ResultSet = stmt.executeQuery()
    if(rs.next())
      {
        while (rs.next) {
        if(rs.getString("erectime").contains(orderDate)) {jg=1}
      }
      }

    stmt.close()
    return jg
  }

  /**
    * 一天只算一次
    *
    * @param gameAccount
    * @param orderDate
    * @return
    */
  def getNewPayAccount(gameAccount: String, orderDate: String,jedis: Jedis): Int ={
    var jg=0
    if(!jedis.exists(gameAccount+"|"+orderDate+"|"+"_NewPayAccount"))
    {
      jg=1
    }
    return  jg

  }

  /**
    * 判断是否充值账号，一天只算1
    *
    * @param gameAccount
    * @param orderDate
    * @param jedis
    * @return
    */
  def getPayAccount(gameAccount: String, orderDate: String,jedis: Jedis): Int ={
    var jg=0
    if(!jedis.exists(gameAccount+"|"+orderDate+"|"+"_PayAccount"))
    {
      jg=1
    }
    return  jg

  }

  /**
    * 判断是否为聚合订单
 *
    * @param gameAccount
    * @return
    */
  def isCommSdkOrDer(gameAccount: String,connXiappeng:Connection): Boolean ={
    var jg=false
    var stmt: PreparedStatement = null
    val sql: String =" select 1 as iscomm from common_sdk_account_tran where account=? limit 1"
    stmt=connXiappeng.prepareStatement(sql)
    stmt.setString(1,gameAccount)
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next) {
      if(rs.getString("iscomm").toInt==1)
        {jg=true}
    }
    stmt.close()
    return jg

  }

  /**
    * 区服充值账号
    * @param gameAccount
    * @param orderDate
    * @param serviceArea
    * @param jedis
    * @return
    */
  def getPayAccountByArea(gameAccount: String, orderDate: String, serviceArea: String, jedis: Jedis):Int={
    var jg=0
    if(!jedis.exists(gameAccount+"|"+orderDate+"|"+serviceArea+"|"+"_PayAccount"))
    {
      jg=1
      jedis.expire(gameAccount+"|"+orderDate+"|"+serviceArea+"|"+"_PayAccount",25*3600)
    }
    return  jg

  }

/**
    * 推送订单数据
  *
  * @param fp
    */
  def orderInfoActions(fp: Iterator[(String, String, String, String, Float,String)]): Unit ={
    val pool = JedisUtil.getJedisPool
    val jedis4 = pool.getResource
    jedis4.select(5)
    val conn=JdbcUtil.getConn()
    val connXiappeng = JdbcUtil.getXiaopeng2Conn()
    for (row <- fp) {
      //订单信息：游戏账号（5），订单日期（6），游戏id（7），渠道id（8）,充值流水(10)+代金券(13)
      val gameAccount=row._1
      //判断是否为聚合订单
      if(isCommSdkOrDer(gameAccount,connXiappeng))
        {
          val orderDate=row._2.substring(0,10)
          val gameId=row._3
          val channelId=getCommonChannelId(gameAccount,connXiappeng)
          val oriPirce =row._5
          val serviceArea=row._6
          val isNewPayAccount=getIsNewPayAccount(gameAccount,orderDate,connXiappeng)
          var newPayAccount=0
          var newPayAmount=0.0.toFloat
          if(isNewPayAccount==1)
          {
            newPayAccount=getNewPayAccount(gameAccount,orderDate,jedis4)
            newPayAmount=oriPirce*100
          }
          val totalPayAccount=getPayAccount(gameAccount,orderDate,jedis4)
          val totalPayAmount=oriPirce*100
          CenturionCommonSdkDao.orderInfoProcessDb(gameId,channelId,orderDate,newPayAccount,newPayAmount,totalPayAccount,totalPayAmount,conn)
          val totalPayAccountArea=getPayAccountByArea(gameAccount,orderDate,serviceArea,jedis4)
          CenturionCommonSdkDao.orderAreaInfoProcessDb(gameId,channelId,orderDate,serviceArea,totalPayAccountArea,totalPayAmount,conn)
          //redis
          jedis4.set(gameAccount+"|"+orderDate+"|"+"_PayAccount","0")
          jedis4.expire(gameAccount+"|"+orderDate+"|"+"_PayAccount",3600*25)

          if(isNewPayAccount==1)
          {
            jedis4.set(gameAccount+"|"+orderDate+"|"+"_NewPayAccount","0")
            jedis4.expire(gameAccount+"|"+orderDate+"|"+"_NewPayAccount",3600*25)
          }
        }
    }
  pool.returnResource(jedis4)
  pool.destroy()
  conn.close()
  connXiappeng.close()
  }
  /**
    *加载登录数据
    *
    * @param fp
    */
  def loginInfoActions(fp: Iterator[(String, String, String)]): Unit ={
    val pool = JedisUtil.getJedisPool
    val jedis4 = pool.getResource
    jedis4.select(5)
    val conn=JdbcUtil.getConn()
    val connXiappeng = JdbcUtil.getXiaopeng2Conn()
    for (row <- fp) {
      val gameAccount=row._1
      val loginDate=row._2.substring(0,10)
      val gameId=row._3
      val channelId=getCommonChannelId(gameAccount,connXiappeng)
      val isActiveByDay=getIsActiveByDay(gameAccount,loginDate,jedis4)
      CenturionCommonSdkDao.loginInfoProcessDb(gameId,channelId,loginDate,isActiveByDay,conn)
      //redis
      jedis4.set(gameAccount+"|"+loginDate+"|"+"_IsActiveByDay","0")
      jedis4.expire(gameAccount+"|"+loginDate+"|"+"_IsActiveByDay",3600*25)
    }
    pool.returnResource(jedis4)
    pool.destroy()
    conn.close()
    connXiappeng.close()


  }


  /**
    * 判断是否当天已经被记录过登录
    *
    * @param gameAccount
    * @param loginDate
    * @param jedis
    * @return
    */
  def getIsActiveByDay(gameAccount: String, loginDate: String,jedis:Jedis):Int ={
    var jg=0
    if(!jedis.exists(gameAccount+"|"+loginDate+"|"+"_IsActiveByDay"))
    {
      jg=1
    }
    return  jg
  }

  /**
    * 获取聚合渠道，通过游戏账号
    *
    * @param gameAccount
    * @param connXiappeng
    * @return
    */
  def getCommonChannelId(gameAccount: String, connXiappeng: Connection):String={
    var jg="0"
    var stmt: PreparedStatement = null
    val sql: String =" select channel_id from common_sdk_account_tran  where account=? limit 1"
    stmt=connXiappeng.prepareStatement(sql)
    stmt.setString(1,gameAccount)
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next) {
      jg=rs.getString("channel_id")
    }
    stmt.close()
    return jg
  }
  /**
    *
    * @param fp
    */
  def regiInfoActions(fp: Iterator[(String, String, String, String)]): Unit ={
    val conn=JdbcUtil.getConn()
    for (row <- fp) {
      val gameId=row._2
      val regiDate=row._3.substring(0,10)
      val channelId=row._4
      val newAccount=1
      CenturionCommonSdkDao.regiInfoProcessDb(gameId,channelId,regiDate,newAccount,conn)
    }
    conn.close()
  }

}
