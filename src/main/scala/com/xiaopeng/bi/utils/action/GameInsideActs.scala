package com.xiaopeng.bi.utils.action

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat

import com.xiaopeng.bi.utils.{JdbcUtil, JedisUtil, SQLContextSingleton}
import com.xiaopeng.bi.utils.dao.GameInsideDao
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by Administrator on 2016/7/15.
  *
  */
object GameInsideActs {

  case class AccountOnlineInfo(gameAccount:String,gameKey:String,channelId:String,startTime:String,endTime:String);

  /**
    *账号登录时长
    *
    * @param accountOnlineInfo
    */
  def loadAccountOnlinenfo(accountOnlineInfo: DStream[(String, String, String, String, String)]) ={
    accountOnlineInfo.foreachRDD(rdd=>{
      if(rdd.count()>0)  //需要有值才操作
      {
        val sc = rdd.sparkContext
        val sqlContext = SQLContextSingleton.getInstance(sc)
        import sqlContext.implicits._
        rdd.map(x=>AccountOnlineInfo(x._1,x._2,x._3,x._4,x._5)).toDF().registerTempTable("accountonlineinfo")
        val dataf=sqlContext.sql("select gameAccount,gameKey,channelId,startTime,endTime from accountonlineinfo order by startTime,endTime")
        dataf.rdd.foreachPartition(pt=>{
          accountOnlineAction(pt)
        })
      }
    })

  }


  /**
    * 加载角色信息
    *
    * @param roleInfo: roleid,Rolename,tRoletype,Rolelevel,Operatime,Servarea,Userid,ServareaName
    */
  def loadRoleInfo(roleInfo: DStream[(String, String, String, String, String, String, String,String)]) ={
    roleInfo.foreachRDD(rdd=>{
      rdd.coalesce(60)
      rdd.foreachPartition(pt=>{
        roleAction(pt)
      })
    })

  }

  /**
    * 登录时长action
    * @param pt
    */
  def accountOnlineAction(pt: Iterator[Row]): Unit = {

    val conn=JdbcUtil.getConn
    val connXiappeng=JdbcUtil.getXiaopeng2Conn()
    val pool=JedisUtil.getJedisPool
    val jedis=pool.getResource
    jedis.select(5)
    for (row <- pt) {
      val gameAccount =row.get(0).toString
      val gameKey=row.get(1).toString
      val channelId=row.get(2).toString
      val gameId=getGameId(gameKey,connXiappeng)
      val startTime=row.get(3).toString
      val startDate=row.get(3).toString.substring(0,10)
      val endTime=row.get(4).toString
      GameInsideDao.accountOnlineInfoProcessDB(gameAccount,gameKey,channelId,startTime,endTime,gameId,startDate,conn,jedis);
    }
    conn.close()
    connXiappeng.close()
    pool.returnResource(jedis)
    pool.destroy()
  }

  /**
    *获取游戏ID
    * @param gameKey
    * @param connXiappeng
    * @return
    */
  def getGameId(gameKey:String,connXiappeng:Connection):Int={
    var gameId=0
    var stmt: PreparedStatement = null
    val sql: String ="SELECT game_id FROM bgame_channel where ch_game_id=? limit 1"
    stmt=connXiappeng.prepareStatement(sql)
    stmt.setString(1,gameKey)
    val rs: ResultSet = stmt.executeQuery()
    while (rs.next) {
      gameId=rs.getString("game_id").toInt
    }
    stmt.close()
    return  gameId
  }

  /**
    * 角色数据action
    *
    * @param pt
    */
  def roleAction(pt: Iterator[(String, String, String, String, String, String, String,String)] ): Unit ={
    val conn=JdbcUtil.getConn
    for (row <- pt) {
     val roleid=row._1
     val rolename=row._2
     val roletype=row._3
     val rolelevel=row._4
     val operetime=row._5
     val serare=row._6.substring(2,row._6.length)
     val serarename=row._8
     val userid=row._7.split("_")(1)
     GameInsideDao.roleInfoProcessDB(roleid,rolename,roletype,rolelevel,operetime,serare,serarename,userid,conn);
    }
    conn.close()
  }

}


