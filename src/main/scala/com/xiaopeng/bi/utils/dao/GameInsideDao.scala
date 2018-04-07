package com.xiaopeng.bi.utils.dao

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat

import redis.clients.jedis.Jedis

/**
  * Created by Administrator on 2016/7/15.
  */
object GameInsideDao {

  /**
    * 推送数据到数据库
    * @param gameAccount
    * @param gameKey
    * @param channelId
    * @param startTime
    * @param endTime
    * @param gameId
    * @param conn
    */
  def accountOnlineInfoProcessDB(gameAccount: String, gameKey: String, channelId: String, startTime: String, endTime: String, gameId: Int,startDate:String, conn: Connection,jedis: Jedis) = {
    val sdf = new   SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    var endToStartSS=0L
    var loginTimes=0
    val instSql="insert into bi_gamepublish_online(game_account,game_id,login_date,login_len,login_times) values(?,?,?,?,?)" +
      " on duplicate key update login_len=login_len+?,login_times=login_times+?"
    var st=""
    if(jedis.exists(gameAccount+"|"+startTime+"|"+"_accountOnline"))
      {
        st=jedis.get(gameAccount+"|"+startTime+"|"+"_accountOnline")
        endToStartSS=(sdf.parse(endTime).getTime-sdf.parse(jedis.get(gameAccount+"|"+startTime+"|"+"_accountOnline")).getTime)/1000;
      }
    else
      {
         st=endTime
         endToStartSS =(sdf.parse(endTime).getTime-sdf.parse(startTime).getTime)/1000;
         loginTimes=1
      }
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    //insert
    ps.setString(1,gameAccount)
    ps.setInt(2,gameId)
    ps.setString(3,startDate)
    ps.setLong(4,endToStartSS)
    ps.setInt(5,loginTimes)
    //update
    ps.setLong(6,endToStartSS)
    ps.setInt(7,loginTimes)
    ps.executeUpdate()
    ps.close()
    jedis.set(gameAccount+"|"+startTime+"|"+"_accountOnline",endTime)
    jedis.expire(gameAccount+"|"+startTime+"|"+"_accountOnline",25*3600)
  }

  /**
    * 推送角色数据
 *
    * @param roleid
    * @param rolename
    * @param roletype
    * @param rolelevel
    * @param operetime
    * @param serare
    * @param serarename
    * @param userid
    * @param conn
    */
  def roleInfoProcessDB(roleid: String, rolename: String, roletype: String, rolelevel: String, operetime: String, serare: String, serarename: String, userid: String,conn:Connection) =
  {
    val instSql="insert into bi_gameinner_roleinfo(uid,role_id,role_name,role_type,role_level,role_ctime,role_utime,serverarea_id,serverarea_name) values(?,?,?,?,?,?,?,?,?)" +
                " on duplicate key update role_level=?,role_utime=?,serverarea_id=?,serverarea_name=?"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
      //insert
      ps.setString(1,userid)
      ps.setString(2,roleid)
      ps.setString(3,rolename)
      ps.setString(4,roletype)
      ps.setString(5,rolelevel)
      ps.setString(6,operetime)
      ps.setString(7,operetime)
      ps.setString(8,serare)
      ps.setString(9,serarename)
      //update
      ps.setString(10,rolelevel)
      ps.setString(11,operetime)
      ps.setString(12,serare)
      ps.setString(13,serarename)
      ps.executeUpdate()
      ps.close()
    }

}
