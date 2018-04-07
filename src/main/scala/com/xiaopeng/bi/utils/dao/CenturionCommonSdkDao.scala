package com.xiaopeng.bi.utils.dao

import java.sql.{Connection, PreparedStatement}

/**
  * Created by Administrator on 2017/7/26.
  */
object CenturionCommonSdkDao {


  /**
    * 推送区服数据到表
    * @param gameId
    * @param channelId
    * @param orderDate
    * @param totalPayAccount
    * @param totalPayAmount
    * @param conn
    */
  def orderAreaInfoProcessDb(gameId: String, channelId: String, orderDate: String,serviceArea:String, totalPayAccount: Int, totalPayAmount: Float, conn: Connection) =

  {
    val instSql = "insert into bi_centurion_commonsdk_serv(stata_date,channel_id,game_id,service_area,total_pay_account,total_pay_amount)" +
      " values(?,?,?,?,?,?)" +
      " on duplicate key update total_pay_account=total_pay_account+?,total_pay_amount=total_pay_amount+?"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    //insert
    ps.setString(1, orderDate)
    ps.setString(2, channelId)
    ps.setString(3, gameId)
    ps.setString(4,serviceArea)
    ps.setInt(5, totalPayAccount)
    ps.setFloat(6,totalPayAmount)
    //update
    ps.setInt(7, totalPayAccount)
    ps.setFloat(8,totalPayAmount)
    ps.executeUpdate()
    ps.close()
  }

  /**
    * 推送订单数据到表
    * @param gameId
    * @param channelId
    * @param orderDate
    * @param newPayAccount
    * @param newPayAmount
    * @param totalPayAccount
    * @param totalPayAmount
    * @param conn
    */
  def orderInfoProcessDb(gameId: String, channelId: String, orderDate: String, newPayAccount: Int, newPayAmount: Float, totalPayAccount: Int, totalPayAmount: Float, conn: Connection) = {

    val instSql = "insert into bi_centurion_commonsdk(stata_date,channel_id,game_id,new_pay_account,new_pay_amount,total_pay_account,total_pay_amount)" +
      " values(?,?,?,?,?,?,?)" +
      " on duplicate key update new_pay_account=new_pay_account+?,new_pay_amount=new_pay_amount+?,total_pay_account=total_pay_account+?,total_pay_amount=total_pay_amount+?"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    //insert
    ps.setString(1, orderDate)
    ps.setString(2, channelId)
    ps.setString(3, gameId)
    ps.setInt(4, newPayAccount)
    ps.setFloat(5,newPayAmount)
    ps.setInt(6, totalPayAccount)
    ps.setFloat(7,totalPayAmount)
    //update
    ps.setInt(8, newPayAccount)
    ps.setFloat(9,newPayAmount)
    ps.setInt(10, totalPayAccount)
    ps.setFloat(11,totalPayAmount)
    ps.executeUpdate()
    ps.close()

  }


  def loginInfoProcessDb(gameId: String, channelId: String, loginDate: String, activeNum: Int, conn: Connection) ={

    val instSql = "insert into bi_centurion_commonsdk(stata_date,channel_id,game_id,active_num)" +
      " values(?,?,?,?)" +
      " on duplicate key update active_num=active_num+?"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    //insert
    ps.setString(1, loginDate)
    ps.setString(2, channelId)
    ps.setString(3, gameId)
    ps.setInt(4, activeNum)
    //update
    ps.setInt(5, activeNum)
    ps.executeUpdate()
    ps.close()


  }


  def regiInfoProcessDb(gameId: String, channelId: String, regiDate: String, newAccount: Int,conn:Connection) ={
    val instSql = "insert into bi_centurion_commonsdk(stata_date,channel_id,game_id,new_regi_account)" +
      " values(?,?,?,?)" +
      " on duplicate key update new_regi_account=new_regi_account+?"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    //insert
    ps.setString(1, regiDate)
    ps.setString(2, channelId)
    ps.setString(3, gameId)
    ps.setInt(4, newAccount)
    //update
    ps.setInt(5, newAccount)
    ps.executeUpdate()
    ps.close()
  }

}
