package com.xiaopeng.bi.utils.dao

import java.sql.{Connection, PreparedStatement}

import com.xiaopeng.bi.checkdata.MissInfo2Redis
import com.xiaopeng.bi.utils.Commons

/**
  * Created by Administrator on 2017/5/20.
  */
object SubCenturionDao {

  /**
    * 推送激活到统计表
    * @param activeDate
    * @param promoCode
    * @param userCode
    * @param pkgCode
    * @param memberId
    * @param userAccount
    * @param gameid
    * @param groupId
    * @param memberRegiTime
    * @param activeDevs
    * @param activeIps
    * @param conn
    */
  def activeInfoProcessPkgStat(activeDate: String, promoCode: String, userCode: String, pkgCode: String, memberId: String, userAccount: String, gameid: String, groupId: Int, memberRegiTime: String, activeDevs: Int, activeIps: Int, conn: Connection) ={
    val instSql = "insert into bi_sub_centurition_pkgstats(statistics_date,pkg_code,promo_code,user_code,member_id,member_name,game_id,group_id,member_regi_time,active_devs,active_ips)" +
      " values(?,?,?,?,?,?,?,?,?,?,?)" +
      " on duplicate key update active_devs=active_devs+?,active_ips=active_ips+?"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    //insert
    ps.setString(1, activeDate)
    ps.setString(2, pkgCode)
    ps.setString(3, promoCode)
    ps.setString(4, userCode)
    ps.setString(5, memberId)
    ps.setString(6, userAccount)
    ps.setString(7, gameid)
    ps.setInt(8, groupId)
    ps.setString(9, memberRegiTime)
    ps.setInt(10, activeDevs)
    ps.setInt(11, activeIps)
    //update
    ps.setInt(12, activeDevs)
    ps.setInt(13, activeIps)
    ps.executeUpdate()
    ps.close()

  }


  /**
    *推送登录到统计表
    * @param loginDate
    * @param promoCode
    * @param userCode
    * @param pkgCode
    * @param memberId
    * @param userAccount
    * @param gameId
    * @param groupId
    * @param memberRegiTime
    * @param loginAccounts
    * @param conn
    */
  def loginInfoProcessPkgStat(loginDate: String, promoCode: String, userCode: String, pkgCode: String, memberId: String, userAccount: String, gameId: String, groupId: Int, memberRegiTime: String, loginAccounts: Int,newLoginAccount:Int, conn: Connection) ={
    val instSql = "insert into bi_sub_centurition_pkgstats(statistics_date,pkg_code,promo_code,user_code,member_id,member_name,game_id,group_id,member_regi_time,login_accounts,new_login_accounts)" +
      " values(?,?,?,?,?,?,?,?,?,?,?)" +
      " on duplicate key update login_accounts=login_accounts+?,new_login_accounts=new_login_accounts+?"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    //insert
    ps.setString(1, loginDate)
    ps.setString(2, pkgCode)
    ps.setString(3, promoCode)
    ps.setString(4, userCode)
    ps.setString(5, memberId)
    ps.setString(6, userAccount)
    ps.setString(7, gameId)
    ps.setInt(8, groupId)
    ps.setString(9, memberRegiTime)
    ps.setInt(10, loginAccounts)
    ps.setInt(11, newLoginAccount)
    //update
    ps.setInt(12, loginAccounts)
    ps.setInt(13, newLoginAccount)
    ps.executeUpdate()
    ps.close()
  }


  /**
    *推送订单到统计表
    *
    * @param orderDate
    * @param promoCode
    * @param userCode
    * @param pkgCode
    * @param memberId
    * @param userAccount
    * @param gameId
    * @param groupId
    * @param memberRegiTime
    * @param recharteAmount
    * @param rebateAmount
    * @param rechargeOrders
    * @param rechargeAccouts
    * @param rechargeDevs
    * @param conn
    */
  def orderInfoProcessPkgStat(orderDate: String, promoCode: String, userCode: String, pkgCode: String, memberId: String, userAccount: String, gameId: String, groupId: Int, memberRegiTime: String, recharteAmount: Float, rebateAmount: Float, rechargeOrders: Int, rechargeAccouts: Int, rechargeDevs: Int,newRechargeAccountByDay: Int,newRechargeAmountByDay:Float, conn: Connection) ={
    val instSql = "insert into bi_sub_centurition_pkgstats(statistics_date,pkg_code,promo_code,user_code,member_id,member_name,game_id,group_id,member_regi_time,recharge_accouts,recharge_devs,recharge_amount,rebate_amount,recharge_orders,new_pay_accounts,new_pay_amount)" +
      " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
      " on duplicate key update recharge_accouts=recharge_accouts+?,recharge_devs=recharge_devs+?,recharge_amount=recharge_amount+?,rebate_amount=rebate_amount+?,recharge_orders=recharge_orders+?,new_pay_accounts=new_pay_accounts+?,new_pay_amount=new_pay_amount+?"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    //insert
    ps.setString(1, orderDate)
    ps.setString(2, pkgCode)
    ps.setString(3, promoCode)
    ps.setString(4, userCode)
    ps.setString(5, memberId)
    ps.setString(6, userAccount)
    ps.setString(7, gameId)
    ps.setInt(8, groupId)
    ps.setString(9, memberRegiTime)
    ps.setInt(10, rechargeAccouts)
    ps.setInt(11, rechargeDevs)
    ps.setFloat(12, recharteAmount)
    ps.setFloat(13, rebateAmount)
    ps.setInt(14, rechargeOrders)
    ps.setInt(15,newRechargeAccountByDay)
    ps.setFloat(16,newRechargeAmountByDay)
    //update
    ps.setInt(17, rechargeAccouts)
    ps.setInt(18, rechargeDevs)
    ps.setFloat(19, recharteAmount)
    ps.setFloat(20, rebateAmount)
    ps.setInt(21, rechargeOrders)
    ps.setInt(22, newRechargeAccountByDay)
    ps.setFloat(23,newRechargeAmountByDay)
    ps.executeUpdate()
    ps.close()
  }

  /**
    * 推送数据到统计表
    * @param regiDate
    * @param promoCode
    * @param userCode
    * @param pkgCode
    * @param memberId
    * @param userAccount
    * @param gamiId
    * @param groupId
    * @param memberRegiTime
    * @param regiAccounts
    * @param regiDevs
    * @param conn
    */
  def regiInfoProcessPkgStat(regiDate: String, promoCode: String, userCode: String, pkgCode: String, memberId: String, userAccount: String, gamiId: String, groupId: Int,
                             memberRegiTime: String, regiAccounts: Int, regiDevs: Int, conn: Connection) ={
    val instSql = "insert into bi_sub_centurition_pkgstats(statistics_date,pkg_code,promo_code,user_code,member_id,member_name,game_id,regi_accounts,regi_devs,group_id,member_regi_time)" +
      " values(?,?,?,?,?,?,?,?,?,?,?)" +
      " on duplicate key update regi_accounts=regi_accounts+?,regi_devs=regi_devs+?"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    //insert
    ps.setString(1, regiDate)
    ps.setString(2, pkgCode)
    ps.setString(3, promoCode)
    ps.setString(4, userCode)
    ps.setString(5, memberId)
    ps.setString(6, userAccount)
    ps.setString(7, gamiId)
    ps.setInt(8, regiAccounts)
    ps.setInt(9, regiDevs)
    ps.setInt(10, groupId)
    ps.setString(11, memberRegiTime)
    //update
    ps.setInt(12, regiAccounts)
    ps.setInt(13, regiDevs)
    ps.executeUpdate()
    ps.close()

  }


  /**
    * 订单数据加载到账号明细
    * @param gameAccount
    * @param orderTime
    * @param recharteAmount
    * @param rebateAmount
    *         orderTime
    */
  def orderInfoProcessAccount(gameAccount: String, orderTime: String, recharteAmount: Float, rebateAmount: Float,conn: Connection) = {
    val instSql = "insert into bi_sub_centurition_account(game_account,last_recharge_time,last_recharee_amount,recharge_amount,rebate_amount,is_recharge_account) values(?,?,?,?,?,?)" +
      " on duplicate key update recharge_amount=recharge_amount+?,rebate_amount=rebate_amount+?,last_recharee_amount=?,last_recharge_time=?,is_recharge_account=1"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    //insert
    ps.setString(1, gameAccount)
    ps.setString(2, orderTime.substring(0,10))
    ps.setFloat(3, recharteAmount)
    ps.setFloat(4, recharteAmount)
    ps.setFloat(5, rebateAmount)
    ps.setInt(6, 1)
    //update
    ps.setFloat(7, recharteAmount)
    ps.setFloat(8, rebateAmount)
    ps.setFloat(9, recharteAmount)
    ps.setString(10, orderTime)
    ps.executeUpdate()
    ps.close();

  }

  /**
    * 订单明细表
    * @param orderNo
    * @param gameAccount
    * @param orderTime
    * @param recharteAmount
    * @param rebateAmount
    * @return
    */
  def orderInfoProcessOrderDetail(orderNo: String, gameAccount: String, orderTime: String, recharteAmount: Float, rebateAmount: Float,serverArea:String,roleName:String,conn: Connection) = {
    val instSql = "insert into bi_sub_centurition_orderdetail(order_no,game_account,order_time,recharge_amount,rebate_amount,server_area,rolename) values(?,?,?,?,?,?,?)" +
      " on duplicate key update recharge_amount=?,rebate_amount=?,server_area=?,rolename=?"
      val ps: PreparedStatement = conn.prepareStatement(instSql)
      //insert
      ps.setString(1, orderNo)
      ps.setString(2, gameAccount)
      ps.setString(3, orderTime)
      ps.setFloat(4, recharteAmount)
      ps.setFloat(5, rebateAmount)
      ps.setString(6, serverArea)
      ps.setString(7, roleName)
      //update
      ps.setFloat(8, recharteAmount)
      ps.setFloat(9, rebateAmount)
      ps.setString(10, serverArea)
      ps.setString(11, roleName)
      ps.executeUpdate()
      ps.close();
  }


  /**
    * 订单对设备的有效性判断
    * @param pkgCode
    * @param imei
    * @param devStatus
    * @param worthinessDate
    * @param orderDate
    */
  def orderInfoProcessDevStatus(pkgCode: String, imei: String, devStatus: Int, worthinessDate: String, orderDate: String,recharteAmount:Float,conn:Connection) ={
    val codeUniq= Commons.isCodeUniq(imei,pkgCode,conn) //判断设备与code一致性
    val instSql = "insert into bi_sub_centurion_devstatus(pkg_code,imei,status,last_recharge_date,worthiness_date,recharge_amount) values(?,?,?,?,?,?)" +
      " on duplicate key update last_recharge_date=if(last_recharge_date>?,last_recharge_date,?),worthiness_date=if(worthiness_date>?,worthiness_date,?)," +
      " recharge_amount=if(last_recharge_date<=?,recharge_amount+?,recharge_amount)"
    if (codeUniq&&(!imei.equals(""))&&(!imei.equals("00000000000000000000000000000000")&&(!imei.equals("000000000000000"))))
       {
         val ps: PreparedStatement = conn.prepareStatement(instSql)
         //insert
         ps.setString(1, pkgCode)
         ps.setString(2, imei)
         ps.setInt(3, devStatus)
         ps.setString(4, orderDate)
         ps.setString(5, worthinessDate)
         ps.setFloat(6, recharteAmount)
         //update
         ps.setString(7, orderDate)
         ps.setString(8, orderDate)
         ps.setString(9, worthinessDate)
         ps.setString(10, worthinessDate)
         ps.setString(11, orderDate)
         ps.setFloat(12, recharteAmount)
         ps.executeUpdate()
         ps.close()
       }

  }


  /**
    * 注册信息判断有效设备
    *
    * @param expandCode
    * @param imei
    * @param devStatus
    * @param worthinessDate
    * @param regiDate
    * @return
    */
  def regiInfoProcessDevStatus(expandCode: String, imei: String, devStatus: Int, worthinessDate: String, regiDate: String,conn:Connection) {
    val instSql = "insert into bi_sub_centurion_devstatus(pkg_code,imei,status,last_regi_date,worthiness_date) values(?,?,?,?,?)" +
      " on duplicate key update last_regi_date=if(last_regi_date>?,last_regi_date,?),worthiness_date=if(worthiness_date>?,worthiness_date,?)"
    val codeUniq= Commons.isCodeUniq(imei,expandCode,conn) //判断设备与code一致性,并排除空设备
    if (codeUniq&&(!imei.equals(""))&&(!imei.equals("00000000000000000000000000000000")&&(!imei.equals("000000000000000")))) {
      val ps: PreparedStatement = conn.prepareStatement(instSql)
      //insert
      ps.setString(1, expandCode)
      ps.setString(2, imei)
      ps.setInt(3, devStatus)
      ps.setString(4, regiDate)
      ps.setString(5, worthinessDate)
      //update
      ps.setString(6, regiDate)
      ps.setString(7, regiDate)
      ps.setString(8, worthinessDate)
      ps.setString(9, worthinessDate)
      ps.executeUpdate()
      ps.close()
    }

  }


  /**
    *
    * @param gameAccount
    * @param gamiId
    * @param regiTime
    * @param regiDate
    * @param imei
    * @param ip
    * @param promoCode
    * @param userCode
    * @param pkgCode
    * @param memberId
    * @param userAccount
    * @param groupId
    * @param bindMember
    * @param memberRegiTime
    */
  def regiInfoProcessAccount(gameAccount: String, gamiId: String, regiTime: String, regiDate: String, imei: String,ip: String, promoCode: String,
                              userCode:String, pkgCode: String, memberId: String, userAccount: String,groupId:Int,bindMember:String,memberRegiTime:String,conn:Connection) = {
    val instSql = "insert into bi_sub_centurition_account" +
      "(game_account,game_id,regi_time,regi_date,imei,ip,promo_code,user_code,pkg_code,member_id,member_name,group_id,bind_phone,member_regi_time) " +
      "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
      " on duplicate key update group_id=?,bind_phone=?"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    //insert
    ps.setString(1, gameAccount)
    ps.setString(2, gamiId)
    ps.setString(3, regiTime)
    ps.setString(4, regiDate)
    ps.setString(5, imei)
    ps.setString(6, ip)
    ps.setString(7, promoCode)
    ps.setString(8, userCode)
    ps.setString(9, pkgCode)
    ps.setString(10, memberId)
    ps.setString(11, userAccount)
    ps.setInt(12, groupId)
    ps.setString(13, bindMember)
    ps.setString(14, memberRegiTime)
    //update
    ps.setInt(15, groupId)
    ps.setString(16, bindMember)
    ps.executeUpdate()
    ps.close()
}


  /**
    *账号明细补数
    * @param gameAccount
    * @param gamiId
    * @param regiTime
    * @param regiDate
    * @param imei
    * @param ip
    * @param promoCode
    * @param userCode
    * @param pkgCode
    * @param memberId
    * @param userAccount
    * @param groupId
    * @param bindMember
    * @param memberRegiTime
    */
  def regiInfoProcessAccountBs(gameAccount: String, gamiId: String, regiTime: String, regiDate: String, imei: String,ip: String, promoCode: String,
                             userCode:String, pkgCode: String, memberId: String, userAccount: String,groupId:Int,bindMember:String,memberRegiTime:String,loginTime:String,conn:Connection) = {
    val instSql = "insert into bi_sub_centurition_account" +
      "(game_account,game_id,regi_time,regi_date,imei,ip,promo_code,user_code,pkg_code,member_id,member_name,group_id,bind_phone,member_regi_time,last_login_time) " +
      "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
      " on duplicate key update group_id=?,bind_phone=?,last_login_time=?"
    val ps: PreparedStatement = conn.prepareStatement(instSql)
    //insert
    ps.setString(1, gameAccount)
    ps.setString(2, gamiId)
    ps.setString(3, regiTime)
    ps.setString(4, regiDate)
    ps.setString(5, imei)
    ps.setString(6, ip)
    ps.setString(7, promoCode)
    ps.setString(8, userCode)
    ps.setString(9, pkgCode)
    ps.setString(10, memberId)
    ps.setString(11, userAccount)
    ps.setInt(12, groupId)
    ps.setString(13, bindMember)
    ps.setString(14, memberRegiTime)
    ps.setString(15, loginTime)
    //update
    ps.setInt(16, groupId)
    ps.setString(17, bindMember)
    ps.setString(18, loginTime)
    ps.executeUpdate()
    ps.close()
  }



  /**
    *
    * @param devStatus
    * @param pkgCode
    * @param imei
    * @param worthinessDate
    * @param loginDate
    */
  def loginInfoProcessDevStatus1(pkgCode: String,imei: String,devStatus:Int,worthinessDate:String, loginDate: String,conn:Connection) = {
    val instSql = "insert into bi_sub_centurion_devstatus(pkg_code,imei,status,last_login_date,worthiness_date) values(?,?,?,?,?)" +
      " on duplicate key update last_login_date=if(last_login_date>?,last_login_date,?),worthiness_date=if(worthiness_date>?,worthiness_date,?)"
    val codeUniq= Commons.isCodeUniq(imei,pkgCode,conn) //判断设备与code一致性
    if (codeUniq&&(!imei.equals("00000000000000000000000000000000")&&(!imei.equals("000000000000000"))))
      {
        val ps: PreparedStatement = conn.prepareStatement(instSql)
        //insert
        ps.setString(1, pkgCode)
        ps.setString(2, imei)
        ps.setInt(3, devStatus)
        ps.setString(4, loginDate)
        ps.setString(5, worthinessDate)
        //update
        ps.setString(6, loginDate)
        ps.setString(7, loginDate)
        ps.setString(8, worthinessDate)
        ps.setString(9, worthinessDate)
        ps.executeUpdate()
        ps.close()
      }
  }

  /**
    * 推送登录数据到账号明细表
    * @param gameAccount
    * @param loginTime
    */
  def loginInfoProcessAccount(gameAccount: String,loginTime: String,expandCode:String,connection: Connection) ={
    val instSql = "update bi_sub_centurition_account set last_login_time=if(last_login_time>?,last_login_time,?),is_login_account=1 where game_account=?"
    val ps: PreparedStatement = connection.prepareStatement(instSql)
    ps.setString(1, loginTime)
    ps.setString(2, loginTime)
    ps.setString(3, gameAccount)
    val rs=ps.executeUpdate()
    if(rs==0&&expandCode.contains("~"))
      {
        MissInfo2Redis.checkAccountExists(gameAccount,loginTime)
      }
    ps.close();
  }

}
