package com.xiaopeng.bi.utils.action

import java.sql.Connection

import com.xiaopeng.bi.checkdata.MissInfo2Redis
import com.xiaopeng.bi.utils.dao.SubCenturionDao
import com.xiaopeng.bi.utils.{Commons, JdbcUtil, JedisUtil}

/**
  * Created by Administrator on 2017/5/20.
  */
object SubCenturionActs {
  /**
    * 对登录数据进行action操作,传入的推广码判断是否需要补充账号数
    * game_account,logintime,imei,expand_code,gameid,expand_channel
    *
    * @param fp
    */
  def loginInfoActions(fp: Iterator[(String, String, String,String,String,String)]): Unit ={
    val pool = JedisUtil.getJedisPool
    val jedis = pool.getResource
    val jedis4 = pool.getResource
    jedis4.select(4)
    val conn=JdbcUtil.getConn()
    val connXiappeng = JdbcUtil.getXiaopeng2Conn()
    for (row <- fp) {
        //是深度联运才算
        if (jedis.exists(row._5.toString + "_publish_game")) {
          val gameAccount=row._1
          val loginTime=row._2
          val loginDate=row._2.substring(0,10)
          val imei=row._3
          val gameId=row._5
          if(Commons.isSubCenturion(row._4))
          {
            if(!jedis.exists(gameAccount))
            {
              MissInfo2Redis.checkAccountSubCen(gameAccount)
            }
          }
          val userCode=Commons.getUserCodeByOrder(gameAccount,jedis)
          val promoCode=jedis.hget(gameAccount,"expand_code")
          if(promoCode==null)
            {
              jedis4.set("subcen_account_no_promocode:"+gameAccount,gameAccount)
              jedis4.expire("subcen_account_no_promocode:"+gameAccount,3600*24*2)
            }
          val devStatus=1
          val expandChnnel = jedis.hget(gameAccount, "expand_channel")
          if(expandChnnel==null)
          {
            jedis4.set("subcen_account_no_expandChnnel:"+gameAccount,gameAccount)
            jedis4.expire("subcen_account_no_expandChnnel:"+gameAccount,3600*24*2)
          }
          if(promoCode!=null)             //空指针不执行
            {
              //取注册中的code
              var accountCode = if (!userCode.equals("0")) promoCode + "~" + userCode else promoCode
              accountCode = if (!(accountCode).trim.equals("")) promoCode + "~" + userCode else if (expandChnnel == null) "" else expandChnnel;
              //有效日期
              val worthinessDate = Commons.getWorthinessDate(imei, accountCode, loginDate, 15, conn)
              //设备有效性
              var channelId: String =jedis.hget(row._5.toString + "_publish_game","channel_id")
              if(channelId==null) channelId="0"
              if(!channelId.equals("22")&&(!channelId.equals("23"))) {
                //设备有效性对深度联运无效,并且登录code与注册code一致
                SubCenturionDao.loginInfoProcessDevStatus1(accountCode, imei, devStatus, worthinessDate, loginDate, conn)
              }
              if(!userCode.equals("0"))
              {
                //其他维度信息
                if(!promoCode.equals(""))
                {
                  val memberId=Commons.getMemberId(promoCode,jedis)
                  if(!memberId.equals("0"))
                  {
                    val userAccount=Commons.getUserAccount(memberId,jedis)
                    val groupId=Commons.getGroupId(memberId,connXiappeng)
                    val memberRegiTime=Commons.getMemberRegiTime(memberId,jedis)
                    val pkgCode=promoCode+"~"+userCode   //分包
                  //是否新充值账号 充值日期=注册日期
                  val newLoginAccount=Commons.getNewLoginAccount(gameAccount, loginDate, jedis, jedis4)
                    //指标
                    val loginAccounts=Commons.getLoginAccounts(gameAccount,loginDate,pkgCode,gameId,jedis4)
                    SubCenturionDao.loginInfoProcessAccount(gameAccount,loginTime,row._4,conn)
                    SubCenturionDao.loginInfoProcessPkgStat(loginDate,promoCode,userCode,pkgCode,memberId,userAccount,gameId,groupId,memberRegiTime,loginAccounts,newLoginAccount,conn)
                    //redis
                    jedis4.set(gameAccount+"|"+loginDate+"|"+pkgCode+""+gameId+"|"+"_SubCentLoginAccounts",pkgCode)
                    jedis4.expire(gameAccount+"|"+loginDate+"|"+pkgCode+""+gameId+"|"+"_SubCentLoginAccounts",3600*24*2)

                    jedis4.set(gameAccount+"|"+loginDate+"|"+"_NewLoginAccountByDay","0")
                    jedis4.expire(gameAccount+"|"+loginDate+"|"+"_NewLoginAccountByDay",3600*24*2)

                  }
                }
              } else {println("-------------------->登录不符合黑专服平台账号-2:"+gameAccount)}

            } else  {println("-------------------->登录推广码或推广渠道有空指针的情况-3:"+gameAccount)}

        }else {println("-------------------->登录不是发行游戏-1:"+row._5.toString)}
    }
    JedisUtil.returnResource(jedis,pool)
    JedisUtil.returnResource(jedis4,pool)
    pool.destroy()
    conn.close()
    connXiappeng.close()
  }

  /**
    * 对激活数据进行action操作
    * gameid  推广码 下黑码  激活时间  imei  IP地址 expand_code
    *
    * @param fp
    */
  def activeInfoActions(fp: Iterator[(String, String, String, String, String, String,String)]): Unit ={
    val pool = JedisUtil.getJedisPool
    val jedis = pool.getResource
    val jedis4 = pool.getResource
    jedis4.select(4)
    val conn=JdbcUtil.getConn()
    val connXiappeng = JdbcUtil.getXiaopeng2Conn()
    for (row <- fp) {
      //是否黑金卡
      if(Commons.isSubCenturion(row._7))
      {
          //是深度联运才算
          if (jedis.exists(row._1.toString + "_publish_game")) {
            val gameid=row._1
            val promoCode=row._2
            val activeDate=row._4.substring(0,10)
            val userCode= row._3
            val imei=row._5
            val ip=row._6
            val pkgCode=row._7
            //其他维度
            val memberId=Commons.getMemberId(promoCode,jedis)
            if(!memberId.equals("0"))
            {
              val userAccount=Commons.getUserAccount(memberId,jedis)
              val groupId=Commons.getGroupId(memberId,connXiappeng)
              val memberRegiTime=Commons.getMemberRegiTime(memberId,jedis)
              val activeDevs=Commons.getActiveDevs(imei,activeDate,pkgCode,gameid,jedis4)
              val activeIps=Commons.getActiveIps(ip,activeDate,pkgCode,gameid,jedis4)
              SubCenturionDao.activeInfoProcessPkgStat(activeDate,promoCode,userCode,pkgCode,memberId,userAccount,gameid,groupId,memberRegiTime,activeDevs,activeIps,conn)
              //redis
              jedis4.set(ip+"|"+activeDate+"|"+pkgCode+"|"+gameid+"|"+"_SubCentActiveIps",pkgCode)
              jedis4.expire(ip+"|"+activeDate+"|"+pkgCode+"|"+gameid+"|"+"_SubCentActiveIps",3600*24*2)

              jedis4.set(imei+"|"+activeDate+"|"+pkgCode+"|"+gameid+"|"+"_SubCentActiveDevs",pkgCode)
              jedis4.expire(imei+"|"+activeDate+"|"+pkgCode+"|"+gameid+"|"+"_SubCentActiveDevs",3600*24*2)
            }

          } else {println("-------------------->激活不是发行游戏-2:"+row._1.toString)}

      }else {println("-------------------->激活不是黑专服-1:"+row._1.toString)}

    }
    JedisUtil.returnResource(jedis,pool)
    JedisUtil.returnResource(jedis4,pool)
    pool.destroy()
    conn.close()
    connXiappeng.close()
  }



/**
    * 对订单数据进行action操作
    * 账号，订单号，订单日期，游戏id，渠道id，返利人id，产品类型，订单状态,充值流水，返利金额,imei,下单时身份,违规状态,区服，角色
  *
  * @param fp
    */
  def orderInfoActions(fp: Iterator[(String, String, String, String, String, String, Int, Int, Float, Float, String, String,String,String,String)]): Unit ={
    val pool = JedisUtil.getJedisPool
    val jedis = pool.getResource
    val jedis4 = pool.getResource
    jedis4.select(4)
    val conn=JdbcUtil.getConn()
    val connXiappeng = JdbcUtil.getXiaopeng2Conn()
    for (row <- fp) {
      //是深度联运才算
      if (jedis.exists(row._4.toString + "_publish_game")) {
          val gameAccount = row._1
          val orderNo = row._2
          val orderTime = row._3
          val orderDate = row._3.substring(0, 10)
          val gameId = row._4
          val orderStatus = row._8
          val recharteAmount = row._9 * 100
          val rebateAmount = row._10 * 100
          val imei = row._11
          val orderSf = row._12
          val wgStatus = row._13
          val devStatus = 1
          //订单通过账号获取推广码
          val userCode = Commons.getUserCodeByOrder(gameAccount, jedis)
          val promoCode = jedis.hget(gameAccount, "expand_code")
          val expandCode = if (!userCode.equals("0")) promoCode + "~" + userCode else promoCode
          val expandChnnel = jedis.hget(gameAccount, "expand_channel")
          val serverArea = row._14
          val roleName = row._15
          //有效设备时间
          val pc = if (!expandCode.trim.equals("")) expandCode else if(expandChnnel==null) "" else expandChnnel;
         val worthinessDate =  Commons.getday(15, orderTime)
          if (!jedis4.exists(gameAccount + "|" + orderNo + "|" + orderTime + "|" + orderStatus + "|" + "_SubCentOrderInfoAction")) //计算一次就不再计算
          {
            var channelId=jedis.hget(row._4.toString + "_publish_game","channel_id")
            if(channelId==null) channelId="0"
            if(!channelId.equals("22")&&(!channelId.equals("23"))&& wgStatus.equals("0") ) {  //设备有效性对深度联运无效
              SubCenturionDao.orderInfoProcessDevStatus(pc, imei, devStatus, worthinessDate, orderDate, recharteAmount, conn)
            }

            if (!userCode.equals("0") && orderSf.contains("3") && wgStatus.equals("0") && (!promoCode.equals(""))) //下级存在并且属于黑专服订单
            {
              //其他维度信息
              val memberId = Commons.getMemberId(promoCode, jedis)
              if (!memberId.equals("0")) {
                val userAccount = Commons.getUserAccount(memberId, jedis)
                val groupId = Commons.getGroupId(memberId, connXiappeng)
                val memberRegiTime = Commons.getMemberRegiTime(memberId, jedis)

                //指标
                val pkgCode = promoCode + "~" + userCode
                val rechargeOrders = 1
                val rechargeAccouts = Commons.getRechargeAccouts(gameAccount, orderDate, pkgCode, gameId, jedis4)
                val rechargeDevs = Commons.getRechargeDevs(imei, orderDate, pkgCode, gameId, jedis4)
                //是否新充值账号 充值日期=注册日期
                val newRechargeAccount=Commons.getNewRechargeAccount(gameAccount, orderDate, pkgCode, gameId, jedis)
                //新充值账号一天之内只算1
                var newRechargeAccountByDay=0
                var newRechargeAmountByDay=0.0.toFloat
                if(newRechargeAccount==1)
                  {
                    newRechargeAccountByDay=Commons.getNewRechargeAccountByDay(gameAccount,orderDate,jedis4)
                    newRechargeAmountByDay=recharteAmount
                  }
                SubCenturionDao.orderInfoProcessOrderDetail(orderNo, gameAccount, orderTime, recharteAmount, rebateAmount, serverArea, roleName, conn)
                SubCenturionDao.orderInfoProcessAccount(gameAccount, orderTime, recharteAmount, rebateAmount, conn)
                SubCenturionDao.orderInfoProcessPkgStat(orderDate, promoCode, userCode, pkgCode, memberId, userAccount, gameId, groupId, memberRegiTime, recharteAmount, rebateAmount, rechargeOrders, rechargeAccouts, rechargeDevs,newRechargeAccountByDay,newRechargeAmountByDay, conn)

                //存储到临时表
                jedis4.set(gameAccount + "|" + orderNo + "|" + orderTime + "|" + orderStatus + "|" + "_SubCentOrderInfoAction", pkgCode)
                jedis4.expire(gameAccount + "|" + orderNo + "|" + orderTime + "|" + orderStatus + "|" + "_SubCentOrderInfoAction", 3600 * 24 * 4)

                //日新增判断
                jedis4.set(imei + "|" + orderDate + "|" + pkgCode + "|" + gameId + "|" + "_SubCentRechargeDevs", pkgCode)
                jedis4.expire(imei + "|" + orderDate + "|" + pkgCode + "|" + gameId + "|" + "_SubCentRechargeDevs", 3600 * 24 * 4)

                jedis4.set(gameAccount + "|" + orderDate + "|" + pkgCode + "|" + gameId + "|" + "_SubCentRechargeAccounts", pkgCode)
                jedis4.expire(gameAccount + "|" + orderDate + "|" + pkgCode + "|" + gameId + "|" + "_SubCentRechargeAccounts", 3600 * 24 * 4)
                //新增充值只算一次
                jedis4.set(gameAccount+"|"+orderDate+"|"+"_NewRechargeAccountByDay", gameAccount)
                jedis4.expire(gameAccount+"|"+orderDate+"|"+"_NewRechargeAccountByDay", 3600 * 24 * 4)
              }
            } else println("-------------------->订单不符合要求违规or非黑专服-3，订单号:" + orderNo + ",订单是否黑专服：" + orderSf + ",是否违规:" + wgStatus)
          } else println("-------------------->订单已被计算一次-2：" + orderNo)
      } else {println("-------------------->订单不是深度游戏-1:"+row._5.toString)}

    }
    JedisUtil.returnResource(jedis,pool)
    JedisUtil.returnResource(jedis4,pool)
    pool.destroy()
    conn.close()
    connXiappeng.close()
  }

  /**
    * 对注册数据进行action操作
    * 游戏帐号  游戏id  注册时间  imei ip  promo_code user_code expand_code expand_channel
    *
    * @param fp
    */
  def regiInfoActions(fp: Iterator[(String, String, String, String, String, String, String,String,String)]): Unit = {
    val pool = JedisUtil.getJedisPool
    val jedis = pool.getResource
    val jedis4 = pool.getResource
    jedis4.select(4)
    val conn=JdbcUtil.getConn()
    val connXiappeng = JdbcUtil.getXiaopeng2Conn()
    for (row <- fp) {
        //是深度联运才算
        if (jedis.exists(row._2.toString + "_publish_game")) {
              val gameAccount=row._1
              val gamiId=row._2
              val regiTime=row._3
              val regiDate=row._3.substring(0,10)
              val imei=row._4
              val ip=row._5
              val expandCode=row._8
              val expandChnnel=row._9
              val promoCode=Commons.getPromoCode(expandCode)
              val userCode=Commons.getUserCode(expandCode)
              val devStatus=1

              val pc= if(!expandCode.trim.equals(""))  expandCode  else expandChnnel;
              /*游戏截止日期，注册日期+15日，或者累计流水大于50元时为永久有效*/
              val  worthinessDate=Commons.getWorthinessDate(imei,expandCode,regiDate,15,conn)

              //有效设备不排除黑金卡专服
              var channelId=jedis.hget(row._2.toString + "_publish_game","channel_id")
              if(channelId==null) channelId="0"
              if(!channelId.equals("22")&&(!channelId.equals("23"))) {  //设备有效性对深度联运无效
                SubCenturionDao.regiInfoProcessDevStatus(pc, imei, devStatus, worthinessDate, regiDate, conn)
              }
              //统计数据只统计黑金卡专服
              if(Commons.isSubCenturion(expandCode))
              {
                //指标
                val pkgCode=expandCode
                if(!jedis4.exists(gameAccount+"|"+regiDate+"|"+pkgCode+"|"+"_SubCentRegiInfoActions"))
                {
                  val memberId=Commons.getMemberId(promoCode,jedis)
                  if(!memberId.equals("0"))
                  {
                    val userAccount=Commons.getUserAccount(memberId,jedis)
                    val groupId=Commons.getGroupId(memberId,connXiappeng)
                    val bindMember=Commons.getBindMember(gameAccount,jedis)
                    val memberRegiTime=Commons.getMemberRegiTime(memberId,jedis)
                    val regiAccounts=1
                    val regiDevs=Commons.getRegiDevs(imei,regiDate,pkgCode,gamiId,jedis4)
                    SubCenturionDao.regiInfoProcessAccount(gameAccount,gamiId,regiTime,regiDate,imei,ip,promoCode,userCode,pkgCode,memberId,userAccount,groupId,bindMember,memberRegiTime,conn)
                    SubCenturionDao.regiInfoProcessPkgStat(regiDate,promoCode,userCode,pkgCode,memberId,userAccount,gamiId,groupId,memberRegiTime,regiAccounts,regiDevs,conn)
                    //存储到redis
                    jedis4.set(imei+"|"+regiDate+"|"+pkgCode+"|"+gamiId+"|"+"_SubCentNewRegiDev",pkgCode)
                    jedis4.expire(imei+"|"+regiDate+"|"+pkgCode+"|"+gamiId+"|"+"_SubCentNewRegiDev",3600*24*4)
                    //只记录一次
                    jedis4.set(gameAccount+"|"+regiDate+"|"+pkgCode+"|"+gamiId+"|"+"_SubCentRegiInfoActions",pkgCode)
                    jedis4.expire(gameAccount+"|"+regiDate+"|"+pkgCode+"|"+gamiId+"|"+"_SubCentRegiInfoActions",3600*24*4)
                  } else println("-------------------->注册账号找不到黑金卡-5："+memberId)
                } else println("-------------------->注册账号已经被计算一次-4："+gameAccount)
              } else {println("-------------------->注册非黑专服平台账号-3："+gameAccount)}
        }  else {println("-------------------->注册不是深度联运游戏-1:"+row._2.toString)}


    }
    JedisUtil.returnResource(jedis,pool)
    JedisUtil.returnResource(jedis4,pool)
    pool.destroy()
    conn.close()
    connXiappeng.close()
  }

}
