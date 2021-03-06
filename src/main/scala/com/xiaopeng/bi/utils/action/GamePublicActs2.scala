package com.xiaopeng.bi.utils.action

import java.sql.Connection

import com.xiaopeng.bi.utils.dao.{GamePublicDao, GamePublicDao2}
import com.xiaopeng.bi.utils._
import org.apache.spark.rdd.RDD
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * Created by Administrator on 2016/7/15.
  */
object GamePublicActs2 {


  /**
    * 加载订单数据
    * 参数：游戏账号（5），订单号（2），订单日期（6），游戏id（7）,充值流水（10）+代金券，imei(24).sub
    */
  def loadOrderInfo(dslogs: RDD[String]) = {
    val rdd = dslogs.filter(x => {
      val arr = x.split("\\|", -1)
      //排除截断日志   只取存在订单号的数据，不存在订单号的为代金券消费  只取直充  只取状态为4的数据
      arr(0).contains("bi_order") && arr.length >= 25 && arr(2).trim.length > 0 && arr(22).contains("6") && arr(19).toInt == 4
    }).map(x => {
      val odInfo = x.split("\\|", -1)
      (odInfo(5).trim.toLowerCase, odInfo(2), odInfo(6), odInfo(7), Commons.getNullTo0(odInfo(10)) + Commons.getNullTo0(odInfo(13)), odInfo(24))
    })
    rdd.foreachPartition(fp => {
      if (!fp.isEmpty) {
        orderActions(fp)
      }
    })
  }

  /**
    * 加载登录数据
    *
    * 参数：game_account(3),logintime(4),game_id(8)，expand_channel(6),imei(7).sub
    */
  def loadLoginInfo(dslogs: RDD[String]) = {

    val rdd = dslogs.filter(x => {
      val lgInfo = x.split("\\|", -1)
      lgInfo(0).contains("bi_login") && lgInfo.length >= 9 && StringUtils.isNumber(lgInfo(8))
    }).map(x => {
      val lgInfo = x.split("\\|", -1)
      //game_account(3),logintime(4),game_id(8)，expand_channel(6),imei(7).sub
      (lgInfo(3).trim().toLowerCase(), lgInfo(4), lgInfo(8).toInt, if (lgInfo(6).equals("") || lgInfo(6) == null) "21" else lgInfo(6), lgInfo(7))
    })
    rdd.foreachPartition(fp => {
      if (!fp.isEmpty) {
        loginActions(fp)
      }
    })
  }

  /**
    * 用户消费
    *
    * @param fp
    * 参数：游戏账号（5），订单号（2），订单日期（6），游戏id（7）,充值流水（10），imei(24).sub
    */
  def orderActions(fp: Iterator[(String, String, String, String, Float, String)]): Unit = {
    val pool: JedisPool = JedisUtil.getJedisPool;
    val jedis0: Jedis = pool.getResource;
    jedis0.select(0)
    val jedis3: Jedis = pool.getResource;
    jedis3.select(3)
    val conn: Connection = JdbcUtil.getConn();
    val connFx = JdbcUtil.getXiaopeng2FXConn()
    for (row <- fp) {
      if (!jedis3.exists(row._1 + "|" + row._2 + "|" + row._3 + "|" + "_publishOrderActions")) {
        /*判断是否为发行游戏,只取发行游戏*/
        if (jedis0.exists(row._4.toString + "_publish_game")) {
          val game_account = row._1
          var expand_channel = jedis0.hget(row._1, "expand_channel")
          if (expand_channel == null || expand_channel.equals("no_acc")) {
            expand_channel = "no_acc";
            jedis3.set("no_acc|" + row._1, row._1)
            jedis3.expire("no_acc|" + row._1, 3600 * 24 * 3)
          }
          val imei = row._6
          val game_id = row._4.toInt
          val ori_price = row._5
          val order_date = row._3.substring(0, 10)
          var parent_game_id = jedis0.hget(game_id.toString + "_publish_game", "mainid")
          if (parent_game_id == null) parent_game_id = "0"
          val medium_channel = StringUtils.getArrayChannel(expand_channel)(0)
          val ad_site_channel = StringUtils.getArrayChannel(expand_channel)(1)
          val pkg_code = StringUtils.getArrayChannel(expand_channel)(2)
          var medium_account = jedis0.hget(pkg_code + "_pkgcode", "medium_account")
          if (medium_account == null) medium_account = ""
          var promotion_channel = jedis0.hget(pkg_code + "_pkgcode", "promotion_channel")
          if (promotion_channel == null) promotion_channel = ""
          var promotion_mode = jedis0.hget(pkg_code + "_" + order_date + "_pkgcode", "promotion_mode")
          if (promotion_mode == null) promotion_mode = ""
          var head_people = jedis0.hget(pkg_code + "_" + order_date + "_pkgcode", "head_people")
          if (head_people == null) head_people = ""
          var os = Commons.getPubGameGroupIdAndOs(game_id, connFx)(1)
          if (os == null) {
            println("orderActions get os is err:" + os);
            os = "1"
          }
          var group_id_pre = Commons.getPubGameGroupIdAndOs(game_id, connFx)(0)
          if (group_id_pre == null) {
            println("orderActions group_id_pre os is err:" + group_id_pre);
            group_id_pre = "0"
          }
          val group_id = group_id_pre.toInt
          /** 发行二期部分指标 */
          val accountInfo = jedis0.hgetAll(game_account)
          //是否为当日新增注册设备
          val isNewRegiDevDay = GamePublicDao.isNewRegiDevDay(pkg_code, imei, order_date, medium_channel, ad_site_channel, game_id, conn)
          //是否为新增注册账号
          val isNewRegiAccountDay = GamePublicDao.isNewRegiAccountDay(game_account, order_date, isNewRegiDevDay, expand_channel, imei, jedis0, accountInfo)
          //是否为当日注册账号，没判断是否新增
          val isNewRechargeDevDay = GamePublicDao.isNewRechargeDevDay(order_date, imei, expand_channel, game_id, isNewRegiDevDay, isNewRegiAccountDay, jedis3)
          //新增充值新定义：当天注册的并消费
          val isCurrentDayRegiAndRecharge = GamePublicDao.isCurrentDayRegiAndRecharge(game_account, order_date, expand_channel, game_id, jedis0)
          val isNewRechargeAccountDay = GamePublicDao.isNewRechargeAccountDay(order_date, game_account, expand_channel, game_id, isCurrentDayRegiAndRecharge, jedis3)
          val rechargeAmountDay = GamePublicDao.rechargeAmountDay(order_date, game_account, expand_channel, isCurrentDayRegiAndRecharge, ori_price)
          val tu17 = new Tuple17(order_date, parent_game_id, game_id, medium_channel, ad_site_channel, pkg_code, medium_account, promotion_channel
            , promotion_mode, head_people, group_id, isNewRechargeDevDay, isNewRechargeAccountDay, rechargeAmountDay, imei, game_account, os)

          /** 发行三期部分指标 */
          //是否为当日新增注册设备
          val isNewRegiDevDay2 = GamePublicDao2.isNewRegiDevDay(imei, order_date, game_id, conn)
          //是否为新增注册账号（旧逻辑）
          val isNewRegiAccountDay2 = GamePublicDao2.isNewRegiAccountDay(game_account, order_date, isNewRegiDevDay2, imei, jedis0)
          //是否为当日注册账号，没判断是否新增
          val isNewRechargeDevDay2 = GamePublicDao2.isNewRechargeDevDay(order_date, imei, game_id, isNewRegiDevDay2, isNewRegiAccountDay2, jedis3)
          //新增充值新定义：当天注册的并消费
          val isCurrentDayRegiAndRecharge2 = GamePublicDao2.isCurrentDayRegiAndRecharge(game_account, order_date, expand_channel, game_id, jedis0)
          val isNewRechargeAccountDay2 = GamePublicDao2.isNewRechargeAccountDay(order_date, game_account, game_id, isCurrentDayRegiAndRecharge2, jedis3)
          val rechargeAmountDay2 = GamePublicDao2.rechargeAmountDay(order_date, game_account, isCurrentDayRegiAndRecharge2, ori_price)
          val tu10 = new Tuple10(order_date, parent_game_id, game_id, group_id, isNewRechargeDevDay2, isNewRechargeAccountDay2, rechargeAmountDay2.toFloat, imei, game_account, os)
          /** 掉用函数对数据库进行数据加载 */
          try {
            GamePublicDao.rechargeActionsByDay(jedis3, tu17, conn, expand_channel)
            GamePublicDao2.rechargeActionsByDay(jedis3, tu10, conn) //发行三期
            /** 对数据进行中间表存放 */
            GamePublicDao.rechargeInfoToMidTb(game_account, order_date, pkg_code, imei, isNewRechargeDevDay, isNewRechargeAccountDay, expand_channel, game_id, jedis3, conn)
            GamePublicDao2.rechargeInfoToMidTb(game_account, order_date, imei, isNewRechargeDevDay2, isNewRechargeAccountDay2, game_id, jedis3, conn)
            jedis3.set(row._1 + "|" + row._2 + "|" + row._3 + "|" + "_publishOrderActions", row._1)
            jedis3.expire(row._1 + "|" + row._2 + "|" + row._3 + "|" + "_publishOrderActions", 48 * 3600)
          } catch {
            case e: Exception => e.printStackTrace
          }
        }
        else
          println("-------> 这不是发行游戏:" + row._3)
      }
    }
    conn.close
    connFx.close()
    pool.returnResource(jedis3)
    pool.returnResource(jedis0)
    pool.destroy()
  }

  /**
    * 用户登录行为
    * game_account(3),logintime(4),game_id(8)，expand_channel(6),imei(7)
    *
    * @param fp
    */
  def loginActions(fp: Iterator[(String, String, Int, String, String)]): Unit = {
    val pool: JedisPool = JedisUtil.getJedisPool;
    val jedis0: Jedis = pool.getResource;
    jedis0.select(0)
    //0库
    val jedis3: Jedis = pool.getResource;
    jedis3.select(3)
    //3库
    val conn: Connection = JdbcUtil.getConn();
    val connFx = JdbcUtil.getXiaopeng2FXConn()
    for (row <- fp) {
      //若已经同一日志被统计一遍 则不需要再统计
      if (!jedis3.exists(row._1 + "|" + row._2 + "|" + "_publicLoginActions")) {

        /*判断是否为发行游戏,只取发行游戏*/
        if (jedis0.exists(row._3.toString + "_publish_game")) {
          val game_id = row._3.toInt;
          val game_account = row._1
          val imei = row._5
          val parentgameid = jedis0.hget(row._3.toString + "_publish_game", "mainid")
          val parent_game_id = if (parentgameid == null || parentgameid.equals("")) "0" else parentgameid
          val publish_time = row._2.substring(0, 13)
          val publish_date = row._2.substring(0, 10)
          val pkgid = row._4
          val medium_channel = StringUtils.getArrayChannel(row._4)(0)
          val ad_site_channel = StringUtils.getArrayChannel(row._4)(1)
          val pkg_code = StringUtils.getArrayChannel(row._4)(2)
          var medium_account = jedis0.hget(pkg_code + "_pkgcode", "medium_account")
          if (medium_account == null) medium_account = ""
          var promotion_channel = jedis0.hget(pkg_code + "_pkgcode", "promotion_channel")
          if (promotion_channel == null) promotion_channel = ""
          var promotion_mode = jedis0.hget(pkg_code + "_" + publish_date + "_pkgcode", "promotion_mode")
          if (promotion_mode == null) promotion_mode = ""
          var head_people = jedis0.hget(pkg_code + "_" + publish_date + "_pkgcode", "head_people")
          if (head_people == null) head_people = ""
          var os = Commons.getPubGameGroupIdAndOs(game_id, connFx)(1)
          if (os == null) {
            println("loginActions get os is err:" + os);
            os = "1"
          }
          var group_id_pre = Commons.getPubGameGroupIdAndOs(game_id, connFx)(0)
          if (group_id_pre == null) {
            println("loginActions groupid os is err:" + group_id_pre);
            group_id_pre = "0"
          }
          val group_id = group_id_pre.toInt
          //通过游戏账号获取游戏账号信息，后面使用到
          val accountInfo = jedis0.hgetAll(game_account)
          /** 发行二期使用指标 */
          //是否为新增注册设备
          val isNewRegiDevDay = GamePublicDao.isNewRegiDevDay(pkg_code, imei, publish_date, medium_channel, ad_site_channel, game_id, conn)
          //是否为当日注册账号，并且与注册设备的imei、推广渠道一致
          val isNewRegiAccountDay = GamePublicDao.isNewRegiAccountDay(game_account, publish_date, isNewRegiDevDay, pkgid, imei, jedis0, accountInfo)
          //是否新登录设备，新注册设备并且新注册账号，并且一天只算一次
          val isNewLgDev = GamePublicDao.isNewLgDevDay(pkgid, imei, publish_date, game_id, isNewRegiDevDay, isNewRegiAccountDay, jedis3);
          //是否为新增登录账号，新注册设备并且新注册账号，并且一天只算一次
          val isNewLgAccount = GamePublicDao.isNewLgAccountDay(pkgid, game_account, publish_date, game_id, isNewRegiDevDay, isNewRegiAccountDay, jedis3);
          /*判断是否为新增活跃设备，新增设备大于1次登陆，是则1*/
          val isNewActiveDev = GamePublicDao.isNewActiveDevDay(pkgid, imei, publish_date, game_id, jedis3)
          /*是否设备当天已经登陆，一天只算一次*/
          val isLoginDevDay = GamePublicDao.isLoginDevDay(pkgid, imei, publish_date, game_id, jedis3)
          /*是否账号当天已经登陆，一天只算一次*/
          val isLoginAccountDay = GamePublicDao.isLoginAccountDay(pkgid, game_account, publish_date, game_id, jedis3)
          /*是否账号当天该小时已经登陆，一天只算一次*/
          val isLoginAccountHour = GamePublicDao.isLoginAccountHour(pkgid, game_account, publish_time, game_id, jedis3)
          /*设置data to tupel19，按日期*/
          val tp19 = new Tuple19(publish_date, parent_game_id, game_id, medium_channel, ad_site_channel, pkg_code,medium_account, promotion_channel, promotion_mode, head_people, os, group_id,
                                  isNewLgDev, isNewLgAccount, isNewActiveDev, isLoginDevDay, isLoginAccountDay, imei, game_account)
          /*小时表使用指标*/
          val tu14 = new Tuple14(publish_time, parent_game_id, game_id, medium_channel, ad_site_channel, pkg_code, medium_account, promotion_channel, promotion_mode, head_people, os, group_id, isLoginAccountHour, game_account)

          /** 发行三期运营报表数据部分指标 */
          //是否为新增注册设备
          val isNewRegiDevDay2 = GamePublicDao2.isNewRegiDevDay(imei, publish_date, game_id, conn)
          //是否为当日注册账号，并且与注册的渠道一致
          val isNewRegiAccountDay2 = GamePublicDao2.isNewRegiAccountDay(game_account, publish_date, isNewRegiDevDay2, imei, jedis0)
          //判断是否设备有记录，若有记录则为0，否则为1
          val isNewLgDev2 = GamePublicDao2.isNewLgDevDay(imei, publish_date, game_id, isNewRegiDevDay2, isNewRegiAccountDay2, jedis3);
          val isNewLgAccount2 = GamePublicDao2.isNewLgAccountDay(game_account, publish_date, game_id, isNewRegiDevDay2, isNewRegiAccountDay2, jedis3);
          /*是否设备当天已经登陆，若已经登陆为0*/
          val isLoginDevDay2 = GamePublicDao2.isLoginDevDay(imei, publish_date, game_id, jedis3)
          /*运营报表小时表和按天算dau，bi_gamepublic_base_opera_kpi.dau_account_num &&  bi_gamepublic_base_opera_hour_kpi.dau_account_num 需求变更 2017-11-30*/
          val isLoginAccDay2 = GamePublicDao2.isLoginAccDayByGame(publish_date, game_id, game_account, jedis3)
          val isLoginAccDayHour2 = GamePublicDao2.isLoginAccDayHourByGame(publish_time, game_id, game_account, jedis3)
          /*实时计算留存：根据登录数据找账号的注册时间；账号一天只算一次（isLoginAccountDay=1），新增需求 2017-08-27**/
          val remaindDays = GamePublicDao2.getRemaindDays(accountInfo, publish_date, isLoginAccountDay)
          val retained_1day = if (remaindDays == 1) 1 else 0
          val retained_3day = if (remaindDays == 2) 1 else 0
          val retained_7day = if (remaindDays == 6) 1 else 0
          /*实时计算留存30：根据登录数据找账号的注册时间；账号一天只算一次（isLoginAccountDay=1），新增需求 2017-12-23**/
          val retained_30day = if (remaindDays == 29) 1 else 0
          /*当天注册并登录数，新增需求 2017-08-27*/
          val isRegiLogin = if (remaindDays == 0) 1 else 0
          /*设置data to tupel16，按日期*/
          val tp14 = new Tuple14(publish_date, parent_game_id, game_id, os, group_id,isNewLgDev2, isNewLgAccount2, isLoginDevDay2, imei, game_account, isRegiLogin, isLoginAccDay2, publish_time, isLoginAccDayHour2)

          /** 调用函数对数据进行加载 */
          try {
            GamePublicDao.loginActionsByDayProcessDB(tp19, conn, pkgid) //发行2期（投放）基础表
            GamePublicDao.loginAccountByDayProcessDB(tp19, conn) //发行1期（旧后台）日活跃
            GamePublicDao.loginAccountDauByHourProcessDB(tu14, conn, pkgid) //发行1期（就后台）小时表
            //三期报表加载
            GamePublicDao2.loginActionsByDayProcessDB(tp14, conn) //发行三期(运营)基础表
            val game_id_pre_regi = accountInfo.get("game_id")
            if ((game_id_pre_regi!=null)&&(remaindDays == 1 || remaindDays == 2 || remaindDays == 6 || remaindDays == 29)) //发行三期（运营）只对这些日期留存进行操作
            {
              val game_id_regi=game_id_pre_regi.toInt
              val parentgameid_regi = jedis0.hget(game_id_regi.toString + "_publish_game", "mainid")
              val parent_game_id_regi = if (parentgameid_regi == null || parentgameid_regi.equals("")) "0" else parentgameid_regi

              val group_id_os = Commons.getPubGameGroupIdAndOs(game_id_regi, connFx)
              var os_regi = group_id_os(1)
              if (os_regi == null) {
                println("loginActions get os is err:" + os);
                os_regi = "1"
              }

              var group_id_pre_regi = group_id_os(0)
              if (group_id_pre_regi == null) {
                println("loginActions groupid os is err:" + group_id_pre_regi);
                group_id_pre_regi = "0"
              }
              val group_id_regi = group_id_pre_regi.toInt
              /*留存实时tup*/
              val tp9 = new Tuple9(accountInfo.get("reg_time").substring(0, 10), parent_game_id_regi, game_id_regi, os_regi, group_id_regi,retained_1day, retained_3day, retained_7day, retained_30day)
              GamePublicDao2.remaindByDayProcessDB(tp9, conn) //发行三期（运营）留存实时
            }
            GamePublicDao2.loginAccountByDayHourProcessDB(tp14, conn) //发行三期（运营）小时表Dau

            /** 缓存一下数据，避免重复计算 */
            //对本小时第一次登录的记录到redis
            jedis3.set("isLoginAccountHour|" + tu14._1 + "|" + tu14._14 + "|" + pkgid + "|" + tu14._3.toString, tu14._6)
            jedis3.expire("isLoginAccountHour|" + tu14._1 + "|" + tu14._14 + "|" + pkgid + "|" + tu14._3.toString, 3600 * 8)
            GamePublicDao.loginInfoToMidTb(tp19._19, tp19._1, tp19._6, tp19._18, tp19._13, tp19._14, pkgid, tp19._3, jedis3)
            GamePublicDao2.loginInfoToMidTb(game_account, publish_date, imei, isNewLgDev2, isNewLgAccount2, game_id, jedis3)
            /*对数据进行中间表存放,game_account,public_date,pkg_code,imei,isNewLgDev,isNewLgAccount*/
            jedis3.set(row._1 + "|" + row._2 + "|" + "_publicLoginActions", game_account) //3
            jedis3.expire(row._1 + "|" + row._2 + "|" + "_publicLoginActions", 3600 * 25) //3
          } catch {
            case e: Exception => e.printStackTrace
          }
        } else
          println("-------> 这不是发行游戏:" + row._3)
      }

    }

    conn.close
    connFx.close()
    pool.returnResource(jedis0)
    pool.returnResource(jedis3)
    pool.destroy()
  }
}


