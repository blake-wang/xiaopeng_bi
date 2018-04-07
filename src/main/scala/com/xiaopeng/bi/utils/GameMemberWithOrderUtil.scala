package com.xiaopeng.bi.utils


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}

import scala.collection.mutable.ArrayBuffer


/**
  * 处理order数据
  * 参数：游戏账号（5），订单号（2），订单日期（6），游戏id（7）,充值流水（10）+代金券，imei(24).sub
  */
object GameMemberWithOrderUtil {


  //处理order日志信息
  def loadOrderInfo(rdd: RDD[String], hiveContext: HiveContext): Unit = {

    val orderRdd = rdd.filter(line => {
      val fields = line.split("\\|", -1)
      //排除截断的日志,订单号不能为空(没有订单号的为代金券消费),取订单状态为4或者8的订单
      fields(0).contains("bi_order") && fields.length >= 22 && !fields(2).equals("") && (!fields(4).equals("")) // && (fields(19).equals("4") || fields(19).equals("8"))
    }).map(line => {
      val fields = line.split("\\|", -1)
      //取出字段 member_id(4),game_account(5),order_id(2),order_time(6),game_id(7),original_price(10),recharge_amount(11),payment_amount(12),voucher_amount(13),order_status(19),
      //充值金额 = original_price  只计算原价                          // +  voucher_amount
      //付款金额 = payment_amount
      Row(fields(4), fields(5).trim.toLowerCase, fields(2), fields(6), fields(7).toInt, Commons.getNullTo0(fields(10)), Commons.getNullTo0(fields(12)), fields(19).toInt)
    })

    if (!orderRdd.isEmpty()) {
      val orderStruct = new StructType()
        .add("member_id", StringType)
        .add("game_account", StringType)
        .add("order_id", StringType)
        .add("order_time", StringType)
        .add("game_id", IntegerType)
        .add("recharge_amount", FloatType)
        .add("payment_amount", FloatType)
        .add("order_status", IntegerType)

      val bi_order = hiveContext.createDataFrame(orderRdd, orderStruct)
      bi_order.registerTempTable("ods_order")

      //更新会员 <订单数><充值金额><实付金额>   充值金额 = 帐号数 * 原价
      val member_order_num = "select distinct member_id,game_account,order_id,order_time,game_id,recharge_amount,payment_amount from ods_order where order_status = '4'"
      val orderNumWithAmountDataFrame = hiveContext.sql(member_order_num)
      foreachOrderNumWithAmountDataFrame(orderNumWithAmountDataFrame)

      //更新会员 <退单>金额
      val member_chargeback_amount = "select distinct member_id,order_id,order_time,sum(recharge_amount) as recharge_amount,sum(payment_amount) as payment_amount from ods_order where order_status = '8' group by member_id,order_id,order_time order by order_time desc"
      val rechargeAmountDataFrame = hiveContext.sql(member_chargeback_amount)
      foreachChargeBackDataFrame(rechargeAmountDataFrame)


    }
  }

  def foreachOrderNumWithAmountDataFrame(orderNumDataFrame: DataFrame): Unit = {
    orderNumDataFrame.foreachPartition(iter => {
      val conn = JdbcUtil.getConn()


      //如果是充值的的，订单数 + 1, 充值金额加上订单的充值金额,实付金额加上订单的实付金额
      val member_order = "update bi_member_info set last_pay_time=?,ordernum = ordernum + 1,oriprice = oriprice + ?,payprice = payprice + ? where member_id = ?"
      val pstmt_member_order = conn.prepareStatement(member_order)
      var member_order_params = ArrayBuffer[Array[Any]]()

      iter.foreach(row => {
        val member_id = row.getAs[String]("member_id")


        val game_account = row.getAs[String]("game_account")
        //计算帐号数
        val game_account_num = game_account.split(",", -1).length

        val order_time = row.getAs[String]("order_time")

        var recharge_amount = row.getAs[Float]("recharge_amount") * 100
        //充值金额 = 帐号数 * 原价
        recharge_amount = game_account_num * recharge_amount

        val payment_amount = row.getAs[Float]("payment_amount") * 100

        member_order_params.+=(Array[Any](order_time, recharge_amount.toInt, payment_amount.toInt, member_id))

        JdbcUtil.executeUpdate(pstmt_member_order, member_order_params, conn)

      })
      pstmt_member_order.close()
      conn.close()
    })
  }

  def foreachChargeBackDataFrame(rechargeAmountDataFrame: DataFrame) = {
    rechargeAmountDataFrame.foreachPartition(iter => {
      val conn = JdbcUtil.getConn()

      //如果是退单的，订单数 - 1, 充值金额减去退单的充值金额,实付金额减去退单的实付金额
      val member_order = "update bi_member_info set last_pay_time=?,ordernum = ordernum - 1,oriprice = oriprice - ?,payprice = payprice - ? where member_id = ?"
      val pstmt_member_order = conn.prepareStatement(member_order)
      var member_order_params = ArrayBuffer[Array[Any]]()


      iter.foreach(row => {
        val member_id = row.getAs[String]("member_id")
        val order_time = row.getAs[String]("order_time")
        val recharge_amount = row.getAs[Double]("recharge_amount") * 100
        val payment_amount = row.getAs[Double]("payment_amount") * 100


        member_order_params.+=(Array[Any](order_time, recharge_amount.toInt, payment_amount.toInt, member_id))

        JdbcUtil.executeUpdate(pstmt_member_order, member_order_params, conn)

      })
      pstmt_member_order.close()
      conn.close()


    })

  }

}
