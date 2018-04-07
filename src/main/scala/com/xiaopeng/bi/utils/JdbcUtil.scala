package com.xiaopeng.bi.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.xiaopeng.bi.constant.Constants

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kequan on 2/22/17.
  */
object JdbcUtil {
  /**
    * 获取xiaopeng2_bi主库连接，推送结果数据目标库
    *
    * @return MysqlConnection
    */
  def getConn(): Connection = {
    val url = ConfigurationUtil.getProperty(Constants.JDBC_URL);
    val driver = ConfigurationUtil.getProperty(Constants.JDBC_DRIVER);
    try {
      Class.forName(driver)
      return DriverManager.getConnection(url)
    } catch {
      case ex: Exception => {
        println("获取数据库连接错误：Exception=" + ex)
      }
        return null
    }
  }

  /**
    * 发行业务库连接
    *
    * @return
    */
  def getXiaopeng2FXConn(): Connection = {
    val url = ConfigurationUtil.getProperty(Constants.JDBC_XIAOPENG2FX_URL);
    val driver = ConfigurationUtil.getProperty(Constants.JDBC_DRIVER);
    try {
      Class.forName(driver)
      return DriverManager.getConnection(url)
    } catch {
      case ex: Exception => {
        println("获取数据库连接错误：Exception=" + ex)
      }
        return null
    }
  }

  /**
    * 获取xiaopeng2库连接，利于补充缺失数据
    *
    * @return MysqlConnection
    */
  def getXiaopeng2Conn(): Connection = {
    val url = ConfigurationUtil.getProperty(Constants.JDBC_XIAOPENG2_URL);
    val driver = ConfigurationUtil.getProperty(Constants.JDBC_DRIVER);
    try {
      Class.forName(driver)
      DriverManager.getConnection(url)
    } catch {
      case ex: Exception => {
        println("获取数据库连接错误：Exception=" + ex)
        null
      }
    }
  }

  /**
    * 获取xiaopeng2_bi从库连接，可以对明细数据做定时处理
    *
    * @return
    */
  def getBiHippoConn(): Connection = {
    val url = ConfigurationUtil.getProperty(Constants.JDBC_XIAOPENG2_BIHIP_URL);
    val driver = ConfigurationUtil.getProperty(Constants.JDBC_DRIVER);
    try {
      Class.forName(driver)
      val connection = DriverManager.getConnection(url)
      connection
    }
  }


  /**
    * @param sqlText
    * @param params
    * @param conn
    */
  def doBatch(sqlText: String, params: ArrayBuffer[Array[Any]], conn: Connection): Unit = {
    if (params.length > 0) {
      val pstat = conn.prepareStatement(sqlText);
      pstat.clearBatch()
      for (param <- params) {
        for (index <- 0 to param.length - 1) {
          pstat.setObject(index + 1, param(index))
        }
        pstat.addBatch()
      }
      pstat.executeBatch
      pstat.close
      params.clear();
    }
  }

  /**
    * @param sqlText
    * @param params
    */
  def executeBatch(sqlText: String, params: ArrayBuffer[Array[Any]]): Unit = {
    if (params.length > 0) {
      var conn: Connection = getConn();
      var pstat: PreparedStatement = conn.prepareStatement(sqlText);
      pstat.clearBatch()
      for (param <- params) {
        for (index <- 0 to param.length - 1) {
          pstat.setObject(index + 1, param(index))
        }
        pstat.addBatch()
      }
      pstat.executeBatch
      pstat.close();
      conn.close();
      params.clear();
    }
  }

  /**
    *
    * @param pstat
    * @param params
    * @param conn
    */
  def executeQuery(pstat: PreparedStatement, params: ArrayBuffer[Array[Any]], conn: Connection): Unit = {
    if (params.length > 0) {
      val param = params(0)
      for (index <- 0 to param.length - 1) {
        pstat.setObject(index + 1, param(index))
      }
      pstat.executeQuery()
      params.clear();
    }
  }

  /**
    *
    * @param pstat
    * @param params
    * @param conn
    */
  def executeUpdate(pstat: PreparedStatement, params: ArrayBuffer[Array[Any]], conn: Connection): Unit = {
    if (params.length > 0) {
      for (param <- params) {
        for (index <- 0 to param.length - 1) {
          pstat.setObject(index + 1, param(index))
        }
        pstat.executeUpdate()
      }
      params.clear();
    }
  }
}
