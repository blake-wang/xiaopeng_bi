package com.xiaopeng.bi.utils

/**
  * Created by sumenghu on 2016/9/1.
  */
object StringUtils {

  val mode = ConfigurationUtil.getProperty("web.url.mode");

  def isNumber(str: String): Boolean = {
    if (str.equals("")) {
      return false;
    }
    for (i <- 0.to(str.length - 1)) {
      if (!Character.isDigit(str.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  //  def isTime(str: String): Boolean = {
  //    if (str.length != 19) {
  //      return false;
  //    }
  //    for (i <- 0.to(str.length - 1)) {
  //      if (!(Character.isDigit(str.charAt(i)) || str.charAt(i).equals(' ') || str.charAt(i).equals('-') || str.charAt(i).equals(':'))) {
  //        return false;
  //      }
  //    }
  //    return true;
  //  }

  def isTime(str: String): Boolean = {
    if (str.length != 19) {
      return false
    }
    for (i <- 0.to(str.length - 1)) {
      if (!(Character.isDigit(str.charAt(i)) || str.charAt(i).equals(' ') || str.charAt(i).equals('-') || str.charAt(i).equals(':'))) {
        return false
      }
    }
    return true
  }

  /**
    * 判断字符串是否符合正则表达式
    *
    * @param log
    * @param regx
    * @return
    */
  def isRequestLog(log: String, regx: String): Boolean = {
    val p1 = regx.r
    val p1Matches = log match {
      case p1() => true // no groups
      case _ => false
    }
    p1Matches
  }

  def defaultEmptyTo21(str: String): String = {
    if ("".equals(str)) {
      "21"
    } else if ("\\N".equals(str)) {
      "pyw"
    } else {
      str
    }
  }

  def getArrayChannel(channelId: String): Array[String] = {
    val splited = channelId.split("_")
    if (channelId == null || channelId.equals("no_acc")) {
      Array[String]("no_acc", "", "")
    } else if (channelId.equals("")) {
      Array[String]("21", "", "")
    } else if (splited.length == 1 || splited.length == 2) {
      Array[String](splited(0), "", "")
    } else {
      Array[String](splited(0), splited(1), splited(2))
    }
  }

}
