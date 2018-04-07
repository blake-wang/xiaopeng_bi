package com.xiaopeng.bi.utils

import java.io.File

/**
  * Created by Administrator on 2016/12/10.
  */
object Hadoop {
  def hd= {
    val path: String = new File(".").getCanonicalPath
    System.getProperties().put("hadoop.home.dir", path)
    new File("./bin").mkdirs()
    new File("./bin/winutils.exe").createNewFile()
  }
}
