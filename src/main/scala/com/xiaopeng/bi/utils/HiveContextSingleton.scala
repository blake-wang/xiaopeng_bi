package com.xiaopeng.bi.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by kequan on 3/27/17.
  */
object HiveContextSingleton {
  @transient private var instance: HiveContext = _

  def getInstance(sparkContext: SparkContext): HiveContext = {
    if (instance == null) {
      instance = new HiveContext(sparkContext)
    }
    instance
  }
}
