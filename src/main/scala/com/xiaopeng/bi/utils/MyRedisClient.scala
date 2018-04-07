package com.xiaopeng.bi.utils

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.xiaopeng.bi.constant.Constants
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
 * Created by JSJSB-0071 on 2016/12/10.
 */
class MyRedisClient extends KryoSerializable {


  val host = ConfigurationUtil.getProperty(Constants.REDIS_HOST)
  val port = new Integer(ConfigurationUtil.getProperty(Constants.REDIS_PORTY))
  val poolConfig = new GenericObjectPoolConfig
  //最大空闲连接数
  poolConfig.setMaxIdle(100)
  //连接池的最大连接数
  poolConfig.setMaxTotal(100)
  //设置获取连接的最大等待时间
  poolConfig.setMaxWaitMillis(2000)
  //从连接池中获取连接的时候是否需要校验，这样可以保证取出的连接都是可用的
  poolConfig.setTestOnBorrow(true)
  //获取jedis连接池
  var pool = new JedisPool(poolConfig, host, port, 30000, "redis");

  //var jedis = pool.getResource

  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeObject(output, host)
    kryo.writeObject(output, poolConfig)
    kryo.writeObject(output, port)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    this.pool = new JedisPool(poolConfig, host, port, 30000, "redis")
    //this.jedis = this.pool.getResource
  }
}
