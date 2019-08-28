package com.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisPoolT extends  Serializable {
  var jedisPool:JedisPool = null
  def getJedisPoolInstance(): JedisPool = {
    if (null == jedisPool){
      val poolConfig: JedisPoolConfig = new JedisPoolConfig
      poolConfig.setMaxTotal(1000)
      poolConfig.setMaxIdle(32)
      poolConfig.setMaxWaitMillis(100 * 1000)
      poolConfig.setTestOnBorrow(true)
      jedisPool = new JedisPool(poolConfig, "192.168.183.17")
    }
    jedisPool
  }

  def release(jp:JedisPool,jedis:Jedis)={
    if (null != jedis) jp.returnResourceObject(jedis)
  }

}
